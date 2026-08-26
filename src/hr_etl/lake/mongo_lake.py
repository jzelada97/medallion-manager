"""MongoDB Data Lake: store raw messages exactly as received, plus metadata."""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any

from hr_etl.logging_conf import get_logger

logger = get_logger(__name__)


class MongoLake:
    """Thin repository over a MongoDB collection for raw messages.

    Supports single-document writes (``store_raw``) and buffered batch writes
    (``buffer_raw`` + ``flush``) for high-throughput ingestion.
    """

    def __init__(self, collection: Any, batch_size: int = 500, flush_interval: float = 1.0) -> None:
        """``collection`` is a pymongo (or mongomock) collection instance.

        ``batch_size``: flush the buffer once this many docs are queued.
        ``flush_interval``: also flush if this many seconds passed since last flush,
        so we never hold data longer than ~1s even under low traffic. This bounds
        the worst-case data-at-risk to one interval; on crash, Kafka replays the
        uncommitted messages (offsets are committed only after a successful flush).
        """
        self._collection = collection
        self._batch_size = batch_size
        self._flush_interval = flush_interval
        self._buffer: list[dict[str, Any]] = []
        self._last_flush = time.monotonic()

    @staticmethod
    def _wrap(message: dict[str, Any], fragment_type: str, offset: int | None) -> dict[str, Any]:
        return {
            "payload": message,
            "fragment_type": fragment_type,
            "ingested_at": datetime.now(timezone.utc),
            "kafka_offset": offset,
        }

    def store_raw(self, message: dict[str, Any], fragment_type: str, offset: int | None = None) -> Any:
        """Insert a single raw message immediately. Returns the inserted id."""
        result = self._collection.insert_one(self._wrap(message, fragment_type, offset))
        logger.debug("raw message stored in lake type=%s offset=%s", fragment_type, offset)
        return result.inserted_id

    def buffer_raw(self, message: dict[str, Any], fragment_type: str, offset: int | None = None) -> bool:
        """Queue a raw message for batch insertion.

        Returns True if a flush happened as a result (buffer full or interval
        elapsed), so the caller can commit Kafka offsets right after.
        """
        self._buffer.append(self._wrap(message, fragment_type, offset))
        if len(self._buffer) >= self._batch_size or self._interval_elapsed():
            self.flush()
            return True
        return False

    def _interval_elapsed(self) -> bool:
        return (time.monotonic() - self._last_flush) >= self._flush_interval

    def flush(self) -> int:
        """Write all buffered documents in one ``insert_many``. Returns count written."""
        if not self._buffer:
            self._last_flush = time.monotonic()
            return 0
        batch = self._buffer
        self._buffer = []
        self._collection.insert_many(batch, ordered=False)
        self._last_flush = time.monotonic()
        logger.debug("lake flushed batch size=%d", len(batch))
        return len(batch)

    def ensure_indexes(self) -> None:
        """Create helpful indexes (idempotent). Speeds up time-based queries."""
        self._collection.create_index("ingested_at")
        self._collection.create_index("fragment_type")

    def count(self) -> int:
        """Return the number of raw documents stored (excludes unflushed buffer)."""
        return self._collection.count_documents({})
