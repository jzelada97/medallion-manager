"""MongoDB Data Lake: store raw messages exactly as received, plus metadata."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from hr_etl.logging_conf import get_logger

logger = get_logger(__name__)


class MongoLake:
    """Thin repository over a MongoDB collection for raw messages."""

    def __init__(self, collection: Any) -> None:
        """`collection` is a pymongo (or mongomock) collection instance."""
        self._collection = collection

    def store_raw(self, message: dict[str, Any], fragment_type: str, offset: int | None = None) -> Any:
        """Insert a raw message with ingestion metadata. Returns the inserted id."""
        doc = {
            "payload": message,
            "fragment_type": fragment_type,
            "ingested_at": datetime.now(timezone.utc),
            "kafka_offset": offset,
        }
        result = self._collection.insert_one(doc)
        logger.debug("raw message stored in lake type=%s offset=%s", fragment_type, offset)
        return result.inserted_id

    def count(self) -> int:
        """Return the number of raw documents stored."""
        return self._collection.count_documents({})
