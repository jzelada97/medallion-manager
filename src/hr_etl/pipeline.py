"""End-to-end pipeline orchestration.

Flow per message: decode -> detect type -> store raw in lake (Mongo) -> buffer in
Redis by person key -> when enough fragments, consolidate -> upsert in warehouse.
"""

from __future__ import annotations

from typing import Any

from hr_etl.cache.redis_buffer import RedisBuffer
from hr_etl.lake.mongo_lake import MongoLake
from hr_etl.logging_conf import get_logger
from hr_etl.metrics.prometheus import (
    MESSAGES_CONSUMED,
    MESSAGES_FAILED,
    PERSONS_PERSISTED,
    PROCESSING_SECONDS,
)
from hr_etl.models.raw import FragmentType
from hr_etl.processing.consolidator import consolidate
from hr_etl.processing.detector import detect_type
from hr_etl.processing.matcher import match_key
from hr_etl.warehouse.person_repo import PersonRepository

logger = get_logger(__name__)


class Pipeline:
    """Wires together lake, cache, processing and warehouse."""

    def __init__(
        self,
        lake: MongoLake,
        buffer: RedisBuffer,
        repo: PersonRepository,
        min_fragments: int = 2,
    ) -> None:
        self._lake = lake
        self._buffer = buffer
        self._repo = repo
        self._min_fragments = min_fragments

    @PROCESSING_SECONDS.time()
    def process_message(self, message: dict[str, Any], offset: int | None = None) -> int | None:
        """Process one raw message. Returns the persisted person id if consolidated."""
        try:
            ftype = detect_type(message)
            self._lake.store_raw(message, ftype.value, offset)
            MESSAGES_CONSUMED.labels(fragment_type=ftype.value).inc()

            if ftype == FragmentType.UNKNOWN:
                MESSAGES_FAILED.inc()
                logger.warning("unknown fragment type; stored raw, skipping consolidation")
                return None

            key = match_key(message, ftype)
            if not key:
                MESSAGES_FAILED.inc()
                logger.warning("fragment has no matching key; kept raw only")
                return None

            count = self._buffer.add_fragment(key, message, ftype.value)
            if count < self._min_fragments:
                return None

            fragments = [(f["message"], FragmentType(f["type"])) for f in self._buffer.get_fragments(key)]
            person = consolidate(fragments)
            if person is None:
                return None

            person_id = self._repo.upsert(person)
            PERSONS_PERSISTED.inc()
            return person_id
        except Exception:  # never let one bad message kill the pipeline
            MESSAGES_FAILED.inc()
            logger.exception("error processing message")
            return None
