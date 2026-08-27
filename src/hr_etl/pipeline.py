"""End-to-end pipeline orchestration.

Flow per message: decode -> detect type -> store raw in lake (Mongo) -> buffer in
Redis by person key -> when enough fragments, consolidate -> upsert in warehouse.

Cross-linking: when a Personal fragment has both passport and a name, we register
an alias (name -> passport key) in Redis. Later, when a Location/Professional
arrives with a name-based key, we resolve the alias and redirect the fragment
to the passport-based key, achieving the cross-join.
"""

from __future__ import annotations

from typing import Any

from hr_etl.cache.redis_buffer import RedisBuffer
from hr_etl.lake.mongo_lake import MongoLake
from hr_etl.logging_conf import get_logger
from hr_etl.metrics.prometheus import (
    CONSOLIDATIONS,
    MESSAGES_CONSUMED,
    MESSAGES_FAILED,
    PENDING_FRAGMENTS,
    PERSIST_SECONDS,
    PERSONS_PERSISTED,
    PROCESSING_SECONDS,
)
from hr_etl.models.raw import FragmentType
from hr_etl.processing.consolidator import consolidate
from hr_etl.processing.detector import detect_type
from hr_etl.processing.matcher import build_full_name, match_key
from hr_etl.processing.normalizer import normalize_message
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

    def _register_cross_link(self, message: dict[str, Any], ftype: FragmentType, key: str) -> None:
        """If a Personal fragment has passport + name, register the name as alias."""
        if ftype != FragmentType.PERSONAL or not key.startswith("passport:"):
            return
        norm = normalize_message(message)
        name = build_full_name(norm)
        if name:
            self._buffer.register_alias(name, key)

    def _resolve_cross_link(self, key: str) -> str:
        """Try to resolve a name-based key to a passport-based key via alias."""
        if not key.startswith("name:"):
            return key
        resolved = self._buffer.resolve_alias(key)
        if resolved:
            logger.debug("cross-link resolved: %s -> %s", key, resolved)
            return resolved
        return key

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

            # Cross-linking: register alias if Personal, resolve if name-based
            self._register_cross_link(message, ftype, key)
            key = self._resolve_cross_link(key)

            logger.debug("fragment type=%s key=%s", ftype.value, key)
            count = self._buffer.add_fragment(key, message, ftype.value)
            PENDING_FRAGMENTS.set(count)
            if count < self._min_fragments:
                return None

            fragments = [
                (f["message"], FragmentType(f["type"])) for f in self._buffer.get_fragments(key)
            ]
            person = consolidate(fragments)
            if person is None:
                return None
            CONSOLIDATIONS.inc()
            self._buffer.clear(key)

            with PERSIST_SECONDS.time():
                person_id = self._repo.upsert(person)
            PERSONS_PERSISTED.inc()
            return person_id
        except Exception:  # never let one bad message kill the pipeline
            MESSAGES_FAILED.inc()
            logger.exception("error processing message")
            return None
