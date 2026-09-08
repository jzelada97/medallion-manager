"""Reprocess raw messages from the MongoDB Data Lake into Silver (Postgres).

Usage:  python -m hr_etl.reprocess [--batch 5000]

Reads every document in ``raw_messages`` (Bronze) and feeds it through the same
pipeline that the streaming consumer uses: detect type → match key → Redis buffer →
consolidate → upsert into ``persons``. The MongoDB documents are NOT re-written (they
already exist); only the Silver layer is rebuilt.

Designed for the "Option A reset": TRUNCATE Silver/Gold + Redis FLUSHALL, then
reprocess from the 6.9M+ raw messages in Mongo. The upsert is idempotent (ON
CONFLICT on match_key), so running it twice is safe.

Progress is printed every ``--batch`` messages (default 5000) so you can watch the
throughput. Ctrl-C stops cleanly at the end of the current batch.

Requires the same environment variables as the main app (MONGO_URI, POSTGRES_DSN,
REDIS_*, etc.) — typically loaded from ``.env`` via the Docker entrypoint.
"""

from __future__ import annotations

import argparse
import signal
import time

import redis
from pymongo import MongoClient

from hr_etl.cache.redis_buffer import RedisBuffer
from hr_etl.config import get_settings
from hr_etl.logging_conf import configure_logging, get_logger
from hr_etl.models.raw import FragmentType
from hr_etl.processing.consolidator import consolidate
from hr_etl.processing.detector import detect_type
from hr_etl.processing.matcher import build_full_name, match_key
from hr_etl.processing.normalizer import normalize_message
from hr_etl.warehouse.engine import create_db_engine, init_schema, make_session_factory
from hr_etl.warehouse.person_repo import PersonRepository

logger = get_logger(__name__)

# Graceful shutdown on Ctrl-C / SIGTERM.
_stop = False


def _handle_signal(signum: int, frame: object) -> None:  # noqa: ARG001
    global _stop  # noqa: PLW0603
    _stop = True
    logger.info("stop signal received, finishing current batch...")


def _register_cross_link(buffer: RedisBuffer, message: dict, ftype: FragmentType, key: str) -> None:
    """Mirror the cross-link logic from Pipeline._register_cross_link."""
    if ftype != FragmentType.PERSONAL or not key.startswith("passport:"):
        return
    norm = normalize_message(message)
    name = build_full_name(norm)
    if name:
        buffer.register_alias(name, key)


def _resolve_cross_link(buffer: RedisBuffer, key: str) -> str:
    """Mirror the cross-link logic from Pipeline._resolve_cross_link."""
    if not key.startswith("name:"):
        return key
    resolved = buffer.resolve_alias(key)
    return resolved if resolved else key


def main() -> None:
    parser = argparse.ArgumentParser(description="Reprocess Bronze (Mongo) → Silver (Postgres)")
    parser.add_argument("--batch", type=int, default=5000, help="Progress log every N messages")
    args = parser.parse_args()

    settings = get_settings()
    configure_logging(settings.log_level)

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    # --- Infra ---
    mongo = MongoClient(settings.mongo_uri)
    collection = mongo[settings.mongo_db][settings.mongo_raw_collection]
    total_raw = collection.count_documents({})

    redis_client = redis.Redis(
        host=settings.redis_host,
        port=settings.redis_port,
        password=settings.redis_password or None,
        decode_responses=True,
    )
    buffer = RedisBuffer(redis_client, ttl=settings.redis_buffer_ttl)

    engine = create_db_engine(settings.postgres_dsn)
    init_schema(engine)
    repo = PersonRepository(make_session_factory(engine))

    logger.info("reprocess started: %d raw documents in Mongo", total_raw)

    processed = 0
    persisted = 0
    failed = 0
    t0 = time.monotonic()
    min_fragments = settings.consolidation_min_fragments

    # Batch consolidated persons and flush them with a single multi-row upsert instead of
    # one round-trip per person — the throughput lever for millions of docs. On Postgres
    # this uses INSERT ... ON CONFLICT (match_key) DO UPDATE COALESCE, idempotent.
    pending: list = []

    def _flush() -> None:
        nonlocal persisted
        if not pending:
            return
        repo.upsert_many_native(pending)
        persisted += len(pending)
        pending.clear()

    # Explicit session so the long-running cursor does NOT hit Mongo's 30-min idle timeout
    # while churning through millions of docs (plain no_cursor_timeout=True is overridden
    # by the session idle timeout otherwise, and pymongo warns about exactly that).
    with mongo.start_session() as mongo_session:
        cursor = collection.find({}, no_cursor_timeout=True, session=mongo_session).batch_size(
            args.batch
        )
        try:
            for doc in cursor:
                if _stop:
                    break

                message = doc.get("payload")
                if not message:
                    failed += 1
                    continue

                try:
                    ftype = detect_type(message)
                    if ftype == FragmentType.UNKNOWN:
                        failed += 1
                        continue

                    key = match_key(message, ftype)
                    if not key:
                        failed += 1
                        continue

                    _register_cross_link(buffer, message, ftype, key)
                    key = _resolve_cross_link(buffer, key)

                    count = buffer.add_fragment(key, message, ftype.value)
                    if count >= min_fragments:
                        fragments = [
                            (f["message"], FragmentType(f["type"]))
                            for f in buffer.get_fragments(key)
                        ]
                        person = consolidate(fragments)
                        if person is not None and person.match_key:
                            pending.append(person)
                        buffer.clear(key)
                except Exception:
                    failed += 1
                    logger.exception("error processing doc _id=%s", doc.get("_id"))

                processed += 1
                if len(pending) >= args.batch:
                    _flush()
                if processed % args.batch == 0:
                    elapsed = time.monotonic() - t0
                    rate = processed / elapsed if elapsed > 0 else 0
                    logger.info(
                        "progress: %d/%d (%.1f%%) · %d persisted · %d failed · %.0f msg/s",
                        processed,
                        total_raw,
                        processed / total_raw * 100 if total_raw else 0,
                        persisted,
                        failed,
                        rate,
                    )
            _flush()  # persist the last partial batch
        finally:
            cursor.close()
            mongo.close()
            engine.dispose()

    elapsed = time.monotonic() - t0
    rate = processed / elapsed if elapsed > 0 else 0
    logger.info(
        "reprocess done: %d processed, %d persisted, %d failed in %.1fs (%.0f msg/s)",
        processed,
        persisted,
        failed,
        elapsed,
        rate,
    )
    print(f"Reprocess done: {processed} processed, {persisted} persisted, {failed} failed")


if __name__ == "__main__":
    main()
