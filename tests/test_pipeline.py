"""Integration-style tests for the Pipeline using in-memory fakes."""

from __future__ import annotations

import fakeredis
import mongomock

from hr_etl.cache.redis_buffer import RedisBuffer
from hr_etl.lake.mongo_lake import MongoLake
from hr_etl.pipeline import Pipeline
from hr_etl.warehouse.person_repo import PersonRepository


def _pipeline(sqlite_session_factory):
    collection = mongomock.MongoClient()["hr_lake"]["raw"]
    lake = MongoLake(collection)
    buffer = RedisBuffer(fakeredis.FakeStrictRedis(decode_responses=True), ttl=60)
    repo = PersonRepository(sqlite_session_factory)
    return Pipeline(lake, buffer, repo, min_fragments=2), lake, repo


def test_pipeline_consolidates_after_two_fragments(
    sqlite_session_factory, personal_fragment, bank_fragment
):
    pipe, lake, repo = _pipeline(sqlite_session_factory)

    # first fragment: stored raw, buffered, not yet consolidated
    assert pipe.process_message(personal_fragment) is None
    assert lake.count() == 1
    assert repo.count() == 0

    # second fragment (same passport): triggers consolidation
    pid = pipe.process_message(bank_fragment)
    assert pid is not None
    assert lake.count() == 2
    assert repo.count() == 1


def test_pipeline_unknown_message_does_not_crash(sqlite_session_factory):
    pipe, lake, repo = _pipeline(sqlite_session_factory)
    assert pipe.process_message({"foo": "bar"}) is None
    assert lake.count() == 1  # still stored raw
    assert repo.count() == 0


def test_pipeline_orphan_fragment_kept_raw_only(sqlite_session_factory):
    pipe, lake, repo = _pipeline(sqlite_session_factory)
    assert pipe.process_message({"Address": "x", "IPv4": "1.1.1.1"}) is None
    # net alone yields addr key, buffered but < min_fragments
    assert repo.count() == 0


def test_pipeline_swallows_persist_errors(sqlite_session_factory, personal_fragment, bank_fragment):
    """A failing repo must not crash the pipeline; failure is counted, not raised."""
    pipe, lake, _ = _pipeline(sqlite_session_factory)

    class BoomRepo:
        def upsert(self, person):
            raise RuntimeError("db down")

    pipe._repo = BoomRepo()
    pipe.process_message(personal_fragment)
    # second fragment triggers consolidation -> upsert raises -> handled
    assert pipe.process_message(bank_fragment) is None
