"""Tests that the pipeline updates Prometheus metrics as expected."""

from __future__ import annotations

import fakeredis
import mongomock

from hr_etl.cache.redis_buffer import RedisBuffer
from hr_etl.lake.mongo_lake import MongoLake
from hr_etl.metrics.prometheus import (
    CONSOLIDATIONS,
    MESSAGES_CONSUMED,
    MESSAGES_FAILED,
    PERSONS_PERSISTED,
)
from hr_etl.pipeline import Pipeline
from hr_etl.warehouse.person_repo import PersonRepository


def _pipeline(sqlite_session_factory):
    collection = mongomock.MongoClient()["hr_lake"]["raw"]
    buffer = RedisBuffer(fakeredis.FakeStrictRedis(decode_responses=True), ttl=60)
    repo = PersonRepository(sqlite_session_factory)
    return Pipeline(MongoLake(collection), buffer, repo, min_fragments=2)


def _counter_value(counter, **labels) -> float:
    """Read the current value of a (possibly labeled) prometheus counter."""
    metric = counter.labels(**labels) if labels else counter
    return metric._value.get()


def test_consumed_and_persisted_metrics_increase(
    sqlite_session_factory, personal_fragment, bank_fragment
):
    pipe = _pipeline(sqlite_session_factory)

    consumed_before = _counter_value(MESSAGES_CONSUMED, fragment_type="personal")
    persisted_before = _counter_value(PERSONS_PERSISTED)
    consolidations_before = _counter_value(CONSOLIDATIONS)

    pipe.process_message(personal_fragment)  # 1st fragment, buffered
    pipe.process_message(bank_fragment)  # 2nd -> consolidate + persist

    assert _counter_value(MESSAGES_CONSUMED, fragment_type="personal") == consumed_before + 1
    assert _counter_value(PERSONS_PERSISTED) == persisted_before + 1
    assert _counter_value(CONSOLIDATIONS) == consolidations_before + 1


def test_failed_metric_increases_on_unknown(sqlite_session_factory):
    pipe = _pipeline(sqlite_session_factory)
    failed_before = _counter_value(MESSAGES_FAILED)

    pipe.process_message({"foo": "bar"})  # unknown fragment type

    assert _counter_value(MESSAGES_FAILED) == failed_before + 1


def test_metrics_endpoint_exposes_names(sqlite_session_factory):
    """The API /metrics output includes our custom metric names."""
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from hr_etl.api.routes import build_router

    app = FastAPI()
    app.include_router(build_router(sqlite_session_factory))
    client = TestClient(app)

    body = client.get("/metrics").text
    assert "hr_etl_messages_consumed_total" in body
    assert "hr_etl_persist_seconds" in body
    assert "hr_etl_persons_persisted_total" in body
