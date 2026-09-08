"""Unit tests for the deferrable Airflow sensor (airflow_ext/persons_threshold).

These exercise the sensor/trigger decision logic WITHOUT touching Postgres: the
synchronous pre-check (`_read_watermark` / `_count_new`) and the async counter
(`_count_new_persons`) are monkeypatched, so no `asyncpg`/`psycopg2` connection is
opened.

Airflow is only present inside the container/CI image (the local `airflow/` folder
is just the DAGs directory, not the `apache-airflow` package). When
`airflow.sdk.bases.sensor` cannot be imported, the whole module is skipped cleanly
so it never breaks the local unit-test run.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import pytest

# Skip cleanly where the real Airflow SDK isn't installed (local dev venv).
pytest.importorskip(
    "airflow.sdk.bases.sensor",
    reason="apache-airflow SDK not installed (runs in the Airflow container/CI)",
)

from airflow.triggers.base import TriggerEvent  # noqa: E402

from hr_etl.airflow_ext import persons_threshold as pt  # noqa: E402
from hr_etl.airflow_ext.persons_threshold import (  # noqa: E402
    NewPersonsSensor,
    NewPersonsTrigger,
    _asyncpg_dsn,
)


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------


def _make_sensor(**kwargs) -> NewPersonsSensor:
    # task_id is required by BaseSensorOperator.
    return NewPersonsSensor(task_id="t", **kwargs)


def _drain(agen):
    """Collect all events yielded by an async generator, driving the loop manually."""
    events = []

    async def _run():
        async for ev in agen:
            events.append(ev)

    asyncio.new_event_loop().run_until_complete(_run())
    return events


# ----------------------------------------------------------------------
# _asyncpg_dsn / env-driven defaults
# ----------------------------------------------------------------------


def test_asyncpg_dsn_uses_plain_postgresql_scheme(monkeypatch):
    monkeypatch.setenv("POSTGRES_HOST", "db")
    monkeypatch.setenv("POSTGRES_PORT", "5555")
    monkeypatch.setenv("POSTGRES_DB", "wh")
    monkeypatch.setenv("POSTGRES_USER", "u")
    monkeypatch.setenv("POSTGRES_PASSWORD", "p")
    assert _asyncpg_dsn() == "postgresql://u:p@db:5555/wh"


def test_sensor_defaults_from_env(monkeypatch):
    monkeypatch.setenv("GOLD_TRIGGER_MIN_NEW_PERSONS", "42")
    monkeypatch.setenv("GOLD_TRIGGER_POLL_SECONDS", "7.5")
    sensor = _make_sensor()
    assert sensor.min_new == 42
    assert sensor.poll_interval == 7.5


def test_sensor_explicit_args_override_env(monkeypatch):
    monkeypatch.setenv("GOLD_TRIGGER_MIN_NEW_PERSONS", "42")
    sensor = _make_sensor(min_new=5, poll_interval=1.0)
    assert sensor.min_new == 5
    assert sensor.poll_interval == 1.0


# ----------------------------------------------------------------------
# NewPersonsSensor.execute() — pre-check decision (defer vs immediate)
# ----------------------------------------------------------------------


def test_execute_returns_ready_without_defer_when_threshold_met(monkeypatch):
    sensor = _make_sensor(min_new=3)
    monkeypatch.setattr(sensor, "_read_watermark", lambda: None)
    monkeypatch.setattr(sensor, "_count_new", lambda wm: 5)

    def _boom(*a, **k):  # defer must NOT be called
        raise AssertionError("should not defer when threshold already met")

    monkeypatch.setattr(sensor, "defer", _boom)

    result = sensor.execute(context={})
    assert result == {"status": "ready", "new_persons": 5, "min_new": 3}


def test_execute_defers_with_trigger_when_below_threshold(monkeypatch):
    watermark = datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)
    sensor = _make_sensor(min_new=100, poll_interval=15.0)
    monkeypatch.setattr(sensor, "_read_watermark", lambda: watermark)
    monkeypatch.setattr(sensor, "_count_new", lambda wm: 10)

    captured = {}

    def _fake_defer(*, trigger, method_name):
        captured["trigger"] = trigger
        captured["method_name"] = method_name

    monkeypatch.setattr(sensor, "defer", _fake_defer)

    sensor.execute(context={})

    assert captured["method_name"] == "execute_complete"
    trigger = captured["trigger"]
    assert isinstance(trigger, NewPersonsTrigger)
    assert trigger.min_new == 100
    assert trigger.poll_interval == 15.0
    # watermark is passed through as ISO string
    assert trigger.watermark_iso == watermark.isoformat()


def test_execute_defers_with_null_watermark(monkeypatch):
    sensor = _make_sensor(min_new=100)
    monkeypatch.setattr(sensor, "_read_watermark", lambda: None)
    monkeypatch.setattr(sensor, "_count_new", lambda wm: 0)

    captured = {}
    monkeypatch.setattr(
        sensor, "defer", lambda *, trigger, method_name: captured.update(trigger=trigger)
    )

    sensor.execute(context={})
    assert captured["trigger"].watermark_iso is None


def test_execute_complete_returns_event():
    sensor = _make_sensor()
    event = {"status": "ready", "new_persons": 200, "min_new": 150}
    assert sensor.execute_complete(context={}, event=event) == event


# ----------------------------------------------------------------------
# NewPersonsTrigger.serialize()
# ----------------------------------------------------------------------


def test_trigger_serialize_roundtrip():
    trigger = NewPersonsTrigger(
        dsn="postgresql://u:p@h:5432/db",
        watermark_iso="2024-01-01T12:00:00+00:00",
        min_new=150,
        poll_interval=30.0,
    )
    path, kwargs = trigger.serialize()
    assert path == "hr_etl.airflow_ext.persons_threshold.NewPersonsTrigger"
    assert kwargs == {
        "dsn": "postgresql://u:p@h:5432/db",
        "watermark_iso": "2024-01-01T12:00:00+00:00",
        "min_new": 150,
        "poll_interval": 30.0,
    }
    # kwargs must be able to reconstruct the trigger
    rebuilt = NewPersonsTrigger(**kwargs)
    assert rebuilt.min_new == 150


# ----------------------------------------------------------------------
# NewPersonsTrigger.run() — async polling loop
# ----------------------------------------------------------------------


def test_trigger_run_yields_ready_immediately_when_count_high(monkeypatch):
    async def _fake_count(dsn, watermark):
        return 300

    monkeypatch.setattr(pt, "_count_new_persons", _fake_count)

    trigger = NewPersonsTrigger(dsn="x", watermark_iso=None, min_new=150, poll_interval=0.0)
    events = _drain(trigger.run())

    assert len(events) == 1
    assert isinstance(events[0], TriggerEvent)
    assert events[0].payload == {"status": "ready", "new_persons": 300, "min_new": 150}


def test_trigger_run_polls_until_threshold(monkeypatch):
    counts = iter([10, 50, 200])  # below, below, then >= min_new

    async def _fake_count(dsn, watermark):
        return next(counts)

    async def _no_sleep(_seconds):
        return None

    monkeypatch.setattr(pt, "_count_new_persons", _fake_count)
    monkeypatch.setattr(asyncio, "sleep", _no_sleep)

    trigger = NewPersonsTrigger(dsn="x", watermark_iso=None, min_new=150, poll_interval=30.0)
    events = _drain(trigger.run())

    assert len(events) == 1
    assert events[0].payload["new_persons"] == 200


def test_trigger_run_parses_watermark_iso(monkeypatch):
    seen = {}

    async def _fake_count(dsn, watermark):
        seen["watermark"] = watermark
        return 999

    monkeypatch.setattr(pt, "_count_new_persons", _fake_count)

    iso = "2024-06-01T08:30:00+00:00"
    trigger = NewPersonsTrigger(dsn="x", watermark_iso=iso, min_new=1, poll_interval=0.0)
    _drain(trigger.run())

    assert seen["watermark"] == datetime.fromisoformat(iso)
