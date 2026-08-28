"""Deferrable sensor that waits until enough *new* persons have landed in the
Silver layer (Postgres `persons`) since the last Gold refresh.

Why deferrable?
---------------
A classic sensor would sit in a worker slot polling the database in a loop, wasting
a worker for the whole wait. A *deferrable* sensor instead hands off a small async
``Trigger`` to Airflow's ``triggerer`` process and frees the worker immediately. The
triggerer runs many such triggers concurrently on a single asyncio event loop, so the
wait is cheap. When the condition is met, the task is resumed on a worker to finish.

Signal & threshold
-------------------
- Signal: ``COUNT(*) FROM persons WHERE created_at > <watermark>``.
- Watermark: the timestamp of the last Gold refresh (``gold_stats.updated_at``). This
  makes the sensor measure exactly what a Gold refresh would consume — new rows in the
  table Gold aggregates from — instead of raw Kafka volume (which is noisy: ~5 raw
  fragments per person, plus fragments still buffered in Redis).
- Fires when ``new_persons >= min_new`` OR when the sensor's own ``timeout`` elapses
  (handled by Airflow), so Gold never goes stale under low load.

The trigger uses ``asyncpg`` (bundled in the Airflow image) for a genuinely async,
non-blocking poll.
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from datetime import datetime
from typing import Any

from airflow.sdk.bases.sensor import BaseSensorOperator
from airflow.triggers.base import BaseTrigger, TriggerEvent

# Dotted path Airflow stores to re-create the trigger in the triggerer process.
_TRIGGER_PATH = "hr_etl.airflow_ext.persons_threshold.NewPersonsTrigger"


def _asyncpg_dsn() -> str:
    """Build an asyncpg DSN from the same env vars the ETL uses.

    asyncpg wants a plain ``postgresql://`` URL (no SQLAlchemy ``+psycopg2`` suffix).
    """
    host = os.environ.get("POSTGRES_HOST", "postgres")
    port = os.environ.get("POSTGRES_PORT", "5432")
    db = os.environ.get("POSTGRES_DB", "hr_warehouse")
    user = os.environ.get("POSTGRES_USER", "hr_user")
    password = os.environ.get("POSTGRES_PASSWORD", "changeme")
    return f"postgresql://{user}:{password}@{host}:{port}/{db}"


def _default_min_new() -> int:
    return int(os.environ.get("GOLD_TRIGGER_MIN_NEW_PERSONS", "150"))


def _default_poll() -> float:
    return float(os.environ.get("GOLD_TRIGGER_POLL_SECONDS", "30"))


async def _count_new_persons(dsn: str, watermark: datetime | None) -> int:
    """Count persons created after ``watermark`` (or all persons if no watermark)."""
    import asyncpg  # imported lazily; only needed inside the triggerer/sensor runtime

    conn = await asyncpg.connect(dsn)
    try:
        if watermark is None:
            row = await conn.fetchval("SELECT COUNT(*) FROM persons")
        else:
            row = await conn.fetchval(
                "SELECT COUNT(*) FROM persons WHERE created_at > $1", watermark
            )
        return int(row or 0)
    finally:
        await conn.close()


class NewPersonsTrigger(BaseTrigger):
    """Async trigger: fires once ``>= min_new`` persons exist after ``watermark``.

    Runs inside the triggerer. It must be cheap and fully async: it opens an asyncpg
    connection, counts, and sleeps between polls without blocking the event loop.
    """

    def __init__(
        self,
        dsn: str,
        watermark_iso: str | None,
        min_new: int,
        poll_interval: float,
    ) -> None:
        super().__init__()
        self.dsn = dsn
        self.watermark_iso = watermark_iso
        self.min_new = min_new
        self.poll_interval = poll_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Tell Airflow how to reconstruct this trigger in the triggerer process."""
        return (
            _TRIGGER_PATH,
            {
                "dsn": self.dsn,
                "watermark_iso": self.watermark_iso,
                "min_new": self.min_new,
                "poll_interval": self.poll_interval,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        import asyncio

        watermark = datetime.fromisoformat(self.watermark_iso) if self.watermark_iso else None
        while True:
            count = await _count_new_persons(self.dsn, watermark)
            if count >= self.min_new:
                yield TriggerEvent(
                    {
                        "status": "ready",
                        "new_persons": count,
                        "min_new": self.min_new,
                    }
                )
                return
            await asyncio.sleep(self.poll_interval)


class NewPersonsSensor(BaseSensorOperator):
    """Deferrable sensor: succeeds when enough new persons landed since last Gold refresh.

    On execute it reads the watermark (last Gold refresh) and does a single cheap
    synchronous count. If the threshold is already met, it returns immediately (no
    defer). Otherwise it defers to :class:`NewPersonsTrigger`, freeing the worker until
    the triggerer reports the condition is satisfied (or the task ``timeout`` fires).
    """

    def __init__(
        self,
        *,
        min_new: int | None = None,
        poll_interval: float | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.min_new = min_new if min_new is not None else _default_min_new()
        self.poll_interval = poll_interval if poll_interval is not None else _default_poll()

    def _read_watermark(self) -> datetime | None:
        """Last Gold refresh time from gold_stats.updated_at (None if never refreshed)."""
        import psycopg2  # sync driver, fine for the one-shot pre-check on the worker

        host = os.environ.get("POSTGRES_HOST", "postgres")
        port = os.environ.get("POSTGRES_PORT", "5432")
        db = os.environ.get("POSTGRES_DB", "hr_warehouse")
        user = os.environ.get("POSTGRES_USER", "hr_user")
        password = os.environ.get("POSTGRES_PASSWORD", "changeme")
        conn = psycopg2.connect(host=host, port=port, dbname=db, user=user, password=password)
        try:
            cur = conn.cursor()
            # gold_stats may not exist yet on a brand-new DB; treat that as "no watermark".
            cur.execute("SELECT to_regclass('public.gold_stats')")
            if cur.fetchone()[0] is None:
                return None
            cur.execute("SELECT updated_at FROM gold_stats WHERE id = 1")
            row = cur.fetchone()
            return row[0] if row else None
        finally:
            conn.close()

    def _count_new(self, watermark: datetime | None) -> int:
        import psycopg2

        host = os.environ.get("POSTGRES_HOST", "postgres")
        port = os.environ.get("POSTGRES_PORT", "5432")
        db = os.environ.get("POSTGRES_DB", "hr_warehouse")
        user = os.environ.get("POSTGRES_USER", "hr_user")
        password = os.environ.get("POSTGRES_PASSWORD", "changeme")
        conn = psycopg2.connect(host=host, port=port, dbname=db, user=user, password=password)
        try:
            cur = conn.cursor()
            if watermark is None:
                cur.execute("SELECT COUNT(*) FROM persons")
            else:
                cur.execute("SELECT COUNT(*) FROM persons WHERE created_at > %s", (watermark,))
            return int(cur.fetchone()[0] or 0)
        finally:
            conn.close()

    def execute(self, context: Any) -> Any:
        watermark = self._read_watermark()

        # Cheap pre-check: if the threshold is already satisfied, don't defer at all.
        already = self._count_new(watermark)
        if already >= self.min_new:
            self.log.info(
                "threshold already met (%d >= %d new persons); no defer needed",
                already,
                self.min_new,
            )
            return {"status": "ready", "new_persons": already, "min_new": self.min_new}

        self.log.info(
            "only %d < %d new persons since last gold refresh; deferring to triggerer",
            already,
            self.min_new,
        )
        watermark_iso = watermark.isoformat() if watermark else None
        self.defer(
            trigger=NewPersonsTrigger(
                dsn=_asyncpg_dsn(),
                watermark_iso=watermark_iso,
                min_new=self.min_new,
                poll_interval=self.poll_interval,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Any, event: dict[str, Any] | None = None) -> Any:
        """Resumed by the triggerer once the condition is met."""
        self.log.info("sensor resumed: %s", event)
        return event
