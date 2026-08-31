"""Tests for the Gold layer refresh (warehouse/gold_layer).

The Gold SQL uses Postgres-only constructs (``TIMESTAMP WITH TIME ZONE``, ``NOW()``,
``::FLOAT`` casts and multi-statement scripts) that SQLite cannot execute, so these
are INTEGRATION tests that run against a real PostgreSQL.

They connect using TEST_POSTGRES_DSN (defaults to the docker-compose Postgres on
localhost:5432). If no Postgres is reachable the whole module is skipped, so the
normal unit-test run is never broken.

Run with the stack up:
    docker compose up -d postgres
    $env:TEST_POSTGRES_DSN="postgresql+psycopg2://hr_user:changeme@localhost:5432/hr_warehouse"
    pytest -m integration
"""

from __future__ import annotations

import os
import uuid

import pytest

pytest.importorskip("psycopg2", reason="psycopg2 not installed")

from sqlalchemy import text  # noqa: E402

from hr_etl.models.db_models import Base, PersonRow  # noqa: E402
from hr_etl.warehouse.engine import create_db_engine, init_schema  # noqa: E402
from hr_etl.warehouse.gold_layer import init_gold_schema, refresh_gold  # noqa: E402

DSN = os.getenv(
    "TEST_POSTGRES_DSN",
    "postgresql+psycopg2://hr_user:changeme@localhost:5432/hr_warehouse",
)


def _postgres_available() -> bool:
    try:
        engine = create_db_engine(DSN)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        engine.dispose()
        return True
    except Exception:
        return False


pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not _postgres_available(), reason="no PostgreSQL reachable at TEST_POSTGRES_DSN"
    ),
]


@pytest.fixture()
def pg_engine():
    engine = create_db_engine(DSN)
    # Clean schema so Gold aggregates only reflect this test's rows.
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS gold_stats"))
        conn.execute(text("DROP TABLE IF EXISTS gold_top_cities"))
        conn.execute(text("DROP TABLE IF EXISTS gold_top_companies"))
        conn.execute(text("DROP TABLE IF EXISTS gold_completeness"))
    Base.metadata.drop_all(engine)
    init_schema(engine)
    yield engine
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS gold_stats"))
        conn.execute(text("DROP TABLE IF EXISTS gold_top_cities"))
        conn.execute(text("DROP TABLE IF EXISTS gold_top_companies"))
        conn.execute(text("DROP TABLE IF EXISTS gold_completeness"))
    Base.metadata.drop_all(engine)
    engine.dispose()


def _seed(engine) -> None:
    """Seed 3 persons. Only the FIRST clears the Gold bar (5 obligatory + >=7/8 fields);
    the second is missing iban/email/phone/ipv4 (below 80%), the third is name-only."""
    prefix = uuid.uuid4().hex[:6]
    rows = [
        PersonRow(
            match_key=f"passport:{prefix}-1",
            passport="P1",
            full_name="ana gil",
            city="madrid",
            company="acme",
            iban="ES1",
            email="ana@x.com",
            phone="600",
            ipv4="1.1.1.1",
        ),
        PersonRow(
            match_key=f"passport:{prefix}-2",
            passport="P2",
            full_name="bea ruiz",
            city="madrid",
            company="globex",
        ),
        PersonRow(
            match_key=f"name:{prefix}-3",
            full_name="carlos sanz",
        ),
    ]
    from hr_etl.warehouse.engine import make_session_factory

    session = make_session_factory(engine)()
    try:
        session.add_all(rows)
        session.commit()
    finally:
        session.close()


def test_init_gold_schema_creates_tables(pg_engine):
    init_gold_schema(pg_engine)
    with pg_engine.connect() as conn:
        for table in ("gold_stats", "gold_top_cities", "gold_top_companies", "gold_completeness"):
            reg = conn.execute(text(f"SELECT to_regclass('public.{table}')")).scalar()
            assert reg is not None, f"{table} should exist"


def test_init_gold_schema_is_idempotent(pg_engine):
    init_gold_schema(pg_engine)
    # second call must not raise (CREATE TABLE IF NOT EXISTS)
    init_gold_schema(pg_engine)


def test_refresh_gold_populates_stats(pg_engine):
    """gold_* stats are now computed over gold_persons (the curated subset), not Silver.
    Of the 3 seeded persons only 1 qualifies for Gold, so the stats reflect that one."""
    init_gold_schema(pg_engine)
    _seed(pg_engine)
    refresh_gold(pg_engine)

    with pg_engine.connect() as conn:
        row = conn.execute(text("SELECT * FROM gold_stats WHERE id = 1")).fetchone()

    assert row is not None
    assert row.total_persons == 1  # only the fully-complete person is Gold
    assert row.with_passport == 1
    assert row.with_city == 1
    assert row.with_company == 1
    assert row.with_bank == 1  # one iban
    assert row.with_ipv4 == 1
    assert row.cross_linked == 1
    assert row.avg_completeness == pytest.approx(8.0)  # the Gold person has all 8 fields


def test_refresh_gold_top_cities_and_companies(pg_engine):
    init_gold_schema(pg_engine)
    _seed(pg_engine)
    refresh_gold(pg_engine)

    with pg_engine.connect() as conn:
        cities = conn.execute(
            text("SELECT city, person_count FROM gold_top_cities ORDER BY person_count DESC")
        ).fetchall()
        companies = conn.execute(
            text("SELECT company, person_count FROM gold_top_companies ORDER BY person_count DESC")
        ).fetchall()

    # Only the Gold person (madrid / acme) contributes.
    assert [c.city for c in cities] == ["madrid"]
    assert cities[0].person_count == 1
    assert {c.company for c in companies} == {"acme"}


def test_refresh_gold_completeness_distribution(pg_engine):
    init_gold_schema(pg_engine)
    _seed(pg_engine)
    refresh_gold(pg_engine)

    with pg_engine.connect() as conn:
        rows = conn.execute(
            text("SELECT fields_filled, person_count FROM gold_completeness ORDER BY fields_filled")
        ).fetchall()

    total = sum(r.person_count for r in rows)
    assert total == 1  # only the Gold person is counted
    assert rows[0].fields_filled == 8  # it has all 8 data fields


def test_refresh_gold_is_rebuild(pg_engine):
    """A second refresh fully rebuilds (no duplicate/stale stats rows)."""
    init_gold_schema(pg_engine)
    _seed(pg_engine)
    refresh_gold(pg_engine)
    refresh_gold(pg_engine)

    with pg_engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM gold_stats")).scalar()

    assert count == 1  # DELETE + INSERT keeps a single stats row


def test_refresh_gold_on_empty_persons(pg_engine):
    """Regression: refreshing with an empty persons table must not fail.

    AVG() over zero rows returns NULL, but gold_stats.avg_completeness is NOT NULL.
    The refresh SQL COALESCEs it to 0, so completeness is 0 and no NotNullViolation.
    """
    init_gold_schema(pg_engine)
    # No _seed(): persons is empty.
    refresh_gold(pg_engine)  # must not raise

    with pg_engine.connect() as conn:
        row = conn.execute(text("SELECT * FROM gold_stats WHERE id = 1")).fetchone()

    assert row is not None
    assert row.total_persons == 0
    assert row.avg_completeness == 0.0


# ----------------------------------------------------------------------
# T-12 — Gold membership: only complete records enter gold_persons, and the
# gold_* stats are computed over that subset (DEC-5, AC-9).
# ----------------------------------------------------------------------


def test_t12_gold_persons_membership_and_stats(pg_engine):
    """Persons below 80% OR missing one of the 5 obligatory fields are NOT Gold; a
    complete person IS Gold; the returned count and gold_persons cardinality agree."""
    init_gold_schema(pg_engine)
    prefix = uuid.uuid4().hex[:6]
    from hr_etl.warehouse.engine import make_session_factory

    session = make_session_factory(pg_engine)()
    try:
        # (a) fully complete -> Gold (8/8 fields, all 5 obligatory).
        complete = PersonRow(
            match_key=f"passport:{prefix}-complete",
            passport="P1",
            full_name="ana gil",
            city="madrid",
            company="acme",
            iban="ES1",
            email="ana@x.com",
            phone="600",
            ipv4="1.1.1.1",
        )
        # (b) 7/8 fields but MISSING an obligatory (company) -> NOT Gold.
        missing_obligatory = PersonRow(
            match_key=f"passport:{prefix}-noobl",
            passport="P2",
            full_name="bea ruiz",
            city="madrid",
            iban="ES2",
            email="bea@x.com",
            phone="601",
            ipv4="2.2.2.2",
        )
        # (c) has all 5 obligatory but only 5/8 fields (< 80%) -> NOT Gold.
        below_threshold = PersonRow(
            match_key=f"passport:{prefix}-below",
            passport="P3",
            full_name="carlos sanz",
            city="madrid",
            company="globex",
            email="carlos@x.com",
        )
        session.add_all([complete, missing_obligatory, below_threshold])
        session.commit()
        complete_id = complete.id
    finally:
        session.close()

    gold_count = refresh_gold(pg_engine)

    with pg_engine.connect() as conn:
        ids = [r.id for r in conn.execute(text("SELECT id FROM gold_persons ORDER BY id"))]
        stats_total = conn.execute(
            text("SELECT total_persons FROM gold_stats WHERE id = 1")
        ).scalar_one()
        stored = conn.execute(
            text("SELECT completeness FROM gold_persons WHERE id = :i"), {"i": complete_id}
        ).scalar_one()

    assert ids == [complete_id]  # only the complete person is Gold
    assert gold_count == 1
    assert stats_total == 1  # stats computed over gold_persons agree with its cardinality
    assert stored == pytest.approx(1.0)  # 8/8 fields -> completeness 1.0
