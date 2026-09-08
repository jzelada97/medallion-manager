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


_GOLD_AGG_TABLES = (
    "gold_stats",
    "gold_top_cities",
    "gold_top_companies",
    "gold_completeness",
    "gold_duplicate_groups",
)


@pytest.fixture()
def pg_engine():
    engine = create_db_engine(DSN)
    # Clean schema so Gold aggregates only reflect this test's rows.
    with engine.begin() as conn:
        for tbl in _GOLD_AGG_TABLES:
            conn.execute(text(f"DROP TABLE IF EXISTS {tbl}"))
    Base.metadata.drop_all(engine)
    init_schema(engine)
    yield engine
    with engine.begin() as conn:
        for tbl in _GOLD_AGG_TABLES:
            conn.execute(text(f"DROP TABLE IF EXISTS {tbl}"))
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


def test_repeated_name_excluded_from_gold(pg_engine):
    """The name-uniqueness gate, checked directly against Silver: a fully-complete person
    whose norm_name appears on MORE THAN ONE persons row must NOT reach Gold — we cannot be
    sure its five fields belong to a single real person. Crucially this does NOT depend on
    duplicate_groups (which reconcile prunes with a frequency guard); it checks persons
    directly, so even a common repeated name is kept out of Gold."""
    init_gold_schema(pg_engine)
    prefix = uuid.uuid4().hex[:6]
    from hr_etl.processing.normalizer import compute_norm_name
    from hr_etl.warehouse.engine import make_session_factory

    def _complete(mk, full_name, **ov):
        base = dict(
            match_key=mk,
            full_name=full_name,
            norm_name=compute_norm_name(full_name),
            passport="P",
            city="madrid",
            company="acme",
            iban="ES",
            email="e@x.com",
            phone="600",
            ipv4="1.1.1.1",
        )
        base.update(ov)
        return PersonRow(**base)

    session = make_session_factory(pg_engine)()
    try:
        # unique name -> Gold
        unique = _complete(f"passport:{prefix}-u", "ana gil", passport="P1", email="a@x.com")
        # two complete persons sharing the SAME norm_name -> neither is Gold
        dupe_a = _complete(f"passport:{prefix}-d1", "bea ruiz", passport="P2", email="b1@x.com")
        dupe_b = _complete(f"passport:{prefix}-d2", "Bea Ruiz", passport="P3", email="b2@x.com")
        session.add_all([unique, dupe_a, dupe_b])
        session.commit()
        unique_id = unique.id
    finally:
        session.close()

    refresh_gold(pg_engine)

    with pg_engine.connect() as conn:
        ids = [r.id for r in conn.execute(text("SELECT id FROM gold_persons ORDER BY id"))]

    assert ids == [unique_id]  # only the unique-named person is Gold; the two dupes excluded


# ----------------------------------------------------------------------
# gold_duplicate_groups — the pre-aggregated duplicate-review groups that the
# /groups endpoint reads (materialized by refresh_gold, one row per group with
# members as JSON). Replaces the old JOIN-and-bundle-in-Python path.
# ----------------------------------------------------------------------


def _add_dupe_group(session, group_id: int, members: list[tuple[int, float, str]]) -> None:
    """Insert duplicate_groups rows: members is a list of (person_id, confidence, reason)."""
    for person_id, confidence, reason in members:
        session.execute(
            text(
                "INSERT INTO duplicate_groups (group_id, person_id, confidence, reason) "
                "VALUES (:g, :p, :c, :r)"
            ),
            {"g": group_id, "p": person_id, "c": confidence, "r": reason},
        )


def test_refresh_materializes_duplicate_groups(pg_engine):
    """refresh_gold fills gold_duplicate_groups: one row per group, member_count,
    max_confidence, a representative reason, and the members resolved as JSON."""
    init_gold_schema(pg_engine)
    prefix = uuid.uuid4().hex[:6]
    from hr_etl.warehouse.engine import make_session_factory

    session = make_session_factory(pg_engine)()
    try:
        a = PersonRow(match_key=f"passport:{prefix}-a", full_name="ana gil", city="madrid")
        b = PersonRow(match_key=f"name:{prefix}-b", full_name="Ana Gil", company="acme")
        session.add_all([a, b])
        session.commit()
        a_id, b_id = a.id, b.id
        _add_dupe_group(session, a_id, [(a_id, 1.0, "exact_name"), (b_id, 1.0, "exact_name")])
        session.commit()
    finally:
        session.close()

    refresh_gold(pg_engine)

    with pg_engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT group_id, member_count, max_confidence, reason, members "
                "FROM gold_duplicate_groups WHERE group_id = :g"
            ),
            {"g": a_id},
        ).fetchone()

    assert row is not None
    assert row.member_count == 2
    assert row.max_confidence == pytest.approx(1.0)
    assert row.reason == "exact_name"
    member_ids = sorted(m["person_id"] for m in row.members)
    assert member_ids == sorted([a_id, b_id])
    # member payload carries the person's display fields for the UI.
    by_id = {m["person_id"]: m for m in row.members}
    assert by_id[a_id]["city"] == "madrid"
    assert by_id[b_id]["company"] == "acme"


def test_refresh_duplicate_groups_drops_singletons(pg_engine):
    """A group with a single member is not a duplicate and is excluded (HAVING >= 2)."""
    init_gold_schema(pg_engine)
    prefix = uuid.uuid4().hex[:6]
    from hr_etl.warehouse.engine import make_session_factory

    session = make_session_factory(pg_engine)()
    try:
        solo = PersonRow(match_key=f"passport:{prefix}-solo", full_name="uno solo")
        session.add(solo)
        session.commit()
        _add_dupe_group(session, solo.id, [(solo.id, 0.9, "fuzzy_name")])
        session.commit()
    finally:
        session.close()

    refresh_gold(pg_engine)

    with pg_engine.connect() as conn:
        n = conn.execute(text("SELECT COUNT(*) FROM gold_duplicate_groups")).scalar()

    assert n == 0  # the singleton group is not materialized


def test_refresh_duplicate_groups_excludes_reviewed(pg_engine):
    """A member already resolved in person_reviews (approved/distinct/merged) is excluded,
    so a group a human settled stops surfacing — even without a fresh reconcile."""
    init_gold_schema(pg_engine)
    prefix = uuid.uuid4().hex[:6]
    from hr_etl.warehouse.engine import make_session_factory

    session = make_session_factory(pg_engine)()
    try:
        a = PersonRow(match_key=f"passport:{prefix}-a", full_name="ana gil")
        b = PersonRow(match_key=f"name:{prefix}-b", full_name="Ana Gil")
        c = PersonRow(match_key=f"passport:{prefix}-c", full_name="ana gil")
        session.add_all([a, b, c])
        session.commit()
        gid = a.id
        _add_dupe_group(
            session,
            gid,
            [(a.id, 1.0, "exact_name"), (b.id, 1.0, "exact_name"), (c.id, 1.0, "exact_name")],
        )
        # A human marked B as a distinct person -> B drops out of the pending group.
        session.execute(
            text("INSERT INTO person_reviews (match_key, status) VALUES (:mk, 'distinct')"),
            {"mk": f"name:{prefix}-b"},
        )
        session.commit()
        a_id, c_id = a.id, c.id
    finally:
        session.close()

    refresh_gold(pg_engine)

    with pg_engine.connect() as conn:
        row = conn.execute(
            text("SELECT member_count, members FROM gold_duplicate_groups WHERE group_id = :g"),
            {"g": gid},
        ).fetchone()

    # B is excluded; the group keeps A and C (still >= 2, still a pending duplicate).
    assert row is not None
    assert row.member_count == 2
    assert sorted(m["person_id"] for m in row.members) == sorted([a_id, c_id])


def test_refresh_duplicate_groups_is_rebuild(pg_engine):
    """A second refresh fully rebuilds the table (DELETE + INSERT), no duplicate rows."""
    init_gold_schema(pg_engine)
    prefix = uuid.uuid4().hex[:6]
    from hr_etl.warehouse.engine import make_session_factory

    session = make_session_factory(pg_engine)()
    try:
        a = PersonRow(match_key=f"passport:{prefix}-a", full_name="ana gil")
        b = PersonRow(match_key=f"name:{prefix}-b", full_name="Ana Gil")
        session.add_all([a, b])
        session.commit()
        _add_dupe_group(session, a.id, [(a.id, 1.0, "exact_name"), (b.id, 1.0, "exact_name")])
        session.commit()
        gid = a.id
    finally:
        session.close()

    refresh_gold(pg_engine)
    refresh_gold(pg_engine)

    with pg_engine.connect() as conn:
        n = conn.execute(
            text("SELECT COUNT(*) FROM gold_duplicate_groups WHERE group_id = :g"), {"g": gid}
        ).scalar()

    assert n == 1  # one row per group, not duplicated across refreshes
