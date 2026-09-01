"""Integration tests for the consolidation fix (processing/consolidate_merge).

The consolidation job merges person rows that are the SAME individual: same ``passport``
AND a very similar ``norm_name``. It runs entirely in Postgres (pg_trgm ``similarity()``,
temp tables, set-based survivorship), so these are integration tests against a REAL
PostgreSQL — SQLite cannot execute the SQL.

Connects via TEST_POSTGRES_DSN (defaults to the docker-compose Postgres on
localhost:5432). If no Postgres is reachable, the module is skipped.

Run it with the stack up:
    docker compose up -d postgres
    $env:TEST_POSTGRES_DSN="postgresql+psycopg2://hr_user:changeme@localhost:5432/hr_warehouse"
    pytest tests/test_consolidate_merge.py -m integration
"""

from __future__ import annotations

import os

import pytest

pytest.importorskip("psycopg2", reason="psycopg2 not installed")

from sqlalchemy import text  # noqa: E402

from hr_etl.models.db_models import Base, PersonRow  # noqa: E402
from hr_etl.processing.consolidate_merge import run_consolidation  # noqa: E402
from hr_etl.warehouse.engine import (  # noqa: E402
    create_db_engine,
    init_schema,
    make_session_factory,
)

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
def pg_session_factory():
    engine = create_db_engine(DSN)
    Base.metadata.drop_all(engine)
    init_schema(engine)
    factory = make_session_factory(engine)
    yield factory
    Base.metadata.drop_all(engine)
    engine.dispose()


def _add_person(session, match_key: str, full_name: str | None, **kwargs) -> PersonRow:
    """Insert a person; norm_name is materialized by run_consolidation's guard, but we
    also set it here to mirror the real (streaming-populated) state."""
    from hr_etl.processing.normalizer import compute_norm_name

    row = PersonRow(
        match_key=match_key,
        full_name=full_name,
        norm_name=compute_norm_name(full_name),
        **kwargs,
    )
    session.add(row)
    session.flush()
    return row


def _persons(session) -> list[PersonRow]:
    # Consolidation mutates `persons` via raw set-based SQL (bypassing the ORM), so the
    # session's identity map holds stale copies. Expire them to re-read fresh from the DB.
    session.expire_all()
    return session.query(PersonRow).order_by(PersonRow.id).all()


# ----------------------------------------------------------------------
# T-1 — same passport + (near) identical name => merged into one survivor
# ----------------------------------------------------------------------


def test_t1_merges_same_passport_similar_name(pg_session_factory):
    """ "William Weiss" / "william weiss" under one passport are complementary fragments
    of the same person: they merge into one row (min id survives)."""
    session = pg_session_factory()
    try:
        a = _add_person(session, "k:ww-1", "William Weiss", passport="000049218", city="Madrid")
        b = _add_person(session, "k:ww-2", "william weiss", passport="000049218", email="w@x.com")
        session.commit()
        expected_survivor = min(a.id, b.id)  # capture ids before the merge deletes a row

        merged = run_consolidation(session)

        assert merged == 1  # one loser removed
        rows = _persons(session)
        assert len(rows) == 1
        survivor = rows[0]
        assert survivor.id == expected_survivor  # min(id) is the anchor
        # complementary data folded in from both fragments
        assert survivor.city == "Madrid"
        assert survivor.email == "w@x.com"
        assert survivor.passport == "000049218"
    finally:
        session.close()


# ----------------------------------------------------------------------
# T-2 — same passport but DIFFERENT names (generator collision) => NOT merged
# ----------------------------------------------------------------------


def test_t2_does_not_merge_same_passport_different_names(pg_session_factory):
    """A shared passport with clearly different names is generator noise, not one
    person: the rows stay separate (AC-2)."""
    session = pg_session_factory()
    try:
        _add_person(session, "k:c-1", "William Weiss", passport="SHARED1")
        _add_person(session, "k:c-2", "Pedro Gomez", passport="SHARED1")
        session.commit()

        merged = run_consolidation(session)

        assert merged == 0
        assert len(_persons(session)) == 2
    finally:
        session.close()


# ----------------------------------------------------------------------
# T-3 — survivorship: complementary fields combine; full_name = longest
# ----------------------------------------------------------------------


def test_t3_survivorship_combines_fields_and_longest_name(pg_session_factory):
    """Row A has a city and no email; row B has an email and no city; the merged survivor
    carries BOTH, and full_name is the longer/more complete of the two."""
    session = pg_session_factory()
    try:
        # Near-identical names (one trailing letter) => trigram similarity clears the
        # strict 0.85 merge bar, so they ARE the same person; the longer full_name wins.
        a = _add_person(session, "k:s-1", "alexander thompson", passport="P-SURV", city="Sevilla")
        b = _add_person(session, "k:s-2", "alexander thompsonn", passport="P-SURV", email="m@x.com")
        session.commit()
        expected_survivor = min(a.id, b.id)  # capture before the merge deletes a row

        merged = run_consolidation(session)

        assert merged == 1
        rows = _persons(session)
        assert len(rows) == 1
        survivor = rows[0]
        assert survivor.id == expected_survivor
        assert survivor.city == "Sevilla"  # from A
        assert survivor.email == "m@x.com"  # from B
        # full_name = the longest of the two (most complete)
        assert survivor.full_name == "alexander thompsonn"
        # norm_name recomputed from the winning full_name
        assert survivor.norm_name == "alexander thompsonn"
    finally:
        session.close()


def test_transitive_chain_merges_to_root_without_data_loss(pg_session_factory):
    """Three near-identical rows under one passport (a~b, b~c) must all fold into the
    single smallest id, and NO fragment's data may be lost in the chain.

    Regression: a naive "nearest linked id" survivor would send c -> b, but b is itself a
    loser of a and gets deleted, dropping c's data. The transitive root (min id) fixes it.
    """
    session = pg_session_factory()
    try:
        # Three near-identical names under one passport. Use a relaxed threshold (0.6) so
        # the trio links; the point is that ALL three resolve to the single min(id) root
        # and every fragment's unique field survives, regardless of link topology.
        a = _add_person(session, "k:ch-1", "ana gil", passport="CHAIN1", city="Vigo")
        b = _add_person(session, "k:ch-2", "ana gill", passport="CHAIN1", email="b@x.com")
        c = _add_person(session, "k:ch-3", "ana gilo", passport="CHAIN1", company="Acme")
        session.commit()
        root = min(a.id, b.id, c.id)

        merged = run_consolidation(session, merge_threshold=0.6)

        assert merged == 2  # two losers removed, one survivor
        rows = _persons(session)
        assert len(rows) == 1
        survivor = rows[0]
        assert survivor.id == root
        # every fragment's unique field survived (no data lost in the chain)
        assert survivor.city == "Vigo"  # from a
        assert survivor.email == "b@x.com"  # from b
        assert survivor.company == "Acme"  # from c
    finally:
        session.close()


def test_consolidation_is_idempotent(pg_session_factory):
    """A second run finds no new pairs and changes nothing (NFR-3)."""
    session = pg_session_factory()
    try:
        _add_person(session, "k:i-1", "William Weiss", passport="IDEM1", city="Madrid")
        _add_person(session, "k:i-2", "william weiss", passport="IDEM1", email="w@x.com")
        session.commit()

        assert run_consolidation(session) == 1
        assert run_consolidation(session) == 0
        assert len(_persons(session)) == 1
    finally:
        session.close()


def test_consolidation_empty_warehouse(pg_session_factory):
    session = pg_session_factory()
    try:
        assert run_consolidation(session) == 0
    finally:
        session.close()


# ======================================================================
# VÍA 2 — split person by IDENTICAL name (the ~203k real case from the VM):
# a Location-side row (address, no passport) and a Personal-side row (passport,
# no address) with the SAME normalized name were never joined in streaming.
# They are the same person and must merge, tightly gated to stay safe.
# ======================================================================


def test_via2_merges_split_person_identical_name(pg_session_factory):
    """Location-side (address, NO passport) + Personal-side (passport, NO address) with
    the same normalized name: the streaming split them, VÍA 2 merges them even though
    they share NO field but the name (disjoint data)."""
    session = pg_session_factory()
    try:
        loc = _add_person(
            session, "name:aaron allan", "Aaron Allan", city="Posadas", address="Rua 71"
        )
        per = _add_person(
            session, "passport:X22", "aaron allan", passport="X22169584", email="a@x.com"
        )
        session.commit()
        expected_survivor = min(loc.id, per.id)

        merged = run_consolidation(session)

        assert merged == 1
        rows = _persons(session)
        assert len(rows) == 1
        survivor = rows[0]
        assert survivor.id == expected_survivor
        # complementary disjoint data folded together
        assert survivor.passport == "X22169584"
        assert survivor.address == "Rua 71"
        assert survivor.city == "Posadas"
        assert survivor.email == "a@x.com"
    finally:
        session.close()


def test_via2_does_not_merge_identical_name_different_passports(pg_session_factory):
    """Two people with the same name who BOTH carry a DIFFERENT passport are provably
    distinct homonyms (the ~44k freq=2 case): VÍA 2 must NOT merge them."""
    session = pg_session_factory()
    try:
        _add_person(session, "k:h-1", "juan ignacio", passport="AAA111", city="Madrid")
        _add_person(session, "k:h-2", "juan ignacio", passport="BBB222", city="Sevilla")
        session.commit()

        merged = run_consolidation(session)

        assert merged == 0
        assert len(_persons(session)) == 2
    finally:
        session.close()


def test_via2_does_not_merge_identical_name_both_without_passport(pg_session_factory):
    """Same name, NEITHER side has a passport: too ambiguous to auto-merge (could be two
    different people). VÍA 2 requires at least one passport, so these stay separate."""
    session = pg_session_factory()
    try:
        _add_person(session, "k:np-1", "maria lopez", city="Madrid", address="A St")
        _add_person(session, "k:np-2", "maria lopez", city="Sevilla", address="B St")
        session.commit()

        merged = run_consolidation(session)

        assert merged == 0
        assert len(_persons(session)) == 2
    finally:
        session.close()


def test_via2_does_not_merge_bucket_larger_than_two(pg_session_factory):
    """Conservative gate: a same-name bucket with MORE than 2 eligible rows is left alone
    for now (may hold real homonyms); only clean freq=2 buckets auto-merge."""
    session = pg_session_factory()
    try:
        _add_person(session, "name:leo diaz", "Leo Diaz", address="Calle 1")  # no passport
        _add_person(session, "passport:D1", "leo diaz", passport="D111", email="l1@x.com")
        _add_person(session, "passport:D2", "leo diaz", passport="D222", email="l2@x.com")
        session.commit()

        merged = run_consolidation(session)

        assert merged == 0
        assert len(_persons(session)) == 3
    finally:
        session.close()


def test_via2_single_word_name_not_merged(pg_session_factory):
    """A single-word normalized name is too ambiguous and is excluded (>= 2 words)."""
    session = pg_session_factory()
    try:
        _add_person(session, "name:madonna", "Madonna", address="Studio")  # no passport
        _add_person(session, "passport:M1", "madonna", passport="M111", email="m@x.com")
        session.commit()

        merged = run_consolidation(session)

        assert merged == 0
        assert len(_persons(session)) == 2
    finally:
        session.close()
