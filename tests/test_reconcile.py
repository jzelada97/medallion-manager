"""Integration tests for the fuzzy batch reconciliation (processing/reconcile).

Reconciliation runs its detection in SQL using the ``pg_trgm`` extension (fuzzy name
similarity) and groups probable duplicates into ``duplicate_groups``. These are
integration tests against a REAL PostgreSQL — the SQL uses Postgres-specific features
(pg_trgm's ``similarity()``, ``split_part``, ``regexp_replace``) that SQLite lacks.

Connects via TEST_POSTGRES_DSN (defaults to the docker-compose Postgres on
localhost:5432). If no Postgres is reachable, the module is skipped.

Run it with the stack up:
    docker compose up -d postgres
    set TEST_POSTGRES_DSN=postgresql+psycopg2://hr_user:changeme@localhost:5432/hr_warehouse
    pytest -m integration
"""

from __future__ import annotations

import os

import pytest

pytest.importorskip("psycopg2", reason="psycopg2 not installed")

from sqlalchemy import text  # noqa: E402

from hr_etl.models.db_models import Base, DuplicateGroup, PersonRow  # noqa: E402
from hr_etl.processing.reconcile import run_reconciliation  # noqa: E402
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
    row = PersonRow(match_key=match_key, full_name=full_name, **kwargs)
    session.add(row)
    session.flush()
    return row


def _groups(session) -> dict[int, list[DuplicateGroup]]:
    """Return current memberships bundled by group_id."""
    out: dict[int, list[DuplicateGroup]] = {}
    for m in session.query(DuplicateGroup).all():
        out.setdefault(m.group_id, []).append(m)
    return out


# ----------------------------------------------------------------------
# Fuzzy name similarity groups near-identical names (typos, extra surname)
# ----------------------------------------------------------------------


def test_fuzzy_groups_typo_surname(pg_session_factory):
    session = pg_session_factory()
    try:
        # One-letter difference at the end: exact matching would miss this.
        a = _add_person(session, "passport:X1", "jean leclerc")
        b = _add_person(session, "name:jean leclercq", "jean leclercq")
        session.commit()

        n = run_reconciliation(session, similarity_threshold=0.6)

        assert n >= 2  # both land in a group
        groups = _groups(session)
        # exactly one group containing both persons
        assert any({m.person_id for m in members} == {a.id, b.id} for members in groups.values())
    finally:
        session.close()


def test_containment_extra_surname_groups(pg_session_factory):
    """A shorter name contained in a longer one links WHEN a strong field corroborates.

    "octavio ponce" ⊆ "octavio ponce gimenez": trigram similarity is < 0.85, so only the
    containment rule can link them — and only because they share a strong corroborating
    signal (same email) that keeps containment from merging unrelated homonyms.
    """
    session = pg_session_factory()
    try:
        a = _add_person(session, "passport:X1", "octavio ponce", email="op@x.com")
        b = _add_person(
            session, "name:octavio ponce gimenez", "octavio ponce gimenez", email="op@x.com"
        )
        session.commit()

        # default strict threshold (0.85): only containment can link these
        n = run_reconciliation(session)

        assert n >= 2
        groups = _groups(session)
        assert any({m.person_id for m in members} == {a.id, b.id} for members in groups.values())
        reasons = {m.reason for m in session.query(DuplicateGroup).all()}
        assert any("name_containment" in r for r in reasons)
    finally:
        session.close()


def test_containment_without_corroboration_no_group(pg_session_factory):
    """Containment with only a WEAK signal (same city alone) must NOT link — a shared city
    is low-cardinality and unrelated homonyms share it. Requires a strong signal or
    city+company together."""
    session = pg_session_factory()
    try:
        _add_person(session, "passport:X1", "octavio ponce", city="Madrid")
        _add_person(session, "name:octavio ponce gimenez", "octavio ponce gimenez", city="Madrid")
        session.commit()

        assert run_reconciliation(session) == 0
    finally:
        session.close()


def test_accents_normalized_together(pg_session_factory):
    """ "María López" and "Maria Lopez" normalize to the same name (accents stripped).

    Same normalized name is only a duplicate CANDIDATE when it is ambiguous: at least one
    side lacks a passport AND they share a corroborating field. Here both lack a passport
    and share a city, so they group (exact_name + corroborated).
    """
    session = pg_session_factory()
    try:
        # Same email is a STRONG corroborating signal (near-unique).
        a = _add_person(session, "passport:X1", "María López", email="ml@x.com")
        b = _add_person(session, "name:maria lopez", "Maria Lopez", email="ml@x.com")
        session.commit()

        n = run_reconciliation(session)

        assert n >= 2
        groups = _groups(session)
        assert any({m.person_id for m in members} == {a.id, b.id} for members in groups.values())
    finally:
        session.close()


def test_group_anchored_to_min_id(pg_session_factory):
    session = pg_session_factory()
    try:
        a = _add_person(session, "passport:X1", "maria lopez")
        b = _add_person(session, "name:maria lopezz", "maria lopezz")
        session.commit()

        run_reconciliation(session, similarity_threshold=0.6)

        groups = _groups(session)
        assert len(groups) == 1
        gid = next(iter(groups))
        # group_id is the smallest person id (canonical anchor)
        assert gid == min(a.id, b.id)
    finally:
        session.close()


def test_fuzzy_pair_gets_fuzzy_reason(pg_session_factory):
    """Near-identical surnames (typo) are grouped with the fuzzy_name reason label."""
    session = pg_session_factory()
    try:
        _add_person(session, "passport:X1", "ana gil", city="Madrid")
        _add_person(session, "name:ana gill", "ana gill", city="Madrid")
        session.commit()

        run_reconciliation(session, similarity_threshold=0.6)

        reasons = [m.reason for m in session.query(DuplicateGroup).all()]
        assert reasons and all(r in _ALLOWED_REASONS for r in reasons)
        assert any(r == "fuzzy_name" for r in reasons)
    finally:
        session.close()


# ----------------------------------------------------------------------
# Non-duplicates and edge cases
# ----------------------------------------------------------------------


def test_distinct_names_no_group(pg_session_factory):
    session = pg_session_factory()
    try:
        # Different first-word blocks -> never compared; clearly not duplicates.
        _add_person(session, "passport:X1", "ana gil")
        _add_person(session, "name:pedro ramirez", "pedro ramirez")
        session.commit()

        assert run_reconciliation(session, similarity_threshold=0.85) == 0
    finally:
        session.close()


def test_high_threshold_rejects_weak_similarity(pg_session_factory):
    session = pg_session_factory()
    try:
        # Same block ("carlos") but the surnames are quite different.
        _add_person(session, "passport:X1", "carlos mendez")
        _add_person(session, "name:carlos villanueva", "carlos villanueva")
        session.commit()

        # A strict threshold should not group them.
        assert run_reconciliation(session, similarity_threshold=0.9) == 0
    finally:
        session.close()


def test_rebuild_clears_previous(pg_session_factory):
    session = pg_session_factory()
    try:
        session.add(DuplicateGroup(group_id=1, person_id=1, confidence=0.9, reason="stale"))
        session.commit()

        # No persons this run -> stale rows cleared, zero written.
        assert run_reconciliation(session) == 0
        assert session.query(DuplicateGroup).count() == 0
    finally:
        session.close()


def test_empty_warehouse(pg_session_factory):
    session = pg_session_factory()
    try:
        assert run_reconciliation(session) == 0
    finally:
        session.close()


# ----------------------------------------------------------------------
# Parity: the Python compute_norm_name MUST equal the SQL _NORM expression, char for
# char. If they diverge, streaming and batch would write different norm_name values and
# reconciliation would group incorrectly (architecture-reconcile.md risk).
# ----------------------------------------------------------------------


def test_norm_name_python_sql_parity(pg_session_factory):
    from hr_etl.processing.normalizer import compute_norm_name
    from hr_etl.processing.sql_norm import norm_sql

    samples = [
        "William Weiss",
        "  William   Weiss  ",
        "María López",
        "Dr Juan Perez",
        "Juan Perez MD",
        "Dr Juan Perez MD",
        "Sr. Octavio Ponce Gimenez",
        "JEAN LECLERCQ",
        "José Ángel Núñez",
    ]
    session = pg_session_factory()
    try:
        expr = norm_sql(":val")
        for raw in samples:
            sql_value = session.execute(text(f"SELECT {expr} AS n"), {"val": raw}).scalar_one()
            py_value = compute_norm_name(raw)
            assert (
                sql_value == py_value
            ), f"parity mismatch for {raw!r}: {sql_value!r} != {py_value!r}"
    finally:
        session.close()


def test_backfill_populates_norm_name_before_detection(pg_session_factory):
    """Rows inserted without norm_name (e.g. legacy/fixture) get it materialized by the
    reconciliation guard, so detection still works over norm_name."""
    session = pg_session_factory()
    try:
        # Insert with full_name only; norm_name left NULL on purpose.
        a = _add_person(session, "k:bf-1", "maria lopez")
        b = _add_person(session, "k:bf-2", "maria lopezz")
        session.commit()

        run_reconciliation(session, similarity_threshold=0.6)

        # norm_name now materialized for both.
        rows = {r.id: r.norm_name for r in session.query(PersonRow).all()}
        assert rows[a.id] == "maria lopez"
        assert rows[b.id] == "maria lopezz"
        groups = _groups(session)
        assert any({m.person_id for m in members} == {a.id, b.id} for members in groups.values())
    finally:
        session.close()


# ======================================================================
# QA gap-filling tests — added to close the T-1..T-13 / SEC-T1..T7 matrix.
# Same real-Postgres integration style as above (pg_trgm).
# ======================================================================

# Fixed set of allowed reason labels. Reconcile builds `reason` ONLY from these literals
# via a SQL CASE (see reconcile._DETECT_SQL). No name/passport/city value ever appears.
_ALLOWED_REASONS = {
    "exact_name + corroborated",
    "fuzzy_name",
    "name_containment + corroborated",
}


# ----------------------------------------------------------------------
# T-4 — typo/near-identical surnames group together. Covers the second
# canonical example from the spec ("maria martin" / "maria martins") in
# addition to the leclerc/leclercq case already tested above.
# ----------------------------------------------------------------------


def test_t4_typo_maria_martin_groups(pg_session_factory):
    session = pg_session_factory()
    try:
        a = _add_person(session, "passport:T4a", "maria martin")
        b = _add_person(session, "name:maria martins", "maria martins")
        session.commit()

        n = run_reconciliation(session, similarity_threshold=0.6)

        assert n >= 2
        groups = _groups(session)
        assert any({m.person_id for m in members} == {a.id, b.id} for members in groups.values())
    finally:
        session.close()


# ----------------------------------------------------------------------
# T-7 — titles (honorifics/suffixes) are stripped at BOTH ends by the same
# canonical normalization used in streaming, so "Dr Juan Perez" and
# "Juan Perez MD" collapse to the same norm_name and land in one group
# (exact_name after normalization). Complements the accents case (T-7).
# ----------------------------------------------------------------------


def test_t7_titles_stripped_group_together(pg_session_factory):
    session = pg_session_factory()
    try:
        # No passport (ambiguous) + shared phone (strong) corroborates -> exact_name.
        a = _add_person(session, "passport:T7a", "Dr Juan Perez", phone="600100200")
        b = _add_person(session, "name:juan perez md", "Juan Perez MD", phone="600100200")
        session.commit()

        n = run_reconciliation(session)  # strict default threshold: exact after normalize

        assert n >= 2
        groups = _groups(session)
        assert any({m.person_id for m in members} == {a.id, b.id} for members in groups.values())
        # Same normalized name -> the reason is the exact-name label.
        reasons = {m.reason for m in session.query(DuplicateGroup).all()}
        assert any(r.startswith("exact_name") for r in reasons)
    finally:
        session.close()


# ----------------------------------------------------------------------
# exact_name is now AMBIGUITY-gated: same normalized name only groups when at
# least one side lacks a passport AND a corroborating field matches. These two
# tests pin the new behavior (the fix for the 373k-homonym-group explosion).
# ----------------------------------------------------------------------


def test_exact_name_different_passports_do_not_group(pg_session_factory):
    """Two same-named people who BOTH have a (different) passport are provably distinct
    people — a shared name must not group them, even sharing a city."""
    session = pg_session_factory()
    try:
        _add_person(session, "k:dp-1", "juan ignacio", passport="AAA111", city="Madrid")
        _add_person(session, "k:dp-2", "juan ignacio", passport="BBB222", city="Madrid")
        session.commit()

        assert run_reconciliation(session) == 0
    finally:
        session.close()


def test_exact_name_without_corroboration_does_not_group(pg_session_factory):
    """Same name, one side without passport, but NO shared field (email/phone/city/
    address/company) -> not enough evidence, so they are not grouped."""
    session = pg_session_factory()
    try:
        _add_person(session, "k:nc-1", "juan ignacio", city="Madrid")  # no passport
        _add_person(session, "k:nc-2", "juan ignacio", city="Sevilla")  # no passport
        session.commit()

        assert run_reconciliation(session) == 0
    finally:
        session.close()


def test_exact_name_missing_passport_with_corroboration_groups(pg_session_factory):
    """The core ambiguous case: same name, one has a passport and the other does not, and
    they share a corroborating field (email) -> a plausible duplicate for human review."""
    session = pg_session_factory()
    try:
        a = _add_person(session, "k:mp-1", "juan ignacio", passport="AAA111", email="ji@x.com")
        b = _add_person(session, "k:mp-2", "juan ignacio", email="ji@x.com")  # no passport
        session.commit()

        n = run_reconciliation(session)

        assert n >= 2
        groups = _groups(session)
        assert any({m.person_id for m in members} == {a.id, b.id} for members in groups.values())
    finally:
        session.close()


# ----------------------------------------------------------------------
# T-8 — a single-word normalized name (only a given name, no surname) is too
# ambiguous to claim a duplicate and must NEVER form a group, even if the
# same word repeats. reconcile requires >= 2 tokens (_MIN_NAME_TOKENS).
# ----------------------------------------------------------------------


def test_t8_single_word_name_never_groups(pg_session_factory):
    session = pg_session_factory()
    try:
        # Same single word twice: without the >=2-token rule these would "match".
        _add_person(session, "passport:T8a", "madonna")
        _add_person(session, "name:madonna-2", "madonna")
        # A title + single word normalizes to a single word too -> still excluded.
        _add_person(session, "name:dr-madonna", "Dr Madonna")
        session.commit()

        assert run_reconciliation(session, similarity_threshold=0.6) == 0
        assert session.query(DuplicateGroup).count() == 0
    finally:
        session.close()


# ----------------------------------------------------------------------
# T-9 — a repeated EMAIL across otherwise-different people is generator
# noise (DEC-3/AC-10). Email is never a grouping signal: two different
# names sharing an email must NOT be grouped.
# ----------------------------------------------------------------------


def test_t9_repeated_email_does_not_group(pg_session_factory):
    session = pg_session_factory()
    try:
        # Same email, clearly different names (different first-word blocks).
        _add_person(session, "passport:T9a", "carlos gonzalez", email="cgonzalez@yahoo.com")
        _add_person(session, "name:pedro ramirez", "pedro ramirez", email="cgonzalez@yahoo.com")
        session.commit()

        assert run_reconciliation(session, similarity_threshold=0.85) == 0
    finally:
        session.close()


# ----------------------------------------------------------------------
# T-10 — idempotence: two consecutive runs over the same data yield the SAME
# groups (rebuild clears the previous set; no accumulation, no drift).
# ----------------------------------------------------------------------


def test_t10_idempotent_two_runs_same_groups(pg_session_factory):
    session = pg_session_factory()
    try:
        _add_person(session, "passport:T10a", "maria lopez")
        _add_person(session, "name:maria lopezz", "maria lopezz")
        session.commit()

        n1 = run_reconciliation(session, similarity_threshold=0.6)
        snapshot1 = {
            (m.group_id, m.person_id, m.reason) for m in session.query(DuplicateGroup).all()
        }

        n2 = run_reconciliation(session, similarity_threshold=0.6)
        snapshot2 = {
            (m.group_id, m.person_id, m.reason) for m in session.query(DuplicateGroup).all()
        }

        assert n1 == n2
        assert snapshot1 == snapshot2  # identical membership set, no duplication
    finally:
        session.close()


# ----------------------------------------------------------------------
# SEC-T1 — `reason` NEVER contains PII. After a run over seeded data, every
# reason value is one of the fixed labels; no name/passport/city/similarity
# digit leaks into it (security-reconcile.md DP-5).
# ----------------------------------------------------------------------


def test_sec_t1_reason_has_no_pii_only_fixed_labels(pg_session_factory):
    session = pg_session_factory()
    try:
        # A mix that exercises several reason branches.
        _add_person(session, "passport:S1a", "jean leclerc", city="Paris")
        _add_person(session, "name:jean leclercq", "jean leclercq", city="Paris")
        _add_person(session, "passport:S1c", "octavio ponce", city="Madrid")
        _add_person(session, "name:opg", "octavio ponce gimenez", city="Madrid")
        session.commit()

        run_reconciliation(session, similarity_threshold=0.6)

        reasons = {m.reason for m in session.query(DuplicateGroup).all()}
        assert reasons, "expected at least one group for this fixture"
        # Every reason is a fixed label — nothing else.
        assert (
            reasons <= _ALLOWED_REASONS
        ), f"unexpected reason label(s): {reasons - _ALLOWED_REASONS}"
        # And explicitly: no seeded PII value appears inside any reason string.
        pii_values = ["leclerc", "leclercq", "octavio", "ponce", "gimenez", "paris", "madrid"]
        for r in reasons:
            low = r.lower()
            for pii in pii_values:
                assert pii not in low, f"PII {pii!r} leaked into reason {r!r}"
            # no similarity digits either
            assert not any(ch.isdigit() for ch in r), f"digit leaked into reason {r!r}"
    finally:
        session.close()


# ----------------------------------------------------------------------
# SEC-T2 — the duplicate_groups schema exposes NO PII columns: only the
# numeric FK + label + confidence + timestamps (security-reconcile.md DP-1).
# ----------------------------------------------------------------------


def test_sec_t2_duplicate_groups_schema_has_no_pii_columns(pg_session_factory):
    # Force schema creation.
    _ = pg_session_factory()
    engine = create_db_engine(DSN)
    try:
        with engine.connect() as conn:
            cols = {
                r[0]
                for r in conn.execute(
                    text(
                        "SELECT column_name FROM information_schema.columns "
                        "WHERE table_name = 'duplicate_groups'"
                    )
                ).all()
            }
        assert cols == {"id", "group_id", "person_id", "confidence", "reason", "created_at"}
        # Belt-and-braces: none of the PII field names is present as a column.
        pii_columns = {
            "passport",
            "iban",
            "salary",
            "email",
            "phone",
            "ipv4",
            "full_name",
            "name",
            "lastname",
            "city",
            "address",
            "company",
        }
        assert cols.isdisjoint(pii_columns)
    finally:
        engine.dispose()


# ----------------------------------------------------------------------
# SEC-T3 — the reconciliation job logs only aggregates (counts/duration),
# never PII. Capture the logger output during a run and assert no seeded
# name/passport/city value appears (security-reconcile.md DP-4).
# ----------------------------------------------------------------------


def test_sec_t3_logs_contain_no_pii(pg_session_factory, caplog):
    import logging

    session = pg_session_factory()
    try:
        _add_person(session, "passport:S3a", "jean leclerc", city="Paris")
        _add_person(session, "name:jean leclercq", "jean leclercq", city="Paris")
        session.commit()

        with caplog.at_level(logging.DEBUG):
            run_reconciliation(session, similarity_threshold=0.6)

        blob = "\n".join(rec.getMessage() for rec in caplog.records).lower()
        for pii in ("leclerc", "leclercq", "jean", "paris", "passport:s3a"):
            assert pii not in blob, f"PII {pii!r} leaked into logs"
        # It SHOULD, however, log the operational summary (proves logging happened).
        assert "reconciliation complete" in blob
    finally:
        session.close()
