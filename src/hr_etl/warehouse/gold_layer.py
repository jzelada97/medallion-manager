"""Gold layer: materialized views and aggregates for fast querying.

The Medallion Architecture organizes data in three layers:
- Bronze: raw messages as received from Kafka (MongoDB, untouched)
- Silver: normalized, consolidated person records (Postgres `persons` table)
- Gold: pre-computed aggregates and curated views optimized for the API/frontend

This module manages the Gold layer — creating and refreshing summary tables that
avoid expensive queries on the Silver layer for every API request.
"""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.engine import Engine

from hr_etl.logging_conf import get_logger
from hr_etl.processing.sql_norm import norm_sql

logger = get_logger(__name__)

# Guarded norm_name backfill (same canonical expression as streaming/reconcile). Fills only
# rows that have a full_name but no norm_name yet; a no-op once populated.
_BACKFILL_NORM_SQL = (
    f"UPDATE persons SET norm_name = {norm_sql('full_name')} "
    "WHERE full_name IS NOT NULL AND (norm_name IS NULL OR norm_name = '');"
)

# Ensure the human-review table exists before the Gold predicate references it. Normally
# created by init_schema (ORM create_all), but refresh_gold is also called directly (tests,
# ad-hoc refreshes) — this idempotent DDL keeps it self-sufficient. Portable to Postgres
# and SQLite (both accept CREATE TABLE IF NOT EXISTS + these column types).
_ENSURE_REVIEWS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS person_reviews (
    id INTEGER PRIMARY KEY,
    match_key VARCHAR(255) UNIQUE,
    status VARCHAR(32),
    survivor_match_key VARCHAR(255),
    note VARCHAR(255),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
"""

# SQL statements to create Gold views/tables.
# Using materialized-style approach: real tables that get TRUNCATE + INSERT on refresh.

_CREATE_GOLD_TABLES = """
-- Gold: summary statistics
CREATE TABLE IF NOT EXISTS gold_stats (
    id INTEGER PRIMARY KEY DEFAULT 1,
    total_persons INTEGER NOT NULL DEFAULT 0,
    with_passport INTEGER NOT NULL DEFAULT 0,
    with_city INTEGER NOT NULL DEFAULT 0,
    with_company INTEGER NOT NULL DEFAULT 0,
    with_bank INTEGER NOT NULL DEFAULT 0,
    with_ipv4 INTEGER NOT NULL DEFAULT 0,
    cross_linked INTEGER NOT NULL DEFAULT 0,
    avg_completeness FLOAT NOT NULL DEFAULT 0.0,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Gold: top cities
CREATE TABLE IF NOT EXISTS gold_top_cities (
    city VARCHAR(128) PRIMARY KEY,
    person_count INTEGER NOT NULL DEFAULT 0
);

-- Gold: top companies
CREATE TABLE IF NOT EXISTS gold_top_companies (
    company VARCHAR(255) PRIMARY KEY,
    person_count INTEGER NOT NULL DEFAULT 0
);

-- Gold: completeness distribution (how many fields are filled per person)
CREATE TABLE IF NOT EXISTS gold_completeness (
    fields_filled INTEGER PRIMARY KEY,
    person_count INTEGER NOT NULL DEFAULT 0
);
"""

# The 8 data fields tracked for completeness (mirrors the model docstring and the
# original gold_layer). completeness = non-null count / 8; >= 0.80 means >= 7 of 8.
_COMPLETENESS_FIELDS = (
    "passport",
    "full_name",
    "city",
    "company",
    "iban",
    "email",
    "phone",
    "ipv4",
)

# Sum-of-non-nulls expression over the 8 completeness fields (0..8).
_FILLED_COUNT = " + ".join(
    f"(CASE WHEN {f} IS NOT NULL THEN 1 ELSE 0 END)" for f in _COMPLETENESS_FIELDS
)

# Gold membership predicate (DEC-5 + name-uniqueness gate):
#   * the 5 business-critical fields present AND >= 7 of 8 data fields filled (>= 80%), AND
#   * the person's ``norm_name`` is UNIQUE across the whole Silver ``persons`` table.
#
# The second clause is the key rule (per the spec: "a Gold record must not have a repeated
# name"). It is checked DIRECTLY against Silver — NOT via ``duplicate_groups`` — on purpose:
# reconcile prunes common name-bases with a frequency guard (to keep the review pane small),
# so ``duplicate_groups`` does NOT list every repeated name. Gating Gold on it would let
# common repeated names ("juan ignacio" x thousands) slip into Gold. Checking uniqueness
# straight against ``persons`` is the correct, guard-independent guarantee: if a norm_name
# appears on more than one Silver row, none of them are Gold — we never risk promoting a
# row whose five fields might actually belong to two different people. Independent of
# reconcile, so it holds regardless of the maintenance DAG order.
#
# The uniqueness test uses a correlated NOT EXISTS (anti-join), backed by the btree index
# ``ix_persons_norm_name``. (An earlier `id NOT IN (SELECT ...)` variant against
# duplicate_groups planned as a per-row SubPlan over 859k rows — cost ~1.1e10, > 8 min;
# NOT EXISTS plans as a Hash Anti Join.) `persons` is aliased `p` in the rebuild.
#
# HUMAN-REVIEW OVERRIDE (person_reviews, keyed by the stable match_key):
#   * ``approved`` — a reviewer confirmed this exact row is the canonical person; it is
#     force-promoted to Gold even if its name repeats (the automatic uniqueness test is
#     bypassed for it). This is the "I checked it, it's the good one" verdict.
#   * ``distinct`` — a reviewer confirmed a row is a DIFFERENT real person that merely
#     shares a name (a legitimate homonym). Such rows are EXCLUDED from the collision
#     count, so they no longer block their same-name peers from Gold. A ``distinct`` row
#     is not itself auto-promoted (it still shares a name), only stops being an obstacle.
# The override joins person_reviews by match_key (its stable business key), so decisions
# survive a full reprocess even though persons.id churns.
_APPROVED_PREDICATE = (
    "EXISTS (SELECT 1 FROM person_reviews r "
    "        WHERE r.match_key = p.match_key AND r.status = 'approved')"
)

# Automatic bar: five business fields + >= 7/8 completeness + a unique norm_name. The
# uniqueness anti-join ignores peers that a human marked 'distinct' (legitimate homonyms),
# so resolving one member of a same-name pair can free the other for Gold.
_AUTO_PREDICATE = (
    "p.full_name IS NOT NULL AND p.passport IS NOT NULL AND p.email IS NOT NULL "
    "AND p.city IS NOT NULL AND p.company IS NOT NULL "
    f"AND ({_FILLED_COUNT}) >= 7 "
    "AND p.norm_name IS NOT NULL "
    "AND NOT EXISTS (SELECT 1 FROM persons p2 "
    "                WHERE p2.norm_name = p.norm_name AND p2.id <> p.id "
    "                  AND NOT EXISTS (SELECT 1 FROM person_reviews r2 "
    "                                  WHERE r2.match_key = p2.match_key "
    "                                    AND r2.status = 'distinct'))"
)

# A row is Gold if a human approved it OR it clears the automatic bar. The approved branch
# wins regardless of name repetition; the auto branch keeps the strict uniqueness guarantee.
_GOLD_PREDICATE = f"(({_APPROVED_PREDICATE}) OR ({_AUTO_PREDICATE}))"

# Rebuild gold_persons: only records that clear the Gold bar. Full DELETE + INSERT so it
# is idempotent (a re-run reproduces the same set). completeness stored for transparency.
_REBUILD_GOLD_PERSONS = f"""
DELETE FROM gold_persons;
INSERT INTO gold_persons (
    id, match_key, passport, full_name, name, lastname, sex, phone, email, city, address,
    company, company_address, company_phone, company_email, job, iban, salary, ipv4,
    completeness, created_at
)
SELECT
    id, match_key, passport, full_name, name, lastname, sex, phone, email, city, address,
    company, company_address, company_phone, company_email, job, iban, salary, ipv4,
    ({_FILLED_COUNT})::FLOAT / 8.0 AS completeness,
    NOW()
FROM persons p
WHERE {_GOLD_PREDICATE};
"""

# gold_* stats are now computed OVER gold_persons (the curated subset), not all of Silver.
_REFRESH_STATS = """
DELETE FROM gold_stats;
INSERT INTO gold_stats (id, total_persons, with_passport, with_city, with_company, with_bank, with_ipv4, cross_linked, avg_completeness)
SELECT
    1,
    COUNT(*),
    COUNT(passport),
    COUNT(city),
    COUNT(company),
    COUNT(iban),
    COUNT(ipv4),
    COUNT(CASE WHEN passport IS NOT NULL AND city IS NOT NULL THEN 1 END),
    -- COALESCE guards the empty-table case: AVG over zero rows returns NULL, but
    -- gold_stats.avg_completeness is NOT NULL. With no Gold persons, completeness is 0.
    -- completeness is already stored per row, so average it directly (over 8 fields).
    COALESCE(AVG(completeness) * 8.0, 0)::FLOAT
FROM gold_persons;
"""

_REFRESH_TOP_CITIES = """
DELETE FROM gold_top_cities;
INSERT INTO gold_top_cities (city, person_count)
SELECT city, COUNT(*) as n
FROM gold_persons
WHERE city IS NOT NULL
GROUP BY city
ORDER BY n DESC
LIMIT 50;
"""

_REFRESH_TOP_COMPANIES = """
DELETE FROM gold_top_companies;
INSERT INTO gold_top_companies (company, person_count)
SELECT company, COUNT(*) as n
FROM gold_persons
WHERE company IS NOT NULL
GROUP BY company
ORDER BY n DESC
LIMIT 50;
"""

_REFRESH_COMPLETENESS = """
DELETE FROM gold_completeness;
INSERT INTO gold_completeness (fields_filled, person_count)
SELECT
    (CASE WHEN passport IS NOT NULL THEN 1 ELSE 0 END) +
    (CASE WHEN full_name IS NOT NULL THEN 1 ELSE 0 END) +
    (CASE WHEN city IS NOT NULL THEN 1 ELSE 0 END) +
    (CASE WHEN company IS NOT NULL THEN 1 ELSE 0 END) +
    (CASE WHEN iban IS NOT NULL THEN 1 ELSE 0 END) +
    (CASE WHEN email IS NOT NULL THEN 1 ELSE 0 END) +
    (CASE WHEN phone IS NOT NULL THEN 1 ELSE 0 END) +
    (CASE WHEN ipv4 IS NOT NULL THEN 1 ELSE 0 END) as fields_filled,
    COUNT(*) as person_count
FROM gold_persons
GROUP BY 1
ORDER BY 1;
"""


def init_gold_schema(engine: Engine) -> None:
    """Create Gold layer tables if they don't exist.

    The ``gold_persons`` table comes from the ORM (``init_schema``); the aggregate
    ``gold_*`` tables are created here (raw DDL) so the Gold refresh has its targets.
    """
    with engine.begin() as conn:
        conn.execute(text(_CREATE_GOLD_TABLES))
    logger.info("gold layer schema initialized")


def refresh_gold(engine: Engine) -> int:
    """Rebuild the Gold layer: gold_persons subset + gold_* stats over that subset.

    Order matters: gold_persons is rebuilt FIRST (the curated subset of Silver that
    clears the completeness bar), then every gold_* stats table is recomputed FROM
    gold_persons — so "Gold" reflects quality, not raw Silver volume (DEC-5).

    Fully idempotent (DELETE + INSERT). Returns the number of Gold persons for metrics.
    Designed to be called periodically or after a batch of consolidations.

    The name-uniqueness gate needs ``norm_name`` populated; it is normally materialized by
    streaming + reconcile, but we run a guarded backfill first so refresh_gold is correct
    on its own (e.g. right after a fresh load, or in tests) and never silently drops a row
    just because its norm_name was not yet computed. WHERE-indexed no-op once populated.

    Human review overrides (person_reviews): an ``approved`` row is force-promoted even if
    its name repeats; a ``distinct`` peer is ignored by the uniqueness anti-join so it no
    longer blocks its same-name twin. The review table is ensured to exist first so this
    stays self-sufficient outside the full init_schema path.
    """
    with engine.begin() as conn:
        conn.execute(text(_ENSURE_REVIEWS_TABLE_SQL))
        conn.execute(text(_BACKFILL_NORM_SQL))
        conn.execute(text(_REBUILD_GOLD_PERSONS))
        conn.execute(text(_REFRESH_STATS))
        conn.execute(text(_REFRESH_TOP_CITIES))
        conn.execute(text(_REFRESH_TOP_COMPANIES))
        conn.execute(text(_REFRESH_COMPLETENESS))
        gold_persons = conn.execute(text("SELECT count(*) FROM gold_persons")).scalar_one()
    logger.info("gold layer refreshed: gold_persons=%d", int(gold_persons))
    try:
        from hr_etl.metrics.prometheus import GOLD_PERSONS

        GOLD_PERSONS.set(int(gold_persons))
    except Exception:  # pragma: no cover - metrics best-effort, never fail the refresh
        pass
    return int(gold_persons)


def main() -> None:
    """CLI entrypoint: refresh the Gold layer."""
    from hr_etl.config import get_settings
    from hr_etl.logging_conf import configure_logging
    from hr_etl.warehouse.engine import create_db_engine, init_schema

    settings = get_settings()
    configure_logging(settings.log_level)

    engine = create_db_engine(settings.postgres_dsn)
    init_schema(engine)  # ensures Silver tables exist
    init_gold_schema(engine)
    refresh_gold(engine)

    # Print summary
    with engine.connect() as conn:
        row = conn.execute(text("SELECT * FROM gold_stats WHERE id = 1")).fetchone()
        if row:
            print("Gold layer refreshed:")
            print(f"  Total persons: {row.total_persons}")
            print(f"  With passport: {row.with_passport}")
            print(f"  Cross-linked:  {row.cross_linked}")
            print(f"  Avg completeness: {row.avg_completeness:.2f} / 8 fields")

    engine.dispose()


if __name__ == "__main__":
    main()
