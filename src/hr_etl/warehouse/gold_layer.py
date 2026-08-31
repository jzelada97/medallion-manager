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

logger = get_logger(__name__)

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

# Gold membership predicate (DEC-5): the 5 business-critical fields present AND at least
# 7 of 8 data fields filled (>= 80%). Applied to the Silver `persons` table on rebuild.
_GOLD_PREDICATE = (
    "full_name IS NOT NULL AND passport IS NOT NULL AND email IS NOT NULL "
    "AND city IS NOT NULL AND company IS NOT NULL "
    f"AND ({_FILLED_COUNT}) >= 7"
)

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
FROM persons
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
    """
    with engine.begin() as conn:
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
