"""Idempotent SQL migration runner for the warehouse.

Applies the schema changes that ``create_all`` cannot express: the ``pg_trgm`` extension,
the ``norm_name`` column on a pre-existing ``persons`` table, its btree + GIN trigram
indexes, and the historical ``norm_name`` backfill.

The migration SQL is EMBEDDED here as a constant (not read from a ``.sql`` file on disk).
Reason: the app is ``pip install``-ed into a venv, and pip does not package non-Python
files, so a ``migrations/*.sql`` file is absent from the installed package (it only
exists in the mounted source tree). Embedding the SQL guarantees it always ships with the
importable code. It is written to be safe to run repeatedly (``IF NOT EXISTS`` / guarded
``UPDATE``), so this runner does not track applied versions — running it again is a no-op.

On a non-Postgres backend (SQLite in unit tests) the pg_trgm/GIN statements are not valid;
the runner detects sqlite/other engines and skips, leaving those paths to the Postgres
integration tests.

The backfill expression MUST stay in sync (character-for-character) with:
  * ``processing/normalizer.py :: compute_norm_name`` (streaming writer)
  * ``processing/sql_norm.py   :: norm_sql``           (batch jobs)
"""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from hr_etl.logging_conf import get_logger
from hr_etl.processing.sql_norm import norm_sql

logger = get_logger(__name__)

# Each element is one statement, applied in order inside a single transaction. Embedding
# them (instead of parsing a .sql file) avoids the pip-packaging pitfall entirely and
# removes the need for a comment-stripping SQL splitter.
_MIGRATION_STATEMENTS: tuple[str, ...] = (
    "CREATE EXTENSION IF NOT EXISTS pg_trgm",
    # create_all() never alters a pre-existing table (the 2.2M-row prod persons), so the
    # column must be added explicitly. Idempotent.
    "ALTER TABLE persons ADD COLUMN IF NOT EXISTS norm_name VARCHAR(255)",
    # btree: final join persons.norm_name = catalog.norm and the GROUP BY.
    "CREATE INDEX IF NOT EXISTS ix_persons_norm_name ON persons (norm_name)",
    # GIN trigram: the `%` operator / similarity() in the fuzzy blocking.
    "CREATE INDEX IF NOT EXISTS ix_persons_norm_name_trgm "
    "ON persons USING gin (norm_name gin_trgm_ops)",
    # Backfill historical rows. Same expression as compute_norm_name / norm_sql, so
    # streaming and batch agree. Guarded => idempotent (skips already-populated rows).
    f"""
    UPDATE persons
    SET norm_name = {norm_sql("full_name")}
    WHERE full_name IS NOT NULL
      AND (norm_name IS NULL OR norm_name = '')
    """,
    "ANALYZE persons",
)


def run_migrations(engine: Engine) -> int:
    """Apply the embedded migration idempotently. Returns the number of statements run.

    Skipped on non-Postgres engines (the SQL uses Postgres-only features). Errors are
    logged (type/message only, no PII — the migration carries no data values) and
    re-raised so a broken migration surfaces at startup rather than silently.
    """
    if engine.dialect.name != "postgresql":
        logger.info("migrations skipped: backend is %s (not postgresql)", engine.dialect.name)
        return 0

    try:
        with engine.begin() as conn:
            for statement in _MIGRATION_STATEMENTS:
                conn.execute(text(statement))
    except SQLAlchemyError as exc:
        logger.error("migration failed: %s", exc)
        raise

    logger.info("migration applied: %d statements", len(_MIGRATION_STATEMENTS))
    return len(_MIGRATION_STATEMENTS)


def main() -> None:
    """CLI entrypoint: apply migrations. Run with ``python -m hr_etl.warehouse.migrate``."""
    from hr_etl.config import get_settings
    from hr_etl.logging_conf import configure_logging
    from hr_etl.warehouse.engine import create_db_engine

    settings = get_settings()
    configure_logging(settings.log_level)

    engine = create_db_engine(settings.postgres_dsn)
    try:
        applied = run_migrations(engine)
        print(f"Migrations applied: {applied} statements.")
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
