"""Idempotent SQL migration runner for the warehouse.

Applies the raw ``.sql`` files under ``warehouse/migrations`` in filename order. Each
migration is written to be safe to run repeatedly (``CREATE ... IF NOT EXISTS``, guarded
``UPDATE``), so this runner does not track applied versions — running it again is a
no-op. It is wired into ``init_schema`` so both the CLIs and the app arrive at the same
schema (extension + GIN index + norm_name backfill) that ``create_all`` cannot express.

On a non-Postgres backend (SQLite in unit tests) the pg_trgm/GIN statements are not
valid; the runner detects sqlite engines and skips, leaving those paths to the Postgres
integration tests.
"""

from __future__ import annotations

from pathlib import Path

from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from hr_etl.logging_conf import get_logger

logger = get_logger(__name__)

_MIGRATIONS_DIR = Path(__file__).parent / "migrations"


def _strip_line_comments(sql: str) -> str:
    """Remove ``--`` line comments so ``;`` inside a comment never splits a statement.

    The migration files use only whole-line ``--`` comments (no inline ``--`` after
    code and no string literals containing ``--``), so dropping any line whose first
    non-space characters are ``--`` is safe and keeps the splitter dependency-free.
    """
    return "\n".join(line for line in sql.splitlines() if not line.lstrip().startswith("--"))


def _split_statements(sql: str) -> list[str]:
    """Split a SQL script into individual statements on ``;`` boundaries.

    Comments are stripped first so a semicolon inside a comment cannot split a
    statement. The migration files use only simple statements (no functions/DO blocks
    with inner semicolons), so splitting on ``;`` is then sufficient.
    """
    code = _strip_line_comments(sql)
    parts = [s.strip() for s in code.split(";")]
    return [s for s in parts if s]


def run_migrations(engine: Engine) -> int:
    """Apply all ``.sql`` migrations idempotently. Returns the number applied.

    Skipped on non-Postgres engines (the SQL uses Postgres-only features). Errors are
    logged (type/message only, no PII — migrations carry no data values) and re-raised
    so a broken migration surfaces at startup rather than silently.
    """
    if engine.dialect.name != "postgresql":
        logger.info("migrations skipped: backend is %s (not postgresql)", engine.dialect.name)
        return 0

    files = sorted(_MIGRATIONS_DIR.glob("*.sql"))
    applied = 0
    for path in files:
        sql = path.read_text(encoding="utf-8")
        statements = _split_statements(sql)
        try:
            with engine.begin() as conn:
                for statement in statements:
                    conn.execute(text(statement))
            applied += 1
            logger.info("migration applied: %s (%d statements)", path.name, len(statements))
        except SQLAlchemyError as exc:
            logger.error("migration failed: %s: %s", path.name, exc)
            raise
    return applied
