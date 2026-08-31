"""Batch consolidation fix (Silver): merge person rows that are the SAME individual.

The streaming pipeline sometimes writes the same person as 2+ rows in ``persons`` (a
Personal fragment and a Location/Professional fragment that ended up with different
``match_key`` values). Symptom in the real data: a ``passport`` shared by two rows whose
names are essentially the same ("William Weiss" / "william weiss") — complementary
fragments of one person the streaming did not join.

This job merges those rows. Scope and rules (DEC-2, confirmed with the user):

* **Only merge same ``passport`` AND very similar ``norm_name``** (trigram similarity
  ``>= _MERGE_THRESHOLD``). The generator can also assign the same passport to DIFFERENT
  people (collision/noise), so passport alone is NOT enough — a same-passport pair with
  clearly different names is left untouched (AC-2).
* **Survivor = ``min(id)``** (stable anchor).
* **Survivorship (which value wins):** first non-null wins (complementary fragments);
  ``full_name`` = the LONGEST (most complete); ``created_at`` = oldest; ``updated_at`` =
  newest. Never overwrites a good value with NULL (C-6).
* **Recompute ``norm_name``** of the survivor from the winning ``full_name`` (same SQL
  ``norm_sql`` the rest of the subsystem uses).
* **Losers are deleted** after their data is folded into the survivor.

Everything runs set-based in Postgres in ONE transaction. Only ``count(*)`` values come
back to Python (RAM-flat, NFR-1). Idempotent: a second run finds no new pairs (NFR-3).
email/phone/iban are NEVER used to merge (DEC-3/AC-10) — they are generator noise / a
unique id, not a person identity signal.

Run: python -m hr_etl.processing.consolidate_merge
"""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from hr_etl.logging_conf import get_logger
from hr_etl.processing.sql_norm import norm_sql

logger = get_logger(__name__)

# Similarity cutoff to decide "same person split in two" vs "generator passport
# collision". High on purpose: only near-identical names within a shared passport merge.
_MERGE_THRESHOLD = 0.85

# Anti-hang safety net (not a result cap): if it fires, the whole transaction rolls back
# and nothing is written (C-5). Casting to int closes any interpolation vector (IV-2).
_STATEMENT_TIMEOUT_MS = 600_000  # 10 minutes

# Person columns filled by first-non-null survivorship (full_name handled separately —
# it takes the longest; created_at/updated_at take oldest/newest). match_key and
# norm_name are handled explicitly. iban/email/phone are still merged as data (they are
# valid person attributes) — they are only excluded as MATCHING signals, not as columns.
_COALESCE_FIELDS = (
    "passport",
    "name",
    "lastname",
    "sex",
    "phone",
    "email",
    "city",
    "address",
    "company",
    "company_address",
    "company_phone",
    "company_email",
    "job",
    "iban",
    "salary",
    "ipv4",
)


def _ensure_pg_trgm(session: Session) -> bool:
    """Enable pg_trgm (idempotent). Returns False if unavailable (non-Postgres / no priv)."""
    try:
        session.execute(text("CREATE EXTENSION IF NOT EXISTS pg_trgm"))
        session.commit()
        return True
    except SQLAlchemyError as exc:
        session.rollback()
        logger.warning("could not enable pg_trgm; skipping consolidation: %s", exc)
        return False


def _build_links_sql() -> str:
    """SQL that builds candidate merge links within each repeated passport."""
    return """
        DROP TABLE IF EXISTS _cm_dup_pass;
        CREATE TEMP TABLE _cm_dup_pass AS
            SELECT passport
            FROM persons
            WHERE passport IS NOT NULL
            GROUP BY passport
            HAVING count(*) > 1;
        CREATE INDEX _cm_dup_pass_idx ON _cm_dup_pass (passport);

        DROP TABLE IF EXISTS _cm_links;
        CREATE TEMP TABLE _cm_links AS
            SELECT a.id AS id_a, b.id AS id_b
            FROM persons a
            JOIN persons b
              ON a.passport = b.passport
             AND a.id < b.id
             AND a.norm_name IS NOT NULL
             AND b.norm_name IS NOT NULL
            JOIN _cm_dup_pass d ON d.passport = a.passport
            WHERE similarity(a.norm_name, b.norm_name) >= :merge_threshold;
    """


# Resolve each row to the ROOT of its merge component (transitive min id), not just its
# nearest linked id. This matters because losers are DELETED: with a chain 1-2, 2-3 a
# naive "min directly-linked id" would send row 3 to survivor 2, but row 2 is itself a
# loser of 1 and gets deleted -> row 3's data would be lost. A short recursive CTE walks
# the (undirected) link graph to the component's minimum id. Components are tiny here
# (rows sharing one passport, typically 2-3), so the recursion is cheap and safe — unlike
# in reconcile, where we only GROUP (never delete) and anchoring imperfectly is tolerable.
_BUILD_SURVIVOR_SQL = text(
    """
    DROP TABLE IF EXISTS _cm_edges;
    CREATE TEMP TABLE _cm_edges AS
        SELECT id_a AS src, id_b AS dst FROM _cm_links
        UNION ALL
        SELECT id_b AS src, id_a AS dst FROM _cm_links;
    CREATE INDEX _cm_edges_src ON _cm_edges (src);

    -- Enumerate every (node, reachable_node) pair by walking the undirected link graph,
    -- then the component root of a node = the minimum reachable node id. Components are
    -- tiny (rows sharing one passport), so the closure is cheap. This gives a correct,
    -- transitive survivor for chains (1-2, 2-3 => root 1 for all three).
    DROP TABLE IF EXISTS _cm_reach;
    CREATE TEMP TABLE _cm_reach AS
    WITH RECURSIVE reach(node, reachable) AS (
        SELECT src, src FROM _cm_edges
        UNION
        SELECT r.node, e.dst
        FROM reach r
        JOIN _cm_edges e ON e.src = r.reachable
    )
    SELECT node, min(reachable) AS root_id
    FROM reach
    GROUP BY node;

    DROP TABLE IF EXISTS _cm_survivor;
    CREATE TEMP TABLE _cm_survivor AS
        SELECT node AS loser_id, root_id AS survivor_id
        FROM _cm_reach
        WHERE root_id < node;   -- only true losers; the root maps to itself and is skipped
    CREATE INDEX _cm_survivor_surv ON _cm_survivor (survivor_id);
    CREATE INDEX _cm_survivor_lose ON _cm_survivor (loser_id);
    """
)


def _build_update_sql() -> str:
    """Survivorship UPDATE: fold each survivor + its losers into the survivor row."""
    # First-non-null per field via array_agg ordered by NULLs last (survivor is included
    # in the group, so its own value participates too). full_name = longest.
    coalesce_selects = ",\n           ".join(
        f"(array_agg(p.{f} ORDER BY (p.{f} IS NULL)))[1] AS {f}" for f in _COALESCE_FIELDS
    )
    coalesce_updates = ",\n            ".join(
        f"{f} = COALESCE(a.{f}, t.{f})" for f in _COALESCE_FIELDS
    )
    # norm_name recomputed from the winning full_name using the canonical expression.
    norm_expr = norm_sql("a.full_name")
    return f"""
        DROP TABLE IF EXISTS _cm_agg;
        CREATE TEMP TABLE _cm_agg AS
            SELECT s.survivor_id,
                   (array_agg(p.full_name ORDER BY length(p.full_name) DESC NULLS LAST))[1]
                       AS full_name,
                   {coalesce_selects},
                   min(p.created_at) AS created_at,
                   max(p.updated_at) AS updated_at
            FROM (
                SELECT DISTINCT survivor_id FROM _cm_survivor
            ) su
            JOIN _cm_survivor s ON s.survivor_id = su.survivor_id
            JOIN persons p
              ON p.id = s.survivor_id OR p.id = s.loser_id
            GROUP BY s.survivor_id;
        CREATE INDEX _cm_agg_idx ON _cm_agg (survivor_id);

        UPDATE persons t
        SET full_name = COALESCE(a.full_name, t.full_name),
            {coalesce_updates},
            created_at = LEAST(t.created_at, a.created_at),
            updated_at = GREATEST(t.updated_at, a.updated_at),
            norm_name = {norm_expr.replace("a.full_name", "COALESCE(a.full_name, t.full_name)")}
        FROM _cm_agg a
        WHERE t.id = a.survivor_id;
    """


_DELETE_LOSERS_SQL = text("DELETE FROM persons WHERE id IN (SELECT loser_id FROM _cm_survivor);")

_COUNT_LOSERS_SQL = text("SELECT count(*) FROM _cm_survivor;")


def run_consolidation(session: Session, merge_threshold: float = _MERGE_THRESHOLD) -> int:
    """Merge same-person rows (same passport + similar name). Returns rows merged away.

    All work runs in Postgres in a single transaction; only the loser count returns to
    Python. Idempotent and memory-flat. If pg_trgm is unavailable the job is a no-op.
    """
    if not _ensure_pg_trgm(session):
        return 0

    try:
        session.execute(text(f"SET statement_timeout = {int(_STATEMENT_TIMEOUT_MS)}"))
        threshold = float(merge_threshold)

        session.execute(text(_build_links_sql()), {"merge_threshold": threshold})
        session.execute(_BUILD_SURVIVOR_SQL)

        merged = session.execute(_COUNT_LOSERS_SQL).scalar_one()
        if merged:
            session.execute(text(_build_update_sql()))
            session.execute(_DELETE_LOSERS_SQL)
        session.commit()
    except SQLAlchemyError as exc:
        session.rollback()
        # Log the DB error type/message only — never the working rows (DP-4, no PII).
        logger.error("consolidation query failed: %s", exc)
        return 0

    merged = int(merged)
    logger.info("consolidation done: merged=%d rows", merged)
    try:
        from hr_etl.metrics.prometheus import CONSOLIDATION_MERGED_ROWS

        CONSOLIDATION_MERGED_ROWS.inc(merged)
    except Exception:  # pragma: no cover - metrics are best-effort, never fail the job
        pass
    return merged


def main() -> None:
    """CLI entrypoint for the consolidation fix job."""
    from hr_etl.config import get_settings
    from hr_etl.logging_conf import configure_logging
    from hr_etl.warehouse.engine import create_db_engine, init_schema, make_session_factory

    settings = get_settings()
    configure_logging(settings.log_level)

    engine = create_db_engine(settings.postgres_dsn)
    init_schema(engine)
    session_factory = make_session_factory(engine)
    session = session_factory()

    try:
        merged = run_consolidation(session)
        print(f"Consolidation done: {merged} rows merged away.")
    finally:
        session.close()
        engine.dispose()


if __name__ == "__main__":
    main()
