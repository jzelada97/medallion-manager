"""Batch reconciliation job: group probable-duplicate persons in the warehouse.

Detects person records that likely refer to the same individual and stores them as
**groups** (not binary pairs) in the ``duplicate_groups`` table for review — never
auto-merged.

Design decisions (read this — it explains the trade-offs):

* **All detection runs in SQL (Postgres), not in Python.** Pulling millions of rows
  into Python to compare them exhausts RAM (it OOM'd the demo VM once). The heavy work
  — normalization, fuzzy matching, grouping — happens inside Postgres; only the final
  membership rows come back.

* **Fuzzy name matching with ``pg_trgm``.** Exact-name matching misses real duplicates
  like "leclerc" vs "leclercq" (one letter) or "martinez" vs "martenez" (letter swap).
  The ``pg_trgm`` extension scores string similarity (0..1) and is index-backed, so we
  can catch these at scale. Threshold ``_SIMILARITY_THRESHOLD`` (0.85) is strict to
  keep false positives low.

* **Blocking by first name word.** Comparing every name against every other is O(n^2)
  and unfeasible on millions of rows. We only compare names that share their first word
  (``split_part(norm,' ',1)``), which is cheap to filter and captures the realistic
  cases (same given name, surname typo/extra surname). This is the standard "blocking"
  technique in entity resolution.

* **Grouping by a canonical anchor, NOT full connected-components clustering.**
  Similarity gives us pairs (A~B, B~C). Turning pairs into true groups means finding
  *connected components* of a graph (so A and C land together via B even if A and C
  aren't directly similar). The two textbook ways to do that are:
    - **Union-Find (Disjoint Set Union)**: elegant and near-linear, but it's an
      imperative, stateful algorithm — a poor fit for set-based SQL, and doing it in
      Python would mean loading the whole graph into memory (the very thing we're
      avoiding for scale/RAM reasons).
    - **Recursive CTE**: the SQL-native way to walk the graph, but recursive CTEs over
      large, densely-connected data can blow up in time and memory (again risky for the
      small VM).
  We deliberately DON'T implement either. Instead each person's group is anchored to
  the smallest person id among itself and its direct similars. For person-name data,
  similarity is effectively mutual within a block (if A~B and B~C then usually A~C too),
  so anchoring yields correct groups in the vast majority of cases without the cost and
  risk of connected-components. If perfect transitive clustering is ever required, a
  recursive CTE would be the place to add it — see the note at the bottom.

Run: python -m hr_etl.processing.reconcile
"""

from __future__ import annotations

from sqlalchemy import delete, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from hr_etl.logging_conf import get_logger
from hr_etl.models.db_models import DuplicateGroup

logger = get_logger(__name__)

# Fuzzy-name similarity threshold (0..1). Strict on purpose: fewer false positives.
_SIMILARITY_THRESHOLD = 0.85

# Safety cap so a pathological dataset can never flood the table or memory.
_MAX_MEMBERSHIPS = 20000

# Abort the query if it runs too long, so reconciliation can never hang the DB/VM.
_STATEMENT_TIMEOUT_MS = 120_000  # 2 minutes

# Normalized-name expression computed IN SQL: lowercase, collapse whitespace, trim.
_NORM = "btrim(regexp_replace(lower(full_name), '\\s+', ' ', 'g'))"

# Core detection query.
#
# Step by step:
#   normed  : persons with a usable normalized name + its first word (the "block").
#   pairs   : self-join within the SAME block where fuzzy similarity >= threshold and
#             a.id <> b.id. This is the O(n^2)-within-block comparison, kept cheap by
#             the block filter and the trigram index.
#   anchored: for every person that appears in a pair, its group_id = the smallest id
#             among itself and all its similars (the canonical anchor). We also keep the
#             best (max) similarity as the member's confidence and whether a corroborating
#             field (city/company) matched, to enrich the reason.
#
# Only persons that actually have at least one similar peer end up in a group.
_DETECT_SQL = text(
    f"""
    SET LOCAL statement_timeout = {_STATEMENT_TIMEOUT_MS};

    WITH normed AS (
        SELECT id,
               {_NORM} AS norm,
               split_part({_NORM}, ' ', 1) AS block,
               city, company
        FROM persons
        WHERE full_name IS NOT NULL AND length({_NORM}) >= 3
    ),
    pairs AS (
        SELECT a.id AS pid,
               b.id AS other,
               similarity(a.norm, b.norm) AS sim,
               (a.city IS NOT NULL AND a.city = b.city) AS same_city,
               (a.company IS NOT NULL AND a.company = b.company) AS same_company
        FROM normed a
        JOIN normed b
          ON a.block = b.block          -- blocking: same first name word
         AND a.id <> b.id
         AND similarity(a.norm, b.norm) >= :threshold
    ),
    anchored AS (
        SELECT pid,
               LEAST(pid, MIN(other)) AS group_id,
               MAX(sim) AS confidence,
               bool_or(same_city) AS any_city,
               bool_or(same_company) AS any_company
        FROM pairs
        GROUP BY pid
    )
    SELECT pid AS person_id,
           group_id,
           confidence,
           CASE
               WHEN any_city THEN 'fuzzy_name + same city'
               WHEN any_company THEN 'fuzzy_name + same company'
               ELSE 'fuzzy_name'
           END AS reason
    FROM anchored
    -- keep only real groups (group_id differs from pid for at least one member);
    -- singletons never reach here because they had no qualifying pair.
    ORDER BY group_id, person_id
    LIMIT :limit
    """
)


def _ensure_pg_trgm(session: Session) -> bool:
    """Enable the pg_trgm extension (idempotent). Returns False if it can't be enabled.

    On a non-Postgres backend or without privileges this fails; the caller then treats
    reconciliation as a no-op instead of crashing.
    """
    try:
        session.execute(text("CREATE EXTENSION IF NOT EXISTS pg_trgm"))
        session.commit()
        return True
    except SQLAlchemyError as exc:
        session.rollback()
        logger.warning("could not enable pg_trgm; skipping reconciliation: %s", exc)
        return False


def run_reconciliation(
    session: Session, similarity_threshold: float = _SIMILARITY_THRESHOLD
) -> int:
    """Full rebuild: clear old groups, detect via SQL, persist memberships.

    Returns the number of group-membership rows written. Memory-safe: the heavy work
    runs in Postgres and only membership rows are materialized (capped).
    """
    # Always start clean (full rebuild). If the table doesn't exist yet, init_schema
    # in main() creates it; here we just clear it.
    session.execute(delete(DuplicateGroup))
    session.commit()

    if not _ensure_pg_trgm(session):
        return 0

    try:
        rows = session.execute(
            _DETECT_SQL,
            {"threshold": similarity_threshold, "limit": _MAX_MEMBERSHIPS},
        ).all()
    except SQLAlchemyError as exc:
        session.rollback()
        logger.error("reconciliation query failed: %s", exc)
        return 0

    memberships = [
        DuplicateGroup(
            group_id=int(r.group_id),
            person_id=int(r.person_id),
            confidence=round(float(r.confidence), 3),
            reason=r.reason,
        )
        for r in rows
    ]

    if memberships:
        session.add_all(memberships)
        session.commit()

    n_groups = len({m.group_id for m in memberships})
    logger.info(
        "reconciliation complete: %d memberships across %d groups",
        len(memberships),
        n_groups,
    )
    return len(memberships)


def main() -> None:
    """CLI entrypoint for batch reconciliation."""
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
        count = run_reconciliation(session)
        print(f"Reconciliation done: {count} group memberships written.")
    finally:
        session.close()
        engine.dispose()


if __name__ == "__main__":
    main()


# ---------------------------------------------------------------------------
# NOTE — where full clustering would go (intentionally NOT implemented):
# If we ever need perfect transitive groups (A~B, B~C  =>  {A,B,C} even when A!~C),
# replace the `anchored` CTE with a RECURSIVE CTE that walks the `pairs` graph to
# connected components, or compute Union-Find in a worker. We skipped it because the
# anchor approach is correct for the vast majority of person-name data and avoids the
# time/memory cost that a recursive walk (or loading the graph into Python) would add
# on a small VM. See the module docstring for the full rationale.
# ---------------------------------------------------------------------------
