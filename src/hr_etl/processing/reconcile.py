"""Batch reconciliation job: surface probable-duplicate persons for HUMAN REVIEW.

Detects person records that likely refer to the same individual and stores them as
**groups** (not binary pairs) in ``duplicate_groups`` — never auto-merged.

────────────────────────────────────────────────────────────────────────────────────────
THE RULE (final, after a long investigation over the real 2.2M rows)
────────────────────────────────────────────────────────────────────────────────────────
Duplicates are searched in SILVER (``persons``) — the dirty layer. The ONLY signal is the
NAME, and NO field corroboration is required. The reasoning (important — it is why earlier
"require a shared field" attempts were wrong):

* A person arrives fragmented across sources that share no id. The passport side (Personal,
  Bank) and the name side (Location, Professional, Net) hold DISJOINT data — they share
  nothing but the name. So a real split person will NOT have a matching email/phone/company
  across the two halves. Demanding a corroborating field would throw away exactly the real
  duplicates.

* Conversely, if two records DID share a strong field (same phone/email) AND a similar
  name, they are almost certainly the same person that simply failed to consolidate — that
  is a job for ``consolidate_merge`` (auto-merge), not a "maybe" for this review pane.

So reconciliation surfaces, for human review, records whose NAMES relate as:
  * identical (same normalized name repeated),
  * typo      (similar but not identical, ``threshold <= sim < 1.0``),
  * containment (one name's words are a subset of the other's — extra surname).

The only negative guard is passport NON-CONTRADICTION: two records with different non-null
passports are provably different people (the "many jose luis, each with their own
passport" case) and are never grouped. Single-word names are excluded (too ambiguous).

This is intentionally permissive: common names ("juan perez" / "juan perez gomez") will
produce sizeable review groups. That is acceptable because (a) it only SUGGESTS, never
merges, and (b) any name that is ambiguous like this is EXCLUDED from Gold upstream (see
``gold_layer``), so the ambiguity never leaks into the clean layer — a human resolves it
from this pane instead.

Performance: detection runs entirely in SQL over a catalog of DISTINCT names (collapsing
millions of rows to far fewer names), so the fuzzy/containment work stays small. Only
counts return to Python (RAM-flat). A ``statement_timeout`` is an anti-hang net only.

Run: python -m hr_etl.processing.reconcile
"""

from __future__ import annotations

import time

from sqlalchemy import delete, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from hr_etl.logging_conf import get_logger
from hr_etl.models.db_models import DuplicateGroup
from hr_etl.processing.sql_norm import norm_sql

logger = get_logger(__name__)

# Fuzzy-name similarity threshold (0..1). Strict on purpose: fewer false positives.
_SIMILARITY_THRESHOLD = 0.85

# Anti-hang safety net ONLY (not a result cap): if it fires the whole run is cancelled and
# nothing is written (never a partial/incorrect group set).
_STATEMENT_TIMEOUT_MS = 600_000  # 10 minutes

# A normalized name must have at least this many words to be eligible. A single token
# ("juan") is far too ambiguous to claim a duplicate.
_MIN_NAME_TOKENS = 2

# Containment frequency guard. A `key2` (given name + first surname) shared by more than
# this many persons is a COMMON name-base, not a duplicate: "juan ignacio" has 3530 people
# and its containment variants ("juan ignacio X") are homonyms, not the same person. Those
# giant buckets are pure noise for review (measured: groups of 3530/1586/... members) and
# also the bulk of the volume. Above the threshold, containment is NOT generated for that
# key2. typo (already tiny) and exact (guarded by passport) are unaffected. Measured
# distribution: 274k groups of 2-5 members (the real maite/lucio cases) vs ~3.5k giant
# groups (>20) that carry the noise — this cutoff keeps the former and drops the latter.
_MAX_CONTAIN_BUCKET = 20

# Canonical SQL norm expression (single source of truth in processing/sql_norm; identical
# to the Python compute_norm_name and the migration backfill).
_NORM = norm_sql("full_name")

# ---------------------------------------------------------------------------
# Phase 0 — ensure norm_name is materialized (idempotent, cheap). Fills any row that has a
# full_name but no norm_name yet (fixtures, or rows written before the column existed). A
# WHERE-indexed no-op once everything is populated.
# ---------------------------------------------------------------------------
_BACKFILL_NORM_SQL = text(
    f"""
    UPDATE persons
    SET norm_name = {_NORM}
    WHERE full_name IS NOT NULL
      AND (norm_name IS NULL OR norm_name = '');
    """
)

# Ensure the human-review table exists before Phase 1 filters against it. Normally created
# by init_schema (ORM create_all); this idempotent DDL keeps run_reconciliation correct when
# called directly (tests, ad-hoc runs). Portable to Postgres and SQLite.
_ENSURE_REVIEWS_TABLE_SQL = text(
    """
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
)

# ---------------------------------------------------------------------------
# Phase 1 — eligible persons (name >= 2 words), and a catalog of DISTINCT names.
# Collapsing to distinct names is the scale lever: millions of rows -> far fewer names, so
# the fuzzy/containment comparison never re-does work on repeated names.
# ---------------------------------------------------------------------------
#
# Persons already RESOLVED by a human (person_reviews) are excluded from the eligible set,
# so a reviewer's verdict is sticky across the 30-min full rebuild: an 'approved' canonical,
# a 'distinct' homonym, or a 'merged' loser never resurfaces in the review pane. The join is
# by ``match_key`` (the stable business key) — persons.id churns on reprocess, match_key does
# not — so the decision holds even after a truncate + reload from the lake.
_BUILD_PN_SQL = text(
    f"""
    DROP TABLE IF EXISTS _recon_pn;
    CREATE TEMP TABLE _recon_pn AS
        SELECT id, norm_name AS norm, passport
        FROM persons p
        WHERE norm_name IS NOT NULL
          AND length(norm_name) >= 3
          AND array_length(regexp_split_to_array(norm_name, ' '), 1) >= {_MIN_NAME_TOKENS}
          AND NOT EXISTS (
              SELECT 1 FROM person_reviews r
              WHERE r.match_key = p.match_key
                AND r.status IN ('approved', 'distinct', 'merged')
          );
    CREATE INDEX _recon_pn_norm ON _recon_pn (norm);
    ANALYZE _recon_pn;
    """
)

# One row per DISTINCT name, with blocking keys and the passport profile of the bucket.
#   key2 = given name + first surname   (containment blocking; extra-surname variants share)
#   keyt = given name + first 3 chars of surname  (typo blocking; splits big first-name blocks)
# n_distinct_pass lets us apply passport non-contradiction at the name level.
_BUILD_NAMES_SQL = text(
    """
    DROP TABLE IF EXISTS _recon_names;
    CREATE TEMP TABLE _recon_names AS
        SELECT norm,
               words,
               (words[1] || ' ' || words[2]) AS key2,
               (words[1] || '|' || left(words[2], 3)) AS keyt,
               n_persons, anchor_id, n_distinct_pass
        FROM (
            SELECT norm,
                   regexp_split_to_array(norm, ' ') AS words,
                   count(*) AS n_persons,
                   min(id) AS anchor_id,
                   count(DISTINCT passport) AS n_distinct_pass
            FROM _recon_pn
            GROUP BY norm
        ) g;
    CREATE INDEX _recon_names_norm ON _recon_names (norm);
    CREATE INDEX _recon_names_key2 ON _recon_names (key2);
    CREATE INDEX _recon_names_keyt ON _recon_names (keyt);
    ANALYZE _recon_names;

    -- Total persons per blocking key. Used to prune common name-bases from BOTH name
    -- rules (containment by key2, typo by keyt): a key shared by more than
    -- _MAX_CONTAIN_BUCKET persons is homonymy, not duplication.
    DROP TABLE IF EXISTS _recon_key2_freq;
    CREATE TEMP TABLE _recon_key2_freq AS
        SELECT key2, sum(n_persons) AS n_persons FROM _recon_names GROUP BY key2;
    CREATE INDEX _recon_key2_freq_k ON _recon_key2_freq (key2);
    ANALYZE _recon_key2_freq;

    DROP TABLE IF EXISTS _recon_keyt_freq;
    CREATE TEMP TABLE _recon_keyt_freq AS
        SELECT keyt, sum(n_persons) AS n_persons FROM _recon_names GROUP BY keyt;
    CREATE INDEX _recon_keyt_freq_k ON _recon_keyt_freq (keyt);
    ANALYZE _recon_keyt_freq;
    """
)

# ---------------------------------------------------------------------------
# Phase 2 — name PAIRS (typo + containment) between DISTINCT names. Each rule is blocked by
# an equality key so no cartesian product forms. No corroboration; only the name relation.
# ---------------------------------------------------------------------------
_BUILD_NAME_PAIRS_SQL = text(
    f"""
    DROP TABLE IF EXISTS _recon_name_pairs;
    CREATE TEMP TABLE _recon_name_pairs AS
        SELECT a.norm AS a, b.norm AS b, similarity(a.norm, b.norm) AS sim, FALSE AS contain
        FROM _recon_names a
        JOIN _recon_keyt_freq ft ON ft.keyt = a.keyt AND ft.n_persons <= {_MAX_CONTAIN_BUCKET}
        JOIN _recon_names b ON b.keyt = a.keyt AND b.norm <> a.norm
        WHERE similarity(a.norm, b.norm) >= :threshold
          AND similarity(a.norm, b.norm) < 1.0
        UNION ALL
        -- containment, but ONLY for key2 buckets that are not a common name-base. A key2
        -- shared by more than {_MAX_CONTAIN_BUCKET} persons ("juan ignacio" x3530) is
        -- homonymy, not duplication — pruned to keep the review pane useful and small.
        SELECT a.norm AS a, b.norm AS b, similarity(a.norm, b.norm) AS sim, TRUE AS contain
        FROM _recon_names a
        JOIN _recon_key2_freq f ON f.key2 = a.key2 AND f.n_persons <= {_MAX_CONTAIN_BUCKET}
        JOIN _recon_names b ON b.key2 = a.key2 AND b.norm <> a.norm
        WHERE array_length(a.words, 1) <> array_length(b.words, 1)
          AND (a.words <@ b.words OR b.words <@ a.words);
    CREATE INDEX _recon_name_pairs_a ON _recon_name_pairs (a);
    ANALYZE _recon_name_pairs;
    """
)

# ---------------------------------------------------------------------------
# Phase 3 — resolve grouping AT THE NAME LEVEL, then expand to persons with a single JOIN.
#
# CRITICAL for scale: we must NEVER materialize person×person pairs. A common blocking key
# like "juan ignacio" holds thousands of persons, so expanding name-pairs to person-pairs
# would create >100M rows and blow up pgsql_tmp (measured: 113M). Instead we work on the
# small DISTINCT-name graph and only expand names→persons once at the very end.
#
# Step 3a — NAME graph: assign every distinct name a group anchor. Edges are the
# typo/containment name-pairs (already small, blocked by keyt/key2), made bidirectional.
# A name's group_norm_anchor = the smallest anchor_id among itself and its direct
# neighbours. (Anchoring, not full connected components — correct for the vast majority of
# person names, and cheap; see module docstring.) Names with no neighbour keep themselves.
# ---------------------------------------------------------------------------
_BUILD_NAME_GROUPS_SQL = text(
    f"""
    DROP TABLE IF EXISTS _recon_name_groups;
    CREATE TEMP TABLE _recon_name_groups AS
        WITH edges AS (
            SELECT a AS x, b AS y, sim, contain FROM _recon_name_pairs
            UNION ALL
            SELECT b AS x, a AS y, sim, contain FROM _recon_name_pairs
        ),
        -- neighbour anchors per name (via the typo/containment edges)
        nbr AS (
            SELECT e.x AS norm,
                   min(nx.anchor_id) AS nbr_anchor,
                   max(e.sim) AS sim,
                   bool_or(e.contain) AS has_containment,
                   bool_or(NOT e.contain) AS has_fuzzy
            FROM edges e
            JOIN _recon_names nx ON nx.norm = e.y
            GROUP BY e.x
        )
        SELECT n.norm,
               n.anchor_id,
               n.n_persons,
               -- group anchor id at the NAME level
               LEAST(n.anchor_id, COALESCE(nb.nbr_anchor, n.anchor_id)) AS group_anchor,
               COALESCE(nb.sim, 1.0) AS sim,
               COALESCE(nb.has_containment, FALSE) AS has_containment,
               COALESCE(nb.has_fuzzy, FALSE) AS has_fuzzy,
               -- a name is a candidate if it has a typo/containment neighbour, OR it is a
               -- repeated identical name of MODERATE size. An identical name shared by
               -- more than {_MAX_CONTAIN_BUCKET} persons ("juan ignacio" x3530) is common
               -- homonymy, not duplication — excluded to keep the review pane useful.
               (nb.norm IS NOT NULL
                OR (n.n_persons > 1 AND n.n_persons <= {_MAX_CONTAIN_BUCKET})) AS is_candidate
        FROM _recon_names n
        LEFT JOIN nbr nb ON nb.norm = n.norm;
    CREATE INDEX _recon_name_groups_norm ON _recon_name_groups (norm);
    ANALYZE _recon_name_groups;
    """
)

# Phase 4 — expand names→persons ONCE and INSERT memberships. Each eligible person takes
# its NAME's group anchor as group_id (LEAST with its own id keeps the canonical min).
# Passport non-contradiction is applied WITHIN an identical-name bucket via the anchor: a
# person whose passport differs from its name-anchor person is dropped (separates the "many
# jose luis, each with own passport" case). Names that relate by typo/containment are kept
# for review regardless (disjoint split persons share no passport anyway). No person×person
# product is ever built, so no tmp explosion. reason from the strongest rule; no PII.
_DETECT_INSERT_SQL = text(
    """
    WITH person_named AS (
        SELECT p.id AS pid, p.passport,
               g.group_anchor, g.anchor_id, g.n_persons,
               g.sim, g.has_containment, g.has_fuzzy, g.is_candidate
        FROM _recon_pn p
        JOIN _recon_name_groups g ON g.norm = p.norm
        WHERE g.is_candidate
    ),
    anchor_pass AS (
        SELECT id AS anchor_id, passport AS anchor_passport FROM _recon_pn
    ),
    eligible AS (
        SELECT pn.pid, pn.group_anchor, pn.anchor_id, pn.n_persons, pn.has_containment,
               pn.has_fuzzy, pn.sim, pn.passport, ap.anchor_passport
        FROM person_named pn
        JOIN anchor_pass ap ON ap.anchor_id = pn.anchor_id
    ),
    kept AS (
        -- drop a person that contradicts its identical-name anchor's passport (homonyms
        -- with distinct passports). Only applies within a repeated identical name
        -- (n_persons > 1); typo/containment neighbours are always kept for review.
        SELECT pid,
               LEAST(pid, group_anchor) AS group_id,
               sim, has_containment, has_fuzzy, n_persons
        FROM eligible
        WHERE NOT (
            n_persons > 1 AND NOT has_containment AND NOT has_fuzzy
            AND passport IS NOT NULL AND anchor_passport IS NOT NULL
            AND passport <> anchor_passport
        )
    ),
    -- a singleton left after dropping its only same-name twin is not a duplicate; keep a
    -- group only if it ends up with >= 2 members.
    grp AS (
        SELECT group_id, count(*) AS n FROM kept GROUP BY group_id HAVING count(*) > 1
    )
    INSERT INTO duplicate_groups (group_id, person_id, confidence, reason, created_at)
    SELECT k.group_id,
           k.pid AS person_id,
           round(k.sim::numeric, 3) AS confidence,
           CASE
               WHEN k.has_containment THEN 'name_containment'
               WHEN k.has_fuzzy       THEN 'fuzzy_name'
               ELSE 'exact_name'
           END AS reason,
           now()
    FROM kept k
    JOIN grp ON grp.group_id = k.group_id
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

    Returns the number of group-membership rows written. Memory-safe: detection runs in
    Postgres over a catalog of DISTINCT names; only a row count returns to Python.
    """
    started = time.monotonic()

    if not _ensure_pg_trgm(session):
        return 0

    try:
        # Anti-hang safety net (not a result cap). int() closes any interpolation vector.
        session.execute(text(f"SET statement_timeout = {int(_STATEMENT_TIMEOUT_MS)}"))
        threshold = float(similarity_threshold)
        # Clear previous groups INSIDE the rebuild transaction so the table is never
        # observed empty: a failed detect rolls back and restores the old groups.
        session.execute(delete(DuplicateGroup))
        # Phase 0: ensure norm_name is materialized (guarded no-op once populated) and the
        # human-review table exists (Phase 1 excludes already-resolved match_keys).
        session.execute(_ENSURE_REVIEWS_TABLE_SQL)
        session.execute(_BACKFILL_NORM_SQL)
        # Phase 1: eligible persons + distinct-name catalog.
        session.execute(_BUILD_PN_SQL)
        session.execute(_BUILD_NAMES_SQL)
        # Phase 2: typo/containment name pairs.
        session.execute(_BUILD_NAME_PAIRS_SQL, {"threshold": threshold})
        # Phase 3: resolve grouping at the NAME level (small graph, no person product).
        session.execute(_BUILD_NAME_GROUPS_SQL)
        # Phase 4: expand names->persons ONCE and insert memberships.
        result = session.execute(_DETECT_INSERT_SQL)
        written = result.rowcount
        session.commit()
    except SQLAlchemyError as exc:
        session.rollback()
        # Log the DB error type/message only — never the working rows (no PII, DP-4).
        logger.error("reconciliation query failed: %s", exc)
        return 0

    n_groups = session.execute(
        text("SELECT count(DISTINCT group_id) FROM duplicate_groups")
    ).scalar_one()
    elapsed = time.monotonic() - started
    logger.info(
        "reconciliation complete: %d memberships across %d groups in %.1fs",
        written,
        n_groups,
        elapsed,
    )
    _record_metrics(elapsed, int(n_groups), int(written))
    return int(written)


def _record_metrics(duration: float, n_groups: int, memberships: int) -> None:
    """Publish reconciliation metrics (numeric only, never PII). Best-effort."""
    try:
        from hr_etl.metrics.prometheus import (
            RECONCILE_DURATION_SECONDS,
            RECONCILE_GROUPS,
            RECONCILE_MEMBERSHIPS,
        )

        RECONCILE_DURATION_SECONDS.observe(duration)
        RECONCILE_GROUPS.set(n_groups)
        RECONCILE_MEMBERSHIPS.set(memberships)
    except Exception:  # pragma: no cover - metrics must never fail the job
        pass


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
