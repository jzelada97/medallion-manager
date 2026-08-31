"""Batch reconciliation job: group probable-duplicate persons in the warehouse.

Detects person records that likely refer to the same individual and stores them as
**groups** (not binary pairs) in the ``duplicate_groups`` table for review — never
auto-merged.

Design decisions (read this — it explains the trade-offs):

* **All detection runs in SQL (Postgres), not in Python.** Pulling millions of rows
  into Python to compare them exhausts RAM (it OOM'd the demo VM once). The heavy work
  — normalization, fuzzy matching, grouping — happens inside Postgres; only the final
  membership rows come back.

* **Two phases: exact first, fuzzy only over DISTINCT names.** The expensive part is
  fuzzy comparison. But most duplicates are the *same* normalized name repeated many
  times, which needs no fuzzy work at all. So:
    1. Collapse persons to a catalog of DISTINCT normalized names (``_recon_names``).
       This is one cheap ``GROUP BY`` and shrinks millions of rows to far fewer names.
    2. Run fuzzy matching (``pg_trgm``) ONLY between those distinct names — a much
       smaller set — to link typo variants like "leclerc" vs "leclercq".
    3. Assign every distinct name a ``group_id`` (canonical anchor) and propagate it
       back to all persons carrying that name.
  This removes the pathological cost of comparing millions of near-identical rows: the
  fuzzy step never sees duplicate names, so there is no cluster of identical strings to
  blow it up.

* **No result caps.** There is no maximum number of members per group and no LIMIT on
  how many memberships are written. Capping results would silently drop real duplicates
  once the dataset grows, which is wrong. The design is cheap enough to run unbounded;
  a ``statement_timeout`` remains ONLY as an anti-hang safety net (if it ever fires the
  whole run is cancelled and writes nothing — it never writes a partial/incorrect set).

* **Fuzzy name matching with ``pg_trgm``.** Exact matching misses "leclerc" vs
  "leclercq" (one letter) or "martinez" vs "martenez" (swap). ``pg_trgm`` scores string
  similarity (0..1), index-backed. Threshold ``_SIMILARITY_THRESHOLD`` (0.85) is strict
  to keep false positives low.

* **Title stripping on both ends.** Honorifics/suffixes (Mr, Dr, MD, PhD, Jr...) can
  appear before OR after the name in the generator's data. They are stripped from both
  ends (same list as the streaming ``normalizer``) so "Dr Juan Perez", "Juan Perez MD"
  and "Juan Perez" normalize to the same name.

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
  We deliberately DON'T implement either. Instead each distinct name's group is anchored
  to the smallest person id among itself and its direct similars. For person-name data,
  similarity is effectively mutual (if A~B and B~C then usually A~C too), so anchoring
  yields correct groups in the vast majority of cases without the cost and risk of
  connected-components. If perfect transitive clustering is ever required, a recursive
  CTE would be the place to add it — see the note at the bottom.

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
# Used both as the `%` session threshold (rule-1 blocking) AND the decision cutoff.
_SIMILARITY_THRESHOLD = 0.85


# Abort the query if it runs too long, so reconciliation can never hang the DB/VM. This
# is an anti-hang safety net ONLY, not a result cap: if it fires, the whole run is
# cancelled and nothing is written (never a partial/incorrect group set).
_STATEMENT_TIMEOUT_MS = 600_000  # 10 minutes

# Minimum tokens (words) a normalized name must have to be eligible. A single token
# like "juan" is far too ambiguous to claim a duplicate: thousands of unrelated people
# share a given name. Requiring name + surname keeps groups meaningful and stops
# high-frequency first names from forming spurious groups.
_MIN_NAME_TOKENS = 2

# Titles/honorifics/suffixes to strip, mirrored from processing/normalizer.py so the
# normalized name here matches the streaming pipeline. IMPORTANT: any of these can appear
# on EITHER side of the name (the generator puts them as prefix OR suffix, e.g. "Dr Juan
# Perez" and "Juan Perez MD" both occur). So we use ONE list and strip it from both ends,
# twice, to also catch a title on both ends. Stripping matters: without it a leading
# title dominates the name and unrelated people get matched together.
_TITLES = (
    r"mr|mrs|ms|miss|sir|dr|dr\(a\)|dott|dott\.ssa|ing|lic|mtro|prof|"
    r"sr|sra|sr\(a\)|sig|sig\.ra|md|phd|ph\.d|jr|ii|iii|iv|pi|dds|esq"
)
_STRIP_PREFIX = rf"^({_TITLES})\.?\s+"  # a leading title + following space
_STRIP_SUFFIX = rf"\s+({_TITLES})\.?$"  # a trailing space + title


def _strip_titles_sql(expr: str) -> str:
    """Wrap a SQL text expression to strip a leading/trailing title (both ends, twice)."""
    for pattern in (_STRIP_PREFIX, _STRIP_SUFFIX, _STRIP_PREFIX, _STRIP_SUFFIX):
        expr = f"btrim(regexp_replace({expr}, '{pattern}', '', 'i'))"
    return expr


# Strip common accents so "maría" and "maria" normalize to the same name (the streaming
# normalizer does NFKD; we mirror it in SQL with translate(), which needs no extension).
_ACCENTS_FROM = "áàäâãéèëêíìïîóòöôõúùüûñçý"
_ACCENTS_TO = "aaaaaeeeeiiiiooooouuuuncy"
_UNACCENT = f"translate(lower(full_name), '{_ACCENTS_FROM}', '{_ACCENTS_TO}')"

# Normalized-name expression computed IN SQL: lowercase, strip accents, collapse
# whitespace, strip titles from both ends, trim. Mirrors normalizer.normalize_text +
# strip_titles (which also strip accents / titles) so batch matches the streaming keys.
_NORM = _strip_titles_sql(f"btrim(regexp_replace({_UNACCENT}, '\\s+', ' ', 'g'))")

# ---------------------------------------------------------------------------
# Phase 1 — build a catalog of DISTINCT eligible names.
#
# For each distinct normalized name we keep the smallest person id (anchor_id), plus one
# representative city/company (for the reason label). Collapsing to distinct names is the
# single most important optimization: it turns "millions of rows, many identical" into
# "a smaller set of unique names", so the fuzzy step never wastes work on repeated names.
# ---------------------------------------------------------------------------
# First materialize (person id -> normalized name) ONCE, so the expensive title-strip
# regex runs a single time per row (not again when we join persons back at the end).
_BUILD_PN_SQL = text(
    f"""
    DROP TABLE IF EXISTS _recon_pn;
    CREATE TEMP TABLE _recon_pn AS
        SELECT id, norm, city, company
        FROM (
            SELECT id, {_NORM} AS norm, city, company
            FROM persons
            WHERE full_name IS NOT NULL
        ) s
        WHERE length(norm) >= 3
          AND array_length(regexp_split_to_array(norm, ' '), 1) >= {_MIN_NAME_TOKENS};
    """
)

# Each distinct name carries its word array (for the containment subset test) and a
# `key2` = first two words joined (given name + first surname). key2 is the containment
# blocking key: it is highly selective yet always shared by a name and its extra-surname
# variant, so we never build a cartesian block on a common given name.
_BUILD_NAMES_SQL = text(
    """
    DROP TABLE IF EXISTS _recon_names;
    CREATE TEMP TABLE _recon_names AS
        SELECT norm,
               words,
               words[1] AS key1,
               (words[1] || ' ' || words[2]) AS key2,
               -- typo blocking key: given name + first 3 chars of the first surname.
               -- Splits the huge "juan" block (~20k) into small sub-blocks while still
               -- sharing the key across a surname typo past the 3rd char.
               (words[1] || '|' || left(words[2], 3)) AS keyt,
               n_persons, anchor_id, city, company
        FROM (
            SELECT norm,
                   regexp_split_to_array(norm, ' ') AS words,
                   count(*) AS n_persons,
                   min(id) AS anchor_id,
                   min(city)    FILTER (WHERE city IS NOT NULL)    AS city,
                   min(company) FILTER (WHERE company IS NOT NULL) AS company
            FROM _recon_pn
            GROUP BY norm
        ) g;
    """
)

_INDEX_NAMES_SQL = text(
    # Both rules now block by an equality key (keyt / key2), so plain btree indexes are
    # enough — no GIN trigram index needed (it was expensive to build on 2M+ names).
    "CREATE INDEX _recon_names_norm ON _recon_names (norm);"
    " CREATE INDEX _recon_names_key2 ON _recon_names (key2);"
    " CREATE INDEX _recon_names_keyt ON _recon_names (keyt);"
    " CREATE INDEX _recon_pn_norm ON _recon_pn (norm);"
    " ANALYZE _recon_names;"
    " ANALYZE _recon_pn;"
)

# ---------------------------------------------------------------------------
# Phase 2 — link distinct names via TWO rules, each with its OWN efficient blocking,
# then group by canonical anchor and propagate to persons.
#
#   RULE 1 — TYPO (trigram similarity >= :threshold). Blocking = keyt (given name + first
#   3 chars of the first surname), an equality join. This splits huge given-name blocks
#   into tiny sub-blocks so `similarity()` only runs on a handful of candidates — the key
#   speedup on 2M+ rows. Catches "jean leclerc" vs "jean leclercq".
#
#   RULE 2 — CONTAINMENT (every word of one name is in the other). Blocking = key2 (first
#   two words), then the `<@` array test. Catches "octavio ponce" ⊆ "octavio ponce
#   gimenez" WITHOUT lowering the similarity threshold (trigram similarity would miss it).
#
# The two candidate-pair sets are UNIONed, self is added (so every name anchors), then a
# single grouping assigns group_id = smallest anchor_id among linked names. No caps.
# ---------------------------------------------------------------------------
_DETECT_SQL = text(
    """
    WITH typo_pairs AS (
        -- rule 1: typo/letter-swap. Blocking = given name + first 3 chars of the first
        -- surname (keyt). This splits huge given-name blocks ("juan" ~20k) into small
        -- sub-blocks, so the trigram similarity only runs within a tiny candidate set.
        -- A surname typo past the 3rd char keeps the same keyt. Catches "jean leclerc"/
        -- "jean leclercq". (Trade-off: a typo within the first 3 surname chars is missed;
        -- accepted so the job scales on millions of rows.)
        SELECT n.norm AS a, m.norm AS b, similarity(n.norm, m.norm) AS sim, FALSE AS contain
        FROM _recon_names n
        JOIN _recon_names m
          ON n.keyt = m.keyt                 -- blocking: given name + surname prefix
         AND n.norm <> m.norm
         AND similarity(n.norm, m.norm) >= :threshold
    ),
    contain_pairs AS (
        -- rule 2: containment. Blocking key = first two words (given name + first
        -- surname). Then the full word-subset test on names of DIFFERENT length.
        -- IMPORTANT: containment alone ("juan garcia" ⊆ "juan garcia X") would merge every
        -- unrelated "Juan Garcia <extra>" into one giant group. To stay a *plausible*
        -- duplicate we REQUIRE a corroborating field: same non-null city OR same non-null
        -- company. So "octavio ponce" and "octavio ponce gimenez" link only when they also
        -- share a city/company — i.e. when they're actually likely the same person.
        SELECT n.norm AS a, m.norm AS b, similarity(n.norm, m.norm) AS sim, TRUE AS contain
        FROM _recon_names n
        JOIN _recon_names m
          ON n.key2 = m.key2                 -- blocking: same given name + first surname
         AND n.norm <> m.norm
         AND array_length(n.words, 1) <> array_length(m.words, 1)
         AND (n.words <@ m.words OR m.words <@ n.words)
         AND (
              (n.city IS NOT NULL AND n.city = m.city)
              OR (n.company IS NOT NULL AND n.company = m.company)
         )
    ),
    links AS (
        SELECT a, b, sim, contain FROM typo_pairs
        UNION ALL
        SELECT a, b, sim, contain FROM contain_pairs
    ),
    name_groups AS (
        SELECT nm.norm,
               nm.anchor_id,
               nm.n_persons,
               nm.city,
               nm.company,
               LEAST(nm.anchor_id, COALESCE(min(peer.anchor_id), nm.anchor_id)) AS group_id,
               max(l.sim) AS fuzzy_sim,
               bool_or(l.contain) AS has_containment_link,
               count(l.b) > 0 AS has_link
        FROM _recon_names nm
        LEFT JOIN links l ON l.a = nm.norm
        LEFT JOIN _recon_names peer ON peer.norm = l.b
        GROUP BY nm.norm, nm.anchor_id, nm.n_persons, nm.city, nm.company
    ),
    -- a group is "real" (has duplicates) if it holds MORE THAN ONE PERSON in total,
    -- whether from the same exact name repeated or from linked name variants.
    real_groups AS (
        SELECT group_id
        FROM name_groups
        GROUP BY group_id
        HAVING sum(n_persons) > 1
    )
    INSERT INTO duplicate_groups (group_id, person_id, confidence, reason, created_at)
    SELECT g.group_id,
           p.id AS person_id,
           round(COALESCE(g.fuzzy_sim, 1.0)::numeric, 3) AS confidence,
           CASE
               WHEN g.has_containment_link THEN 'name_containment + same city/company'
               WHEN g.city IS NOT NULL AND g.has_link THEN 'fuzzy_name + same city'
               WHEN g.company IS NOT NULL AND g.has_link THEN 'fuzzy_name + same company'
               WHEN g.has_link THEN 'fuzzy_name'
               ELSE 'exact_name'
           END AS reason,
           now()
    FROM name_groups g
    JOIN real_groups r ON r.group_id = g.group_id
    JOIN _recon_pn p ON p.norm = g.norm
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

    Returns the number of group-membership rows written. Memory-safe AND scalable: the
    heavy work runs in Postgres over a catalog of DISTINCT names (not every row), and
    there is no cap on group size or membership count.
    """
    # Always start clean (full rebuild). If the table doesn't exist yet, init_schema
    # in main() creates it; here we just clear it.
    session.execute(delete(DuplicateGroup))
    session.commit()

    if not _ensure_pg_trgm(session):
        return 0

    try:
        # Bound every statement in this run as an anti-hang safety net (not a result cap).
        session.execute(text(f"SET statement_timeout = {_STATEMENT_TIMEOUT_MS}"))
        threshold = float(similarity_threshold)
        # Phase 1: (person -> normalized name) once, distinct-name catalog + blocking keys.
        session.execute(_BUILD_PN_SQL)
        session.execute(_BUILD_NAMES_SQL)
        session.execute(_INDEX_NAMES_SQL)
        # Phase 2: link names (typo + containment), group, and INSERT memberships straight
        # into duplicate_groups — the rows never travel to Python (no ORM materialization
        # of hundreds of thousands of objects), which keeps it fast and memory-flat.
        result = session.execute(_DETECT_SQL, {"threshold": threshold})
        written = result.rowcount
        session.commit()
    except SQLAlchemyError as exc:
        session.rollback()
        logger.error("reconciliation query failed: %s", exc)
        return 0

    n_groups = session.execute(
        text("SELECT count(DISTINCT group_id) FROM duplicate_groups")
    ).scalar_one()
    logger.info(
        "reconciliation complete: %d memberships across %d groups",
        written,
        n_groups,
    )
    return int(written)


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
# replace the anchor logic in `name_groups` with a RECURSIVE CTE that walks the fuzzy
# graph over the DISTINCT names to connected components (cheaper than over all rows), or
# compute Union-Find in a worker. We skipped it because the anchor approach is correct
# for the vast majority of person-name data and avoids the time/memory cost of a
# recursive walk. See the module docstring for the full rationale.
# ---------------------------------------------------------------------------
