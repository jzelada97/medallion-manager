-- Migration 001 — reconcile/consolidation/gold subsystem (idempotent).
--
-- Safe to run repeatedly. It:
--   1. enables pg_trgm (fuzzy trigram similarity used by reconcile/consolidate),
--   2. ensures the btree + GIN trigram indexes on persons.norm_name exist,
--   3. backfills norm_name for historical rows using the SAME expression the batch jobs
--      and the Python compute_norm_name() produce (character-for-character parity), and
--   4. ANALYZEs persons so the planner has fresh stats for the new indexes.
--
-- The norm_name column and the gold_persons/duplicate_groups tables come from the ORM
-- (Base.metadata.create_all / init_schema); this migration only adds what create_all
-- cannot express (the extension, the GIN index, and the historical backfill).
--
-- The backfill expression MUST stay in sync with:
--   * src/hr_etl/processing/normalizer.py :: compute_norm_name (streaming writer)
--   * src/hr_etl/processing/reconcile.py  :: _NORM (batch reconciliation)
-- Steps mirrored below: lower -> translate(accents) -> collapse whitespace -> btrim ->
-- strip a leading/trailing title (both ends, applied prefix/suffix/prefix/suffix).

CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- btree: used by the final join persons.norm_name = catalog.norm and the GROUP BY.
CREATE INDEX IF NOT EXISTS ix_persons_norm_name
    ON persons (norm_name);

-- GIN trigram: used by the `%` operator / similarity() in the fuzzy blocking.
CREATE INDEX IF NOT EXISTS ix_persons_norm_name_trgm
    ON persons USING gin (norm_name gin_trgm_ops);

-- Backfill norm_name for rows that have a full_name but no materialized norm_name yet.
-- Idempotent: rows already populated (norm_name not null/empty) are skipped.
UPDATE persons
SET norm_name = btrim(regexp_replace(
                  btrim(regexp_replace(
                    btrim(regexp_replace(
                      btrim(regexp_replace(
                        btrim(regexp_replace(
                          translate(lower(full_name),
                                    'áàäâãéèëêíìïîóòöôõúùüûñçý',
                                    'aaaaaeeeeiiiiooooouuuuncy'),
                          '\s+', ' ', 'g'))
                      , '^(mr|mrs|ms|miss|sir|dr|dr\(a\)|dott|dott\.ssa|ing|lic|mtro|prof|sr|sra|sr\(a\)|sig|sig\.ra|md|phd|ph\.d|jr|ii|iii|iv|pi|dds|esq)\.?\s+', '', 'i'))
                    , '\s+(mr|mrs|ms|miss|sir|dr|dr\(a\)|dott|dott\.ssa|ing|lic|mtro|prof|sr|sra|sr\(a\)|sig|sig\.ra|md|phd|ph\.d|jr|ii|iii|iv|pi|dds|esq)\.?$', '', 'i'))
                  , '^(mr|mrs|ms|miss|sir|dr|dr\(a\)|dott|dott\.ssa|ing|lic|mtro|prof|sr|sra|sr\(a\)|sig|sig\.ra|md|phd|ph\.d|jr|ii|iii|iv|pi|dds|esq)\.?\s+', '', 'i'))
                , '\s+(mr|mrs|ms|miss|sir|dr|dr\(a\)|dott|dott\.ssa|ing|lic|mtro|prof|sr|sra|sr\(a\)|sig|sig\.ra|md|phd|ph\.d|jr|ii|iii|iv|pi|dds|esq)\.?$', '', 'i'))
WHERE full_name IS NOT NULL
  AND (norm_name IS NULL OR norm_name = '');

ANALYZE persons;
