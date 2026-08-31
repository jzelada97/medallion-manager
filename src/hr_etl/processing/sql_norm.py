"""Canonical SQL ``norm_name`` expression — single source of truth for the batch jobs.

``norm_name`` is a derived, persisted column on ``persons``. It must be produced
IDENTICALLY by three places:

* the streaming warehouse writer (Python: :func:`hr_etl.processing.normalizer.compute_norm_name`),
* the SQL backfill (``warehouse/migrations/001_reconcile.sql``), and
* the batch jobs (reconciliation and consolidation), which recompute it in SQL.

To avoid drift, the SQL side is built here once via :func:`norm_sql` and imported by
``reconcile.py`` and ``consolidate_merge.py``. The migration file embeds the same
expression inline (it cannot import Python), so any change here must be mirrored there —
guarded by an integration parity test.

Pipeline (mirrors :func:`compute_norm_name`):
    1. lowercase
    2. translate a fixed accent set to ASCII (``translate``; NOT full NFKD)
    3. collapse whitespace runs to one space, then ``btrim``
    4. strip a leading/trailing title from both ends (prefix, suffix, prefix, suffix)

None of these interpolate any external input: the column name is a fixed literal chosen
by the caller from a closed set, never user/network data.
"""

from __future__ import annotations

# Same accent map as normalizer._NORM_ACCENT_TABLE and the migration's translate().
_ACCENTS_FROM = "áàäâãéèëêíìïîóòöôõúùüûñçý"
_ACCENTS_TO = "aaaaaeeeeiiiiooooouuuuncy"

# Titles/honorifics/suffixes, mirrored from normalizer._NORM_TITLES.
_TITLES = (
    r"mr|mrs|ms|miss|sir|dr|dr\(a\)|dott|dott\.ssa|ing|lic|mtro|prof|"
    r"sr|sra|sr\(a\)|sig|sig\.ra|md|phd|ph\.d|jr|ii|iii|iv|pi|dds|esq"
)
_STRIP_PREFIX = rf"^({_TITLES})\.?\s+"
_STRIP_SUFFIX = rf"\s+({_TITLES})\.?$"


def _strip_titles_sql(expr: str) -> str:
    """Wrap a SQL expression to strip a leading/trailing title (both ends, twice)."""
    for pattern in (_STRIP_PREFIX, _STRIP_SUFFIX, _STRIP_PREFIX, _STRIP_SUFFIX):
        expr = f"btrim(regexp_replace({expr}, '{pattern}', '', 'i'))"
    return expr


def norm_sql(column: str = "full_name") -> str:
    """Return the SQL expression that normalizes ``column`` into a ``norm_name``.

    ``column`` is a fixed identifier chosen by the caller (e.g. ``'full_name'`` or a
    survivor alias), never external input, so it is safe to embed as an identifier.
    """
    unaccent = f"translate(lower({column}), '{_ACCENTS_FROM}', '{_ACCENTS_TO}')"
    collapsed = f"btrim(regexp_replace({unaccent}, '\\s+', ' ', 'g'))"
    return _strip_titles_sql(collapsed)
