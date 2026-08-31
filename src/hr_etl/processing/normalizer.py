"""Field normalization utilities.

The incoming data is intentionally inconsistent, so every value is normalized
(strip, collapse whitespace, lowercase keys, strip accents) before it is compared
or used as a matching key.
"""

from __future__ import annotations

import re
import unicodedata
from typing import Any

_WS_RE = re.compile(r"\s+")

# Titles/honorifics that appear as prefixes in fullnames from the generator.
# These are stripped before matching to allow cross-linking between Personal
# (which has no title) and Location/Professional (which may include one).
_TITLE_PREFIX_RE = re.compile(
    r"^(mr\.?|mrs\.?|ms\.?|dr\.?|dr\(a\)\.?|dott\.?|dott\.ssa|"
    r"ing\.?|lic\.?|mtro\.?|prof\.?|"
    r"sr\.?|sra\.?|sr\(a\)\.?|sig\.?|sig\.ra)\s+",
    re.IGNORECASE,
)

# Suffixes (professional/generational) that appear AFTER the name.
_TITLE_SUFFIX_RE = re.compile(
    r"\s+(md|phd|ph\.d\.?|jr\.?|sr\.?|ii|iii|iv|pi|dds|esq\.?)$",
    re.IGNORECASE,
)


def strip_titles(text: str) -> str:
    """Remove honorific/professional title prefixes and suffixes from a name."""
    result = _TITLE_PREFIX_RE.sub("", text).strip()
    result = _TITLE_SUFFIX_RE.sub("", result).strip()
    return result


def strip_accents(text: str) -> str:
    """Remove diacritics from a string."""
    normalized = unicodedata.normalize("NFKD", text)
    return "".join(c for c in normalized if not unicodedata.combining(c))


def normalize_text(value: Any) -> str:
    """Lowercase, strip accents and collapse whitespace. Returns '' for None."""
    if value is None:
        return ""
    text = strip_accents(str(value)).lower().strip()
    return _WS_RE.sub(" ", text)


_KEY_ALIASES: dict[str, str] = {
    "companyadress": "companyaddress",  # README typo: "Company Adress" -> canonical
}


def normalize_key(key: str) -> str:
    """Normalize a raw message key: lowercase, remove spaces/underscores/hyphens.

    Maps variants like 'E-Mail', 'Company Address', 'company_email' to canonical
    lowercase alphanumeric keys ('email', 'companyaddress', 'companyemail').
    Also resolves known typos (e.g. 'Company Adress' -> 'companyaddress').
    """
    normalized = re.sub(r"[\s_\-]+", "", str(key).strip().lower())
    return _KEY_ALIASES.get(normalized, normalized)


def normalize_message(message: dict[str, Any]) -> dict[str, Any]:
    """Return a copy of the message with normalized keys (values untouched)."""
    return {normalize_key(k): v for k, v in message.items()}


def clean_salary(value: Any) -> float | None:
    """Parse a salary value that may contain currency symbols or separators."""
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        return float(value)
    digits = re.sub(r"[^\d.,\-]", "", str(value)).replace(",", ".")
    # keep only the last dot as decimal separator
    if digits.count(".") > 1:
        parts = digits.split(".")
        digits = "".join(parts[:-1]) + "." + parts[-1]
    try:
        return float(digits) if digits not in ("", "-", ".") else None
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Canonical normalized name (``norm_name``) — single source of truth.
#
# ``norm_name`` is a persisted, derived column on ``persons`` used by the batch
# reconciliation/consolidation jobs so they never recompute a heavy regex over millions
# of rows. It MUST be produced identically by:
#   * the streaming warehouse writer (Python: ``compute_norm_name``), and
#   * the SQL backfill / survivorship recompute (the ``_NORM`` expression in
#     ``processing/reconcile.py`` and ``warehouse/migrations/001_reconcile.sql``).
#
# So this function deliberately MIRRORS the SQL expression character-for-character
# instead of reusing ``normalize_text``/``strip_titles`` (which use full NFKD and a
# different regex order). The reference pipeline is:
#   1. lowercase
#   2. translate a fixed accent set to ASCII (same map as SQL ``translate``)
#   3. collapse any whitespace run to a single space, then trim
#   4. strip a leading/trailing title (both ends, applied prefix/suffix/prefix/suffix)
#
# A parity test (Python vs the SQL ``_NORM``) guards against divergence.
# ---------------------------------------------------------------------------

# Same accent map as the SQL ``translate(lower(full_name), _ACCENTS_FROM, _ACCENTS_TO)``.
_NORM_ACCENTS_FROM = "áàäâãéèëêíìïîóòöôõúùüûñçý"
_NORM_ACCENTS_TO = "aaaaaeeeeiiiiooooouuuuncy"
_NORM_ACCENT_TABLE = str.maketrans(_NORM_ACCENTS_FROM, _NORM_ACCENTS_TO)

# Titles/honorifics/suffixes, mirrored from the SQL ``_TITLES`` list in reconcile.py.
# Any of these may appear as a prefix OR suffix; they are stripped from both ends.
_NORM_TITLES = (
    r"mr|mrs|ms|miss|sir|dr|dr\(a\)|dott|dott\.ssa|ing|lic|mtro|prof|"
    r"sr|sra|sr\(a\)|sig|sig\.ra|md|phd|ph\.d|jr|ii|iii|iv|pi|dds|esq"
)
# Mirror the SQL ``regexp_replace(..., '^(<titles>)\.?\s+', '', 'i')`` (prefix) and the
# trailing variant. Case-insensitive, one optional dot, then a required space boundary.
_NORM_STRIP_PREFIX_RE = re.compile(rf"^({_NORM_TITLES})\.?\s+", re.IGNORECASE)
_NORM_STRIP_SUFFIX_RE = re.compile(rf"\s+({_NORM_TITLES})\.?$", re.IGNORECASE)


def compute_norm_name(full_name: str | None) -> str | None:
    """Return the canonical ``norm_name`` for a full name, mirroring the SQL ``_NORM``.

    Steps (identical to the SQL expression used by the batch jobs and backfill):
    lowercase, translate a fixed accent set to ASCII, collapse whitespace, trim, then
    strip a leading/trailing title from both ends (applied prefix, suffix, prefix,
    suffix — so a title on both ends is caught).

    Returns ``None`` when there is no usable name (``None``/empty), matching the SQL
    ``WHERE full_name IS NOT NULL`` guard: rows without a full name get a NULL
    ``norm_name`` rather than an empty string.
    """
    if full_name is None:
        return None
    # 1. lowercase, 2. translate accents (fixed map, NOT full NFKD — matches SQL).
    text = str(full_name).lower().translate(_NORM_ACCENT_TABLE)
    # 3. collapse whitespace runs to a single space, then trim.
    text = _WS_RE.sub(" ", text).strip()
    # 4. strip titles from both ends, twice (prefix, suffix, prefix, suffix).
    for pattern in (
        _NORM_STRIP_PREFIX_RE,
        _NORM_STRIP_SUFFIX_RE,
        _NORM_STRIP_PREFIX_RE,
        _NORM_STRIP_SUFFIX_RE,
    ):
        text = pattern.sub("", text).strip()
    return text or None
