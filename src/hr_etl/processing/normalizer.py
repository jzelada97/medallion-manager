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
_TITLE_RE = re.compile(
    r"^(mr\.?|mrs\.?|ms\.?|dr\.?|dr\(a\)\.?|dott\.?|dott\.ssa|"
    r"ing\.?|lic\.?|mtro\.?|prof\.?|"
    r"sr\.?|sra\.?|sr\(a\)\.?|sig\.?|sig\.ra)\s+",
    re.IGNORECASE,
)


def strip_titles(text: str) -> str:
    """Remove honorific/professional title prefixes from a name."""
    return _TITLE_RE.sub("", text).strip()


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
