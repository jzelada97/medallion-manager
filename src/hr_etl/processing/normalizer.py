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


def normalize_key(key: str) -> str:
    """Normalize a raw message key: lowercase, remove spaces/underscores/hyphens.

    Maps variants like 'E-Mail', 'Company Address', 'company_email' to canonical
    lowercase alphanumeric keys ('email', 'companyaddress', 'companyemail').
    """
    return re.sub(r"[\s_\-]+", "", str(key).strip().lower())


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
