"""Detect the fragment type of an incoming message based on its keys."""

from __future__ import annotations

from typing import Any

from hr_etl.models.raw import FragmentType, schema_keys
from hr_etl.processing.normalizer import normalize_message


def detect_type(message: dict[str, Any]) -> FragmentType:
    """Return the FragmentType that best matches the message keys.

    The best match is the schema whose defining keys are most fully covered by
    the (normalized) message keys, requiring the schema's signature key(s).
    """
    if not message:
        return FragmentType.UNKNOWN

    keys = set(normalize_message(message).keys())
    schemas = schema_keys()

    best_type = FragmentType.UNKNOWN
    best_score = 0.0
    for ftype, expected in schemas.items():
        overlap = len(keys & expected)
        if overlap == 0:
            continue
        score = overlap / len(expected)
        # Prefer the schema with the highest coverage ratio; break ties by overlap size.
        if score > best_score or (score == best_score and overlap > 0 and ftype == best_type):
            best_score = score
            best_type = ftype

    # Require at least half the schema keys to be present to accept the match.
    return best_type if best_score >= 0.5 else FragmentType.UNKNOWN
