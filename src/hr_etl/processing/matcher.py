"""Matching strategy: map a fragment to a stable person key.

There is no global unique id. We derive a matching key by priority:
1. passport (Personal, Bank)
2. normalized full name (Location, Professional; Personal via name+lastname)
3. normalized address (Location, Net bridge)

Fragments that produce no key are treated as orphans by the consolidator.
"""

from __future__ import annotations

from typing import Any

from hr_etl.models.raw import FragmentType
from hr_etl.processing.normalizer import normalize_message, normalize_text


def build_full_name(norm_msg: dict[str, Any]) -> str:
    """Build a normalized full name from either fullname or name+lastname."""
    if norm_msg.get("fullname"):
        return normalize_text(norm_msg["fullname"])
    name = normalize_text(norm_msg.get("name"))
    lastname = normalize_text(norm_msg.get("lastname"))
    return normalize_text(f"{name} {lastname}")


def match_key(message: dict[str, Any], ftype: FragmentType) -> str:
    """Compute the person matching key for a fragment.

    Returns an empty string if no usable key can be derived (orphan fragment).
    """
    norm = normalize_message(message)

    passport = normalize_text(norm.get("passport"))
    if passport:
        return f"passport:{passport}"

    full_name = build_full_name(norm)
    if full_name:
        return f"name:{full_name}"

    address = normalize_text(norm.get("address"))
    if address:
        return f"addr:{address}"

    return ""
