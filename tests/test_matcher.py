"""Tests for the matching-key strategy."""

from __future__ import annotations

from hr_etl.models.raw import FragmentType
from hr_etl.processing.matcher import build_full_name, match_key


def test_full_name_from_fullname():
    assert build_full_name({"fullname": "Ana Gil"}) == "ana gil"


def test_full_name_from_name_lastname():
    assert build_full_name({"name": "Ana", "lastname": "Gil"}) == "ana gil"


def test_match_key_prefers_passport(personal_fragment):
    from hr_etl.processing.normalizer import normalize_message

    key = match_key(normalize_message(personal_fragment), FragmentType.PERSONAL)
    assert key == "passport:x1234567"


def test_match_key_name_when_no_passport(location_fragment):
    key = match_key(location_fragment, FragmentType.LOCATION)
    assert key == "name:ana gil"


def test_match_key_address_bridge():
    key = match_key({"Address": "Calle Mayor 1", "IPv4": "10.0.0.1"}, FragmentType.NET)
    assert key == "addr:calle mayor 1"


def test_match_key_orphan_returns_empty():
    assert match_key({"IPv4": "10.0.0.1"}, FragmentType.NET) == ""
