"""Tests for the consolidator (join of fragments)."""

from __future__ import annotations

from hr_etl.models.raw import FragmentType
from hr_etl.processing.consolidator import consolidate, fragment_to_person


def test_fragment_to_person_personal(personal_fragment):
    p = fragment_to_person(personal_fragment, FragmentType.PERSONAL, "passport:x1234567")
    assert p.name == "Ana"
    assert p.email == "ana@example.com"
    assert p.full_name == "ana gil"


def test_consolidate_personal_and_bank(personal_fragment, bank_fragment):
    person = consolidate(
        [(personal_fragment, FragmentType.PERSONAL), (bank_fragment, FragmentType.BANK)]
    )
    assert person is not None
    assert person.match_key == "passport:x1234567"
    assert person.iban == "ES9121000418450200051332"
    assert person.salary == 1500.75
    assert person.name == "Ana"


def test_consolidate_merges_location(personal_fragment, location_fragment):
    # location matches by name (no passport), personal has passport -> distinct keys.
    # Consolidate as a single logical person passing both explicitly.
    person = consolidate(
        [(personal_fragment, FragmentType.PERSONAL), (location_fragment, FragmentType.LOCATION)]
    )
    assert person is not None
    # first fragment's key wins as the canonical key
    assert person.match_key == "passport:x1234567"
    assert person.city == "Madrid"
    assert person.address == "Calle Mayor 1"


def test_consolidate_no_usable_key_returns_none():
    assert consolidate([({"IPv4": "10.0.0.1"}, FragmentType.NET)]) is None


def test_consolidate_empty_returns_none():
    assert consolidate([]) is None


def test_consolidate_skips_orphan_but_keeps_valid(personal_fragment):
    """A fragment without a usable key is skipped; the valid one still consolidates."""
    orphan = ({"IPv4": "9.9.9.9"}, FragmentType.NET)
    person = consolidate([(personal_fragment, FragmentType.PERSONAL), orphan])
    assert person is not None
    assert person.match_key == "passport:x1234567"


def test_fragment_to_person_net_and_professional():
    net = fragment_to_person({"Address": "Calle 1", "IPv4": "1.2.3.4"}, FragmentType.NET, "addr:calle 1")
    assert net.ipv4 == "1.2.3.4"
    prof = fragment_to_person(
        {"Fullname": "Ana Gil", "Company": "ACME", "Job": "Eng"},
        FragmentType.PROFESSIONAL,
        "name:ana gil",
    )
    assert prof.company == "ACME"
    assert prof.job == "Eng"
