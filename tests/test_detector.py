"""Tests for the fragment type detector."""

from __future__ import annotations

from hr_etl.models.raw import FragmentType
from hr_etl.processing.detector import detect_type


def test_detect_personal(personal_fragment):
    assert detect_type(personal_fragment) == FragmentType.PERSONAL


def test_detect_bank(bank_fragment):
    assert detect_type(bank_fragment) == FragmentType.BANK


def test_detect_location(location_fragment):
    assert detect_type(location_fragment) == FragmentType.LOCATION


def test_detect_professional():
    msg = {
        "Fullname": "Ana Gil",
        "Company": "ACME",
        "Company Address": "Av 1",
        "Company Telfnumber": "900",
        "Company E-Mail": "hr@acme.com",
        "Job": "Engineer",
    }
    assert detect_type(msg) == FragmentType.PROFESSIONAL


def test_detect_net():
    assert detect_type({"Address": "Calle Mayor 1", "IPv4": "10.0.0.1"}) == FragmentType.NET


def test_detect_empty_is_unknown():
    assert detect_type({}) == FragmentType.UNKNOWN


def test_detect_garbage_is_unknown():
    assert detect_type({"foo": 1, "bar": 2}) == FragmentType.UNKNOWN
