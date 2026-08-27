"""Tests for the normalizer."""

from __future__ import annotations

import pytest

from hr_etl.processing.normalizer import (
    clean_salary,
    normalize_key,
    normalize_message,
    normalize_text,
    strip_accents,
)


def test_strip_accents():
    assert strip_accents("Málaga") == "Malaga"
    assert strip_accents("José") == "Jose"


def test_normalize_text_none_and_whitespace():
    assert normalize_text(None) == ""
    assert normalize_text("  Ana   Gil  ") == "ana gil"
    assert normalize_text("MÁLAGA") == "malaga"


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("E-Mail", "email"),
        ("Company Address", "companyaddress"),
        ("company_email", "companyemail"),
        ("  Telfnumber ", "telfnumber"),
    ],
)
def test_normalize_key(raw, expected):
    assert normalize_key(raw) == expected


def test_normalize_message_keys():
    out = normalize_message({"E-Mail": "a@b.c", "Company Address": "x"})
    assert out == {"email": "a@b.c", "companyaddress": "x"}


@pytest.mark.parametrize(
    "value,expected",
    [
        ("1.500,75", 1500.75),
        ("2000", 2000.0),
        (3000, 3000.0),
        (2500.5, 2500.5),
        ("$1,200.50", 1200.50),
        (None, None),
        ("", None),
        ("abc", None),
    ],
)
def test_clean_salary(value, expected):
    assert clean_salary(value) == expected


def test_normalize_key_typo_alias():
    """'Company Adress' (single d, as in generator README) resolves to canonical key."""
    assert normalize_key("Company Adress") == "companyaddress"
