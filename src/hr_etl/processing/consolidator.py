"""Consolidate fragments of the same person into a single Person record."""

from __future__ import annotations

from typing import Any

from hr_etl.models.person import Person
from hr_etl.models.raw import FragmentType
from hr_etl.processing.matcher import build_full_name, match_key
from hr_etl.processing.normalizer import clean_salary, normalize_message


def fragment_to_person(message: dict[str, Any], ftype: FragmentType, key: str) -> Person:
    """Map a single normalized fragment into a partial Person."""
    n = normalize_message(message)
    data: dict[str, Any] = {"match_key": key}

    if ftype == FragmentType.PERSONAL:
        data.update(
            name=n.get("name"),
            lastname=n.get("lastname"),
            sex=n.get("sex"),
            phone=n.get("telfnumber"),
            passport=n.get("passport"),
            email=n.get("email"),
            full_name=build_full_name(n) or None,
        )
    elif ftype == FragmentType.LOCATION:
        data.update(
            full_name=n.get("fullname"),
            city=n.get("city"),
            address=n.get("address"),
        )
    elif ftype == FragmentType.PROFESSIONAL:
        data.update(
            full_name=n.get("fullname"),
            company=n.get("company"),
            company_address=n.get("companyaddress"),
            company_phone=n.get("companytelfnumber"),
            company_email=n.get("companyemail"),
            job=n.get("job"),
        )
    elif ftype == FragmentType.BANK:
        data.update(
            passport=n.get("passport"),
            iban=n.get("iban"),
            salary=clean_salary(n.get("salary")),
        )
    elif ftype == FragmentType.NET:
        data.update(
            address=n.get("address"),
            ipv4=n.get("ipv4"),
        )

    # Drop empty values so merge() does not overwrite good data with blanks.
    clean = {k: v for k, v in data.items() if v not in (None, "")}
    clean["match_key"] = key
    return Person(**clean)


def consolidate(fragments: list[tuple[dict[str, Any], FragmentType]]) -> Person | None:
    """Merge a list of (message, type) fragments into one Person.

    Returns None if no fragment yields a usable matching key.
    """
    people: list[Person] = []
    key = ""
    for message, ftype in fragments:
        k = match_key(message, ftype)
        if not k:
            continue
        key = key or k
        people.append(fragment_to_person(message, ftype, key))

    if not people:
        return None

    result = people[0]
    for p in people[1:]:
        result = result.merge(p)
    result.match_key = key
    return result
