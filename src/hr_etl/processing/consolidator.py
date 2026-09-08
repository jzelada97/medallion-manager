"""Consolidate fragments of the same person into a single Person record."""

from __future__ import annotations

from typing import Any

from hr_etl.models.person import Person
from hr_etl.models.raw import FragmentType
from hr_etl.processing.matcher import build_full_name, match_key
from hr_etl.processing.normalizer import clean_salary, normalize_message


def _scalar(value: Any) -> Any:
    """If value is a list, return the first element; otherwise return as-is."""
    if isinstance(value, list):
        return value[0] if value else None
    return value


def fragment_to_person(message: dict[str, Any], ftype: FragmentType, key: str) -> Person:
    """Map a single normalized fragment into a partial Person."""
    n = normalize_message(message)
    data: dict[str, Any] = {"match_key": key}

    if ftype == FragmentType.PERSONAL:
        data.update(
            name=_scalar(n.get("name")),
            lastname=_scalar(n.get("lastname")),
            sex=_scalar(n.get("sex")),
            phone=_scalar(n.get("telfnumber")),
            passport=_scalar(n.get("passport")),
            email=_scalar(n.get("email")),
            full_name=build_full_name(n) or None,
        )
    elif ftype == FragmentType.LOCATION:
        data.update(
            full_name=_scalar(n.get("fullname")),
            city=_scalar(n.get("city")),
            address=_scalar(n.get("address")),
        )
    elif ftype == FragmentType.PROFESSIONAL:
        data.update(
            full_name=_scalar(n.get("fullname")),
            company=_scalar(n.get("company")),
            company_address=_scalar(n.get("companyaddress")),
            company_phone=_scalar(n.get("companytelfnumber")),
            company_email=_scalar(n.get("companyemail")),
            job=_scalar(n.get("job")),
        )
    elif ftype == FragmentType.BANK:
        data.update(
            passport=_scalar(n.get("passport")),
            iban=_scalar(n.get("iban")),
            salary=clean_salary(_scalar(n.get("salary"))),
        )
    elif ftype == FragmentType.NET:
        data.update(
            address=_scalar(n.get("address")),
            ipv4=_scalar(n.get("ipv4")),
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
