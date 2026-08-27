"""Raw fragment types.

Messages arrive fragmented: each Kafka message carries ONE of five schemas
(Personal, Location, Professional, Bank, Net). The consolidation step joins the
fragments that belong to the same person.

The field names below come exclusively from the public README of the data server;
no generator code was inspected (Rule #1).
"""

from __future__ import annotations

from enum import Enum


class FragmentType(str, Enum):
    """The five HR fragment schemas."""

    PERSONAL = "personal"
    LOCATION = "location"
    PROFESSIONAL = "professional"
    BANK = "bank"
    NET = "net"
    UNKNOWN = "unknown"


# Distinguishing keys per schema (as documented in the server README).
# Detection is based on the set of keys present in the message.
_SCHEMA_KEYS: dict[FragmentType, frozenset[str]] = {
    FragmentType.PERSONAL: frozenset(
        {"name", "lastname", "sex", "telfnumber", "passport", "email"}
    ),
    FragmentType.LOCATION: frozenset({"fullname", "city", "address"}),
    FragmentType.PROFESSIONAL: frozenset(
        {"fullname", "company", "companyaddress", "companytelfnumber", "companyemail", "job"}
    ),
    FragmentType.BANK: frozenset({"passport", "iban", "salary"}),
    FragmentType.NET: frozenset({"address", "ipv4"}),
}


def schema_keys() -> dict[FragmentType, frozenset[str]]:
    """Return a copy of the schema-key mapping (for detector/tests)."""
    return dict(_SCHEMA_KEYS)
