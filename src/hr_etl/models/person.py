"""Consolidated person model (the curated record stored in the warehouse)."""

from __future__ import annotations

from pydantic import BaseModel, Field


class Person(BaseModel):
    """A consolidated person built by joining fragments of the same individual."""

    # Identity / personal
    passport: str | None = None
    full_name: str | None = None
    name: str | None = None
    lastname: str | None = None
    sex: str | None = None
    phone: str | None = None
    email: str | None = None

    # Location
    city: str | None = None
    address: str | None = None

    # Professional
    company: str | None = None
    company_address: str | None = None
    company_phone: str | None = None
    company_email: str | None = None
    job: str | None = None

    # Bank
    iban: str | None = None
    salary: float | None = None

    # Net
    ipv4: str | None = None

    # Matching key used for dedup/upsert when passport is missing
    match_key: str = Field(default="")

    def merge(self, other: "Person") -> "Person":
        """Return a new Person combining self with non-null fields from other."""
        data = self.model_dump()
        for field, value in other.model_dump().items():
            if value not in (None, "") and data.get(field) in (None, ""):
                data[field] = value
        return Person(**data)

    def filled_fields(self) -> int:
        """Count non-empty fields (excludes match_key)."""
        return sum(
            1
            for k, v in self.model_dump().items()
            if k != "match_key" and v not in (None, "")
        )
