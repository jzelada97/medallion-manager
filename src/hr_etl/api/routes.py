"""API routes: health, metrics, and read-only person queries."""

from __future__ import annotations

from fastapi import APIRouter, Query
from fastapi.responses import PlainTextResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from sqlalchemy import select

from hr_etl.models.db_models import PersonRow


def _row_to_dict(row: PersonRow) -> dict:
    return {
        "id": row.id,
        "passport": row.passport,
        "full_name": row.full_name,
        "name": row.name,
        "lastname": row.lastname,
        "sex": row.sex,
        "phone": row.phone,
        "email": row.email,
        "city": row.city,
        "address": row.address,
        "company": row.company,
        "job": row.job,
        "iban": row.iban,
        "salary": row.salary,
        "ipv4": row.ipv4,
    }


def build_router(session_factory) -> APIRouter:
    """Build the API router bound to a SQLAlchemy session factory."""
    router = APIRouter()

    @router.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @router.get("/metrics")
    def metrics() -> PlainTextResponse:
        return PlainTextResponse(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    @router.get("/persons")
    def list_persons(
        limit: int = Query(50, ge=1, le=500),
        offset: int = Query(0, ge=0),
        city: str | None = None,
        company: str | None = None,
    ) -> dict:
        session = session_factory()
        try:
            stmt = select(PersonRow)
            if city:
                stmt = stmt.where(PersonRow.city == city.strip().lower())
            if company:
                stmt = stmt.where(PersonRow.company == company.strip().lower())
            stmt = stmt.limit(limit).offset(offset)
            rows = session.execute(stmt).scalars().all()
            return {"count": len(rows), "items": [_row_to_dict(r) for r in rows]}
        finally:
            session.close()

    @router.get("/persons/{person_id}")
    def get_person(person_id: int) -> dict:
        session = session_factory()
        try:
            row = session.get(PersonRow, person_id)
            if row is None:
                return {"error": "not found"}
            return _row_to_dict(row)
        finally:
            session.close()

    return router
