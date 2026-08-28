"""API routes: health, metrics, and read-only person queries."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import PlainTextResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from sqlalchemy import func, or_, select, text

from hr_etl.models.db_models import MatchCandidate, PersonRow


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
        "company_address": row.company_address,
        "company_phone": row.company_phone,
        "company_email": row.company_email,
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
        q: str | None = Query(None, description="Free-text search on name/company/email"),
        city: str | None = None,
        company: str | None = None,
        job: str | None = None,
    ) -> dict:
        """List consolidated persons with filters, free-text search and pagination.

        Returns total (across all matches), plus the requested page of items.
        """
        session = session_factory()
        try:
            filters = []
            if city:
                filters.append(func.lower(PersonRow.city) == city.strip().lower())
            if company:
                filters.append(PersonRow.company.ilike(f"%{company.strip()}%"))
            if job:
                filters.append(PersonRow.job.ilike(f"%{job.strip()}%"))
            if q:
                like = f"%{q.strip()}%"
                filters.append(
                    or_(
                        PersonRow.full_name.ilike(like),
                        PersonRow.name.ilike(like),
                        PersonRow.lastname.ilike(like),
                        PersonRow.company.ilike(like),
                        PersonRow.email.ilike(like),
                    )
                )

            base = select(PersonRow)
            count_stmt = select(func.count()).select_from(PersonRow)
            for f in filters:
                base = base.where(f)
                count_stmt = count_stmt.where(f)

            total = session.execute(count_stmt).scalar_one()
            rows = (
                session.execute(base.order_by(PersonRow.id).limit(limit).offset(offset))
                .scalars()
                .all()
            )

            return {
                "total": total,
                "count": len(rows),
                "limit": limit,
                "offset": offset,
                "items": [_row_to_dict(r) for r in rows],
            }
        finally:
            session.close()

    @router.get("/persons/{person_id}")
    def get_person(person_id: int) -> dict:
        session = session_factory()
        try:
            row = session.get(PersonRow, person_id)
            if row is None:
                raise HTTPException(status_code=404, detail="person not found")
            return _row_to_dict(row)
        finally:
            session.close()

    @router.get("/stats")
    def stats() -> dict:
        """Aggregated summary for dashboards/demo: totals and top groupings."""
        session = session_factory()
        try:
            total = session.execute(select(func.count()).select_from(PersonRow)).scalar_one()

            def top(column, limit: int = 5) -> list[dict]:
                stmt = (
                    select(column, func.count().label("n"))
                    .where(column.isnot(None))
                    .group_by(column)
                    .order_by(func.count().desc())
                    .limit(limit)
                )
                return [{"value": v, "count": n} for v, n in session.execute(stmt).all()]

            return {
                "total_persons": total,
                "top_cities": top(PersonRow.city),
                "top_companies": top(PersonRow.company),
                "with_bank": session.execute(
                    select(func.count()).select_from(PersonRow).where(PersonRow.iban.isnot(None))
                ).scalar_one(),
            }
        finally:
            session.close()

    @router.get("/candidates")
    def list_candidates(
        limit: int = Query(50, ge=1, le=500),
        min_confidence: float = Query(0.5, ge=0.0, le=1.0),
    ) -> dict:
        """List probable duplicate candidates detected by batch reconciliation."""
        session = session_factory()
        try:
            stmt = (
                select(MatchCandidate)
                .where(MatchCandidate.confidence >= min_confidence)
                .order_by(MatchCandidate.confidence.desc())
                .limit(limit)
            )
            rows = session.execute(stmt).scalars().all()
            total = session.execute(
                select(func.count())
                .select_from(MatchCandidate)
                .where(MatchCandidate.confidence >= min_confidence)
            ).scalar_one()
            return {
                "total": total,
                "count": len(rows),
                "items": [
                    {
                        "id": r.id,
                        "person_id_a": r.person_id_a,
                        "person_id_b": r.person_id_b,
                        "confidence": r.confidence,
                        "reason": r.reason,
                    }
                    for r in rows
                ],
            }
        finally:
            session.close()

    @router.get("/gold/stats")
    def gold_stats() -> dict:
        """Pre-computed Gold layer statistics (faster than live aggregation)."""
        session = session_factory()
        try:
            row = session.execute(text("SELECT * FROM gold_stats WHERE id = 1")).fetchone()
            if row is None:
                return {"error": "gold layer not refreshed yet"}
            return {
                "total_persons": row.total_persons,
                "with_passport": row.with_passport,
                "with_city": row.with_city,
                "with_company": row.with_company,
                "with_bank": row.with_bank,
                "with_ipv4": row.with_ipv4,
                "cross_linked": row.cross_linked,
                "avg_completeness": round(row.avg_completeness, 2),
            }
        finally:
            session.close()

    @router.get("/gold/completeness")
    def gold_completeness() -> dict:
        """Distribution of field completeness across persons (Gold layer)."""
        session = session_factory()
        try:
            rows = session.execute(
                text(
                    "SELECT fields_filled, person_count FROM gold_completeness ORDER BY fields_filled"
                )
            ).fetchall()
            return {
                "distribution": [
                    {"fields_filled": r.fields_filled, "count": r.person_count} for r in rows
                ]
            }
        finally:
            session.close()

    return router
