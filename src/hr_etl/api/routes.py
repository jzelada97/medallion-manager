"""API routes: health, metrics, and read-only person queries."""

from __future__ import annotations

import json
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import PlainTextResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from sqlalchemy import func, or_, select, text

from hr_etl.models.db_models import FragmentLog, MatchCandidate, PersonReview, PersonRow
from hr_etl.processing.consolidator import consolidate
from hr_etl.models.raw import FragmentType


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

    @router.get("/gold/persons")
    def gold_persons(
        limit: int = Query(50, ge=1, le=500),
        offset: int = Query(0, ge=0),
        q: str | None = Query(None),
        city: str | None = None,
        company: str | None = None,
        job: str | None = None,
    ) -> dict:
        """Paginated person list optimised for the Gold/dashboard view.

        Uses the same Silver table but returns only the columns needed by the
        frontend, reducing payload size. Keyset pagination hint: pass the last
        seen ``id`` as ``after_id`` for O(1) seeks on large datasets.
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
                        PersonRow.norm_name.ilike(like),
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
                "items": [
                    {
                        "id": r.id,
                        "full_name": r.full_name,
                        "city": r.city,
                        "company": r.company,
                        "job": r.job,
                        "passport": r.passport,
                        "email": r.email,
                        "iban": r.iban,
                        "salary": r.salary,
                        "norm_name": r.norm_name,
                    }
                    for r in rows
                ],
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

    # ------------------------------------------------------------------ #
    # Review queue (duplicates)
    # ------------------------------------------------------------------ #

    @router.get("/review/queue")
    def review_queue(
        limit: int = Query(50, ge=1, le=500),
        offset: int = Query(0, ge=0),
    ) -> dict:
        """Pending duplicate pairs awaiting human review."""
        session = session_factory()
        try:
            stmt = (
                select(PersonReview)
                .where(PersonReview.status == "pending")
                .order_by(PersonReview.id)
                .limit(limit)
                .offset(offset)
            )
            rows = session.execute(stmt).scalars().all()
            total = session.execute(
                select(func.count())
                .select_from(PersonReview)
                .where(PersonReview.status == "pending")
            ).scalar_one()
            return {
                "total": total,
                "items": [
                    {
                        "id": r.id,
                        "person_id_a": r.person_id_a,
                        "person_id_b": r.person_id_b,
                        "status": r.status,
                    }
                    for r in rows
                ],
            }
        finally:
            session.close()

    @router.post("/review/{review_id}/same")
    def review_same(review_id: int) -> dict:
        """Mark a review pair as confirmed same person (merge A into B).

        Survivorship: all non-null fields from A fill gaps in B; A is deleted.
        """
        session = session_factory()
        try:
            review = session.get(PersonReview, review_id)
            if review is None:
                raise HTTPException(status_code=404, detail="review not found")
            if review.status != "pending":
                raise HTTPException(status_code=409, detail=f"review already {review.status}")

            row_a = session.get(PersonRow, review.person_id_a)
            row_b = session.get(PersonRow, review.person_id_b)
            if row_a is None or row_b is None:
                raise HTTPException(status_code=404, detail="one or both persons not found")

            # Merge A into B (fill B's gaps with A's values)
            for field in (
                "passport", "full_name", "name", "lastname", "sex", "phone", "email",
                "city", "address", "company", "company_address", "company_phone",
                "company_email", "job", "iban", "salary", "ipv4",
            ):
                val_a = getattr(row_a, field)
                if val_a not in (None, "") and getattr(row_b, field) in (None, ""):
                    setattr(row_b, field, val_a)

            # Re-point A's fragment_log rows to B, then delete A
            session.execute(
                text("UPDATE fragment_log SET person_id = :b WHERE person_id = :a"),
                {"b": row_b.id, "a": row_a.id},
            )
            session.delete(row_a)

            review.status = "same"
            review.reviewed_at = datetime.now(timezone.utc)
            session.commit()
            return {"merged_into": row_b.id}
        except HTTPException:
            raise
        except Exception as exc:
            session.rollback()
            raise HTTPException(status_code=500, detail=str(exc)) from exc
        finally:
            session.close()

    @router.post("/review/{review_id}/distinct")
    def review_distinct(review_id: int) -> dict:
        """SPLIT/UNMERGE: the two persons are different people.

        Recovers the original fragments from fragment_log, re-consolidates each
        person independently under their correct match_key, and refreshes both
        rows in the warehouse. Removes the pair from the review queue.
        """
        session = session_factory()
        try:
            review = session.get(PersonReview, review_id)
            if review is None:
                raise HTTPException(status_code=404, detail="review not found")
            if review.status != "pending":
                raise HTTPException(status_code=409, detail=f"review already {review.status}")

            row_a = session.get(PersonRow, review.person_id_a)
            row_b = session.get(PersonRow, review.person_id_b)
            if row_a is None or row_b is None:
                raise HTTPException(status_code=404, detail="one or both persons not found")

            results = []
            for row in (row_a, row_b):
                logs = (
                    session.execute(
                        select(FragmentLog).where(FragmentLog.person_id == row.id)
                    )
                    .scalars()
                    .all()
                )
                if not logs:
                    # No audit trail — nothing to split, leave row as-is
                    results.append({"person_id": row.id, "action": "unchanged", "reason": "no fragment_log"})
                    continue

                # Group fragments by their original match_key
                by_key: dict[str, list[tuple[dict, FragmentType]]] = {}
                for log in logs:
                    ftype = FragmentType(log.fragment_type)
                    msg = json.loads(log.payload)
                    by_key.setdefault(log.match_key, []).append((msg, ftype))

                if len(by_key) == 1:
                    # All fragments share the same key — nothing to split
                    results.append({"person_id": row.id, "action": "unchanged", "reason": "single key"})
                    continue

                # Re-consolidate each key group independently
                new_ids = []
                for key, frags in by_key.items():
                    person = consolidate(frags)
                    if person is None:
                        continue
                    person.match_key = key

                    # Check if a row already exists for this key
                    existing = session.execute(
                        select(PersonRow).where(PersonRow.match_key == key)
                    ).scalar_one_or_none()

                    if existing is None:
                        new_row = PersonRow(match_key=key)
                        for field in (
                            "passport", "full_name", "name", "lastname", "sex", "phone",
                            "email", "city", "address", "company", "company_address",
                            "company_phone", "company_email", "job", "iban", "salary", "ipv4",
                        ):
                            setattr(new_row, field, getattr(person, field))
                        session.add(new_row)
                        session.flush()  # get new_row.id
                        # Re-point fragment_log rows
                        for log in logs:
                            if log.match_key == key:
                                log.person_id = new_row.id
                        new_ids.append(new_row.id)
                    else:
                        new_ids.append(existing.id)

                # Delete the original merged row if it was replaced
                if row.id not in new_ids:
                    session.delete(row)

                results.append({"person_id": row.id, "action": "split", "new_ids": new_ids})

            review.status = "distinct"
            review.reviewed_at = datetime.now(timezone.utc)
            session.commit()
            return {"status": "split", "results": results}
        except HTTPException:
            raise
        except Exception as exc:
            session.rollback()
            raise HTTPException(status_code=500, detail=str(exc)) from exc
        finally:
            session.close()

    @router.post("/review/enqueue")
    def enqueue_review(person_id_a: int, person_id_b: int) -> dict:
        """Add a candidate pair to the review queue (idempotent)."""
        session = session_factory()
        try:
            # Check both persons exist
            for pid in (person_id_a, person_id_b):
                if session.get(PersonRow, pid) is None:
                    raise HTTPException(status_code=404, detail=f"person {pid} not found")
            # Idempotent: skip if already pending
            existing = session.execute(
                select(PersonReview).where(
                    PersonReview.person_id_a == min(person_id_a, person_id_b),
                    PersonReview.person_id_b == max(person_id_a, person_id_b),
                    PersonReview.status == "pending",
                )
            ).scalar_one_or_none()
            if existing:
                return {"id": existing.id, "created": False}
            review = PersonReview(
                person_id_a=min(person_id_a, person_id_b),
                person_id_b=max(person_id_a, person_id_b),
            )
            session.add(review)
            session.commit()
            return {"id": review.id, "created": True}
        except HTTPException:
            raise
        except Exception as exc:
            session.rollback()
            raise HTTPException(status_code=500, detail=str(exc)) from exc
        finally:
            session.close()

    return router
