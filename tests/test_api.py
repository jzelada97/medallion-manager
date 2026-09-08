"""Tests for the FastAPI read-only API."""

from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient

from hr_etl.api.routes import build_router
from hr_etl.models.person import Person
from hr_etl.warehouse.person_repo import PersonRepository


def _client(sqlite_session_factory) -> TestClient:
    app = FastAPI()
    app.include_router(build_router(sqlite_session_factory))
    return TestClient(app)


def test_health(sqlite_session_factory):
    client = _client(sqlite_session_factory)
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


def test_metrics_endpoint(sqlite_session_factory):
    client = _client(sqlite_session_factory)
    resp = client.get("/metrics")
    assert resp.status_code == 200
    assert "hr_etl" in resp.text


def test_list_and_get_persons(sqlite_session_factory):
    repo = PersonRepository(sqlite_session_factory)
    repo.upsert(Person(match_key="passport:x1", name="Ana", city="madrid", full_name="ana gil"))
    client = _client(sqlite_session_factory)

    resp = client.get("/persons")
    assert resp.status_code == 200
    body = resp.json()
    assert body["count"] == 1
    pid = body["items"][0]["id"]

    resp2 = client.get(f"/persons/{pid}")
    assert resp2.status_code == 200
    assert resp2.json()["name"] == "Ana"


def test_get_missing_person_returns_404(sqlite_session_factory):
    client = _client(sqlite_session_factory)
    resp = client.get("/persons/999")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "person not found"


def test_list_persons_filter(sqlite_session_factory):
    repo = PersonRepository(sqlite_session_factory)
    repo.upsert(Person(match_key="k1", city="madrid"))
    repo.upsert(Person(match_key="k2", city="sevilla"))
    client = _client(sqlite_session_factory)
    resp = client.get("/persons", params={"city": "madrid"})
    assert resp.json()["count"] == 1


def test_free_text_search(sqlite_session_factory):
    repo = PersonRepository(sqlite_session_factory)
    repo.upsert(Person(match_key="k1", full_name="ana gil", company="acme"))
    repo.upsert(Person(match_key="k2", full_name="beatriz ruiz", company="globex"))
    client = _client(sqlite_session_factory)

    # search by partial name
    resp = client.get("/persons", params={"q": "ana"})
    body = resp.json()
    assert body["total"] == 1
    assert body["items"][0]["full_name"] == "ana gil"

    # search by company
    resp2 = client.get("/persons", params={"q": "globex"})
    assert resp2.json()["total"] == 1


def test_pagination_total_vs_page(sqlite_session_factory):
    repo = PersonRepository(sqlite_session_factory)
    for i in range(5):
        repo.upsert(Person(match_key=f"k{i}", name=f"p{i}"))
    client = _client(sqlite_session_factory)

    resp = client.get("/persons", params={"limit": 2, "offset": 0})
    body = resp.json()
    assert body["total"] == 5  # total across all matches
    assert body["count"] == 2  # only this page
    assert body["limit"] == 2
    assert body["offset"] == 0


def test_stats_endpoint(sqlite_session_factory):
    repo = PersonRepository(sqlite_session_factory)
    repo.upsert(Person(match_key="k1", city="madrid", company="acme", iban="ES1"))
    repo.upsert(Person(match_key="k2", city="madrid", company="globex"))
    client = _client(sqlite_session_factory)

    resp = client.get("/stats")
    assert resp.status_code == 200
    body = resp.json()
    assert body["total_persons"] == 2
    assert body["with_bank"] == 1
    # madrid appears twice -> should be the top city
    assert body["top_cities"][0]["value"] == "madrid"
    assert body["top_cities"][0]["count"] == 2


def test_list_persons_company_and_job_filters(sqlite_session_factory):
    repo = PersonRepository(sqlite_session_factory)
    repo.upsert(Person(match_key="k1", company="Acme Corp", job="Engineer"))
    repo.upsert(Person(match_key="k2", company="Globex", job="Analyst"))
    client = _client(sqlite_session_factory)

    assert client.get("/persons", params={"company": "acme"}).json()["count"] == 1
    assert client.get("/persons", params={"job": "analyst"}).json()["count"] == 1


def test_candidates_endpoint(sqlite_session_factory):
    from hr_etl.models.db_models import MatchCandidate

    session = sqlite_session_factory()
    try:
        session.add_all(
            [
                MatchCandidate(person_id_a=1, person_id_b=2, confidence=0.9, reason="strong"),
                MatchCandidate(person_id_a=3, person_id_b=4, confidence=0.3, reason="weak"),
            ]
        )
        session.commit()
    finally:
        session.close()

    client = _client(sqlite_session_factory)
    # default min_confidence=0.5 filters out the weak pair
    body = client.get("/candidates").json()
    assert body["total"] == 1
    assert body["count"] == 1
    assert body["items"][0]["reason"] == "strong"

    # lower threshold surfaces both, ordered by confidence desc
    body_all = client.get("/candidates", params={"min_confidence": 0.0}).json()
    assert body_all["total"] == 2
    assert body_all["items"][0]["confidence"] == 0.9


def _create_sqlite_gold_tables(session_factory) -> None:
    """Create SQLite-compatible Gold tables so the read-only endpoints can be tested.

    The production Gold schema is Postgres-only (tested in test_gold_layer.py); here we
    only need tables shaped like the ones the API reads from.
    """
    from sqlalchemy import text

    session = session_factory()
    try:
        session.execute(
            text(
                "CREATE TABLE gold_stats (id INTEGER PRIMARY KEY, total_persons INTEGER, "
                "with_passport INTEGER, with_city INTEGER, with_company INTEGER, "
                "with_bank INTEGER, with_ipv4 INTEGER, cross_linked INTEGER, "
                "avg_completeness FLOAT)"
            )
        )
        session.execute(
            text(
                "CREATE TABLE gold_completeness (fields_filled INTEGER PRIMARY KEY, "
                "person_count INTEGER)"
            )
        )
        # members is JSONB in production; TEXT here (SQLite has no JSONB). The endpoint
        # parses a JSON string back to a list, so the response shape is identical.
        session.execute(
            text(
                "CREATE TABLE gold_duplicate_groups (group_id INTEGER PRIMARY KEY, "
                "member_count INTEGER, max_confidence FLOAT, reason VARCHAR(255), "
                "members TEXT)"
            )
        )
        session.commit()
    finally:
        session.close()


def test_gold_persons_endpoint_lists_only_gold(sqlite_session_factory):
    """/gold/persons reads from gold_persons, not Silver — Silver-only rows never show.

    ``gold_persons`` is an ORM table (``GoldPerson``), already created by the
    ``sqlite_session_factory`` fixture's ``Base.metadata.create_all`` — no extra setup.
    """
    from hr_etl.models.db_models import GoldPerson
    from hr_etl.models.person import Person

    # Silver has 2 persons; only one of them "graduated" to Gold.
    repo = PersonRepository(sqlite_session_factory)
    repo.upsert(Person(match_key="k1", full_name="ana gil", city="madrid", company="acme"))
    repo.upsert(Person(match_key="k2", full_name="beatriz ruiz", city="sevilla"))

    session = sqlite_session_factory()
    try:
        session.add(
            GoldPerson(
                id=999,
                full_name="ana gil",
                city="madrid",
                company="acme",
                completeness=1.0,
            )
        )
        session.commit()
    finally:
        session.close()

    client = _client(sqlite_session_factory)
    # with_total=true asks for the exact COUNT (skipped by default during keyset paging).
    resp = client.get("/gold/persons", params={"with_total": True})
    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 1
    assert body["items"][0]["full_name"] == "ana gil"
    assert body["items"][0]["completeness"] == 1.0


def test_gold_persons_endpoint_filters_and_pagination(sqlite_session_factory):
    from hr_etl.models.db_models import GoldPerson

    session = sqlite_session_factory()
    try:
        session.add_all(
            [
                GoldPerson(id=1, full_name="ana gil", city="madrid", company="acme"),
                GoldPerson(id=2, full_name="beatriz ruiz", city="sevilla", company="globex"),
                GoldPerson(id=3, full_name="carla diaz", city="madrid", company="acme"),
            ]
        )
        session.commit()
    finally:
        session.close()

    client = _client(sqlite_session_factory)

    assert client.get("/gold/persons", params={"city": "madrid"}).json()["count"] == 2
    assert (
        client.get("/gold/persons", params={"q": "beatriz", "with_total": True}).json()["total"]
        == 1
    )

    # First page (with total) + keyset cursor for the next page.
    page = client.get("/gold/persons", params={"limit": 2, "with_total": True}).json()
    assert page["total"] == 3
    assert page["count"] == 2
    assert page["has_more"] is True
    # Next page via the cursor returns the remaining row (id > next_cursor).
    page2 = client.get("/gold/persons", params={"limit": 2, "after_id": page["next_cursor"]}).json()
    assert page2["count"] == 1
    assert page2["has_more"] is False


def test_gold_persons_endpoint_empty_when_not_refreshed(sqlite_session_factory):
    """No error if gold_persons exists but is empty (Gold not refreshed yet)."""
    client = _client(sqlite_session_factory)
    body = client.get("/gold/persons", params={"with_total": True}).json()
    assert body == {
        "total": 0,
        "count": 0,
        "limit": 50,
        "offset": 0,
        "next_cursor": None,
        "has_more": False,
        "items": [],
    }


def test_gold_stats_endpoint(sqlite_session_factory):
    _create_sqlite_gold_tables(sqlite_session_factory)
    from sqlalchemy import text

    session = sqlite_session_factory()
    try:
        session.execute(text("INSERT INTO gold_stats VALUES (1, 10, 8, 7, 6, 3, 2, 5, 4.5)"))
        session.commit()
    finally:
        session.close()

    client = _client(sqlite_session_factory)
    body = client.get("/gold/stats").json()
    assert body["total_persons"] == 10
    assert body["with_passport"] == 8
    assert body["cross_linked"] == 5
    assert body["avg_completeness"] == 4.5


def test_gold_stats_endpoint_not_refreshed(sqlite_session_factory):
    _create_sqlite_gold_tables(sqlite_session_factory)  # table exists but empty
    client = _client(sqlite_session_factory)
    body = client.get("/gold/stats").json()
    assert body == {"error": "gold layer not refreshed yet"}


def test_gold_completeness_endpoint(sqlite_session_factory):
    _create_sqlite_gold_tables(sqlite_session_factory)
    from sqlalchemy import text

    session = sqlite_session_factory()
    try:
        session.execute(text("INSERT INTO gold_completeness VALUES (2, 4), (5, 1)"))
        session.commit()
    finally:
        session.close()

    client = _client(sqlite_session_factory)
    body = client.get("/gold/completeness").json()
    dist = body["distribution"]
    assert dist == [
        {"fields_filled": 2, "count": 4},
        {"fields_filled": 5, "count": 1},
    ]


# ======================================================================
# QA security tests for the exposure layer of the reconcile/Gold subsystem
# (security-reconcile.md SEC-T4, SEC-T6). Unit-level, SQLite-backed.
# ======================================================================

# Financial/identifier fields that must NEVER appear in a /groups member payload.
_FORBIDDEN_GROUP_MEMBER_FIELDS = {"passport", "iban", "salary", "email", "phone", "ipv4"}


def _seed_duplicate_group(session_factory) -> None:
    """Seed a materialized gold_duplicate_groups row whose members carry ONLY the four
    display fields (person_id/full_name/city/company). The endpoint reads this table
    directly; the materialization in gold_layer.py is what guarantees the member payload
    never includes PII, and test_gold_layer.py covers that build against Postgres. Here we
    verify the endpoint itself does not leak extra fields."""
    import json as _json

    from sqlalchemy import text

    _create_sqlite_gold_tables(session_factory)
    members = [
        {"person_id": 1, "full_name": "jean leclerc", "city": "paris", "company": "acme"},
        {"person_id": 2, "full_name": "jean leclercq", "city": "paris", "company": "acme"},
    ]
    session = session_factory()
    try:
        session.execute(
            text(
                "INSERT INTO gold_duplicate_groups "
                "(group_id, member_count, max_confidence, reason, members) "
                "VALUES (1, 2, 0.92, 'fuzzy_name', :members)"
            ),
            {"members": _json.dumps(members)},
        )
        session.commit()
    finally:
        session.close()


def test_sec_t4_groups_payload_excludes_financial_and_identifier_pii(sqlite_session_factory):
    """SEC-T4: /groups members expose only person_id/full_name/city/company — never
    passport/iban/salary/email/phone/ipv4 (DP-2 data minimization)."""
    _seed_duplicate_group(sqlite_session_factory)
    client = _client(sqlite_session_factory)

    resp = client.get("/groups", params={"min_confidence": 0.5})
    assert resp.status_code == 200
    body = resp.json()

    assert body["total_groups"] == 1
    group = body["groups"][0]
    assert len(group["members"]) == 2
    for member in group["members"]:
        assert set(member.keys()) == {"person_id", "full_name", "city", "company"}
        assert _FORBIDDEN_GROUP_MEMBER_FIELDS.isdisjoint(member.keys())


def test_sec_t6_injection_in_q_is_sanitized(sqlite_session_factory):
    """SEC-T6: SQL-injection-shaped free text in `q` is treated as a literal search
    string (ORM ilike bind param), returning a safe empty result — no error, no
    table drop, no unfiltered dump."""
    repo = PersonRepository(sqlite_session_factory)
    repo.upsert(Person(match_key="k1", full_name="ana gil", company="acme"))
    repo.upsert(Person(match_key="k2", full_name="beatriz ruiz", company="globex"))
    client = _client(sqlite_session_factory)

    for payload in ("' OR '1'='1", "'; DROP TABLE persons; --", "%' OR 1=1 --"):
        resp = client.get("/persons", params={"q": payload})
        assert resp.status_code == 200
        body = resp.json()
        # Treated as a literal -> matches nothing, and never dumps all rows.
        assert body["total"] == 0
        assert body["count"] == 0

    # The table still exists and the original data is intact (no injection executed).
    intact = client.get("/persons").json()
    assert intact["total"] == 2


def test_sec_t6_out_of_range_params_rejected_422(sqlite_session_factory):
    """SEC-T6: out-of-range limit / min_confidence are rejected by FastAPI validation
    (422), not silently coerced or interpolated."""
    client = _client(sqlite_session_factory)

    # /persons: limit above the le=500 cap and below the ge=1 floor.
    assert client.get("/persons", params={"limit": 9999}).status_code == 422
    assert client.get("/persons", params={"limit": 0}).status_code == 422
    assert client.get("/persons", params={"offset": -1}).status_code == 422

    # /groups: min_confidence outside [0, 1].
    assert client.get("/groups", params={"min_confidence": 2.5}).status_code == 422
    assert client.get("/groups", params={"min_confidence": -0.1}).status_code == 422
    assert client.get("/groups", params={"limit": 99999}).status_code == 422
