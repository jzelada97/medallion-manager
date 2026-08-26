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
    assert body["total"] == 5     # total across all matches
    assert body["count"] == 2     # only this page
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
