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


def test_get_missing_person(sqlite_session_factory):
    client = _client(sqlite_session_factory)
    resp = client.get("/persons/999")
    assert resp.json() == {"error": "not found"}


def test_list_persons_filter(sqlite_session_factory):
    repo = PersonRepository(sqlite_session_factory)
    repo.upsert(Person(match_key="k1", city="madrid"))
    repo.upsert(Person(match_key="k2", city="sevilla"))
    client = _client(sqlite_session_factory)
    resp = client.get("/persons", params={"city": "madrid"})
    assert resp.json()["count"] == 1
