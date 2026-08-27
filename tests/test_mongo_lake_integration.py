"""Integration tests for MongoLake against a REAL MongoDB (issue #3).

These validate that raw messages are stored verbatim with ingestion metadata in
MongoDB (the Data Lake), which is what runs in production via docker-compose.

Connects using TEST_MONGO_URI (defaults to the docker-compose Mongo on
localhost:27017). If no Mongo is reachable, the whole module is skipped so it
never breaks the normal unit-test run.

Run it with the stack up:
    docker compose up -d mongo
    set TEST_MONGO_URI=mongodb://hr_user:changeme@localhost:27017/
    pytest -m integration --no-cov
"""

from __future__ import annotations

import os
import uuid

import pytest

pytest.importorskip("pymongo", reason="pymongo not installed")

from pymongo import MongoClient  # noqa: E402

from hr_etl.lake.mongo_lake import MongoLake  # noqa: E402

MONGO_URI = os.getenv("TEST_MONGO_URI", "mongodb://hr_user:changeme@localhost:27017/")


def _mongo_available() -> bool:
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=1500)
        client.admin.command("ping")
        client.close()
        return True
    except Exception:
        return False


pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(not _mongo_available(), reason="no MongoDB reachable at TEST_MONGO_URI"),
]


@pytest.fixture()
def lake_collection():
    """A unique throwaway collection on the real Mongo, dropped after the test."""
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=1500)
    db = client["hr_lake_test"]
    coll_name = f"raw_{uuid.uuid4().hex[:8]}"
    collection = db[coll_name]
    yield collection
    collection.drop()
    client.close()


def test_store_raw_persists_verbatim_with_metadata(lake_collection):
    lake = MongoLake(lake_collection)
    message = {"Name": "Ana", "Passport": "X1", "E-Mail": "ana@example.com"}

    inserted_id = lake.store_raw(message, "personal", offset=7)
    assert inserted_id is not None
    assert lake.count() == 1

    doc = lake_collection.find_one({"_id": inserted_id})
    assert doc["payload"] == message  # stored exactly as received
    assert doc["fragment_type"] == "personal"
    assert doc["kafka_offset"] == 7
    assert "ingested_at" in doc  # metadata added


def test_store_multiple_fragments(lake_collection):
    lake = MongoLake(lake_collection)
    lake.store_raw({"Passport": "X1", "IBAN": "ES1"}, "bank")
    lake.store_raw({"Fullname": "Ana Gil", "City": "Madrid"}, "location")
    assert lake.count() == 2
