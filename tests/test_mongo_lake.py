"""Tests for the MongoDB lake using mongomock."""

from __future__ import annotations

from hr_etl.lake.mongo_lake import MongoLake


def test_store_raw_inserts_document(mongo_collection, personal_fragment):
    lake = MongoLake(mongo_collection)
    inserted_id = lake.store_raw(personal_fragment, "personal", offset=42)
    assert inserted_id is not None
    assert lake.count() == 1

    doc = mongo_collection.find_one({"_id": inserted_id})
    assert doc["fragment_type"] == "personal"
    assert doc["kafka_offset"] == 42
    assert doc["payload"] == personal_fragment
    assert "ingested_at" in doc


def test_store_multiple(mongo_collection, personal_fragment, bank_fragment):
    lake = MongoLake(mongo_collection)
    lake.store_raw(personal_fragment, "personal")
    lake.store_raw(bank_fragment, "bank")
    assert lake.count() == 2
