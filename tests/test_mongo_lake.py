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


def test_buffer_flushes_when_batch_size_reached(mongo_collection, personal_fragment):
    lake = MongoLake(mongo_collection, batch_size=3, flush_interval=9999)
    assert lake.buffer_raw(personal_fragment, "personal") is False  # 1 buffered
    assert lake.buffer_raw(personal_fragment, "personal") is False  # 2 buffered
    assert lake.count() == 0  # nothing written yet
    flushed = lake.buffer_raw(personal_fragment, "personal")  # 3rd -> flush
    assert flushed is True
    assert lake.count() == 3


def test_buffer_flushes_on_interval(mongo_collection, personal_fragment):
    # flush_interval=0 means "always elapsed" -> flushes on first buffer
    lake = MongoLake(mongo_collection, batch_size=999, flush_interval=0)
    assert lake.buffer_raw(personal_fragment, "personal") is True
    assert lake.count() == 1


def test_manual_flush_and_empty_flush(mongo_collection, personal_fragment):
    lake = MongoLake(mongo_collection, batch_size=999, flush_interval=9999)
    lake.buffer_raw(personal_fragment, "personal")
    lake.buffer_raw(personal_fragment, "personal")
    assert lake.count() == 0
    assert lake.flush() == 2   # writes the 2 buffered
    assert lake.count() == 2
    assert lake.flush() == 0   # nothing left to flush


def test_ensure_indexes(mongo_collection):
    lake = MongoLake(mongo_collection)
    lake.ensure_indexes()  # must not raise on mongomock
    names = [idx["key"] for idx in mongo_collection.list_indexes()]
    # at least the ingested_at index exists
    assert any("ingested_at" in dict(k) for k in names)
