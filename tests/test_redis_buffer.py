"""Tests for the Redis fragment buffer using fakeredis."""

from __future__ import annotations

import fakeredis

from hr_etl.cache.redis_buffer import RedisBuffer


def _buffer():
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    return RedisBuffer(client, ttl=60)


def test_add_and_get_fragments():
    buf = _buffer()
    assert buf.add_fragment("passport:x1", {"Passport": "X1"}, "personal") == 1
    assert buf.add_fragment("passport:x1", {"IBAN": "ES1"}, "bank") == 2

    frags = buf.get_fragments("passport:x1")
    assert len(frags) == 2
    assert frags[0]["type"] == "personal"
    assert frags[1]["message"] == {"IBAN": "ES1"}


def test_clear():
    buf = _buffer()
    buf.add_fragment("k", {"a": 1}, "personal")
    buf.clear("k")
    assert buf.get_fragments("k") == []


def test_register_and_resolve_alias():
    buf = _buffer()
    buf.register_alias("ana gil", "passport:X1")
    # name-based key resolves to the canonical passport key on exact match
    assert buf.resolve_alias("name:ana gil") == "passport:X1"


def test_register_alias_ignores_empty_values():
    buf = _buffer()
    buf.register_alias("", "passport:X1")
    buf.register_alias("ana gil", "")
    # nothing was stored -> nothing resolves
    assert buf.resolve_alias("name:ana gil") is None


def test_resolve_alias_requires_name_prefix():
    buf = _buffer()
    buf.register_alias("ana gil", "passport:X1")
    # a non name: key is never resolved
    assert buf.resolve_alias("passport:ana gil") is None


def test_resolve_alias_unknown_name_returns_none():
    buf = _buffer()
    assert buf.resolve_alias("name:desconocido") is None


def test_resolve_alias_decodes_bytes():
    """When the redis client returns bytes (decode_responses=False), it's decoded."""
    client = fakeredis.FakeStrictRedis(decode_responses=False)
    buf = RedisBuffer(client, ttl=60)
    buf.register_alias("ana gil", "passport:X1")
    assert buf.resolve_alias("name:ana gil") == "passport:X1"
