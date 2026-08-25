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
