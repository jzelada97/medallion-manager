"""Redis-backed buffer that groups fragments by person key until consolidation.

Fragments of the same person do not arrive together, so we accumulate them under
their matching key (with a TTL) and consolidate once enough fragments are present.
"""

from __future__ import annotations

import json
from typing import Any

from hr_etl.logging_conf import get_logger

logger = get_logger(__name__)


class RedisBuffer:
    """Accumulates fragments per person key in a Redis list with a TTL."""

    def __init__(self, client: Any, ttl: int = 300, prefix: str = "frag") -> None:
        self._client = client
        self._ttl = ttl
        self._prefix = prefix

    def _redis_key(self, match_key: str) -> str:
        return f"{self._prefix}:{match_key}"

    def add_fragment(self, match_key: str, message: dict[str, Any], fragment_type: str) -> int:
        """Append a fragment for a person key. Returns current fragment count."""
        rkey = self._redis_key(match_key)
        payload = json.dumps({"message": message, "type": fragment_type})
        count = self._client.rpush(rkey, payload)
        self._client.expire(rkey, self._ttl)
        return int(count)

    def get_fragments(self, match_key: str) -> list[dict[str, Any]]:
        """Return all buffered fragments for a person key."""
        rkey = self._redis_key(match_key)
        raw = self._client.lrange(rkey, 0, -1)
        return [json.loads(item) for item in raw]

    def clear(self, match_key: str) -> None:
        """Remove the buffer for a person key (after consolidation)."""
        self._client.delete(self._redis_key(match_key))
