"""Redis-backed buffer that groups fragments by person key until consolidation.

Fragments of the same person do not arrive together, so we accumulate them under
their matching key (with a TTL) and consolidate once enough fragments are present.

Cross-linking: when a fragment with a passport also carries a name, we register
a name->passport alias so that later fragments (Location/Professional) that only
have a fullname can be redirected to the correct passport-based key.
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
        self._alias_prefix = "alias"

    def _redis_key(self, match_key: str) -> str:
        return f"{self._prefix}:{match_key}"

    def _alias_key(self, name: str) -> str:
        return f"{self._alias_prefix}:{name}"

    # ------------------------------------------------------------------
    # Alias management (cross-linking passport <-> name)
    # ------------------------------------------------------------------

    def register_alias(self, name: str, canonical_key: str) -> None:
        """Register a name as alias for a passport-based canonical key.

        Called when a Personal fragment has both passport and name.
        """
        if not name or not canonical_key:
            return
        akey = self._alias_key(name)
        self._client.set(akey, canonical_key, ex=self._ttl * 3)  # longer TTL for aliases
        logger.debug("alias registered: %s -> %s", name, canonical_key)

    def resolve_alias(self, name_key: str) -> str | None:
        """Try to resolve a name-based match_key to a passport-based one.

        Only resolves on EXACT name match to avoid false positives
        (e.g. 'octavio ponce' and 'octavio ponce gimenez' could be different people).
        Returns the canonical passport key if found, None otherwise.
        """
        if not name_key.startswith("name:"):
            return None
        name = name_key[5:]  # strip "name:" prefix

        # Exact lookup only — no prefix/fuzzy matching to avoid false merges
        akey = self._alias_key(name)
        result = self._client.get(akey)
        if result:
            return result if isinstance(result, str) else result.decode("utf-8")

        return None

    # ------------------------------------------------------------------
    # Fragment buffering
    # ------------------------------------------------------------------

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
