"""Kafka consumer wrapper.

Connects to the external Kafka broker (black box) over the network and yields
decoded JSON messages. The confluent-kafka import is deferred so the module can
be imported and unit-tested without the native library installed.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from typing import Any

from hr_etl.config import Settings
from hr_etl.logging_conf import get_logger

logger = get_logger(__name__)


class KafkaMessageConsumer:
    """Thin wrapper around confluent_kafka.Consumer yielding parsed dicts."""

    def __init__(self, settings: Settings, consumer: Any | None = None) -> None:
        self._settings = settings
        self._consumer = consumer  # injectable for tests

    def _build_consumer(self) -> Any:
        from confluent_kafka import Consumer  # deferred import

        return Consumer(
            {
                "bootstrap.servers": self._settings.kafka_bootstrap_servers,
                "group.id": self._settings.kafka_group_id,
                "auto.offset.reset": self._settings.kafka_auto_offset_reset,
                "enable.auto.commit": False,
            }
        )

    @staticmethod
    def decode(raw: bytes | str | None) -> dict[str, Any] | None:
        """Decode a raw Kafka value into a dict, or None if invalid."""
        if raw is None:
            return None
        try:
            text = raw.decode("utf-8") if isinstance(raw, bytes) else raw
            value = json.loads(text)
            return value if isinstance(value, dict) else None
        except (ValueError, UnicodeDecodeError):
            logger.warning("failed to decode kafka message")
            return None

    def consume(
        self, max_messages: int | None = None, timeout: float = 1.0
    ) -> Iterator[dict[str, Any]]:
        """Yield decoded messages. Commits offsets after yielding.

        `max_messages` bounds the loop (useful for tests/demos); None runs forever.
        """
        if self._consumer is None:
            self._consumer = self._build_consumer()
        self._consumer.subscribe([self._settings.kafka_topic])

        count = 0
        try:
            while max_messages is None or count < max_messages:
                msg = self._consumer.poll(timeout)
                if msg is None:
                    continue
                if msg.error():
                    logger.error("kafka error: %s", msg.error())
                    continue
                decoded = self.decode(msg.value())
                if decoded is not None:
                    yield decoded
                    count += 1
                self._consumer.commit(msg, asynchronous=False)
        finally:
            self._consumer.close()
