"""Prometheus metric definitions for the ETL pipeline."""

from __future__ import annotations

from prometheus_client import Counter, Histogram

MESSAGES_CONSUMED = Counter(
    "hr_etl_messages_consumed_total",
    "Total messages consumed from Kafka",
    ["fragment_type"],
)

MESSAGES_FAILED = Counter(
    "hr_etl_messages_failed_total",
    "Total messages that could not be processed",
)

PERSONS_PERSISTED = Counter(
    "hr_etl_persons_persisted_total",
    "Total consolidated persons written to the warehouse",
)

PROCESSING_SECONDS = Histogram(
    "hr_etl_processing_seconds",
    "Time spent processing a single message",
)

PERSIST_SECONDS = Histogram(
    "hr_etl_persist_seconds",
    "Time spent persisting a consolidated person",
)
