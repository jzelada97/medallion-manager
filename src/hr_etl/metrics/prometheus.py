"""Prometheus metric definitions for the ETL pipeline.

These instruments answer the questions asked by the project brief:
- how many messages are consumed (and of which type),
- at what rate (derive with rate() over the counters in Prometheus),
- how long processing takes, and how long persistence takes,
- how many consolidated persons were written, and how many messages failed.
"""

from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

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
    "Time spent processing a single message end to end",
)

PERSIST_SECONDS = Histogram(
    "hr_etl_persist_seconds",
    "Time spent persisting a consolidated person into the warehouse",
)

CONSOLIDATIONS = Counter(
    "hr_etl_consolidations_total",
    "Total consolidation attempts that produced a person record",
)

PENDING_FRAGMENTS = Gauge(
    "hr_etl_pending_fragments",
    "Fragments currently buffered for a person key awaiting consolidation",
)
