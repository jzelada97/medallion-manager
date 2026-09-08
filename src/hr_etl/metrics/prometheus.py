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

# --- Batch maintenance jobs (consolidation / reconciliation / gold) ---
# All labels/values are numeric or fixed job names — never PII (NFR-5, C-4).

CONSOLIDATION_MERGED_ROWS = Counter(
    "hr_etl_consolidation_merged_rows_total",
    "Total person rows merged away (deleted losers) by the consolidation fix job",
)

RECONCILE_DURATION_SECONDS = Histogram(
    "hr_etl_reconcile_duration_seconds",
    "Wall-clock duration of a full batch reconciliation run",
)

RECONCILE_GROUPS = Gauge(
    "hr_etl_reconcile_groups",
    "Number of duplicate groups produced by the last reconciliation run",
)

RECONCILE_MEMBERSHIPS = Gauge(
    "hr_etl_reconcile_memberships",
    "Number of duplicate-group memberships written by the last reconciliation run",
)

GOLD_PERSONS = Gauge(
    "hr_etl_gold_persons",
    "Number of persons in the Gold subset after the last gold refresh",
)
