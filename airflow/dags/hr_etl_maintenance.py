"""Airflow DAG for HR ETL batch maintenance.

A single SEQUENTIAL DAG runs the three maintenance jobs in strict data-dependency order:

    consolidate_merge  >>  reconcile  >>  refresh_gold

Order is mandatory (see architecture-reconcile.md):
  1. consolidate_merge — merges same-person rows in `persons` (same passport + very
     similar name), so downstream steps see de-duplicated, consolidated Silver data.
  2. reconcile — groups probable duplicates by fuzzy name over the CLEAN state into
     `duplicate_groups` (review candidates, never auto-merged).
  3. refresh_gold — rebuilds `gold_persons` (the completeness-qualified subset) and the
     `gold_*` stats over that subset, reflecting the final state.

Running them out of order would build groups/Gold over dirty (un-consolidated) data.

Each task is a BashOperator invoking the ETL CLIs. The hr_etl package is importable via
PYTHONPATH=/opt/airflow/src (set in docker-compose.airflow.yml); its runtime deps are in
the Airflow image (Airflow 3 uses SQLAlchemy 2.x, matching hr_etl).
"""

from datetime import datetime, timedelta

from airflow import DAG

# Airflow 3: BashOperator lives in the bundled `standard` provider.
try:  # pragma: no cover - import shim for Airflow 2/3 compatibility
    from airflow.providers.standard.operators.bash import BashOperator
except ImportError:  # Airflow 2.x fallback
    from airflow.operators.bash import BashOperator

default_args = {
    "owner": "hr-etl",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

# Cadence is driven by the expensive step (reconciliation, ~2-4 min on the VM), so the
# whole chain runs every 30 minutes. max_active_runs=1 prevents overlapping runs from
# stepping on each other's `persons`/`gold_*` rebuilds.
with DAG(
    dag_id="hr_etl_maintenance",
    default_args=default_args,
    description="Sequential maintenance: consolidate -> reconcile -> refresh gold",
    schedule="*/30 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["hr-etl", "maintenance", "medallion", "data-quality"],
) as dag:

    consolidate_merge = BashOperator(
        task_id="consolidate_merge",
        bash_command="python -m hr_etl.processing.consolidate_merge",
    )

    reconcile = BashOperator(
        task_id="batch_reconciliation",
        bash_command="python -m hr_etl.processing.reconcile",
    )

    refresh_gold = BashOperator(
        task_id="refresh_gold_layer",
        bash_command="python -m hr_etl.warehouse.gold_layer",
    )

    consolidate_merge >> reconcile >> refresh_gold
