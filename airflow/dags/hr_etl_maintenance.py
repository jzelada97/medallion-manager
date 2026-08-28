"""Airflow DAGs for HR ETL batch maintenance tasks.

These DAGs schedule periodic jobs that complement the real-time streaming pipeline:
- Gold layer refresh (every 5 minutes)
- Batch reconciliation for duplicate detection (every 30 minutes)

Each DAG uses a BashOperator to invoke the ETL CLI commands. The hr_etl package is
made importable via PYTHONPATH=/opt/airflow/src (set in docker-compose.airflow.yml),
and its runtime deps are installed into the Airflow image (Airflow 3 uses SQLAlchemy
2.x, matching hr_etl, so no isolated environment is required).
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

# --- DAG 1: Refresh Gold layer (every 5 minutes) ---

with DAG(
    dag_id="hr_etl_refresh_gold",
    default_args=default_args,
    description="Refresh Gold layer aggregates (stats, top cities/companies, completeness)",
    schedule="*/5 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["hr-etl", "gold", "medallion"],
) as dag_gold:

    refresh_gold = BashOperator(
        task_id="refresh_gold_layer",
        bash_command="python -m hr_etl.warehouse.gold_layer",
    )


# --- DAG 2: Batch reconciliation (every 30 minutes) ---

with DAG(
    dag_id="hr_etl_reconciliation",
    default_args=default_args,
    description="Detect probable duplicate persons via name prefix matching",
    schedule="*/30 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["hr-etl", "reconciliation", "data-quality"],
) as dag_reconcile:

    run_reconciliation = BashOperator(
        task_id="batch_reconciliation",
        bash_command="python -m hr_etl.processing.reconcile",
    )
