"""Event-driven Gold refresh DAG (Experto+).

Instead of refreshing Gold blindly on a fixed clock, this DAG waits until enough
*new* persons have been consolidated into the Silver layer since the last refresh,
then refreshes Gold. The waiting is done by a **deferrable** sensor
(:class:`hr_etl.airflow_ext.persons_threshold.NewPersonsSensor`), so no worker slot is
held during the wait — Airflow's triggerer watches the condition asynchronously.

Flow:  wait_for_new_persons  ->  refresh_gold_layer

The DAG is still scheduled on a slow clock (every 15 min) purely as a *fallback* so
Gold never goes stale under low load; the sensor's ``timeout`` bounds each wait. The
sensor is the primary, event-driven trigger; the schedule is the safety net.

Thresholds are configurable via env vars (see docker-compose.airflow.yml):
- GOLD_TRIGGER_MIN_NEW_PERSONS (default 150)
- GOLD_TRIGGER_POLL_SECONDS    (default 30)
"""

from datetime import datetime, timedelta

from airflow import DAG

try:  # Airflow 3: BashOperator lives in the bundled `standard` provider.
    from airflow.providers.standard.operators.bash import BashOperator
except ImportError:  # Airflow 2.x fallback
    from airflow.operators.bash import BashOperator

from hr_etl.airflow_ext.persons_threshold import NewPersonsSensor

default_args = {
    "owner": "hr-etl",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="hr_etl_gold_eventdriven",
    default_args=default_args,
    description="Refresh Gold when enough new persons land (deferrable sensor), not on a blind clock",
    schedule="*/15 * * * *",  # fallback cadence; the sensor is the real trigger
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,  # never overlap runs
    tags=["hr-etl", "gold", "medallion", "deferrable"],
) as dag:

    # Deferrable wait: succeeds when >= min_new persons exist since the last Gold
    # refresh, or when `timeout` elapses (fallback so Gold never goes stale).
    wait_for_new_persons = NewPersonsSensor(
        task_id="wait_for_new_persons",
        # Bound the wait; on timeout we still refresh (see below).
        timeout=15 * 60,
        # If the wait times out, skip (don't fail) so the refresh still runs.
        soft_fail=True,
    )

    refresh_gold = BashOperator(
        task_id="refresh_gold_layer",
        bash_command="python -m hr_etl.warehouse.gold_layer",
        # Run the refresh even if the sensor soft-failed on timeout.
        trigger_rule="all_done",
    )

    wait_for_new_persons >> refresh_gold
