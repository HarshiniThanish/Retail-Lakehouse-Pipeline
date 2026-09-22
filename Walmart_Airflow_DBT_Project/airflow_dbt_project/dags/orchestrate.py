"""
orchestrate.py
---------------------------------------------------------------------------
Master DAG for the Walmart Lakehouse pipeline.

Sequential task graph:
    ingest_cdc -> clean_target -> source_freshness
        -> silver_technical -> silver_technical_tests
        -> silver_business  -> silver_business_tests
        -> gold_ephemeral -> gold_dimensions (dbt snapshot) -> gold_facts

`ingest_cdc` triggers a Databricks job (CDC ingestion into Bronze) via the
Databricks SDK and polls its lifecycle state until it reaches a terminal
state. All other tasks shell out to `dbt` against the project mounted at
DBT_PROJECT_DIR.
---------------------------------------------------------------------------
"""
from __future__ import annotations

import os
import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException

try:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState
except ImportError:  # allows the DAG to parse even before deps are installed
    WorkspaceClient = None

DBT_PROJECT_DIR = os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/dbt_project")
DBT_PROFILES_DIR = os.environ.get("DBT_PROFILES_DIR", DBT_PROJECT_DIR)
DATABRICKS_INGEST_JOB_ID = os.environ.get("DATABRICKS_INGEST_JOB_ID")

DBT_BASE_CMD = f"dbt --no-use-colors {{cmd}} --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}"

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "depends_on_past": False,
}


def _run_databricks_ingestion(**context) -> None:
    """
    Triggers the Bronze-layer CDC ingestion job in Databricks via the SDK
    and polls RUNNING -> TERMINATED before allowing downstream tasks to run.
    """
    if WorkspaceClient is None:
        raise AirflowException("databricks-sdk is not installed in this environment")
    if not DATABRICKS_INGEST_JOB_ID:
        raise AirflowException("DATABRICKS_INGEST_JOB_ID is not set")

    ws = WorkspaceClient()
    run = ws.jobs.run_now(job_id=int(DATABRICKS_INGEST_JOB_ID))
    run_id = run.run_id
    print(f"[ingest_cdc] Triggered Databricks job run_id={run_id}")

    poll_interval_seconds = 15
    max_wait_seconds = 60 * 60  # 1 hour safety cap
    waited = 0

    while waited < max_wait_seconds:
        run_status = ws.jobs.get_run(run_id=run_id)
        life_cycle_state = run_status.state.life_cycle_state
        print(f"[ingest_cdc] run_id={run_id} state={life_cycle_state}")

        if life_cycle_state == RunLifeCycleState.TERMINATED:
            result_state = run_status.state.result_state
            if result_state != RunResultState.SUCCESS:
                raise AirflowException(
                    f"Databricks ingestion job failed: result_state={result_state}"
                )
            print(f"[ingest_cdc] run_id={run_id} completed successfully")
            return
        if life_cycle_state in (RunLifeCycleState.INTERNAL_ERROR, RunLifeCycleState.SKIPPED):
            raise AirflowException(f"Databricks ingestion job entered {life_cycle_state}")

        time.sleep(poll_interval_seconds)
        waited += poll_interval_seconds

    raise AirflowException(f"Timed out waiting for Databricks run_id={run_id}")


with DAG(
    dag_id="orchestrate",
    description="Walmart Lakehouse: Bronze CDC ingest -> Silver (technical + OBT) -> Gold (SCD2 + facts)",
    default_args=default_args,
    schedule="0 3 * * *",  # daily at 03:00
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["walmart", "lakehouse", "dbt", "databricks"],
) as dag:

    ingest_cdc = PythonOperator(
        task_id="ingest_cdc",
        python_callable=_run_databricks_ingestion,
    )

    clean_target = BashOperator(
        task_id="clean_target",
        bash_command=DBT_BASE_CMD.format(cmd="clean"),
    )

    source_freshness = BashOperator(
        task_id="source_freshness",
        bash_command=DBT_BASE_CMD.format(cmd="source freshness"),
    )

    silver_technical = BashOperator(
        task_id="silver_technical",
        bash_command=DBT_BASE_CMD.format(cmd="run --select silver_t"),
    )

    silver_technical_tests = BashOperator(
        task_id="silver_technical_tests",
        bash_command=DBT_BASE_CMD.format(cmd="test --select silver_t"),
    )

    silver_business = BashOperator(
        task_id="silver_business",
        bash_command=DBT_BASE_CMD.format(cmd="run --select silver_b"),
    )

    silver_business_tests = BashOperator(
        task_id="silver_business_tests",
        bash_command=DBT_BASE_CMD.format(cmd="test --select silver_b"),
    )

    gold_ephemeral = BashOperator(
        task_id="gold_ephemeral",
        bash_command=DBT_BASE_CMD.format(cmd="run --select tag:gold_ephemeral"),
    )

    gold_dimensions = BashOperator(
        task_id="gold_dimensions",
        bash_command=DBT_BASE_CMD.format(cmd="snapshot"),
    )

    gold_facts = BashOperator(
        task_id="gold_facts",
        bash_command=DBT_BASE_CMD.format(cmd="run --select fact_orders"),
    )

    (
        ingest_cdc
        >> clean_target
        >> source_freshness
        >> silver_technical
        >> silver_technical_tests
        >> silver_business
        >> silver_business_tests
        >> gold_ephemeral
        >> gold_dimensions
        >> gold_facts
    )
