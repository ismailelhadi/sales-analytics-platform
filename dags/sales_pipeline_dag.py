"""Daily sales pipeline: validate + load each source, build dbt models, publish, refresh the RAG index.

A source whose file fails a blocking data quality check is quarantined and its task fails
loudly (red in the UI + alert callback). dbt still runs for the sources that did load, and
reconciliation automatically skips any date where one system of record is missing.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

try:  # Airflow 3
    from airflow.sdk import dag, task
    from airflow.exceptions import AirflowFailException
    from airflow.providers.standard.operators.bash import BashOperator
except ImportError:  # Airflow 2.x
    from airflow.decorators import dag, task
    from airflow.exceptions import AirflowFailException
    from airflow.operators.bash import BashOperator

PROJECT_DIR = os.getenv("PROJECT_DIR", os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
log = logging.getLogger(__name__)


def alert_on_failure(context):
    ti = context["task_instance"]
    # Swap for Slack / PagerDuty / email in a real deployment.
    log.error("ALERT: %s.%s failed for %s", ti.dag_id, ti.task_id, context.get("logical_date"))


default_args = {
    "owner": "sales-analytics",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "on_failure_callback": alert_on_failure,
}


@dag(
    dag_id="sales_daily_pipeline",
    schedule="0 6 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["sales", "etl", "dbt", "rag"],
)
def sales_daily_pipeline():

    def make_load_task(source: str):
        @task(task_id=f"validate_and_load_{source}", retries=0)
        def validate_and_load(ds=None):
            import sys
            sys.path.insert(0, PROJECT_DIR)
            from datetime import date
            from pipeline import db
            from pipeline.extract_load import load_stores, process_source

            with db.connect() as conn:
                db.init_schema(conn)
                known = load_stores(conn)
                report = process_source(conn, source, date.fromisoformat(ds), "airflow-" + ds, known)
            if not report.passed:
                # Fail loudly instead of silently loading a bad file.
                raise AirflowFailException("; ".join(f"{r.check}: {r.detail}" for r in report.failures))
            return {"source": source, "rows": report.rows}
        return validate_and_load()

    loads = [make_load_task(s) for s in ("pos", "carrier")]

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"cd {PROJECT_DIR}/dbt && dbt build --profiles-dir . --fail-fast",
        trigger_rule="all_done",  # build whatever loaded; reconciliation skips incomplete dates
    )

    @task(task_id="publish_to_snowflake")
    def publish():
        import sys
        sys.path.insert(0, PROJECT_DIR)
        from pipeline.snowflake_publish import publish_marts
        return publish_marts()  # no-op (returns "skipped") when Snowflake isn't configured

    @task(task_id="refresh_rag_index")
    def refresh_index():
        import sys
        sys.path.insert(0, PROJECT_DIR)
        from rag.index import build_index
        return build_index()

    @task(task_id="fail_run_if_any_task_failed", trigger_rule="one_failed", retries=0)
    def watcher():
        # Airflow marks a run successful when its leaf tasks succeed, even if an upstream load
        # failed. This leaf only runs when something failed, and fails the whole run so a
        # quarantined file can never end in a green run.
        raise AirflowFailException("an upstream task failed; see alerts")

    published, indexed = publish(), refresh_index()
    loads >> dbt_build >> [published, indexed]
    [*loads, dbt_build, published, indexed] >> watcher()


sales_daily_pipeline()
