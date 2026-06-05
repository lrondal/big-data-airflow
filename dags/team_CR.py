"""
Lab 4 - copy to dags/team_<yourname>.py and complete the capstone.

Mandatory:
  - >= 5 Airflow tasks in your dag
  - 3 Spark transforms in include/team_<yourname>_spark.py
  - Try to be creative with the tasks

Steps:
  1. Change dag_id below.
  2. Copy include/team_spark_TEMPLATE.py -> include/team_<yourname>_spark.py
  3. Define 5 tasks
  4. Wire spark task to YOUR run_daily() in include/team_<yourname>_spark.py
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.sensors.filesystem import FileSensor
from airflow.exceptions import AirflowFailException

from include.ingest import ingest_day, validate_silver
from include.paths import report_json
from include.team_CR_spark import run_daily

# TODO: after creating team_<yourname>_spark.py, import run_daily from there:
# from include.team_<yourname>_spark import run_daily

DEFAULT_ARGS = {
    "owner": "team",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}


@dag(
    dag_id="team_CR",
    description="Capstone retail KPI pipeline",
    start_date=datetime(2026, 6, 1),
    end_date=datetime(2026, 6, 14),
    schedule="@daily",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["lab4", "capstone"],
)
def team_CR_dag():
    ds = "{{ ds }}"
    wait_for_csv = FileSensor(
        task_id="wait_for_csv",
        filepath=f"incoming/transactions_{ds}.csv",
        poke_interval=30,
        timeout=timedelta(hours=1).total_seconds(),
        mode="reschedule",
    )

    @task(task_id="ingest_to_silver")
    def ingest_to_silver(ds: str) -> str:
        """
        Call ingest_day function to generate Silver Parquait
        """

        silver_path = ingest_day(ds)
        return str(silver_path)

    @task(task_id="validate_silver")
    def validate(ds: str):
        """
        Validate the silver Parquet file using validate_silver function
        """
        try:
            return validate_silver(ds)
        except RuntimeError as e:
            raise AirflowFailException(str(e)) from e

    @task(task_id="run_spark_kpis")
    def run_spark_kpis(ds: str) -> str:
        """
        Execute run_daily that apply spark transformation
        """

        report_path = run_daily(ds)
        return str(report_path)

    silver = ingest_to_silver(ds=ds)
    validation = validate(ds=ds)
    report = run_spark_kpis(ds=ds)

    wait_for_csv >> silver >> validation >> report


team_CR_dag()
