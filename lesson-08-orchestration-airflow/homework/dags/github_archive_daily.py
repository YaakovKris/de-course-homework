"""github_archive_daily — ВАШ DAG. Специфікація: ../SPEC.md → «DAG».

Готові ETL-цеглинки вже є — імпортуйте і викликайте їх у задачах (не переписуйте):

    from include.gh_etl import download, validate, load_to_duckdb, summarize
    from gh_sensor import GHArchiveSensor   # ваш custom sensor із plugins/

Що треба зібрати (деталі й бали — у SPEC.md):
  * DAG `github_archive_daily`, розклад «щодня о 06:00 UTC», catchup=False;
  * усі задачі працюють із logical date {{ ds }}, а не datetime.now() — це дає
    ідемпотентність і коректний backfill;
  * граф:
        check_availability -> download_archive -> validate_file
            -> load_to_duckdb -> notify_completion
  * download_archive кладе шлях у XCom; validate_file і load_to_duckdb беруть його з XCom;
  * шляхи (дано):
        DB_PATH     = "/opt/airflow/data/github_analytics.duckdb"
        LANDING_DIR = "/opt/airflow/data/landing"

Перевірка: `airflow dags test github_archive_daily 2024-01-14` має пройти всі задачі;
наскрізно — `./verify.sh` із кореня homework/.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator
from gh_sensor import GHArchiveSensor

from include.gh_etl import download as gh_download
from include.gh_etl import load_to_duckdb as gh_load_to_duckdb
from include.gh_etl import summarize as gh_summarize
from include.gh_etl import validate as gh_validate

DB_PATH = "/opt/airflow/data/github_analytics.duckdb"
LANDING_DIR = "/opt/airflow/data/landing"


def _download_task(**context):
    ds = context["ds"]
    path = gh_download(ds=ds, landing_dir=LANDING_DIR)
    return path


def _validate_task(**context):
    ti = context["ti"]
    path = ti.xcom_pull(task_ids="download_archive")
    gh_validate(path)
    return path


def _load_task(**context):
    ti = context["ti"]
    ds = context["ds"]
    path = ti.xcom_pull(task_ids="download_archive")
    rows = gh_load_to_duckdb(path=path, ds=ds, db_path=DB_PATH)
    return rows


def _notify_task(**context):
    ds = context["ds"]
    summary = gh_summarize(ds=ds, db_path=DB_PATH)
    return summary


with DAG(
    dag_id="github_archive_daily",
    schedule="0 6 * * *",
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["github", "archive", "etl"],
    default_args={
        "owner": "airflow",
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    },
) as dag:
    check_availability = GHArchiveSensor(
        task_id="check_availability",
        hour=14,
        timeout=600,
        poke_interval=60,
        mode="reschedule",
    )

    download_archive = PythonOperator(
        task_id="download_archive",
        python_callable=_download_task,
    )

    validate_file = PythonOperator(
        task_id="validate_file",
        python_callable=_validate_task,
    )

    load_to_duckdb = PythonOperator(
        task_id="load_to_duckdb",
        python_callable=_load_task,
    )

    notify_completion = PythonOperator(
        task_id="notify_completion",
        python_callable=_notify_task,
    )

    check_availability >> download_archive >> validate_file >> load_to_duckdb >> notify_completion
