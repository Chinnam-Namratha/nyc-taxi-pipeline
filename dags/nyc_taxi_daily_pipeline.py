"""
nyc_taxi_daily_pipeline

Runs the NYC Yellow Taxi ELT pipeline daily at 02:00 UTC. The six tasks run
in sequence — if any of them fail the DAG stops and sends an alert email rather
than silently writing partial data downstream.

Tasks:
  1. check_source_freshness  — makes sure the Parquet file for the run month exists
                               before wasting time on dbt
  2. run_dbt_seed            — loads the taxi zone lookup CSV into DuckDB (idempotent)
  3. run_dbt_staging         — builds the staging views
  4. run_dbt_intermediate    — joins + filters in the intermediate layer
  5. run_dbt_marts           — materialises the mart tables (the actual output)
  6. run_dbt_tests           — runs all dbt tests; fails the DAG if anything breaks
  7. notify_success          — logs a summary (trip count + revenue) so you know it worked

Configuration via environment variables:
  DBT_PROJECT_DIR  — path to the dbt/ directory (defaults to sibling dbt/ folder)
  DBT_TARGET       — dbt target to use (default: dev)
  PARQUET_DIR      — where the raw Parquet files live
  DUCKDB_PATH      — path to the DuckDB file for the success summary query
  ALERT_EMAIL      — comma-separated email addresses for failure notifications

Backfill: catchup=True means you can trigger this for any past date and it will
process the right monthly Parquet file. The models are full-refresh today —
switching to incremental + --vars '{"run_date": "{{ ds }}"}' would make
large backfills much faster.

Local runner (Windows, no Airflow scheduler needed):
  python dags/nyc_taxi_daily_pipeline.py 2023-01-01
"""

from __future__ import annotations

import logging
import os
import subprocess
from datetime import datetime, timedelta

from constants import (
    AGG_DAILY_REVENUE_TABLE,
    ALERT_EMAIL_RAW,
    DAG_ID,
    DAG_MAX_ACTIVE_RUNS,
    DAG_OWNER,
    DAG_SCHEDULE,
    DAG_TAGS,
    DBT_PROJECT_DIR,
    DBT_SELECT_INTERMEDIATE,
    DBT_SELECT_MARTS,
    DBT_SELECT_STAGING,
    DBT_TARGET,
    DUCKDB_PATH,
    PARQUET_DIR,
    PARQUET_FILE_PATTERN,
    SHELL_TIMEOUT_SECONDS,
    SOURCE_STALENESS_WARN_DAYS,
    TASK_RETRIES,
    TASK_RETRY_DELAY,
)

if __name__ != "__main__":
    from airflow import DAG
    from airflow.providers.standard.operators.python import PythonOperator

log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Default task arguments
# ─────────────────────────────────────────────────────────────────────────────
_ALERT_EMAILS = [
    addr.strip()
    for addr in ALERT_EMAIL_RAW.split(",")
    if addr.strip()
]

DEFAULT_ARGS = {
    "owner":            DAG_OWNER,
    "depends_on_past":  False,
    "email":            _ALERT_EMAILS,
    "email_on_failure": True,
    "email_on_retry":   False,
    "retries":          TASK_RETRIES,
    "retry_delay":      TASK_RETRY_DELAY,
}


# ─────────────────────────────────────────────────────────────────────────────
# Shared helper
# ─────────────────────────────────────────────────────────────────────────────
def _run_shell(cmd: str) -> None:
    """Run a shell command; raise RuntimeError on non-zero exit."""
    log.info("Running: %s", cmd)
    result = subprocess.run(
        cmd,
        shell=True,
        capture_output=True,
        text=True,
        timeout=SHELL_TIMEOUT_SECONDS,
    )
    if result.stdout:
        log.info("STDOUT:\n%s", result.stdout)
    if result.returncode != 0:
        log.error("STDERR:\n%s", result.stderr)
        raise RuntimeError(
            f"Command exited with code {result.returncode}: {cmd}\n{result.stderr}"
        )


def _dbt_cmd(select: str) -> str:
    """Build a dbt run command for the given node selector."""
    return (
        f"cd {DBT_PROJECT_DIR} && "
        f"dbt run --select {select} --target {DBT_TARGET} --no-partial-parse"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Task 1: check_source_freshness
# ─────────────────────────────────────────────────────────────────────────────
def check_source_freshness(**context) -> None:
    """
    Validate that the Parquet file covering the pipeline execution date exists.

    For the monthly TLC dataset, the relevant file is:
      yellow_tripdata_{YYYY}-{MM}.parquet

    Raises AirflowException (treated as task failure) if the file is absent.

    For Snowflake pipelines, replace this with:
      _run_shell(f"cd {dbt_dir} && dbt source freshness --target {target}")
    and parse the exit code.
    """
    try:
        from airflow.exceptions import AirflowException
    except ImportError:
        AirflowException = RuntimeError  # type: ignore[misc,assignment]

    execution_date: datetime = context["data_interval_start"]
    year  = execution_date.year
    month = execution_date.month
    expected_file = os.path.join(
        PARQUET_DIR, PARQUET_FILE_PATTERN.format(year=year, month=month)
    )

    log.info("Checking source file: %s", expected_file)

    if not os.path.exists(expected_file):
        raise AirflowException(
            f"Source freshness check FAILED — file not found: {expected_file}"
        )

    file_age_days = (
        datetime.now().timestamp() - os.path.getmtime(expected_file)
    ) / 86400

    if file_age_days > SOURCE_STALENESS_WARN_DAYS:
        # Warn but don't fail — monthly files are refreshed once a month.
        log.warning(
            "Source file %s is %.1f days old — ensure it is the current month's data.",
            expected_file,
            file_age_days,
        )

    log.info("Source freshness check PASSED for %s", expected_file)


# ─────────────────────────────────────────────────────────────────────────────
# Tasks 2–4: dbt model layers
# ─────────────────────────────────────────────────────────────────────────────
def run_dbt_seed(**context) -> None:
    """Load seed files (taxi_zone_lookup) into DuckDB."""
    _run_shell(f"cd {DBT_PROJECT_DIR} && dbt seed --target {DBT_TARGET} --no-partial-parse")


def run_dbt_staging(**context) -> None:
    """Run dbt staging models."""
    _run_shell(_dbt_cmd(DBT_SELECT_STAGING))


def run_dbt_intermediate(**context) -> None:
    """Run dbt intermediate models."""
    _run_shell(_dbt_cmd(DBT_SELECT_INTERMEDIATE))


def run_dbt_marts(**context) -> None:
    """Run dbt mart models."""
    _run_shell(_dbt_cmd(DBT_SELECT_MARTS))


# ─────────────────────────────────────────────────────────────────────────────
# Task 5: run_dbt_tests
# ─────────────────────────────────────────────────────────────────────────────
def run_dbt_tests(**context) -> None:
    """
    Run all dbt tests.  Intentionally raises on failure — this propagates as
    a task failure and prevents notify_success from running, while
    email_on_failure triggers the on-call alert.

    Blue/green note:
      To prevent bad data from reaching downstream consumers, the mart models
      should be renamed to a staging schema during the run and atomically
      swapped only after run_dbt_tests succeeds.  See README for full details.
    """
    _run_shell(
        f"cd {DBT_PROJECT_DIR} && "
        f"dbt test --target {DBT_TARGET} --no-partial-parse"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Task 6: notify_success
# ─────────────────────────────────────────────────────────────────────────────
def notify_success(**context) -> None:
    """
    Log a success summary (trip count + revenue) for the pipeline run date.
    In production, extend this to post to Slack, PagerDuty, or a dashboard.
    """
    execution_date: datetime = context["data_interval_start"]
    run_date = execution_date.date()

    try:
        import duckdb

        conn = duckdb.connect(DUCKDB_PATH, read_only=True)
        row = conn.execute(
            f"""
            SELECT total_trips, total_revenue
            FROM   {AGG_DAILY_REVENUE_TABLE}
            WHERE  pickup_date = ?
            """,
            [str(run_date)],
        ).fetchone()
        conn.close()

        if row:
            total_trips, total_revenue = row
            log.info(
                "Pipeline SUCCESS | date=%s | trips=%d | revenue=$%.2f",
                run_date,
                total_trips,
                total_revenue,
            )
        else:
            log.warning(
                "Pipeline completed but no rows found in agg_daily_revenue for %s",
                run_date,
            )

    except Exception as exc:  # noqa: BLE001 — notification failures are non-fatal
        log.warning("Could not retrieve success summary (%s). Pipeline still succeeded.", exc)

    log.info("✓ nyc_taxi_daily_pipeline completed for %s", run_date)


# ─────────────────────────────────────────────────────────────────────────────
# DAG definition  (only constructed when loaded by the Airflow scheduler)
# ─────────────────────────────────────────────────────────────────────────────
if __name__ != "__main__":
    with DAG(
        dag_id=DAG_ID,
        description=(
            "NYC Yellow Taxi daily ELT: source freshness check → "
            "dbt staging → intermediate → marts → tests → notify"
        ),
        default_args=DEFAULT_ARGS,
        schedule=DAG_SCHEDULE,
        start_date=datetime(2023, 1, 1),
        catchup=True,                    # enables backfill from start_date
        max_active_runs=DAG_MAX_ACTIVE_RUNS,
        tags=DAG_TAGS,
        doc_md=__doc__,
    ) as dag:

        t_check_source = PythonOperator(
            task_id="check_source_freshness",
            python_callable=check_source_freshness,
        )

        t_seed = PythonOperator(
            task_id="run_dbt_seed",
            python_callable=run_dbt_seed,
        )

        t_staging = PythonOperator(
            task_id="run_dbt_staging",
            python_callable=run_dbt_staging,
        )

        t_intermediate = PythonOperator(
            task_id="run_dbt_intermediate",
            python_callable=run_dbt_intermediate,
        )

        t_marts = PythonOperator(
            task_id="run_dbt_marts",
            python_callable=run_dbt_marts,
        )

        t_tests = PythonOperator(
            task_id="run_dbt_tests",
            python_callable=run_dbt_tests,
        )

        t_notify = PythonOperator(
            task_id="notify_success",
            python_callable=notify_success,
        )

        # ── Dependency chain ─────────────────────────────────────────────────
        t_check_source >> t_seed >> t_staging >> t_intermediate >> t_marts >> t_tests >> t_notify


# ─────────────────────────────────────────────────────────────────────────────
# Local runner — execute task functions in order without the Airflow scheduler.
# Useful on Windows where POSIX signals are not available.
#
# Usage:
#   python dags/nyc_taxi_daily_pipeline.py               # today's date
#   python dags/nyc_taxi_daily_pipeline.py 2023-01-01    # specific run date
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import sys
    from datetime import timezone

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)-8s] %(name)s — %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    run_date_str = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime("%Y-%m-%d")
    run_date = datetime.strptime(run_date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    ctx = {"data_interval_start": run_date, "ds": run_date_str}

    tasks = [
        ("check_source_freshness", check_source_freshness),
        ("run_dbt_seed",           run_dbt_seed),
        ("run_dbt_staging",        run_dbt_staging),
        ("run_dbt_intermediate",   run_dbt_intermediate),
        ("run_dbt_marts",          run_dbt_marts),
        ("run_dbt_tests",          run_dbt_tests),
        ("notify_success",         notify_success),
    ]

    log.info("=" * 60)
    log.info("Pipeline run for date: %s", run_date_str)
    log.info("=" * 60)

    for task_id, fn in tasks:
        log.info("── TASK START: %s", task_id)
        try:
            fn(**ctx)
            log.info("── TASK SUCCESS: %s", task_id)
        except Exception as exc:
            log.error("── TASK FAILED: %s — %s", task_id, exc)
            sys.exit(1)

    log.info("=" * 60)
    log.info("Pipeline completed for %s", run_date_str)
    log.info("=" * 60)
