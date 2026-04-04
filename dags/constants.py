"""
constants.py — Central configuration for nyc_taxi_daily_pipeline.

All tuneable values and environment-variable defaults live here.
Override any value by setting the corresponding environment variable
before running the pipeline.

"""

from __future__ import annotations

import os
from datetime import timedelta
from pathlib import Path

# ─────────────────────────────────────────────────────────────────────────────
# Repo layout
# ─────────────────────────────────────────────────────────────────────────────

# Absolute path to the dags/ directory (where this file lives).
DAGS_DIR: str = os.path.dirname(os.path.abspath(__file__))

# Root of the repository (one level above dags/).
REPO_ROOT: str = str(Path(DAGS_DIR).parent)

# ─────────────────────────────────────────────────────────────────────────────
# Environment-variable driven paths
# ─────────────────────────────────────────────────────────────────────────────

# Path to the dbt project directory.
DBT_PROJECT_DIR: str = os.getenv(
    "DBT_PROJECT_DIR",
    os.path.join(REPO_ROOT, "dbt"),
)

# dbt target profile to use (maps to an entry in profiles.yml).
DBT_TARGET: str = os.getenv("DBT_TARGET", "dev")

# Directory containing raw monthly Parquet files.
PARQUET_DIR: str = os.getenv(
    "PARQUET_DIR",
    os.path.join(REPO_ROOT, "data"),
)

# Path to the DuckDB database file (used for the success-summary query).
DUCKDB_PATH: str = os.getenv(
    "DUCKDB_PATH",
    os.path.join(REPO_ROOT, "data", "nyc_taxi.duckdb"),
)

# ─────────────────────────────────────────────────────────────────────────────
# Source data
# ─────────────────────────────────────────────────────────────────────────────

# Filename pattern for TLC monthly Parquet files.
# Format args: year (int), month (zero-padded int).
PARQUET_FILE_PATTERN: str = "yellow_tripdata_{year}-{month:02d}.parquet"

# Number of days after which a Parquet file is considered stale (warn only).
SOURCE_STALENESS_WARN_DAYS: int = 35

# ─────────────────────────────────────────────────────────────────────────────
# dbt selectors
# ─────────────────────────────────────────────────────────────────────────────

DBT_SELECT_STAGING: str      = "staging"
DBT_SELECT_INTERMEDIATE: str = "intermediate"
DBT_SELECT_MARTS: str        = "marts"

# ─────────────────────────────────────────────────────────────────────────────
# Output mart table used for the success-summary query
# ─────────────────────────────────────────────────────────────────────────────

MART_SCHEMA: str              = "main_marts"
AGG_DAILY_REVENUE_TABLE: str  = f"{MART_SCHEMA}.agg_daily_revenue"

# ─────────────────────────────────────────────────────────────────────────────
# Airflow DAG settings
# ─────────────────────────────────────────────────────────────────────────────

DAG_ID: str          = "nyc_taxi_daily_pipeline"
DAG_OWNER: str       = "data-engineering"
DAG_SCHEDULE: str    = "0 2 * * *"   # 02:00 UTC daily
DAG_MAX_ACTIVE_RUNS: int = 3
DAG_TAGS: list[str]  = ["nyc-taxi", "dbt", "daily"]

# ─────────────────────────────────────────────────────────────────────────────
# Retry / alerting
# ─────────────────────────────────────────────────────────────────────────────

TASK_RETRIES: int              = 2
TASK_RETRY_DELAY: timedelta    = timedelta(minutes=5)
SHELL_TIMEOUT_SECONDS: int     = 3600   # 1-hour hard limit per dbt invocation

# Comma-separated email addresses for failure alerts (set via env var).
ALERT_EMAIL_RAW: str = os.getenv("ALERT_EMAIL", "")


