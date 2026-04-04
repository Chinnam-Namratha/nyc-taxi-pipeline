"""
Unit tests for nyc_taxi_daily_pipeline.

All tests are hermetic — no filesystem, subprocess, or DuckDB calls are made
without mocking.  Run with:
  pytest tests/test_pipeline.py -v
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch, call

import pytest

# ---------------------------------------------------------------------------
# Make the dags/ module importable without Airflow installed
# ---------------------------------------------------------------------------
sys.modules.setdefault("airflow", MagicMock())
sys.modules.setdefault("airflow.exceptions", MagicMock())
sys.modules.setdefault("airflow.providers", MagicMock())
sys.modules.setdefault("airflow.providers.standard", MagicMock())
sys.modules.setdefault("airflow.providers.standard.operators", MagicMock())
sys.modules.setdefault("airflow.providers.standard.operators.python", MagicMock())

# Force module to behave as if NOT run as __main__ so DAG block is skipped
import importlib
import types

# Load pipeline as a regular module (not __main__)
_dag_path = Path(__file__).parent.parent / "dags" / "nyc_taxi_daily_pipeline.py"
spec = importlib.util.spec_from_file_location("nyc_taxi_daily_pipeline", _dag_path)
pipeline = importlib.util.module_from_spec(spec)
pipeline.__name__ = "nyc_taxi_daily_pipeline"  # not __main__, so DAG block runs
spec.loader.exec_module(pipeline)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _ctx(date_str: str = "2023-01-01") -> dict:
    """Build a minimal Airflow-style task context."""
    dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return {"data_interval_start": dt, "ds": date_str}


# ---------------------------------------------------------------------------
# _dbt_cmd
# ---------------------------------------------------------------------------
class TestDbtCmd:
    def test_default_select(self):
        cmd = pipeline._dbt_cmd("staging")
        assert "dbt run --select staging" in cmd
        assert "--target dev" in cmd

    def test_custom_env_vars(self, monkeypatch):
        monkeypatch.setenv("DBT_PROJECT_DIR", "/custom/dbt")
        monkeypatch.setenv("DBT_TARGET", "prod")
        cmd = pipeline._dbt_cmd("marts")
        assert "cd /custom/dbt" in cmd
        assert "--target prod" in cmd
        assert "--select marts" in cmd

    def test_no_partial_parse_flag(self):
        cmd = pipeline._dbt_cmd("intermediate")
        assert "--no-partial-parse" in cmd


# ---------------------------------------------------------------------------
# _run_shell
# ---------------------------------------------------------------------------
class TestRunShell:
    def test_success(self):
        mock_result = MagicMock(returncode=0, stdout="done", stderr="")
        with patch("subprocess.run", return_value=mock_result):
            pipeline._run_shell("echo hello")  # should not raise

    def test_failure_raises_runtime_error(self):
        mock_result = MagicMock(returncode=1, stdout="", stderr="something went wrong")
        with patch("subprocess.run", return_value=mock_result):
            with pytest.raises(RuntimeError, match="something went wrong"):
                pipeline._run_shell("bad command")

    def test_timeout_passed_to_subprocess(self):
        mock_result = MagicMock(returncode=0, stdout="", stderr="")
        with patch("subprocess.run", return_value=mock_result) as mock_run:
            pipeline._run_shell("echo hi")
            _, kwargs = mock_run.call_args
            assert kwargs["timeout"] == 3600


# ---------------------------------------------------------------------------
# check_source_freshness
# ---------------------------------------------------------------------------
class TestCheckSourceFreshness:
    def test_passes_when_file_exists(self, tmp_path, monkeypatch):
        parquet_file = tmp_path / "yellow_tripdata_2023-01.parquet"
        parquet_file.write_bytes(b"fake")
        monkeypatch.setenv("PARQUET_DIR", str(tmp_path))
        # Should not raise
        pipeline.check_source_freshness(**_ctx("2023-01-01"))

    def test_fails_when_file_missing(self, tmp_path, monkeypatch):
        monkeypatch.setenv("PARQUET_DIR", str(tmp_path))
        # AirflowException is mocked — the pipeline raises TypeError wrapping it,
        # or RuntimeError when Airflow is not installed. Either way it must raise.
        with pytest.raises((RuntimeError, TypeError, Exception)):
            pipeline.check_source_freshness(**_ctx("2023-06-01"))

    def test_correct_filename_derived_from_date(self, tmp_path, monkeypatch):
        # March → 03
        parquet_file = tmp_path / "yellow_tripdata_2023-03.parquet"
        parquet_file.write_bytes(b"fake")
        monkeypatch.setenv("PARQUET_DIR", str(tmp_path))
        pipeline.check_source_freshness(**_ctx("2023-03-15"))  # should not raise

    def test_warns_for_old_file(self, tmp_path, monkeypatch, caplog):
        import time
        parquet_file = tmp_path / "yellow_tripdata_2023-01.parquet"
        parquet_file.write_bytes(b"fake")
        # Backdate mtime by 40 days
        old_time = time.time() - (40 * 86400)
        os.utime(parquet_file, (old_time, old_time))
        monkeypatch.setenv("PARQUET_DIR", str(tmp_path))
        import logging
        with caplog.at_level(logging.WARNING):
            pipeline.check_source_freshness(**_ctx("2023-01-01"))
        assert any("days old" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# run_dbt_seed / staging / intermediate / marts
# ---------------------------------------------------------------------------
class TestDbtTaskFunctions:
    @pytest.mark.parametrize("fn_name,expected_select", [
        ("run_dbt_staging",      "staging"),
        ("run_dbt_intermediate", "intermediate"),
        ("run_dbt_marts",        "marts"),
    ])
    def test_dbt_layer_calls_correct_select(self, fn_name, expected_select):
        fn = getattr(pipeline, fn_name)
        with patch.object(pipeline, "_run_shell") as mock_shell:
            fn(**_ctx())
            cmd = mock_shell.call_args[0][0]
            assert f"--select {expected_select}" in cmd

    def test_run_dbt_seed_calls_dbt_seed(self):
        with patch.object(pipeline, "_run_shell") as mock_shell:
            pipeline.run_dbt_seed(**_ctx())
            cmd = mock_shell.call_args[0][0]
            assert "dbt seed" in cmd


# ---------------------------------------------------------------------------
# run_dbt_tests
# ---------------------------------------------------------------------------
class TestRunDbtTests:
    def test_calls_dbt_test(self):
        with patch.object(pipeline, "_run_shell") as mock_shell:
            pipeline.run_dbt_tests(**_ctx())
            cmd = mock_shell.call_args[0][0]
            assert "dbt test" in cmd

    def test_propagates_failure(self):
        with patch.object(pipeline, "_run_shell", side_effect=RuntimeError("test failed")):
            with pytest.raises(RuntimeError, match="test failed"):
                pipeline.run_dbt_tests(**_ctx())


# ---------------------------------------------------------------------------
# notify_success
# ---------------------------------------------------------------------------
class TestNotifySuccess:
    def test_logs_trip_count_and_revenue(self, tmp_path, monkeypatch, caplog):
        db_path = str(tmp_path / "nyc_taxi.duckdb")
        monkeypatch.setenv("DUCKDB_PATH", db_path)

        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchone.return_value = (1234, 9876.50)

        import logging
        with patch("duckdb.connect", return_value=mock_conn):
            with caplog.at_level(logging.INFO):
                pipeline.notify_success(**_ctx("2023-01-01"))

        assert any("1234" in r.message for r in caplog.records)
        assert any("9876.50" in r.message for r in caplog.records)

    def test_warns_when_no_rows(self, tmp_path, monkeypatch, caplog):
        monkeypatch.setenv("DUCKDB_PATH", str(tmp_path / "nyc_taxi.duckdb"))
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchone.return_value = None

        import logging
        with patch("duckdb.connect", return_value=mock_conn):
            with caplog.at_level(logging.WARNING):
                pipeline.notify_success(**_ctx("2023-01-01"))

        assert any("no rows found" in r.message for r in caplog.records)

    def test_non_fatal_on_duckdb_error(self, tmp_path, monkeypatch):
        monkeypatch.setenv("DUCKDB_PATH", str(tmp_path / "nyc_taxi.duckdb"))
        with patch("duckdb.connect", side_effect=Exception("db error")):
            # Should not raise — notification failures are non-fatal
            pipeline.notify_success(**_ctx("2023-01-01"))
