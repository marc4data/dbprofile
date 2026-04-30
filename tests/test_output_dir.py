"""Tests for dbprofile.output_dir — auto_name, resolve_output_dir, run_stem."""

from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace

import pytest

from dbprofile.output_dir import (
    DQ_EDA_SUBDIR,
    LEGACY_OUTPUT_DIR,
    auto_name,
    resolve_output_dir,
    run_stem,
)

# ── auto_name ────────────────────────────────────────────────────────────────

class TestAutoName:
    def test_basic(self):
        when = datetime(2026, 4, 30)
        assert auto_name("my_run", "html", run_at=when) == "my_run_20260430.html"

    def test_with_prefix(self):
        when = datetime(2026, 4, 30)
        result = auto_name("fct_trips", "ipynb", prefix="eda_", run_at=when)
        assert result == "eda_fct_trips_20260430.ipynb"

    def test_uses_now_when_run_at_omitted(self):
        # Just verify it doesn't crash and produces an 8-digit date.
        result = auto_name("x", "html")
        stamp = result.replace("x_", "").replace(".html", "")
        assert len(stamp) == 8 and stamp.isdigit()

    def test_zero_padded_month_and_day(self):
        when = datetime(2026, 1, 5)
        assert auto_name("x", "html", run_at=when) == "x_20260105.html"


# ── resolve_output_dir ───────────────────────────────────────────────────────

class TestResolveOutputDir:
    def test_with_project_dir_creates_dq_eda(self, tmp_path):
        proj = tmp_path / "myproj"
        out = resolve_output_dir(str(proj))
        assert out == proj / DQ_EDA_SUBDIR
        assert out.is_dir()

    def test_without_project_dir_returns_legacy(self, tmp_path, monkeypatch):
        # Run inside tmp_path so we don't pollute the real ./reports/
        monkeypatch.chdir(tmp_path)
        out = resolve_output_dir(None)
        assert out.name == LEGACY_OUTPUT_DIR
        assert out.is_dir()

    def test_idempotent_on_existing_dir(self, tmp_path):
        proj = tmp_path / "p"
        first = resolve_output_dir(str(proj))
        second = resolve_output_dir(str(proj))
        assert first == second
        assert first.is_dir()

    def test_expands_user_home(self, tmp_path, monkeypatch):
        # Point HOME at tmp_path so ~/foo resolves into tmp_path/foo.
        monkeypatch.setenv("HOME", str(tmp_path))
        out = resolve_output_dir("~/foo")
        assert out == tmp_path / "foo" / DQ_EDA_SUBDIR
        assert out.is_dir()


# ── run_stem ─────────────────────────────────────────────────────────────────

def _make_cfg(*, dialect=None, database=None, dataset=None, project=None, schemas=None):
    """Mint a minimal duck-typed config object for run_stem to walk."""
    return SimpleNamespace(
        connection=SimpleNamespace(dialect=dialect),
        scope=SimpleNamespace(
            database=database,
            dataset=dataset,
            project=project,
            schemas=schemas or [],
        ),
    )


class TestRunStem:
    def test_snowflake_with_schema(self):
        cfg = _make_cfg(dialect="snowflake", database="ANALYTICS", schemas=["DBT_MALEX_MARTS"])
        assert run_stem(cfg) == "snowflake_analytics_dbt_malex_marts"

    def test_bigquery_uses_dataset(self):
        cfg = _make_cfg(dialect="bigquery", dataset="nyc_taxi", schemas=[])
        assert run_stem(cfg) == "bigquery_nyc_taxi"

    def test_no_date_in_stem(self):
        # The stem must NOT include a date — auto_name owns dating.
        cfg = _make_cfg(dialect="duckdb", database="dev")
        stem = run_stem(cfg)
        assert "2026" not in stem and "_20" not in stem

    def test_sanitizes_special_chars(self):
        cfg = _make_cfg(dialect="snowflake", database="my-db.with.dots", schemas=["s 1"])
        # Non-[a-z0-9_] chars collapse to underscores
        assert run_stem(cfg) == "snowflake_my_db_with_dots_s_1"

    def test_lowercases(self):
        cfg = _make_cfg(dialect="SNOWFLAKE", database="ANALYTICS")
        assert run_stem(cfg) == "snowflake_analytics"

    def test_handles_empty_schemas(self):
        cfg = _make_cfg(dialect="duckdb", database="dev")
        assert run_stem(cfg) == "duckdb_dev"

    @pytest.mark.parametrize("dialect", ["snowflake", "bigquery", "duckdb"])
    def test_all_supported_dialects_produce_a_stem(self, dialect):
        cfg = _make_cfg(dialect=dialect, database="x")
        assert run_stem(cfg).startswith(dialect)
