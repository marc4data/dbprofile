"""Tests for the `dbprofile run` export-default behavior.

When --project-dir is set, run should produce HTML + JSON + Excel + per-table
notebooks by default (opt-out via --export-* none). When --project-dir is
omitted, only the HTML report writes by default — the legacy ./reports/
fallback stays opt-in for backward compatibility.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pytest
from click.testing import CliRunner

from dbprofile.cli import (
    _resolve_export_path,
    _resolve_export_toggle,
    main,
)

# ── Unit tests for the resolver helpers ──────────────────────────────────────


class TestResolveExportPath:
    def _kwargs(self, **overrides):
        defaults = dict(
            stem="snowflake_analytics_marts",
            ext="json",
            run_at=datetime(2026, 4, 30),
            out_dir=Path("/tmp/dq_eda"),
        )
        defaults.update(overrides)
        return defaults

    def test_explicit_none_skips(self):
        assert _resolve_export_path("none", "/proj", **self._kwargs()) is None
        assert _resolve_export_path("none", None, **self._kwargs()) is None

    def test_default_when_project_dir_set(self):
        # None + project-dir → auto path
        path = _resolve_export_path(None, "/proj", **self._kwargs())
        assert path == "/tmp/dq_eda/snowflake_analytics_marts_20260430.json"

    def test_default_when_no_project_dir(self):
        # None + no project-dir → skip (legacy opt-in)
        assert _resolve_export_path(None, None, **self._kwargs()) is None

    def test_explicit_auto(self):
        path = _resolve_export_path("auto", None, **self._kwargs())
        assert path == "/tmp/dq_eda/snowflake_analytics_marts_20260430.json"

    def test_custom_path_passes_through(self):
        custom = "/some/where/results.json"
        assert _resolve_export_path(custom, "/proj", **self._kwargs()) == custom


class TestResolveExportToggle:
    def test_explicit_none_is_off(self):
        assert _resolve_export_toggle("none", "/proj") is False
        assert _resolve_export_toggle("none", None) is False

    def test_default_with_project_dir_is_on(self):
        assert _resolve_export_toggle(None, "/proj") is True

    def test_default_without_project_dir_is_off(self):
        assert _resolve_export_toggle(None, None) is False

    def test_explicit_auto_is_on(self):
        assert _resolve_export_toggle("auto", None) is True


# ── End-to-end CLI tests against the DuckDB fixture ──────────────────────────


@pytest.fixture
def dev_duckdb(tmp_path) -> Path:
    """Tiny DuckDB seed — same pattern as tests/test_notebook_e2e.py."""
    import duckdb
    db_path = tmp_path / "test.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute(
        "CREATE TABLE fct_orders AS "
        "SELECT i AS order_id, "
        "       'cat_' || (i % 5) AS category, "
        "       i * 1.25 AS amount, "
        "       (i * 0.05) AS discount, "
        "       (DATE '2026-01-01' + INTERVAL (i % 60) DAY)::DATE AS order_date "
        "FROM range(1, 201) t(i)"
    )
    conn.close()
    return db_path


@pytest.fixture
def dev_config(tmp_path, dev_duckdb) -> Path:
    cfg_path = tmp_path / "cfg.yaml"
    cfg_path.write_text(
        f"""
connection:
  dialect: duckdb
  database_path: {dev_duckdb}

scope:
  schemas: [main]
  tables: [fct_orders]

checks:
  enabled: [all]
  disabled: []
  sample_rate: 1.0
""",
        encoding="utf-8",
    )
    return cfg_path


def _run(args):
    return CliRunner().invoke(main, args, catch_exceptions=False)


def _real_files(out_dir: Path, ext: str) -> list[Path]:
    """Glob outputs but exclude hidden state files (.dbprofile_state.json)."""
    return [p for p in out_dir.glob(f"*.{ext}") if not p.name.startswith(".")]


class TestRunWithProjectDir:
    """When --project-dir is set, run should emit every artifact by default."""

    def test_emits_all_four_artifact_types_by_default(self, tmp_path, dev_config):
        proj = tmp_path / "proj"
        result = _run(["run", "--config", str(dev_config), "--project-dir", str(proj)])
        # Exit may be 1 if critical findings exist — that's documented behavior.
        assert result.exit_code in (0, 1), result.output

        dq = proj / "dq_eda"
        assert dq.is_dir()
        assert len(_real_files(dq, "html")) == 1
        assert len(_real_files(dq, "json")) == 1
        assert len(_real_files(dq, "xlsx")) == 1
        assert len(list(dq.glob("eda_*.ipynb"))) >= 1

    def test_export_json_none_skips_json_only(self, tmp_path, dev_config):
        proj = tmp_path / "proj"
        result = _run([
            "run", "--config", str(dev_config),
            "--project-dir", str(proj), "--export-json", "none",
        ])
        assert result.exit_code in (0, 1), result.output

        dq = proj / "dq_eda"
        assert _real_files(dq, "json") == []           # skipped
        assert len(_real_files(dq, "html")) == 1       # still present
        assert len(_real_files(dq, "xlsx")) == 1       # still present

    def test_export_notebook_none_skips_notebooks(self, tmp_path, dev_config):
        proj = tmp_path / "proj"
        result = _run([
            "run", "--config", str(dev_config),
            "--project-dir", str(proj), "--export-notebook", "none",
        ])
        assert result.exit_code in (0, 1), result.output

        dq = proj / "dq_eda"
        assert list(dq.glob("eda_*.ipynb")) == []
        # Other artifacts still produced
        assert len(_real_files(dq, "html")) == 1
        assert len(_real_files(dq, "json")) == 1

    def test_force_overwrites_analyst_modified_notebooks(self, tmp_path, dev_config):
        """`run --force` should propagate down to write_notebook so analyst-
        modified notebooks get overwritten in-place (originals to .backups/)."""
        import nbformat
        proj = tmp_path / "proj"

        # First run: create the initial notebooks
        r1 = _run(["run", "--config", str(dev_config), "--project-dir", str(proj)])
        assert r1.exit_code in (0, 1), r1.output

        canonical = next((proj / "dq_eda").glob("eda_fct_orders_*.ipynb"))
        # Simulate an analyst editing a cell
        nb = nbformat.read(canonical, as_version=4)
        nb.cells.append(nbformat.v4.new_markdown_cell("# analyst added this"))
        nbformat.write(nb, canonical)

        # Second run with --force: should overwrite + back up
        r2 = _run([
            "run", "--config", str(dev_config),
            "--project-dir", str(proj), "--force",
        ])
        assert r2.exit_code in (0, 1), r2.output
        # Backup written
        backups = list((proj / "dq_eda" / ".backups").glob("eda_fct_orders_*backup*.ipynb"))
        assert len(backups) == 1
        # Canonical no longer contains the analyst's cell
        new = nbformat.read(canonical, as_version=4)
        assert not any("# analyst added this" in str(c.source) for c in new.cells)


class TestRunWithoutProjectDir:
    """Without --project-dir, behavior must stay backward-compatible:
    only the HTML report writes unless --export-* is explicitly passed."""

    def test_html_only_by_default(self, tmp_path, dev_config, monkeypatch):
        # Run inside tmp_path so we don't pollute ./reports/ in the repo.
        monkeypatch.chdir(tmp_path)
        result = _run(["run", "--config", str(dev_config)])
        assert result.exit_code in (0, 1), result.output

        reports = tmp_path / "reports"
        assert len(_real_files(reports, "html")) == 1
        assert _real_files(reports, "json") == []
        assert _real_files(reports, "xlsx") == []
        assert list(reports.glob("eda_*.ipynb")) == []

    def test_explicit_export_json_auto_still_works(self, tmp_path, dev_config, monkeypatch):
        monkeypatch.chdir(tmp_path)
        result = _run([
            "run", "--config", str(dev_config), "--export-json", "auto",
        ])
        assert result.exit_code in (0, 1), result.output

        reports = tmp_path / "reports"
        assert len(_real_files(reports, "json")) == 1
