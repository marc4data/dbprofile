"""End-to-end test: dbprofile notebook command against the DuckDB fixture.

Exercises the full path: load config → run profile → classify columns →
build notebook → write notebook with hash detection. Confirms the
generated notebooks are valid nbformat documents that can be read back.
"""

from __future__ import annotations

from pathlib import Path

import nbformat
import pytest
from click.testing import CliRunner

from dbprofile.cli import main


@pytest.fixture
def dev_duckdb(tmp_path) -> Path:
    """Build a tiny DuckDB seed file in tmp_path.

    We don't reuse inputs/dev.duckdb because the CI environment doesn't
    have it (it's gitignored). A two-table seed is enough to exercise the
    notebook generator end-to-end.
    """
    import duckdb
    db_path = tmp_path / "test.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute(
        "CREATE TABLE fct_orders AS "
        "SELECT i AS order_id, "
        "       'cat_' || (i % 5) AS category, "
        "       i * 1.25 AS amount, "
        "       (DATE '2026-01-01' + INTERVAL (i % 60) DAY)::DATE AS order_date "
        "FROM range(1, 201) t(i)"
    )
    conn.execute(
        "CREATE TABLE dim_customers AS "
        "SELECT i AS customer_id, "
        "       'name_' || i AS name, "
        "       (i % 2 = 0) AS is_active "
        "FROM range(1, 51) t(i)"
    )
    conn.close()
    return db_path


@pytest.fixture
def dev_config(tmp_path, dev_duckdb) -> Path:
    """Write a minimal YAML pointing at the seed DuckDB."""
    cfg_path = tmp_path / "cfg.yaml"
    cfg_path.write_text(
        f"""
connection:
  dialect: duckdb
  database_path: {dev_duckdb}

scope:
  schemas: [main]
  tables: [fct_orders, dim_customers]

checks:
  enabled: [all]
  disabled: []
  sample_rate: 1.0
""",
        encoding="utf-8",
    )
    return cfg_path


def test_notebook_command_produces_valid_ipynb(tmp_path, dev_config):
    runner = CliRunner()
    project_dir = tmp_path / "proj"

    result = runner.invoke(
        main,
        ["notebook", "--config", str(dev_config), "--project-dir", str(project_dir)],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output

    dq_eda = project_dir / "dq_eda"
    notebooks = sorted(dq_eda.glob("eda_*.ipynb"))
    # One notebook per table in scope
    assert len(notebooks) == 2
    expected_names = {"fct_orders", "dim_customers"}
    assert {n.stem.split("_", 1)[1].rsplit("_", 1)[0] for n in notebooks} == expected_names

    # Each notebook is valid nbformat and has the dbprofile metadata block
    for nb_path in notebooks:
        nb = nbformat.read(nb_path, as_version=4)
        nbformat.validate(nb)
        assert "dbprofile" in nb["metadata"]
        assert "source_hash" in nb["metadata"]["dbprofile"]

        code_sources = [c.source for c in nb.cells if c.cell_type == "code"]
        md_sources = [c.source for c in nb.cells if c.cell_type == "markdown"]

        # s01 Setup
        assert any("from eda_helpers import" in s for s in code_sources)
        assert any("duckdb.connect(DATABASE_PATH, read_only=True)" in s
                   for s in code_sources)
        assert any("FORCE_RELOAD = False" in s for s in code_sources)

        # s02 Data Gathering
        assert any(s.startswith("## Data Gathering") for s in md_sources)
        assert any("TABLE_REF" in s and "sample_df" in s for s in code_sources)
        assert any("profile(sample_df, charts=False)" in s for s in code_sources)

        # s00 Header — H1 title cell + at least one DQ callout
        assert any(s.startswith("# ") and "EDA / Data Quality Review" in s
                   for s in md_sources)
        assert any(s.startswith("> [!") for s in md_sources)

        # s03 Schema & Grain
        assert any(s.startswith("## Schema & Grain") for s in md_sources)
        assert any("schema(sample_df)" in s for s in code_sources)
        assert any("describe_by_type(sample_df)" in s for s in code_sources)

        # s04 Univariate Analysis (the dev DuckDB tables have continuous
        # columns, so plot_distribution is guaranteed to appear)
        assert any(s.startswith("## Univariate Analysis") for s in md_sources)
        assert any("plot_distribution(" in s for s in code_sources)

    # Helpers were seeded
    for h in ("eda_helpers.py", "eda_profile.py", "eda_helpers_call_templates.py"):
        assert (dq_eda / h).is_file()


def test_tables_filter_limits_output(tmp_path, dev_config):
    runner = CliRunner()
    project_dir = tmp_path / "proj"

    result = runner.invoke(
        main,
        [
            "notebook",
            "--config", str(dev_config),
            "--project-dir", str(project_dir),
            "--tables", "fct_orders",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output

    notebooks = sorted((project_dir / "dq_eda").glob("eda_*.ipynb"))
    assert len(notebooks) == 1
    assert "fct_orders" in notebooks[0].name


def test_json_path_works_without_db_connection(tmp_path, dev_config):
    """First run produces JSON; second run with --json regenerates the
    notebook from JSON without re-querying."""
    runner = CliRunner()
    project_dir = tmp_path / "proj"

    # First produce a JSON via `run`
    r1 = runner.invoke(
        main,
        [
            "run",
            "--config", str(dev_config),
            "--project-dir", str(project_dir),
            "--export-json", "auto",
        ],
        catch_exceptions=False,
    )
    assert r1.exit_code == 0, r1.output
    # Glob matches .dbprofile_state.json too — filter to the run JSON.
    json_files = [
        p for p in (project_dir / "dq_eda").glob("*.json")
        if not p.name.startswith(".")
    ]
    assert len(json_files) == 1

    # Now regenerate notebook from JSON with --json
    r2 = runner.invoke(
        main,
        [
            "notebook",
            "--config", str(dev_config),
            "--project-dir", str(project_dir),
            "--json", str(json_files[0]),
        ],
        catch_exceptions=False,
    )
    assert r2.exit_code == 0, r2.output
    notebooks = sorted((project_dir / "dq_eda").glob("eda_*.ipynb"))
    assert len(notebooks) == 2
