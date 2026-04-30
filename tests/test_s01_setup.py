"""Tests for dbprofile.notebook.sections.s01_setup.

For each connector branch we assert two things:

  1. Structural — the setup source compiles as Python (modulo IPython
     line-magic markers, which we strip first).
  2. Content   — the connector-specific block contains the expected
     identifiers (env var names, helper functions, library imports).
"""

from __future__ import annotations

import ast
from types import SimpleNamespace

import pytest

from dbprofile.notebook.sections.s01_setup import (
    _build_setup_source,
    build_setup_cells,
)

# ── Test config builders ─────────────────────────────────────────────────────

def _snowflake_cfg(database="ANALYTICS"):
    return SimpleNamespace(
        connection=SimpleNamespace(dialect="snowflake"),
        scope=SimpleNamespace(database=database, dataset=None, project=None, schemas=[]),
    )


def _bigquery_cfg(project="my-proj", dataset="nyc_taxi"):
    return SimpleNamespace(
        connection=SimpleNamespace(dialect="bigquery", project=project),
        scope=SimpleNamespace(database=None, dataset=dataset, project=None, schemas=[]),
    )


def _duckdb_cfg(path="./inputs/dev.duckdb"):
    return SimpleNamespace(
        connection=SimpleNamespace(dialect="duckdb", database_path=path),
        scope=SimpleNamespace(database=None, dataset=None, project=None, schemas=[]),
    )


def _strip_ipython_magics(src: str) -> str:
    """Remove %magic and %%magic lines so plain ast.parse() can validate."""
    return "\n".join(
        line for line in src.splitlines()
        if not line.lstrip().startswith("%")
    )


# ── Cell structure ───────────────────────────────────────────────────────────

class TestCellStructure:
    def test_returns_three_cells(self):
        cells = build_setup_cells(
            cfg=_duckdb_cfg(), schema_name="main", connector_type="duckdb",
        )
        # H2 header + intro markdown + code cell
        assert [c["cell_type"] for c in cells] == ["markdown", "markdown", "code"]

    def test_header_is_h2(self):
        cells = build_setup_cells(
            cfg=_duckdb_cfg(), schema_name="main", connector_type="duckdb",
        )
        assert cells[0]["source"].startswith("## ")


# ── Universal blocks present in every dialect ────────────────────────────────

class TestUniversalBlocks:
    @pytest.mark.parametrize("dialect", ["snowflake", "bigquery", "duckdb"])
    def test_imports_helpers_and_pandas(self, dialect):
        cfg = {"snowflake": _snowflake_cfg(),
               "bigquery":  _bigquery_cfg(),
               "duckdb":    _duckdb_cfg()}[dialect]
        src = _build_setup_source(cfg=cfg, schema_name="s", connector_type=dialect)
        assert "from eda_helpers import *" in src
        assert "from eda_profile import" in src
        assert "import pandas as pd" in src
        assert "import matplotlib.pyplot as plt" in src

    @pytest.mark.parametrize("dialect", ["snowflake", "bigquery", "duckdb"])
    def test_includes_force_reload_guard(self, dialect):
        cfg = {"snowflake": _snowflake_cfg(),
               "bigquery":  _bigquery_cfg(),
               "duckdb":    _duckdb_cfg()}[dialect]
        src = _build_setup_source(cfg=cfg, schema_name="s", connector_type=dialect)
        assert "FORCE_RELOAD = False" in src

    @pytest.mark.parametrize("dialect", ["snowflake", "bigquery", "duckdb"])
    def test_defines_sql_helper(self, dialect):
        cfg = {"snowflake": _snowflake_cfg(),
               "bigquery":  _bigquery_cfg(),
               "duckdb":    _duckdb_cfg()}[dialect]
        src = _build_setup_source(cfg=cfg, schema_name="s", connector_type=dialect)
        assert "def sql(query: str)" in src


# ── Snowflake branch ─────────────────────────────────────────────────────────

class TestSnowflake:
    def test_uses_env_vars_not_hardcoded_creds(self):
        src = _build_setup_source(
            cfg=_snowflake_cfg(database="ANALYTICS"),
            schema_name="DBT_MALEX_MARTS",
            connector_type="snowflake",
        )
        assert "os.environ['SNOWFLAKE_ACCOUNT']" in src
        assert "os.environ['SNOWFLAKE_USER']" in src
        assert "os.environ['SNOWFLAKE_PRIVATE_KEY_PATH']" in src

    def test_database_and_schema_baked_from_config(self):
        src = _build_setup_source(
            cfg=_snowflake_cfg(database="analytics"),
            schema_name="dbt_malex_marts",
            connector_type="snowflake",
        )
        # Both should be uppercased in the generated code.
        assert "DATABASE = 'ANALYTICS'" in src
        assert "SCHEMA   = 'DBT_MALEX_MARTS'" in src

    def test_loads_dotenv(self):
        src = _build_setup_source(
            cfg=_snowflake_cfg(),
            schema_name="s",
            connector_type="snowflake",
        )
        assert "from dotenv import load_dotenv" in src
        assert "load_dotenv()" in src

    def test_key_pair_uses_pkcs8_der(self):
        src = _build_setup_source(
            cfg=_snowflake_cfg(),
            schema_name="s",
            connector_type="snowflake",
        )
        # Mirrors dbprofile.connectors.base.SnowflakeConnector._load_private_key
        assert "load_pem_private_key" in src
        assert "PrivateFormat.PKCS8" in src
        assert "Encoding.DER" in src


# ── BigQuery branch ──────────────────────────────────────────────────────────

class TestBigQuery:
    def test_project_and_dataset_baked_from_config(self):
        src = _build_setup_source(
            cfg=_bigquery_cfg(project="acme-warehouse", dataset="orders"),
            schema_name="orders",
            connector_type="bigquery",
        )
        assert "PROJECT = 'acme-warehouse'" in src
        assert "DATASET = 'orders'" in src

    def test_uses_bigquery_client(self):
        src = _build_setup_source(
            cfg=_bigquery_cfg(),
            schema_name="s",
            connector_type="bigquery",
        )
        assert "from google.cloud import bigquery" in src
        assert "bigquery.Client(project=PROJECT)" in src


# ── DuckDB branch ────────────────────────────────────────────────────────────

class TestDuckDB:
    def test_database_path_baked_from_config(self):
        src = _build_setup_source(
            cfg=_duckdb_cfg(path="./inputs/dev.duckdb"),
            schema_name="main",
            connector_type="duckdb",
        )
        assert "DATABASE_PATH = './inputs/dev.duckdb'" in src

    def test_read_only_connection(self):
        src = _build_setup_source(
            cfg=_duckdb_cfg(),
            schema_name="main",
            connector_type="duckdb",
        )
        # Read-only protects the analyst from accidental mutations.
        assert "duckdb.connect(DATABASE_PATH, read_only=True)" in src


# ── Unknown connector falls through gracefully ───────────────────────────────

class TestUnknownConnector:
    def test_emits_placeholder_with_todo(self):
        src = _build_setup_source(
            cfg=_duckdb_cfg(),
            schema_name="s",
            connector_type="postgres",   # not supported
        )
        assert "TODO" in src
        assert "postgres" in src


# ── The setup source must be valid Python (after stripping line-magics) ─────

class TestSyntacticValidity:
    @pytest.mark.parametrize("dialect", ["snowflake", "bigquery", "duckdb"])
    def test_compiles(self, dialect):
        cfg = {"snowflake": _snowflake_cfg(),
               "bigquery":  _bigquery_cfg(),
               "duckdb":    _duckdb_cfg()}[dialect]
        src = _build_setup_source(cfg=cfg, schema_name="s", connector_type=dialect)
        # ast.parse can't handle %magics; strip them first.
        cleaned = _strip_ipython_magics(src)
        ast.parse(cleaned)   # raises SyntaxError on failure
