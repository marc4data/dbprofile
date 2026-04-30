"""Tests for dbprofile.notebook.sections.s02_data_gather."""

from __future__ import annotations

import ast
from types import SimpleNamespace

import pytest

from dbprofile.notebook.classify import ColumnKind
from dbprofile.notebook.sections.s02_data_gather import (
    _bernoulli_pct,
    _first_date_column,
    _row_count_from_results,
    _sample_clause,
    _table_ref,
    build_data_gather_cells,
)

# ── Tiny config builders ─────────────────────────────────────────────────────

def _duckdb_cfg():
    return SimpleNamespace(
        connection=SimpleNamespace(dialect="duckdb", database_path="./inputs/dev.duckdb"),
        scope=SimpleNamespace(database=None, dataset=None, project=None, schemas=[]),
    )

def _snowflake_cfg(database="ANALYTICS"):
    return SimpleNamespace(
        connection=SimpleNamespace(dialect="snowflake"),
        scope=SimpleNamespace(database=database, dataset=None, project=None, schemas=[]),
    )

def _bigquery_cfg(project="my-proj"):
    return SimpleNamespace(
        connection=SimpleNamespace(dialect="bigquery", project=project),
        scope=SimpleNamespace(database=None, dataset="ds", project=None, schemas=[]),
    )


# ── _bernoulli_pct ───────────────────────────────────────────────────────────

class TestBernoulliPct:
    @pytest.mark.parametrize(
        "row_count,expected",
        [
            (None,        100.0),   # no row count → no sampling
            (0,           100.0),   # empty table → no sampling
            (1_000,       100.0),   # under target → no sampling
            (50_000,      100.0),   # exactly target → no sampling (pct = 100)
            (100_000,     50.0),    # 50K / 100K = 50%
            (1_000_000,   5.0),     # 50K / 1M = 5%
            (1_000_000_000, 0.1),   # huge table → clamped to floor 0.1%
        ],
    )
    def test_target_50k_default(self, row_count, expected):
        assert _bernoulli_pct(row_count) == pytest.approx(expected)


# ── _sample_clause ───────────────────────────────────────────────────────────

class TestSampleClause:
    def test_pct_100_returns_empty_for_every_dialect(self):
        for d in ["snowflake", "bigquery", "duckdb"]:
            assert _sample_clause(d, 100.0) == ""

    def test_snowflake_uses_sample_bernoulli(self):
        assert _sample_clause("snowflake", 5.0) == "SAMPLE BERNOULLI (5.00)"

    def test_bigquery_falls_back_to_system(self):
        # BigQuery doesn't support BERNOULLI, so we degrade to SYSTEM.
        assert "SYSTEM" in _sample_clause("bigquery", 5.0)
        assert "BERNOULLI" not in _sample_clause("bigquery", 5.0)

    def test_duckdb_uses_using_sample(self):
        clause = _sample_clause("duckdb", 5.0)
        assert clause == "USING SAMPLE 5.00 PERCENT (BERNOULLI)"


# ── _table_ref ───────────────────────────────────────────────────────────────

class TestTableRef:
    def test_snowflake_uppercases_and_dot_qualifies(self):
        ref = _table_ref(connector_type="snowflake", table="fct_orders",
                         schema_name="dbt_marts", cfg=_snowflake_cfg())
        assert ref == "ANALYTICS.DBT_MARTS.FCT_ORDERS"

    def test_bigquery_uses_backticks(self):
        ref = _table_ref(connector_type="bigquery", table="fct_orders",
                         schema_name="orders_dataset", cfg=_bigquery_cfg())
        assert ref == "`my-proj.orders_dataset.fct_orders`"

    def test_duckdb_uses_main_default(self):
        ref = _table_ref(connector_type="duckdb", table="fct_orders",
                         schema_name=None, cfg=_duckdb_cfg())
        assert ref == "main.fct_orders"

    def test_duckdb_with_explicit_schema(self):
        ref = _table_ref(connector_type="duckdb", table="fct_orders",
                         schema_name="staging", cfg=_duckdb_cfg())
        assert ref == "staging.fct_orders"


# ── _row_count_from_results ──────────────────────────────────────────────────

class TestRowCountFromResults:
    def test_finds_matching_table(self):
        rs = [
            SimpleNamespace(table="fct_orders", check_name="row_count",
                            metric="row_count", value=12_345),
        ]
        assert _row_count_from_results(rs, "fct_orders") == 12_345

    def test_returns_none_when_no_match(self):
        rs = [SimpleNamespace(table="other", check_name="row_count",
                              metric="row_count", value=100)]
        assert _row_count_from_results(rs, "fct_orders") is None

    def test_ignores_non_row_count_metrics(self):
        rs = [SimpleNamespace(table="fct_orders", check_name="row_count",
                              metric="daily_distribution", value=42)]
        assert _row_count_from_results(rs, "fct_orders") is None


# ── _first_date_column ───────────────────────────────────────────────────────

class TestFirstDateColumn:
    def test_returns_first_date_in_column_order(self):
        cols = [
            {"name": "id"},
            {"name": "created_at"},
            {"name": "updated_at"},
        ]
        classified = {
            "id":         ColumnKind.STRING_ID,
            "created_at": ColumnKind.DATE,
            "updated_at": ColumnKind.DATE,
        }
        assert _first_date_column(cols, classified) == "created_at"

    def test_returns_none_when_no_date_columns(self):
        cols = [{"name": "id"}, {"name": "amount"}]
        classified = {"id": ColumnKind.STRING_ID, "amount": ColumnKind.CONTINUOUS}
        assert _first_date_column(cols, classified) is None


# ── build_data_gather_cells — end-to-end shape ───────────────────────────────

class TestBuildDataGatherCells:
    def _basic_inputs(self, *, with_date: bool, row_count: int = 200):
        cols = [{"name": "amount", "data_type": "DOUBLE"}]
        classified = {"amount": ColumnKind.CONTINUOUS}
        if with_date:
            cols.insert(0, {"name": "order_date", "data_type": "DATE"})
            classified["order_date"] = ColumnKind.DATE
        check_results = [SimpleNamespace(
            table="fct_orders", check_name="row_count",
            metric="row_count", value=row_count,
        )]
        return cols, classified, check_results

    def test_emits_header_intro_and_sample_df(self):
        cols, classified, results = self._basic_inputs(with_date=False)
        cells = build_data_gather_cells(
            cfg=_duckdb_cfg(), table="fct_orders", schema_name="main",
            columns=cols, classified=classified, check_results=results,
            connector_type="duckdb",
        )
        # H2 + intro markdown + sample_df code + profile() code
        assert [c["cell_type"] for c in cells] == ["markdown", "markdown", "code", "code"]
        assert cells[0]["source"].startswith("## ")
        assert "TABLE_REF" in cells[2]["source"]
        assert "FORCE_RELOAD" in cells[2]["source"]
        assert cells[3]["source"] == "profile(sample_df, charts=False)"

    def test_appends_daily_df_when_date_column_present(self):
        cols, classified, results = self._basic_inputs(with_date=True)
        cells = build_data_gather_cells(
            cfg=_duckdb_cfg(), table="fct_orders", schema_name="main",
            columns=cols, classified=classified, check_results=results,
            connector_type="duckdb",
        )
        # header + intro + sample_df + profile + h3 + intro + daily_df + profile
        assert len(cells) == 8
        types = [c["cell_type"] for c in cells]
        assert types == ["markdown", "markdown", "code", "code",
                         "markdown", "markdown", "code", "code"]
        assert "daily_df" in cells[6]["source"]
        assert "GROUP BY 1" in cells[6]["source"]

    def test_no_sampling_clause_when_table_under_target(self):
        cols, classified, results = self._basic_inputs(with_date=False, row_count=1_000)
        cells = build_data_gather_cells(
            cfg=_duckdb_cfg(), table="fct_orders", schema_name="main",
            columns=cols, classified=classified, check_results=results,
            connector_type="duckdb",
        )
        # 1,000 rows < 50K target → no SAMPLE clause emitted
        sample_src = cells[2]["source"]
        assert "USING SAMPLE" not in sample_src
        assert "SAMPLE BERNOULLI" not in sample_src

    def test_emits_sample_clause_when_table_over_target(self):
        cols, classified, results = self._basic_inputs(with_date=False, row_count=1_000_000)
        cells = build_data_gather_cells(
            cfg=_duckdb_cfg(), table="fct_orders", schema_name="main",
            columns=cols, classified=classified, check_results=results,
            connector_type="duckdb",
        )
        sample_src = cells[2]["source"]
        assert "USING SAMPLE 5.00 PERCENT (BERNOULLI)" in sample_src

    def test_bigquery_note_for_system_fallback(self):
        cols, classified, results = self._basic_inputs(with_date=False, row_count=1_000_000)
        cells = build_data_gather_cells(
            cfg=_bigquery_cfg(), table="fct_orders", schema_name="ds",
            columns=cols, classified=classified, check_results=results,
            connector_type="bigquery",
        )
        intro = cells[1]["source"]
        # The intro should warn that BERNOULLI degrades to SYSTEM on BigQuery.
        assert "BERNOULLI" in intro and "SYSTEM" in intro


# ── Generated SQL is syntactically valid Python ──────────────────────────────

class TestSyntacticValidity:
    def test_sample_df_cell_compiles(self):
        cols, classified, results = (
            [{"name": "amount", "data_type": "DOUBLE"}],
            {"amount": ColumnKind.CONTINUOUS},
            [SimpleNamespace(table="t", check_name="row_count",
                             metric="row_count", value=1_000_000)],
        )
        cells = build_data_gather_cells(
            cfg=_duckdb_cfg(), table="t", schema_name="main",
            columns=cols, classified=classified, check_results=results,
            connector_type="duckdb",
        )
        for c in cells:
            if c["cell_type"] == "code":
                ast.parse(c["source"])   # raises on bad syntax
