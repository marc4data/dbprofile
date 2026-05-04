"""Tests for the cfg.notebook config surface.

Exercises every override path the YAML schema exposes:
  * Pydantic validation (defaults, typo rejection)
  * Per-column kind overrides applied by classify.py
  * Section enable/disable toggles + per-section knobs in s02–s07
  * DQ Follow-up max_subsections cap + skip_checks + skip_columns + overflow
  * notebook_config.example.yaml dropped into dq_eda/
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest
import yaml

from dbprofile.config import NotebookConfig, ProfileConfig, load_config
from dbprofile.notebook.classify import ColumnKind, classify_columns
from dbprofile.notebook.helper_copy import (
    NOTEBOOK_CONFIG_EXAMPLE_FILENAME,
    copy_helpers,
)
from dbprofile.notebook.sections.s02_data_gather import build_data_gather_cells
from dbprofile.notebook.sections.s03_grain import build_grain_cells
from dbprofile.notebook.sections.s04_univariate import build_univariate_cells
from dbprofile.notebook.sections.s05_bivariate import build_bivariate_cells
from dbprofile.notebook.sections.s07_dq_followup import build_dq_followup_cells

# ── Pydantic validation ──────────────────────────────────────────────────────

class TestPydanticValidation:
    def test_default_notebook_config_loads_with_no_yaml_block(self):
        """A YAML config that omits `notebook:` entirely should still load
        and produce a NotebookConfig with all defaults."""
        nb = NotebookConfig()
        assert nb.columns == {}
        assert nb.sections.header.enabled is True
        assert nb.sections.dq_followup.max_subsections == 20

    def test_unknown_kind_value_raises(self):
        with pytest.raises(Exception):  # ValidationError
            NotebookConfig(columns={"FOO": {"kind": "potato"}})

    def test_valid_kind_passes(self):
        nb = NotebookConfig(columns={"FOO": {"kind": "binary"}})
        assert nb.columns["FOO"].kind == "binary"

    def test_full_config_round_trips_through_yaml(self, tmp_path):
        """Write a YAML config with a notebook block, load it through
        load_config, verify the parsed object."""
        cfg_path = tmp_path / "cfg.yaml"
        cfg_path.write_text(
            """
connection:
  dialect: duckdb
  database_path: /tmp/x.duckdb

scope:
  schemas: [main]

notebook:
  columns:
    AIRPORT_PICKUP_IND: { kind: binary }
    PICKUP_MONTH:       { kind: ordinal_cat }
  sections:
    dq_followup:
      max_subsections: 5
      skip_checks: [frequency_distribution]
""",
            encoding="utf-8",
        )
        cfg = load_config(cfg_path)
        assert cfg.notebook.columns["AIRPORT_PICKUP_IND"].kind == "binary"
        assert cfg.notebook.columns["PICKUP_MONTH"].kind == "ordinal_cat"
        assert cfg.notebook.sections.dq_followup.max_subsections == 5
        assert cfg.notebook.sections.dq_followup.skip_checks == ["frequency_distribution"]


# ── Per-column overrides applied by classifier ──────────────────────────────

class TestColumnOverrides:
    def test_override_wins_over_auto_classification(self):
        """An *_IND column would auto-classify as CONTINUOUS (numeric, no
        cardinality info). With an override it becomes BINARY."""
        cols = [{"name": "AIRPORT_IND", "data_type": "INTEGER"}]
        # Without override: CONTINUOUS
        out_default = classify_columns(cols, [])
        assert out_default["AIRPORT_IND"] == ColumnKind.CONTINUOUS
        # With override: BINARY
        out_override = classify_columns(cols, [], overrides={"AIRPORT_IND": "binary"})
        assert out_override["AIRPORT_IND"] == ColumnKind.BINARY

    def test_override_for_unknown_column_is_silently_ignored(self):
        cols = [{"name": "REAL_COL", "data_type": "INTEGER"}]
        out = classify_columns(cols, [], overrides={"NOPE": "binary"})
        # No KeyError — REAL_COL still classified normally
        assert "REAL_COL" in out
        assert "NOPE" not in out

    def test_override_works_for_every_kind_value(self):
        cols = [{"name": "X", "data_type": "VARCHAR"}]
        for kind_value in [
            "date", "binary", "ordinal_cat", "low_cat", "high_cat",
            "string_id", "count_metric", "continuous", "unknown",
        ]:
            out = classify_columns(cols, [], overrides={"X": kind_value})
            assert out["X"].value == kind_value


# ── Section toggles + per-section knobs ─────────────────────────────────────


def _cols(*names_kinds):
    cols = [{"name": n, "data_type": "x"} for n, _ in names_kinds]
    classified = {n: k for n, k in names_kinds}
    return cols, classified


class TestS02DataGatherKnobs:
    def test_sample_target_rows_threads_through(self):
        # row_count = 100K, target = 10K → expect 10% sample
        cols, classified = _cols(("amount", ColumnKind.CONTINUOUS))
        results = [SimpleNamespace(table="t", check_name="row_count",
                                   metric="row_count", value=100_000)]
        section_cfg = SimpleNamespace(
            sample_target_rows=10_000, sample_floor_pct=0.1,
        )
        cfg = SimpleNamespace(
            scope=SimpleNamespace(database=None, dataset=None,
                                  project=None, schemas=[]),
            connection=SimpleNamespace(dialect="duckdb",
                                       database_path="/tmp/x.duckdb"),
        )
        cells = build_data_gather_cells(
            cfg=cfg, table="t", schema_name="main",
            columns=cols, classified=classified, check_results=results,
            connector_type="duckdb", section_cfg=section_cfg,
        )
        sample_src = next(c["source"] for c in cells
                          if c["cell_type"] == "code" and "TABLE_REF" in c["source"])
        assert "USING SAMPLE 10.00 PERCENT" in sample_src


class TestS03GrainToggles:
    def test_include_boundary_false_skips_boundary(self):
        cols, classified = _cols(
            ("amount",   ColumnKind.CONTINUOUS),
            ("category", ColumnKind.LOW_CAT),
        )
        section_cfg = SimpleNamespace(
            include_boundary=False, include_cardinality=True,
        )
        cells = build_grain_cells(
            columns=cols, classified=classified, section_cfg=section_cfg,
        )
        sources = [c["source"] for c in cells]
        assert not any("_boundary_cols" in s for s in sources)
        assert any("schema(sample_df)" in s for s in sources)

    def test_include_cardinality_false_skips_summary(self):
        cols, classified = _cols(("amount", ColumnKind.CONTINUOUS))
        section_cfg = SimpleNamespace(
            include_boundary=True, include_cardinality=False,
        )
        cells = build_grain_cells(
            columns=cols, classified=classified, section_cfg=section_cfg,
        )
        sources = [c["source"] for c in cells]
        assert not any("schema(sample_df)" in s for s in sources)
        assert not any("describe_by_type" in s for s in sources)


class TestS04UnivariateKnobs:
    def _cfg(self, **overrides):
        d = dict(
            max_continuous_panels=12,
            flag_panel=SimpleNamespace(enabled=True, label_threshold=12),
            categorical=SimpleNamespace(enabled=True, low_cat_threshold=15, hc_top_n=20),
            count_metrics=SimpleNamespace(enabled=True),
            distributions=SimpleNamespace(enabled=True),
        )
        d.update(overrides)
        return SimpleNamespace(**d)

    def test_max_continuous_panels_caps_distributions(self):
        cols, classified = _cols(*[
            (f"c{i}", ColumnKind.CONTINUOUS) for i in range(20)
        ])
        cells = build_univariate_cells(
            columns=cols, classified=classified, check_results=[],
            section_cfg=self._cfg(max_continuous_panels=3),
        )
        dist_calls = [c["source"] for c in cells
                      if c["cell_type"] == "code" and "plot_distribution(" in c["source"]]
        assert len(dist_calls) == 3

    def test_label_threshold_threads_into_flag_panel(self):
        cols, classified = _cols(("flag", ColumnKind.BINARY))
        cells = build_univariate_cells(
            columns=cols, classified=classified, check_results=[],
            section_cfg=self._cfg(
                flag_panel=SimpleNamespace(enabled=True, label_threshold=99),
            ),
        )
        flag_src = next(c["source"] for c in cells
                        if c["cell_type"] == "code" and "plot_histograms(" in c["source"])
        assert "label_threshold = 99" in flag_src

    def test_hc_top_n_threads_into_categorical_panel(self):
        cols, classified = _cols(("zone", ColumnKind.HIGH_CAT))
        cells = build_univariate_cells(
            columns=cols, classified=classified, check_results=[],
            section_cfg=self._cfg(
                categorical=SimpleNamespace(
                    enabled=True, low_cat_threshold=15, hc_top_n=50,
                ),
            ),
        )
        hc_src = next(c["source"] for c in cells
                      if c["cell_type"] == "code"
                      and "plot_string_profile_hc(" in c["source"])
        assert "top_n  = 50" in hc_src

    def test_disabled_sub_panel_is_skipped(self):
        cols, classified = _cols(
            ("flag",   ColumnKind.BINARY),
            ("amount", ColumnKind.CONTINUOUS),
        )
        cells = build_univariate_cells(
            columns=cols, classified=classified, check_results=[],
            section_cfg=self._cfg(
                flag_panel=SimpleNamespace(enabled=False, label_threshold=12),
            ),
        )
        sources = [c["source"] for c in cells]
        assert not any("plot_histograms(" in s for s in sources)
        # Distributions still emitted (its sub-panel still enabled)
        assert any("plot_distribution(" in s for s in sources)


class TestS05BivariateKnobs:
    def test_top_pairs_and_corr_thresholds_thread_through(self):
        cols, classified = _cols(
            ("a", ColumnKind.CONTINUOUS),
            ("b", ColumnKind.CONTINUOUS),
        )
        section_cfg = SimpleNamespace(top_pairs=2, corr_floor=0.2, corr_ceiling=0.95)
        cells = build_bivariate_cells(
            columns=cols, classified=classified, section_cfg=section_cfg,
        )
        scatter_src = next(c["source"] for c in cells
                           if c["cell_type"] == "code" and "_pairs.sort" in c["source"])
        assert "_pairs[:2]" in scatter_src
        assert "0.2 <= r <= 0.95" in scatter_src


# ── DQ Follow-up filtering + overflow summary ───────────────────────────────


def _result(table, check_name, severity, column=None, **detail):
    return SimpleNamespace(
        table=table, check_name=check_name, severity=severity, column=column,
        metric="x", value=0, detail=detail,
    )


class TestS07DQFollowupFiltering:
    def test_skip_checks_drops_those_findings(self):
        results = [
            _result("t", "null_density",         "warn", column="email"),
            _result("t", "frequency_distribution", "warn", column="cat"),
        ]
        section_cfg = SimpleNamespace(
            max_subsections=20, skip_checks=["frequency_distribution"],
            skip_columns=[],
        )
        cells = build_dq_followup_cells(
            table="t", check_results=results, section_cfg=section_cfg,
        )
        h3 = [c["source"] for c in cells
              if c["cell_type"] == "markdown" and c["source"].startswith("### ")]
        assert any("`email`" in h for h in h3)
        assert not any("`cat`" in h for h in h3)

    def test_skip_columns_drops_those_findings(self):
        results = [
            _result("t", "null_density", "warn", column="EMAIL"),
            _result("t", "null_density", "warn", column="PHONE"),
        ]
        section_cfg = SimpleNamespace(
            max_subsections=20, skip_checks=[], skip_columns=["PHONE"],
        )
        cells = build_dq_followup_cells(
            table="t", check_results=results, section_cfg=section_cfg,
        )
        h3 = [c["source"] for c in cells
              if c["cell_type"] == "markdown" and c["source"].startswith("### ")]
        assert any("`EMAIL`" in h for h in h3)
        assert not any("`PHONE`" in h for h in h3)


class TestS07OverflowSummary:
    def test_max_subsections_cap_emits_overflow_table(self):
        # Generate 25 findings; cap at 5
        results = [
            _result("t", "null_density", "critical", column=f"col_{i}")
            for i in range(25)
        ]
        section_cfg = SimpleNamespace(
            max_subsections=5, skip_checks=[], skip_columns=[],
        )
        cells = build_dq_followup_cells(
            table="t", check_results=results, section_cfg=section_cfg,
        )
        h3 = [c["source"] for c in cells
              if c["cell_type"] == "markdown" and c["source"].startswith("### ")]
        # 5 expanded sub-sections + 1 overflow summary section
        assert len(h3) == 6
        assert any(s.startswith("### Additional findings") for s in h3)
        # Overflow table mentions "20" remaining (25 total - 5 expanded)
        md_sources = [c["source"] for c in cells if c["cell_type"] == "markdown"]
        joined = " ".join(md_sources)
        assert "20" in joined
        # Table format
        assert "| Severity | Check | Column |" in joined

    def test_no_overflow_when_under_cap(self):
        results = [
            _result("t", "null_density", "warn", column=f"c{i}")
            for i in range(3)
        ]
        section_cfg = SimpleNamespace(
            max_subsections=10, skip_checks=[], skip_columns=[],
        )
        cells = build_dq_followup_cells(
            table="t", check_results=results, section_cfg=section_cfg,
        )
        md_sources = [c["source"] for c in cells if c["cell_type"] == "markdown"]
        assert not any("Additional findings" in s for s in md_sources)


# ── notebook_config.example.yaml dropped into dq_eda/ ───────────────────────


class TestExampleConfigDrop:
    def test_drops_example_config_on_first_copy(self, tmp_path):
        copy_helpers(tmp_path)
        cfg = tmp_path / NOTEBOOK_CONFIG_EXAMPLE_FILENAME
        assert cfg.is_file()
        body = cfg.read_text()
        # Mentions every section by name
        for section in ["header", "setup", "data_gather", "grain", "univariate",
                        "bivariate", "temporal", "dq_followup"]:
            assert section in body

    def test_example_config_yaml_parses(self, tmp_path):
        """The commented example must remain valid YAML — uncommenting any
        block should produce a config that load_config accepts."""
        copy_helpers(tmp_path)
        body = (tmp_path / NOTEBOOK_CONFIG_EXAMPLE_FILENAME).read_text()
        parsed = yaml.safe_load(body)
        # Top-level key is `notebook`
        assert "notebook" in parsed

    def test_does_not_overwrite_customized_config(self, tmp_path):
        copy_helpers(tmp_path)
        cfg = tmp_path / NOTEBOOK_CONFIG_EXAMPLE_FILENAME
        cfg.write_text("# custom analyst notes\n", encoding="utf-8")

        copy_helpers(tmp_path)

        assert cfg.read_text() == "# custom analyst notes\n"


# ── End-to-end through ProfileConfig ─────────────────────────────────────────


class TestProfileConfigIntegration:
    def test_default_profile_config_has_notebook_block(self):
        # Build a minimal ProfileConfig — no notebook key
        cfg = ProfileConfig.model_validate({
            "connection": {"dialect": "duckdb", "database_path": "/tmp/x.db"},
            "scope": {"schemas": ["main"]},
        })
        assert cfg.notebook is not None
        assert cfg.notebook.sections.header.enabled is True
        assert cfg.notebook.columns == {}
