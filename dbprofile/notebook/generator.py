"""Top-level notebook builder.

Concatenates per-section cell builders into one notebook. Each section
module under dbprofile.notebook.sections owns one part of the
narrative; this file is just the conductor.
"""

from __future__ import annotations

from typing import Iterable

import nbformat

from dbprofile.notebook.classify import classify_columns
from dbprofile.notebook.sections.s00_header import build_header_cells
from dbprofile.notebook.sections.s01_setup import build_setup_cells
from dbprofile.notebook.sections.s02_data_gather import build_data_gather_cells
from dbprofile.notebook.sections.s03_grain import build_grain_cells
from dbprofile.notebook.sections.s04_univariate import build_univariate_cells
from dbprofile.notebook.sections.s05_bivariate import build_bivariate_cells
from dbprofile.notebook.sections.s06_temporal import build_temporal_cells
from dbprofile.notebook.sections.s07_dq_followup import build_dq_followup_cells


def build_notebook(
    *,
    table: str,
    schema_name: str,
    columns: list[dict],
    check_results: Iterable,
    config,
    connector_type: str,
) -> nbformat.NotebookNode:
    """Build a notebook for one table.

    Parameters
    ----------
    table           Table name (unqualified).
    schema_name     Schema/dataset.
    columns         list of {name, data_type, ...} from connector.get_columns()
    check_results   Iterable of CheckResult — used by the classifier;
                    later sections will use them for DQ callouts.
    config          ProfileConfig — section builders read what they need.
    connector_type  'snowflake' | 'bigquery' | 'duckdb' — drives the
                    connector-specific setup cell.
    """
    nb_cfg = getattr(config, "notebook", None)
    sections = nb_cfg.sections if nb_cfg else None

    # Per-column kind overrides from cfg.notebook.columns.
    overrides = {
        col_name: ov.kind
        for col_name, ov in (nb_cfg.columns.items() if nb_cfg else {})
        if ov.kind is not None
    }
    classified = classify_columns(columns, check_results, overrides=overrides)

    # Build each section, gated by its `enabled` flag. Default config has all
    # sections enabled so omitting `notebook:` from the YAML keeps current
    # behavior.
    cells: list = []

    if sections is None or sections.header.enabled:
        cells.extend(build_header_cells(
            table=table, schema_name=schema_name, connector_type=connector_type,
            check_results=check_results,
        ))
    if sections is None or sections.setup.enabled:
        cells.extend(build_setup_cells(
            cfg=config, schema_name=schema_name, connector_type=connector_type,
        ))
    if sections is None or sections.data_gather.enabled:
        cells.extend(build_data_gather_cells(
            cfg=config, table=table, schema_name=schema_name, columns=columns,
            classified=classified, check_results=check_results,
            connector_type=connector_type,
            section_cfg=(sections.data_gather if sections else None),
        ))
    if sections is None or sections.grain.enabled:
        cells.extend(build_grain_cells(
            columns=columns, classified=classified,
            section_cfg=(sections.grain if sections else None),
        ))
    if sections is None or sections.univariate.enabled:
        cells.extend(build_univariate_cells(
            columns=columns, classified=classified, check_results=check_results,
            section_cfg=(sections.univariate if sections else None),
        ))
    if sections is None or sections.bivariate.enabled:
        cells.extend(build_bivariate_cells(
            columns=columns, classified=classified,
            section_cfg=(sections.bivariate if sections else None),
        ))
    if sections is None or sections.temporal.enabled:
        cells.extend(build_temporal_cells(columns=columns, classified=classified))
    if sections is None or sections.dq_followup.enabled:
        cells.extend(build_dq_followup_cells(
            table=table, check_results=check_results,
            section_cfg=(sections.dq_followup if sections else None),
        ))

    nb = nbformat.v4.new_notebook()
    nb.cells = cells
    nb.metadata["kernelspec"] = {
        "display_name": "Python 3",
        "language":     "python",
        "name":         "python3",
    }
    return nb
