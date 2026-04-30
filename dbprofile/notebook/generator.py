"""Top-level notebook builder.

PR 2 emits a runnable stub: title, schema/source markdown, an H2 'Setup'
heading, and one code cell that imports the helpers + matplotlib. That's
enough to:

  * exercise every other module in the notebook package end-to-end
  * give the analyst a notebook that opens, runs, and renders cleanly

Real section content (sections 0–7 from the feature plan) lands in
later phases.
"""

from __future__ import annotations

from typing import Iterable

import nbformat

from dbprofile.notebook.cells import code_cell, md_cell, section_header
from dbprofile.notebook.classify import classify_columns


def build_notebook(
    *,
    table: str,
    schema_name: str,
    columns: list[dict],
    check_results: Iterable,
    config,
    connector_type: str,
) -> nbformat.NotebookNode:
    """Build a runnable stub notebook for one table.

    Parameters
    ----------
    table           Table name (unqualified).
    schema_name     Schema/dataset.
    columns         list of {name, data_type, ...} from connector.get_columns()
    check_results   Iterable of CheckResult — used by the classifier;
                    Phase 2 will use them for DQ callouts and section gating.
    config          ProfileConfig — Phase 2 will read notebook.* keys.
    connector_type  'snowflake' | 'bigquery' | 'duckdb' — drives Phase 2's
                    connector-specific setup cell.
    """
    # Run the classifier so Phase 2 work has the column kinds available.
    # We don't yet branch on the result; the call surface is in place.
    classify_columns(columns, check_results)

    cells = [
        section_header(1, f"{table} — EDA / Data Quality Review"),
        md_cell(
            f"**Schema:** `{schema_name}`  \n"
            f"**Table:** `{table}`  \n"
            f"**Connector:** `{connector_type}`  \n\n"
            f"_PR 2 stub — sections will be filled in by Phase 2+._"
        ),
        section_header(2, "Setup"),
        code_cell(_stub_setup_source()),
    ]

    nb = nbformat.v4.new_notebook()
    nb.cells = cells
    nb.metadata["kernelspec"] = {
        "display_name": "Python 3",
        "language":     "python",
        "name":         "python3",
    }
    return nb


# ── Internal helpers ─────────────────────────────────────────────────────────


def _stub_setup_source() -> str:
    """Minimal setup cell — imports + matplotlib defaults.

    Phase 2 replaces this with a connector-specific setup that wires
    .env credentials, sql() helper, and FORCE_RELOAD.
    """
    return (
        "# Stub setup cell — full connector wiring arrives in Phase 2.\n"
        "import warnings\n"
        "warnings.filterwarnings('ignore')\n"
        "\n"
        "import numpy as np\n"
        "import pandas as pd\n"
        "import matplotlib.pyplot as plt\n"
        "from IPython.display import HTML, display\n"
        "\n"
        "from eda_helpers import *\n"
        "from eda_profile import profile, peek, summarize, schema, describe_by_type\n"
        "\n"
        "%matplotlib inline\n"
        "plt.style.use('seaborn-v0_8-whitegrid')\n"
    )
