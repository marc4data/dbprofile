"""Bivariate Analysis section — correlation heatmap + top scatter pairs.

Two sub-sections, both gated on having ≥2 CONTINUOUS columns:

  5a — Correlation heatmap
       seaborn.heatmap on sample_df.select_dtypes('number').corr().
       This is the one section that uses raw seaborn rather than an
       eda_helpers chart — there's no correlation-matrix helper in the
       package today, and a heatmap is a single matplotlib call. If
       the analyst doesn't have seaborn installed, the import will
       fail with a clear message.

  5b — Top scatter pairs
       Picks pairs at notebook-runtime (not generation-time) by computing
       abs correlation in sample_df, filtering pairs > 0.98 (likely
       derived columns) and pairs < 0.10 (uninteresting), sorting
       descending, and emitting a plot_scatter() call for each of the
       top N pairs in a loop.
"""

from __future__ import annotations

import nbformat

from dbprofile.notebook.cells import code_cell, md_cell, section_header
from dbprofile.notebook.classify import ColumnKind

# The minimum number of CONTINUOUS columns needed for either sub-section
# to make sense — a single number can't have a correlation pair.
MIN_NUMERIC_COLS = 2

# Number of top-correlated pairs to emit scatter plots for.
TOP_PAIRS = 4

# Correlation thresholds for pair selection at runtime.
PAIR_CORR_FLOOR = 0.10   # below: probably uninteresting
PAIR_CORR_CEIL  = 0.98   # above: probably derived (e.g. col_a, col_a*2)


def build_bivariate_cells(
    *,
    columns: list[dict],
    classified: dict[str, ColumnKind],
) -> list[nbformat.NotebookNode]:
    """Return the cells for the Bivariate Analysis section."""
    continuous_cols = [
        col["name"] for col in columns
        if classified.get(col["name"]) == ColumnKind.CONTINUOUS
    ]
    if len(continuous_cols) < MIN_NUMERIC_COLS:
        # Not enough numeric columns — entire section is skipped silently.
        # (No "this section was skipped" placeholder; would be noise.)
        return []

    cells: list[nbformat.NotebookNode] = [
        section_header(2, "Bivariate Analysis"),
        md_cell(
            "Pairwise relationships between continuous columns. The "
            "heatmap surfaces strong correlations at a glance; the "
            "scatter plots below pick the top pairs at runtime so they "
            "stay relevant if you re-sample `sample_df`."
        ),
    ]
    cells.extend(_correlation_heatmap_cells())
    cells.extend(_scatter_pairs_cells())
    return cells


# ── 5a — Correlation heatmap ─────────────────────────────────────────────────


def _correlation_heatmap_cells() -> list[nbformat.NotebookNode]:
    return [
        section_header(3, "Correlation matrix"),
        md_cell(
            "Pearson correlation across all numeric columns in `sample_df`. "
            "Look for off-diagonal cells with |r| > 0.7 — those are columns "
            "that move together and may be candidates for feature reduction "
            "or hidden duplicates."
        ),
        code_cell(_heatmap_source()),
    ]


def _heatmap_source() -> str:
    return (
        "import seaborn as sns\n"
        "\n"
        "_num_cols = sample_df.select_dtypes('number').columns.tolist()\n"
        "_corr = sample_df[_num_cols].corr()\n"
        "\n"
        "_size = max(min(len(_num_cols), 16), 4)\n"
        "fig, ax = plt.subplots(figsize=(_size, _size * 0.85))\n"
        "sns.heatmap(\n"
        "    _corr, annot=True, fmt='.2f', center=0,\n"
        "    cmap='RdBu_r', ax=ax, square=True, annot_kws={'size': 9},\n"
        ")\n"
        "ax.set_title('Correlation matrix — numeric columns', fontsize=14)\n"
        "plt.tight_layout()\n"
        "plt.show()"
    )


# ── 5b — Top scatter pairs ───────────────────────────────────────────────────


def _scatter_pairs_cells() -> list[nbformat.NotebookNode]:
    return [
        section_header(3, "Top scatter pairs"),
        md_cell(
            f"Computes |corr| at runtime, filters out pairs above "
            f"{PAIR_CORR_CEIL:.2f} (likely derived columns) and below "
            f"{PAIR_CORR_FLOOR:.2f} (uninteresting), and emits up to "
            f"{TOP_PAIRS} `plot_scatter()` calls in a loop."
        ),
        code_cell(_scatter_pairs_source()),
    ]


def _scatter_pairs_source() -> str:
    """Pick top N pairs at runtime, then loop plot_scatter over them."""
    return (
        "import numpy as np\n"
        "\n"
        "_corr_matrix = sample_df.select_dtypes('number').corr().abs()\n"
        "np.fill_diagonal(_corr_matrix.values, 0.0)\n"
        "\n"
        "_pairs = []\n"
        "_cols = _corr_matrix.columns.tolist()\n"
        "for i, a in enumerate(_cols):\n"
        "    for b in _cols[i + 1:]:\n"
        "        r = _corr_matrix.loc[a, b]\n"
        f"        if {PAIR_CORR_FLOOR} <= r <= {PAIR_CORR_CEIL}:\n"
        "            _pairs.append((a, b, r))\n"
        "_pairs.sort(key=lambda p: -p[2])\n"
        f"_pairs = _pairs[:{TOP_PAIRS}]\n"
        "\n"
        "if _pairs:\n"
        "    print('Top scatter pairs:',\n"
        "          [(a, b, round(r, 2)) for a, b, r in _pairs])\n"
        "    for _x, _y, _ in _pairs:\n"
        "        plot_scatter(\n"
        "            df      = sample_df,\n"
        "            x_field = _x,\n"
        "            y_field = _y,\n"
        "            trend   = 'linear',\n"
        "        )\n"
        "else:\n"
        f"    print('No correlation pairs in [{PAIR_CORR_FLOOR}, "
        f"{PAIR_CORR_CEIL}] range — skipping scatter plots.')"
    )
