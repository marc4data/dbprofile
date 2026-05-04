"""Schema & Grain Exploration section — quick orientation on the table.

Three small cell groups, all operating on `sample_df` (no warehouse
re-queries):

  3a — Boundary Conditions
       Per-column nunique / min / max for non-id, non-date columns.
       Surfaces value ranges so the analyst can spot zero-distinct,
       all-same, or wildly extreme columns before drilling in.

  3c — Cardinality Summary
       schema(sample_df) and describe_by_type(sample_df) from
       eda_profile — typed views of the dataset.

(3b "Grain Verification" from the feature plan is intentionally
deferred — it depends on grain hints we'll surface from cfg.notebook
in a later PR.)
"""

from __future__ import annotations

import nbformat

from dbprofile.notebook.cells import code_cell, md_cell, section_header
from dbprofile.notebook.classify import ColumnKind

# Column kinds we exclude from the boundary-conditions table.
# Date ranges and ID columns are not informative as min/max/nunique.
_BOUNDARY_EXCLUDE = {ColumnKind.DATE, ColumnKind.STRING_ID}


def build_grain_cells(
    *,
    columns: list[dict],
    classified: dict[str, ColumnKind],
    section_cfg=None,
) -> list[nbformat.NotebookNode]:
    """Return the cells for the Schema & Grain section.

    section_cfg is a GrainSectionConfig or None. Honors include_boundary
    and include_cardinality toggles when present.
    """
    include_boundary = getattr(section_cfg, "include_boundary", True)
    include_cardinality = getattr(section_cfg, "include_cardinality", True)

    boundary_cols = [
        col["name"] for col in columns
        if classified.get(col["name"]) not in _BOUNDARY_EXCLUDE
    ]

    cells: list[nbformat.NotebookNode] = [
        section_header(2, "Schema & Grain Exploration"),
    ]

    # 3a Boundary Conditions
    if include_boundary and boundary_cols:
        cells.extend([
            section_header(3, "Boundary conditions"),
            md_cell(
                "Per-column distinct count + min + max for non-date, "
                "non-id columns. Helps spot zero-cardinality, all-same, "
                "or wildly extreme values before drilling into a column."
            ),
            code_cell(_boundary_source(boundary_cols)),
        ])

    # 3c Cardinality Summary
    if include_cardinality:
        cells.extend([
            section_header(3, "Cardinality summary"),
            md_cell(
                "Typed views of `sample_df` — `schema()` shows per-column "
                "non-null %, distinct counts, and sample values. "
                "`describe_by_type()` groups columns into numeric / "
                "categorical / temporal blocks with type-appropriate stats."
            ),
            code_cell("schema(sample_df)"),
            code_cell("describe_by_type(sample_df)"),
        ])

    return cells


# ── Cell sources ─────────────────────────────────────────────────────────────


def _boundary_source(boundary_cols: list[str]) -> str:
    """Build the boundary-conditions code cell source.

    Generates a list literal of column names so the analyst can edit it
    directly in the notebook without referring back to the generator.

    Uses a defensive per-column aggregation rather than a single
    sample_df[cols].agg(...) call. The single-call form crashes with
    'str <= float' when any object column has NULLs mixed with strings
    (a common Snowflake → pandas pattern). Per-column .min()/.max()
    respects skipna=True and falls back to '(mixed types)' for the
    rare truly-mixed columns.
    """
    cols_repr = ",\n    ".join(f'"{c}"' for c in boundary_cols)
    return (
        f"_boundary_cols = [\n    {cols_repr},\n]\n"
        "\n"
        "def _safe_boundary(s):\n"
        "    out = {'distinct_count': int(s.nunique(dropna=True))}\n"
        "    try:\n"
        "        out['min'] = s.min()\n"
        "        out['max'] = s.max()\n"
        "    except TypeError:\n"
        "        out['min'] = '(mixed types)'\n"
        "        out['max'] = '(mixed types)'\n"
        "    return out\n"
        "\n"
        "_summary = pd.DataFrame(\n"
        "    {c: _safe_boundary(sample_df[c]) for c in _boundary_cols}\n"
        ").T\n"
        "display(_summary.style.format({'distinct_count': '{:,}'}))"
    )
