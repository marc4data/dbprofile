"""Data Gathering section — pulls a sample DataFrame and (when applicable)
a daily-aggregated DataFrame for time-series work.

Two cells the analyst will run, each guarded by FORCE_RELOAD so a
parameter-tweak Run-All doesn't re-query the warehouse:

  * sample_df  — BERNOULLI sample of the table for column profiling.
                 Sample percentage is chosen at notebook-generation time
                 so the analyst pulls roughly TARGET_ROWS rows regardless
                 of how big the source is.

  * daily_df   — emitted only when the classifier finds at least one
                 DATE-kinded column. Aggregates rows-per-day for the
                 first such column. Phase-5 temporal section will
                 chart this.

Each query is followed by a profile(df, charts=False) call so the
analyst sees the schema/null-rate summary inline.

The BERNOULLI percentage formula and floor are hardcoded to the
feature plan defaults (target_rows=50_000, floor_pct=0.1). A later PR
will wire these to the YAML config under `notebook:`.
"""

from __future__ import annotations

import nbformat

from dbprofile.notebook.cells import code_cell, md_cell, section_header
from dbprofile.notebook.classify import ColumnKind

# Feature-plan defaults — Phase 6 will wire these to cfg.notebook.*
TARGET_ROWS = 50_000
FLOOR_PCT = 0.1


def build_data_gather_cells(
    *,
    cfg,
    table: str,
    schema_name: str,
    columns: list[dict],
    classified: dict[str, ColumnKind],
    check_results,
    connector_type: str,
    section_cfg=None,
) -> list[nbformat.NotebookNode]:
    """Return the cells for the Data Gathering section.

    section_cfg is a DataGatherSectionConfig or None. When None we use the
    module defaults (TARGET_ROWS / FLOOR_PCT) so existing tests + callers
    that haven't been updated keep working.
    """
    target_rows = getattr(section_cfg, "sample_target_rows", TARGET_ROWS)
    floor_pct = getattr(section_cfg, "sample_floor_pct", FLOOR_PCT)

    row_count = _row_count_from_results(check_results, table)
    sample_pct = _bernoulli_pct(row_count, target_rows=target_rows, floor_pct=floor_pct)
    table_ref = _table_ref(connector_type=connector_type, table=table,
                           schema_name=schema_name, cfg=cfg)
    sample_clause = _sample_clause(connector_type, sample_pct)

    cells: list[nbformat.NotebookNode] = [
        section_header(2, "Data Gathering"),
        md_cell(_intro_markdown(row_count=row_count, sample_pct=sample_pct,
                                connector_type=connector_type)),
        code_cell(_sample_df_source(table_ref=table_ref,
                                    sample_clause=sample_clause,
                                    sample_pct=sample_pct,
                                    row_count=row_count)),
        code_cell("profile(sample_df, charts=False)"),
    ]

    # Daily-aggregated DataFrame, gated on a DATE-kinded column.
    date_col = _first_date_column(columns, classified)
    if date_col:
        cells.extend([
            section_header(3, "Daily volume"),
            md_cell(
                f"Per-day row count from `{date_col}`. "
                "Used by the temporal section in a later phase; available "
                "now for ad-hoc time-series exploration."
            ),
            code_cell(_daily_df_source(
                table_ref=table_ref,
                date_col=date_col,
                connector_type=connector_type,
            )),
            code_cell("profile(daily_df, charts=False)"),
        ])

    return cells


# ── Sampling helpers ─────────────────────────────────────────────────────────


def _bernoulli_pct(
    row_count: int | None,
    target_rows: int = TARGET_ROWS,
    floor_pct: float = FLOOR_PCT,
) -> float:
    """Return the sample percentage that approximates `target_rows`.

    Clamped to [floor_pct, 100]. When row_count is unknown (no RowCountCheck
    result), fall back to 100% — the analyst will hit the full table,
    which is safer than guessing too low.
    """
    if not row_count or row_count <= 0:
        return 100.0
    pct = (target_rows / row_count) * 100.0
    return round(max(floor_pct, min(100.0, pct)), 2)


def _sample_clause(connector_type: str, pct: float) -> str:
    """Dialect-aware SAMPLE clause. Empty string when pct >= 100."""
    if pct >= 100.0:
        return ""
    if connector_type == "snowflake":
        return f"SAMPLE BERNOULLI ({pct:.2f})"
    if connector_type == "bigquery":
        # BigQuery does not support BERNOULLI; fall back to SYSTEM.
        return f"TABLESAMPLE SYSTEM ({pct:.2f} PERCENT)"
    if connector_type == "duckdb":
        return f"USING SAMPLE {pct:.2f} PERCENT (BERNOULLI)"
    return ""


def _table_ref(*, connector_type: str, table: str, schema_name: str, cfg) -> str:
    """Fully-qualified table reference suitable for the dialect's SQL."""
    if connector_type == "snowflake":
        db = (cfg.scope.database or "").upper()
        s = (schema_name or "").upper()
        return f"{db}.{s}.{table.upper()}"
    if connector_type == "bigquery":
        proj = cfg.connection.project or ""
        ds = schema_name or cfg.scope.dataset or ""
        return f"`{proj}.{ds}.{table}`"
    if connector_type == "duckdb":
        return f"{schema_name or 'main'}.{table}"
    return table


def _row_count_from_results(check_results, table: str) -> int | None:
    """Pull the row_count metric for `table` from RowCountCheck results."""
    for r in check_results:
        if (
            getattr(r, "table", None) == table
            and getattr(r, "check_name", "") == "row_count"
            and getattr(r, "metric", "") == "row_count"
        ):
            try:
                return int(r.value)
            except (TypeError, ValueError):
                return None
    return None


def _first_date_column(columns: list[dict], classified: dict[str, ColumnKind]) -> str | None:
    """First column the classifier flagged as DATE, preserving column order."""
    for col in columns:
        name = col.get("name")
        if name and classified.get(name) == ColumnKind.DATE:
            return name
    return None


# ── Cell sources ─────────────────────────────────────────────────────────────


def _intro_markdown(*, row_count: int | None, sample_pct: float, connector_type: str) -> str:
    rc_str = f"{row_count:,}" if row_count else "(unknown — no RowCountCheck result)"
    note = ""
    if connector_type == "bigquery" and sample_pct < 100.0:
        note = (
            "\n\n_Note: BigQuery does not support BERNOULLI; SYSTEM "
            "(block-level) sampling is used instead. Distributions may "
            "skew if data clusters by storage block._"
        )
    return (
        "Pull a `sample_df` for column-level work, plus a per-day "
        "aggregate when a date column is available. Each query is "
        "guarded by `FORCE_RELOAD` — flip it to `True` in the Setup "
        "cell to re-query the warehouse on the next Run All."
        f"\n\n**Source rows:** {rc_str}"
        f"  \n**Sample rate:** {sample_pct:.2f}%"
        f"{note}"
    )


def _sample_df_source(
    *,
    table_ref: str,
    sample_clause: str,
    sample_pct: float,
    row_count: int | None,    # noqa: ARG001 — kept for future "expected ~N rows" message
) -> str:
    """sample_df query cell with FORCE_RELOAD guard."""
    sql_body = f"        SELECT *\n        FROM {table_ref}"
    if sample_clause:
        sql_body += f"\n        {sample_clause}"
    return (
        f'TABLE_REF = "{table_ref}"\n'
        "\n"
        "if FORCE_RELOAD or 'sample_df' not in dir():\n"
        f'    sample_df = sql("""\n'
        f"{sql_body}\n"
        '    """)\n'
        f"    print(f'Queried: {{len(sample_df):,}} rows | "
        f"{{len(sample_df.columns)}} columns "
        f"(sample rate {sample_pct:.2f}%)')\n"
        "else:\n"
        "    print(f'Cached:  {len(sample_df):,} rows | "
        "{len(sample_df.columns)} columns')"
    )


def _date_trunc_day(connector_type: str, col: str) -> str:
    """Dialect-aware DATE_TRUNC('day', col) — mirrors what dbprofile uses."""
    if connector_type == "bigquery":
        return f"DATE_TRUNC({col}, DAY)"
    # Snowflake + DuckDB share the standard 'day-first' signature.
    return f"DATE_TRUNC('day', {col})"


def _daily_df_source(*, table_ref: str, date_col: str, connector_type: str) -> str:
    """daily_df query — date column truncated to day so timestamps aggregate.

    Snowflake (and BigQuery, with backtick aliases) returns column names
    UPPERCASE unless the alias is quoted. The downstream temporal section
    indexes daily_df['day'] / ['row_cnt'], so we lowercase the columns at
    load to keep the chart cell dialect-agnostic.
    """
    day_expr = _date_trunc_day(connector_type, date_col)
    return (
        "if FORCE_RELOAD or 'daily_df' not in dir():\n"
        '    daily_df = sql("""\n'
        f"        SELECT {day_expr} AS day, COUNT(*) AS row_cnt\n"
        f"        FROM {table_ref}\n"
        f"        GROUP BY 1\n"
        f"        ORDER BY 1\n"
        '    """)\n'
        "    daily_df.columns = daily_df.columns.str.lower()\n"
        "    print(f'Queried: {len(daily_df):,} days')\n"
        "else:\n"
        "    print(f'Cached:  {len(daily_df):,} days')"
    )
