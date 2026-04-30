"""
eda_profile.py — lightweight, in-notebook DataFrame profiling helpers.

Designed for the start of an EDA flow: dump a quick overview of any DataFrame
already in memory. No Snowflake metadata, no dbt YAML, no widget UI — those
live in table_profiler.ipynb. This is the in-flow companion.

All functions emit HTML / Markdown / DataFrames via IPython.display so the
output is captured by both the live notebook and nb2report HTML exports.

Public API:
    peek(df, n=5, max_colwidth=40)
        df.head(n) with ALL columns visible (no truncation).

    summarize(df, high_card_threshold=0.95)
        Top-level shape, memory, datetime range, and auto-flagged DQ issues.

    schema(df, sample_n=3)
        Per-column overview: name, dtype, non-null %, n_unique, sample values.

    describe_by_type(df)
        Type-aware describe — separate sections for numeric / categorical /
        datetime / boolean columns.

    profile(df, charts=True, n=5, **plot_kwargs)
        Wrapper that runs all of the above with markdown headers between
        sections. If charts=True, also emits plot_distribution() for each
        usable numeric column.
"""

from __future__ import annotations

import pandas as pd
from IPython.display import display, HTML, Markdown


# ─────────────────────────────────────────────────────────────────────────────
# peek
# ─────────────────────────────────────────────────────────────────────────────

def peek(df: pd.DataFrame, n: int = 5, max_colwidth: int = 40) -> None:
    """Show df.head(n) with all columns visible — no truncation."""
    with pd.option_context(
        'display.max_columns', None,
        'display.width', None,
        'display.max_colwidth', max_colwidth,
    ):
        display(df.head(n))


# ─────────────────────────────────────────────────────────────────────────────
# summarize
# ─────────────────────────────────────────────────────────────────────────────

def _format_bytes(n_bytes: int) -> str:
    val = float(n_bytes)
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if val < 1024:
            return f'{val:.1f} {unit}'
        val /= 1024
    return f'{val:.1f} PB'


def summarize(df: pd.DataFrame, high_card_threshold: float = 0.95) -> None:
    """
    Top-level summary block + auto-flagged DQ issues.

    Issues flagged:
      - Columns that are 100% null
      - Columns with a single unique value (zero variance)
      - High-cardinality columns (>= high_card_threshold unique fraction; likely PKs)
    """
    n_rows, n_cols = df.shape
    mem = _format_bytes(int(df.memory_usage(deep=True).sum()))

    total_cells = n_rows * n_cols
    null_cells = int(df.isna().sum().sum())
    null_pct = (null_cells / total_cells * 100) if total_cells else 0

    # Datetime range across all datetime cols
    dt_cols = df.select_dtypes(include=['datetime', 'datetimetz']).columns.tolist()
    dt_range_html = ''
    if dt_cols:
        mins = [df[c].min() for c in dt_cols if df[c].notna().any()]
        maxs = [df[c].max() for c in dt_cols if df[c].notna().any()]
        if mins and maxs:
            mn, mx = min(mins), max(maxs)
            days = (mx - mn).days
            dt_range_html = (
                f'<div style="font-size:12px;color:#4A5568;margin-top:4px;">'
                f'<b>Datetime range:</b> {mn.date()} → {mx.date()} '
                f'({days} days, {len(dt_cols)} datetime cols)</div>'
            )

    summary_html = f"""
<div style="border:1px solid #E2E8F0;border-radius:6px;padding:14px 18px;
            background:#FAFBFC;margin:8px 0;font-family:'Inter',system-ui,sans-serif;">
  <div style="font-size:14px;font-weight:600;color:#1E2F4D;">
    {n_rows:,} rows &times; {n_cols} columns &middot; {mem}
  </div>
  <div style="font-size:12px;color:#4A5568;margin-top:4px;">
    Total null rate: {null_pct:.1f}%
  </div>
  {dt_range_html}
</div>
"""
    display(HTML(summary_html))

    # DQ issues — emitted as a markdown WARNING blockquote so it renders
    # as a styled callout box in nb2report (and as a quoted block in Jupyter).
    issue_lines = []
    null_cols = [c for c in df.columns if df[c].isna().all()]
    if null_cols:
        issue_lines.append(
            f"- **{len(null_cols)} column(s) 100% null:** "
            + ", ".join(f"`{c}`" for c in null_cols)
        )

    single_val_cols = [
        c for c in df.columns
        if df[c].nunique(dropna=True) <= 1 and not df[c].isna().all()
    ]
    if single_val_cols:
        issue_lines.append(
            f"- **{len(single_val_cols)} column(s) with a single value:** "
            + ", ".join(f"`{c}`" for c in single_val_cols)
        )

    high_card_cols = []
    for c in df.columns:
        non_null = int(df[c].notna().sum())
        if non_null > 100:  # avoid flagging tiny tables
            uniq = int(df[c].nunique(dropna=True))
            if uniq / non_null >= high_card_threshold:
                high_card_cols.append(c)
    if high_card_cols:
        issue_lines.append(
            f"- **{len(high_card_cols)} high-cardinality column(s) "
            f"(≥ {int(high_card_threshold*100)}% unique, possible PKs):** "
            + ", ".join(f"`{c}`" for c in high_card_cols)
        )

    if issue_lines:
        body = "\n".join(issue_lines)
        display(Markdown(f"> [!WARNING]\n> **Issues flagged**\n>\n> {body.replace(chr(10), chr(10) + '> ')}"))


# ─────────────────────────────────────────────────────────────────────────────
# schema
# ─────────────────────────────────────────────────────────────────────────────

def _sample_repr(values, max_each: int = 30) -> str:
    parts = []
    for v in values:
        if isinstance(v, str):
            s = v if len(v) <= max_each else v[:max_each] + '…'
            parts.append(f"'{s}'")
        else:
            parts.append(repr(v))
    return ', '.join(parts)


def schema(df: pd.DataFrame, sample_n: int = 3) -> None:
    """Per-column overview: dtype, non-null %, cardinality, sample values."""
    n = len(df)
    rows = []
    for col in df.columns:
        s = df[col]
        non_null = int(s.notna().sum())
        non_null_pct = (non_null / n * 100) if n else 0
        # Pick distinct sample values; fall back to first non-null if dropna gives nothing
        distinct = pd.Series(s.dropna().unique()).head(sample_n).tolist()
        rows.append({
            'column': col,
            'dtype': str(s.dtype),
            'non_null_pct': f'{non_null_pct:.1f}%',
            'n_unique': int(s.nunique(dropna=True)),
            'sample_values': _sample_repr(distinct),
        })
    display(pd.DataFrame(rows))


# ─────────────────────────────────────────────────────────────────────────────
# describe_by_type
# ─────────────────────────────────────────────────────────────────────────────

def _fmt_num(x):
    if pd.isna(x):
        return ''
    if isinstance(x, (int,)) or (isinstance(x, float) and x.is_integer() and abs(x) < 1e15):
        return f'{int(x):,}'
    return f'{x:,.3f}'


def describe_by_type(df: pd.DataFrame) -> None:
    """
    Type-aware descriptive statistics. Emits one DataFrame per type group
    found in the input: numeric, categorical/object, datetime, boolean.
    """
    # Numeric
    num_cols = df.select_dtypes(include='number').columns.tolist()
    if num_cols:
        d = df[num_cols].describe().T
        d['%zero'] = [(df[c] == 0).sum() / max(int(df[c].notna().sum()), 1) * 100 for c in num_cols]
        d['%neg']  = [(df[c] < 0).sum()  / max(int(df[c].notna().sum()), 1) * 100 for c in num_cols]
        d['skew']  = [df[c].skew() for c in num_cols]
        for col in ['mean', 'std', 'min', '25%', '50%', '75%', 'max', 'skew']:
            d[col] = d[col].apply(_fmt_num)
        for col in ['%zero', '%neg']:
            d[col] = d[col].apply(lambda x: f'{x:.1f}%')
        d['count'] = d['count'].astype(int).apply(lambda x: f'{x:,}')
        display(Markdown('**Numeric columns**'))
        display(d)

    # Categorical / object / string
    cat_cols = df.select_dtypes(include=['object', 'category', 'string']).columns.tolist()
    if cat_cols:
        rows = []
        for c in cat_cols:
            s = df[c]
            non_null = int(s.notna().sum())
            blank_pct = (s.astype(str).str.strip() == '').sum() / max(len(s), 1) * 100
            top = s.value_counts(dropna=True).head(1)
            top_val = top.index[0] if len(top) else ''
            top_freq = int(top.iloc[0]) if len(top) else 0
            top_pct = (top_freq / non_null * 100) if non_null else 0
            rows.append({
                'column': c,
                'count': f'{non_null:,}',
                'unique': int(s.nunique(dropna=True)),
                'top': str(top_val)[:40],
                'top_freq': f'{top_freq:,} ({top_pct:.1f}%)',
                '%blank': f'{blank_pct:.1f}%',
            })
        display(Markdown('**Categorical / object columns**'))
        display(pd.DataFrame(rows))

    # Datetime
    dt_cols = df.select_dtypes(include=['datetime', 'datetimetz']).columns.tolist()
    if dt_cols:
        rows = []
        for c in dt_cols:
            s = df[c]
            mn, mx = s.min(), s.max()
            range_days = (mx - mn).days if pd.notna(mn) and pd.notna(mx) else 0
            rows.append({
                'column': c,
                'count': f'{int(s.notna().sum()):,}',
                'min': str(mn) if pd.notna(mn) else '',
                'max': str(mx) if pd.notna(mx) else '',
                'range_days': f'{range_days:,}',
            })
        display(Markdown('**Datetime columns**'))
        display(pd.DataFrame(rows))

    # Boolean
    bool_cols = df.select_dtypes(include='bool').columns.tolist()
    if bool_cols:
        rows = []
        for c in bool_cols:
            s = df[c]
            non_null = int(s.notna().sum())
            true_pct = (s.sum() / max(non_null, 1) * 100) if non_null else 0
            rows.append({
                'column': c,
                'count': f'{non_null:,}',
                'true_pct': f'{true_pct:.1f}%',
            })
        display(Markdown('**Boolean columns**'))
        display(pd.DataFrame(rows))


# ─────────────────────────────────────────────────────────────────────────────
# profile (composition)
# ─────────────────────────────────────────────────────────────────────────────

def profile(df: pd.DataFrame, charts: bool = True, n: int = 5,
            heading_level: int = 4, **plot_kwargs) -> None:
    """
    Run summarize → peek → schema → describe_by_type in sequence with section
    headers. If charts=True, also emit plot_distribution() for each usable
    numeric column (skips constants and all-null columns).

    Easy to remove charts: pass charts=False, or delete the bottom block.

    Parameters
    ----------
    df             : DataFrame to profile
    charts         : if True, plot a distribution for each usable numeric column
    n              : rows shown by peek()
    heading_level  : markdown heading level (1-6) used for the five subsection
                     headers. nb2report turns these into navigable TOC entries.
                     Default 4 nests cleanly under an `### H3 section` (the
                     typical "Dataframe Profile" cell pattern). Pass 3 if you
                     want them under an `## H2 chapter` instead, or 5/6 for
                     deeper nesting. nb2report renders H1-H3 as collapsible
                     sections; H4-H6 as inline anchored sub-sections.
    **plot_kwargs  : forwarded to eda_helpers.plot_distribution()
    """
    if not 1 <= heading_level <= 6:
        raise ValueError(f"heading_level must be 1-6, got {heading_level}")
    h = '#' * heading_level

    display(Markdown(f'{h} Summary'))
    summarize(df)

    display(Markdown(f'{h} Sample rows'))
    peek(df, n=n)

    display(Markdown(f'{h} Schema'))
    schema(df)

    display(Markdown(f'{h} Descriptive statistics'))
    describe_by_type(df)

    if not charts:
        return

    try:
        from eda_helpers import plot_distribution
    except ImportError:
        display(Markdown('_charts skipped — `eda_helpers.plot_distribution` not importable_'))
        return

    num_cols = df.select_dtypes(include='number').columns.tolist()
    plottable = [
        c for c in num_cols
        if df[c].notna().sum() > 0 and df[c].nunique(dropna=True) > 1
    ]
    if not plottable:
        return

    display(Markdown(f'{h} Numeric distributions'))
    for c in plottable:
        # Per-chart label stays as bold text (not a heading) so the TOC isn't
        # spammed with one entry per numeric column.
        display(Markdown(f'**{c}**'))
        plot_distribution(df, field=c, **plot_kwargs)
