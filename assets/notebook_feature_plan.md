# dbprofile — Notebook Generation Feature Plan

**Status:** Planning / Pre-implementation  
**Date:** 2026-04-29  
**Scope:** Add `dbprofile notebook` command that generates a runnable, pre-filled Jupyter notebook (.ipynb) for each profiled table, following the EDA style and helper conventions established in `eda_fct_daily_demand.ipynb`.

---

## 1. Goal

When the profiler runs against a table, it currently produces an HTML report (DQ-focused, automated) and an Excel workbook. The new feature adds a third output: a **Jupyter notebook** that provides a structured, narrative EDA starting point for each table.

The notebook is:
- **Runnable** — not a blank template. All queries, imports, and helper calls are pre-wired.
- **Opinionated** — follows the same section structure, font/size constants, and chart helper calls as the sample notebook.
- **DQ-aware** — wherever the profiler found warnings or critical results, those are surfaced as callout cells so the analyst sees them immediately.
- **nb2report-compatible** — heading hierarchy (`##` / `###`), callout syntax, and figure placement are all structured so the notebook can be run and immediately passed to `nb2report` to produce a clean stakeholder HTML report.

---

## 2. What Already Exists (Do Not Re-Implement)

| File | Purpose | Location |
|---|---|---|
| `eda_helpers.py` | All chart helpers | project root |
| `eda_profile.py` | `profile()`, `peek()`, `schema()`, `describe_by_type()` | project root |
| `eda_helpers_call_templates.py` | Reference templates for every helper | project root |
| `dbprofile/orchestrator.py` | Runs checks, returns `list[CheckResult]` | package |
| `dbprofile/cli.py` | Click CLI with `run`, `excel` commands | package |
| `dbprofile/connectors/base.py` | Connector ABC + Snowflake/BigQuery/DuckDB | package |

The notebook generator must consume `list[CheckResult]` from the existing orchestrator — no re-running checks.

---

## 3. New Module Structure

```
dbprofile/
  notebook/
    __init__.py
    generator.py       # Top-level: build_notebook(table, schema, columns, results, config) -> dict
    cells.py           # Cell factory: md_cell(text), code_cell(src), callout_cell(severity, text)
    classify.py        # Column classification: date / binary / low_card_cat / string_id /
                       #   continuous_numeric / count_metric / high_card_cat
    sql_builders.py    # Builds query strings for each grain/pattern
    sections/
      __init__.py
      s00_header.py       # Title, purpose, DQ summary callouts
      s01_setup.py        # Imports, rcParams, connector, sql(), FORCE_RELOAD
      s02_data_gather.py  # Queries, FORCE_RELOAD guard, profile() calls
      s03_grain.py        # Schema summary, cardinality, boundary conditions
      s04_univariate.py   # Numeric distributions, categorical freq, binary flags
      s05_bivariate.py    # Correlation, scatter pairs, group breakdowns
      s06_temporal.py     # Time series (emitted only if date columns detected)
      s07_dq_followup.py  # Null/uniqueness/format deep-dives (only for flagged columns)
```

---

## 4. CLI Changes

### New `notebook` command

```bash
# Generate notebook from scratch (runs profile first, then writes notebook)
dbprofile notebook --config examples/config_snowflake.yaml

# Generate notebook from an existing JSON export (no DB connection needed)
dbprofile notebook --json reports/snowflake_analytics_20260422.json \
                   --config examples/config_snowflake.yaml

# One notebook per table (default) or one combined notebook
dbprofile notebook --config ... --mode per-table   # default
dbprofile notebook --config ... --mode combined
```

### Alternative: add `--export-notebook` flag to existing `run` command

```bash
dbprofile run --config examples/config_snowflake.yaml \
  --export-json auto --export-excel auto --export-notebook auto
```

**Recommendation:** Implement as a separate `notebook` command first (cleaner separation of concerns), with a `--json` path for re-generation without re-querying. Mirror the existing `excel` command pattern which can reconstruct from JSON.

### Output naming

Same auto-naming convention as existing outputs:
```
reports/<source>_<schema>_<table>_<YYYYMMDD_HHMM>.ipynb
```

---

## 5. Column Classification (`classify.py`)

The generator must automatically decide which helper to call for each column. Classification is based on dtype + column name heuristics + cardinality from check results.

```python
class ColumnKind(str, Enum):
    DATE        = "date"         # datetime / date type
    BINARY      = "binary"       # int/bool with exactly 2 unique values (0/1, True/False)
    ORDINAL_CAT = "ordinal_cat"  # low-cardinality numeric that's actually categorical (e.g. day_of_week, month)
    LOW_CAT     = "low_cat"      # string with ≤ 15 distinct values
    HIGH_CAT    = "high_cat"     # string with > 15 distinct values (zone names, etc.)
    STRING_ID   = "string_id"    # string with near-unique values (ID columns)
    COUNT_METRIC= "count_metric" # non-negative int with name matching *_count, *_trips, *_cnt
    CONTINUOUS  = "continuous"   # float or high-cardinality numeric
    UNKNOWN     = "unknown"
```

### Classification priority order

1. If dtype is datetime/date → `DATE`
2. If dtype is bool OR (int with n_unique == 2 and values in {0, 1}) → `BINARY`
3. If name matches `*_id`, `demand_id`, `uuid`, etc. AND string type → `STRING_ID` (skip distribution)
4. If string dtype AND n_unique ≤ `low_cardinality_threshold` (default 15, configurable) → `LOW_CAT`
5. If string dtype AND n_unique > `low_cardinality_threshold` → `HIGH_CAT`
6. If numeric AND name matches `*_count`, `*_cnt`, `*_trips`, `*_revenue`, `*_total` → `COUNT_METRIC`
7. If numeric AND n_unique ≤ 12 AND plausible ordinal name (month, year, dow, day_of_week, hour) → `ORDINAL_CAT`
8. Otherwise if numeric → `CONTINUOUS`
9. → `UNKNOWN`

### Cardinality source

Pull `n_unique` from `FrequencyDistributionCheck` results or `SchemaAuditCheck` results if available. Fall back to a fast APPROX_COUNT_DISTINCT query if not.

---

## 6. Notebook Section Design

### Section 0 — Title & DQ Summary (`s00_header.py`)

**Markdown cell:**
```markdown
# {TABLE_NAME} — EDA / Data Quality Review

**Schema:** `{DATABASE}.{SCHEMA}`
**Generated:** {DATE}
**Profiler run:** {JSON_PATH or "live run"}

**Purpose:** Exploratory analysis of `{TABLE_NAME}`.
Investigate columns, distributions, and potential data quality issues.

> [!WARNING]
> **{N} DQ issues flagged by the profiler.** See the DQ Follow-up section for details.
```

Critical results get `> [!CAUTION]`, warn results get `> [!WARNING]`, clean runs get `> [!NOTE]` with a green "no issues found" message.

One callout per flagged check is listed, with the column name and the finding summary pulled from `CheckResult.detail`.

---

### Section 1 — Setup (`s01_setup.py`)

**Single code cell.** Content is almost identical to the sample notebook setup cell. Parameterized by connector type (Snowflake / BigQuery / DuckDB).

Must include:
- `import warnings; warnings.filterwarnings('ignore')`
- numpy, pandas, matplotlib imports
- `from eda_helpers import *`
- `from eda_profile import profile, peek, summarize, schema, describe_by_type`
- `%matplotlib inline`
- `plt.style.use('seaborn-v0_8-whitegrid')`
- `plt.rcParams.update({...})` with the canonical values from the sample notebook
- DataFrame CSS block (the `display(HTML(...))` with table borders)
- Connector setup (connector-appropriate — Snowflake key-pair, BigQuery, or DuckDB)
- `sql()` helper function (same pattern from sample)
- `FORCE_RELOAD = False`

For Snowflake, use the same `_load_creds()` / `_connect()` pattern from the sample notebook, reading from `.dbt/profiles.yml`. Pull the profile name and target schema from the dbprofile config.

---

### Section 2 — Data Gathering (`s02_data_gather.py`)

**Markdown heading:** `## Data Gathering`

Intro markdown: explain FORCE_RELOAD pattern (same as sample notebook).

**Sampling strategy:**

BERNOULLI sampling is the preferred method — it samples each row independently with equal probability, giving the most representative distribution of column values. It is better than TABLESAMPLE SYSTEM (which samples disk blocks and can over-represent contiguous data ranges) and better than `ORDER BY RANDOM() LIMIT N` (which scans the full table first).

The sample rate is computed automatically at notebook-generation time using the row count already available from `RowCountCheck` results (or a fast `SELECT COUNT(*)` if not). The goal is to target a fixed **target row count** (default 50,000) that is large enough for reliable distribution plots but small enough to load quickly.

```python
def _bernoulli_pct(row_count: int, target_rows: int = 50_000) -> float:
    """
    Returns the BERNOULLI percentage (0.0–100.0) that approximates target_rows.
    Clamped to [0.1, 100.0] — never sample less than 0.1% or more than 100%.
    """
    if row_count <= 0:
        return 100.0
    pct = (target_rows / row_count) * 100.0
    return round(max(0.1, min(100.0, pct)), 2)
```

| Row count | Sample % | Expected rows pulled |
|---|---|---|
| < 50K | 100% (no SAMPLE clause) | All rows |
| 100K | 50% | ~50K |
| 500K | 10% | ~50K |
| 1M | 5% | ~50K |
| 10M | 0.5% | ~50K |
| 100M | 0.1% (floor) | ~100K |

When `pct >= 100`, the query omits the `SAMPLE BERNOULLI` clause entirely (no noise from a 100% sample).

The `target_rows` and `bernoulli_floor_pct` values are configurable in the `notebook:` YAML section (see Section 12).

The generated notebook prints the actual row count and sample rate applied so the analyst always knows what they're working with:
```python
print(f'Table size:  {row_count:,} rows')
print(f'Sample rate: {pct:.1f}%  (BERNOULLI — target {target_rows:,} rows)')
print(f'Queried:     {len(sample_df):,} rows  |  {len(sample_df.columns)} columns')
```

**Per-grain query strategy:**

For each table, generate two DataFrames:

1. **`sample_df`** — a BERNOULLI sample of the table for column profiling and numeric distributions. Apply filters to exclude known-bad rows if the profiler flagged them.

2. **Date-aggregated df** — only if a date column is detected. `SELECT {date_col}, COUNT(*) AS row_cnt FROM {table} GROUP BY 1 ORDER BY 1`. Name: `{table_prefix}_daily_df`.

3. **Group-aggregated dfs** — only if both a date column AND a categorical column with ≤8 distinct values are detected. Similar to the `pickup_df` in the sample: `SELECT {date_col}, {group_col}, COUNT(*) AS row_cnt FROM {table} GROUP BY 1, 2 ORDER BY 1, 2`. 

Each query is wrapped in the FORCE_RELOAD guard:
```python
if FORCE_RELOAD or 'sample_df' not in dir():
    sample_df = sql("""...""")
    print(f'Queried: {len(sample_df):,} rows | {len(sample_df.columns)} columns')
else:
    print(f'Cached: {len(sample_df):,} rows')
```

After each query block: a `profile(df, charts=False)` call to show the data summary.

Also include the DataFrame inventory cell (the `df_inventory` block from the sample).

---

### Section 3 — Schema & Grain Exploration (`s03_grain.py`)

**Markdown heading:** `## Schema & Grain Exploration`

Three sub-sections:

#### 3a — Boundary Conditions

Code cell building the summary table (from the sample):
```python
cols = [all non-date, non-id columns]
summary = sample_df[cols].agg(["nunique", "min", "max"]).T
summary.columns = ["distinct_count", "min", "max"]
display(summary.style.format({"distinct_count": "{:,}"}))
```

#### 3b — Grain Verification

If candidate grain columns are detected (columns where per-row distinctcount = 1 at a hypothesized grain), emit a code cell checking that the grain is clean.

Markdown cell: `**Grain:** {grain_cols} — confirm each combination is unique.`

#### 3c — Cardinality Summary

Emit `schema(sample_df)` and `describe_by_type(sample_df)` calls.

---

### Section 4 — Univariate Analysis (`s04_univariate.py`)

**Markdown heading:** `## Univariate Analysis`

#### 4a — Binary / Flag Fields

If any `BINARY` or `ORDINAL_CAT` columns exist:

**Markdown:** `### Flag & Ordinal Fields`

One `plot_histograms()` call grouping all binary/ordinal columns:
```python
plot_histograms(
    df     = sample_df,
    fields = ['col_a', 'col_b', ...],
    label_threshold = 12,
)
```

#### 4b — Categorical Frequencies

If any `LOW_CAT` or `HIGH_CAT` (but not `STRING_ID`) columns exist:

**Markdown:** `### Categorical Columns`

For LOW_CAT (≤15 values): `plot_string_profile(df=sample_df, fields=[...])`  
For HIGH_CAT (>15 values): `plot_string_profile_hc(df=sample_df, fields=[...])`

#### 4c — Count / Aggregate Metrics

If any `COUNT_METRIC` columns exist:

**Markdown:** `### Count & Aggregate Metrics`

One `plot_field_aggregates()` call for all count metric columns:
```python
plot_field_aggregates(
    df     = sample_df,
    fields = ['col_a', 'col_b', ...],
)
```

If a low-cardinality categorical column exists, add a follow-up `plot_field_aggregates_by_group()`:
```python
plot_field_aggregates_by_group(
    df          = sample_df,
    group_field = '{best_cat_col}',
    fields      = ['col_a', 'col_b', ...],
    max_cols    = 2,
)
```

#### 4d — Continuous Numeric Distributions

For each `CONTINUOUS` column (up to a configurable max, default 12):

**Markdown heading:** `### {column_label} | Distribution`

One `plot_distribution()` call per column with sensible defaults:
```python
plot_distribution(
    df         = sample_df,
    field      = '{col}',
    bin_cnt    = 20,
    bin_min    = 0,       # only if column is known non-negative
    cumulative_line = True,
)
```

If the `NumericDistributionCheck` result exists for this column, pull `p1`, `p99` from it and use them as `bin_min`/`bin_max` to auto-zoom past outliers.

If a group column exists, add a grouped boxplot call:
```python
plot_boxplot(
    df          = sample_df,
    value_field = '{col}',
    group_field = '{best_cat_col}',
)
```

---

### Section 5 — Bivariate & Multivariate Analysis (`s05_bivariate.py`)

**Markdown heading:** `## Bivariate Analysis`

Only emitted if there are ≥2 `CONTINUOUS` columns.

#### 5a — Correlation Heatmap

```python
import seaborn as sns

num_cols = sample_df.select_dtypes('number').columns.tolist()
corr = sample_df[num_cols].corr()

fig, ax = plt.subplots(figsize=(min(len(num_cols), 16), min(len(num_cols), 14)))
sns.heatmap(corr, annot=True, fmt='.2f', center=0,
            cmap='RdBu_r', ax=ax, square=True,
            annot_kws={'size': 9})
ax.set_title('Correlation Matrix — Numeric Columns', fontsize=FONT_TITLE)
plt.tight_layout()
plt.show()
```

#### 5b — Key Scatter Pairs

Intelligently select the top 3–4 numeric pairs by absolute correlation (excluding pairs > 0.98 which are likely derived). For each:

```python
plot_scatter(
    df      = sample_df,
    x_field = '{col_a}',
    y_field = '{col_b}',
    trend   = 'linear',
)
```

If a low-cardinality categorical exists, add a group-faceted version:
```python
plot_scatter(
    df          = sample_df,
    x_field     = '{col_a}',
    y_field     = '{col_b}',
    group_field = '{best_cat_col}',
    color_field = '{best_cat_col}',
)
```

---

### Section 6 — Temporal Analysis (`s06_temporal.py`)

**Markdown heading:** `## Temporal Analysis`

Only emitted when at least one `DATE` column was detected and the date-aggregated df was built.

#### 6a — Overall Time Series

```python
plot_daily_trips(
    df       = {daily_df},
    date_col = '{date_col}',
)
```

If a group column was detected and group-aggregated df was built:
```python
plot_borough_detail(
    df       = {group_df},
    date_col = '{date_col}',
)
```

#### 6b — Temporal Consistency Callouts

If `TemporalConsistencyCheck` results exist and flagged gaps or volume anomalies, add a markdown callout:
```
> [!WARNING]
> **Date gaps detected in `{col}`.** The profiler found {n} gaps of 1+ day.
> Investigate the cells below for specific dates.
```

Followed by a code cell listing the gap dates.

---

### Section 7 — DQ Follow-up (`s07_dq_followup.py`)

**Markdown heading:** `## Data Quality Follow-up`

Only emitted when the profiler found warn or critical results.

For each flagged check result, emit a sub-section:

**`## Null Density — {column}`** (if NullDensityCheck flagged a column)
- Code cell: `sample_df['{col}'].isnull().sum()` and null count by group
- Markdown callout with the severity and detail from the CheckResult

**`## Uniqueness — {column}`** (if UniquenessCheck flagged duplicates)
- Code cell: duplicate detection query / groupby
- Detail from CheckResult

**`## Format Validation — {column}`** (if FormatValidationCheck flagged bad formats)
- Code cell: `.str.match(pattern)` check on sample_df
- Show bad-format examples

**`## Row Count — {table}`** (if RowCountCheck flagged empty/near-empty table)
- Markdown callout only, no additional code

---

## 7. Cell Factory API (`cells.py`)

All section builders work by returning a list of nbformat cell dicts. The generator assembles them into a notebook.

```python
import nbformat

def md_cell(text: str) -> dict:
    """Markdown cell."""
    return nbformat.v4.new_markdown_cell(text)

def code_cell(source: str) -> dict:
    """Code cell (no outputs)."""
    return nbformat.v4.new_code_cell(source)

def callout_cell(severity: str, message: str) -> dict:
    """Markdown cell formatted as an nb2report callout."""
    tag_map = {"critical": "[!CAUTION]", "warn": "[!WARNING]", "info": "[!NOTE]"}
    tag = tag_map.get(severity, "[!NOTE]")
    return md_cell(f"> {tag}\n> {message}")

def section_header(level: int, title: str) -> dict:
    """Markdown heading cell at the given level (1=# 2=## 3=###)."""
    return md_cell(f"{'#' * level} {title}")
```

---

## 8. Top-Level Generator (`generator.py`)

```python
def build_notebook(
    table: str,
    schema_name: str,
    columns: list[dict],          # from connector.get_columns()
    check_results: list[CheckResult],
    config: ProfileConfig,
    connector_type: str,          # 'snowflake' | 'bigquery' | 'duckdb'
) -> dict:
    """
    Returns an nbformat v4 notebook dict ready to write with nbformat.write().
    """
    col_map = classify_columns(columns, check_results)

    cells = []
    cells += build_header(table, schema_name, check_results)
    cells += build_setup(config, connector_type)
    cells += build_data_gather(table, schema_name, col_map, config)
    cells += build_grain(col_map)
    cells += build_univariate(col_map, check_results)
    cells += build_bivariate(col_map)
    cells += build_temporal(col_map, check_results)
    cells += build_dq_followup(check_results)

    nb = nbformat.v4.new_notebook()
    nb.cells = cells
    nb.metadata["kernelspec"] = {
        "display_name": "Python 3",
        "language": "python",
        "name": "python3",
    }
    return nb
```

---

## 9. Integration with `cli.py`

### `--project-dir` on all commands

`--project-dir` is added as a shared option on `run`, `excel`, and `notebook`. When provided, all outputs land in `{project_dir}/dq_eda/` (created automatically if it doesn't exist). When omitted, outputs fall back to `./reports/` with a deprecation warning.

```bash
# Full run — profiler + all exports go to the project folder
dbprofile run --config examples/config_snowflake.yaml \
  --project-dir ~/projects/nyc_taxi_analysis \
  --export-json auto --export-excel auto --export-notebook auto

# Re-generate Excel from a previous JSON (no DB connection needed)
dbprofile excel --json ~/projects/nyc_taxi_analysis/dq_eda/fct_trips_20260429.json \
  --config examples/config_snowflake.yaml \
  --project-dir ~/projects/nyc_taxi_analysis

# Re-generate or add notebooks from a previous JSON
dbprofile notebook --json ~/projects/nyc_taxi_analysis/dq_eda/fct_trips_20260429.json \
  --config examples/config_snowflake.yaml \
  --project-dir ~/projects/nyc_taxi_analysis
```

### Updated command signatures

**`run` command** (existing — add `--project-dir` and `--export-notebook`):
```python
@cli.command("run")
@click.option("--config", "-c", required=True, type=click.Path(exists=True))
@click.option("--project-dir", "-p", default=None, type=click.Path(),
              help="Project folder. Outputs go to <project-dir>/dq_eda/. "
                   "Defaults to ./reports/ if omitted.")
@click.option("--export-json", default=None)
@click.option("--export-excel", default=None)
@click.option("--export-notebook", default=None,   # NEW
              help="'auto' = generate notebook for each table profiled.")
@click.option("--dry-run", is_flag=True)
def run_cmd(config, project_dir, export_json, export_excel, export_notebook, dry_run):
    ...
```

**`excel` command** (existing — add `--project-dir`):
```python
@cli.command("excel")
@click.option("--json", "json_path", required=True, type=click.Path(exists=True))
@click.option("--config", "-c", required=True, type=click.Path(exists=True))
@click.option("--project-dir", "-p", default=None, type=click.Path())
@click.option("--output", "-o", default=None)
def excel_cmd(json_path, config, project_dir, output):
    ...
```

**`notebook` command** (new):
```python
@cli.command("notebook")
@click.option("--config", "-c", required=True, type=click.Path(exists=True))
@click.option("--project-dir", "-p", default=None, type=click.Path())
@click.option("--json", "json_path", default=None, type=click.Path(exists=True),
              help="Re-generate from an existing JSON export (no DB connection needed).")
@click.option("--tables", default=None, multiple=True,
              help="Limit to specific tables (default: all tables in config scope).")
@click.option("--update-helpers", is_flag=True,
              help="Overwrite helper files in dq_eda/ with the current package version.")
def notebook_cmd(config, project_dir, json_path, tables, update_helpers):
    """Generate a Jupyter EDA notebook for each profiled table."""
    ...
```

### Output path resolution (`output_dir.py`)

Centralise output path logic in a small helper so all three commands use the same rules:

```python
def resolve_output_dir(project_dir: str | None) -> Path:
    """
    Returns the resolved output directory.
    If project_dir is given: <project_dir>/dq_eda/   (created if absent)
    Otherwise:               ./reports/               (backward-compat fallback)
    """
    if project_dir:
        out = Path(project_dir) / "dq_eda"
    else:
        console.print(
            "[yellow]No --project-dir specified. Writing to ./reports/ "
            "(pass --project-dir for project-based output).[/yellow]"
        )
        out = Path("reports")
    out.mkdir(parents=True, exist_ok=True)
    return out

def auto_name(table: str, ext: str, prefix: str = "") -> str:
    """e.g. auto_name('fct_trips', 'html') → 'fct_trips_20260429.html'"""
    date_str = datetime.now().strftime("%Y%m%d")
    return f"{prefix}{table}_{date_str}.{ext}"
```

---

## 10. Output Location — `--project-dir` and `dq_eda/`

### Design decision

dbprofile is a **utility tool**. Its outputs — HTML reports, Excel workbooks, JSON exports, and generated notebooks — are **analyst artifacts** that belong with the project the analyst is working on, not inside the dbprofile package directory. All outputs therefore go to a `dq_eda/` subfolder inside the target project, specified via `--project-dir`.

This makes analytic work **repeatable and portable**: the `dq_eda/` folder can be checked into version control alongside the rest of the project, shared with teammates, or re-generated in place when the profiler is re-run against updated data.

### Output structure

```
your_project/
  dq_eda/
    ├── eda_helpers.py                         ← copied from dbprofile package on first run
    ├── eda_profile.py                         ← copied from dbprofile package on first run
    ├── eda_helpers_call_templates.py          ← reference doc for the analyst
    │
    ├── fct_trips_20260429.html                ← DQ HTML report
    ├── fct_trips_20260429.xlsx                ← DQ Excel workbook
    ├── fct_trips_20260429.json                ← DQ JSON export (used to re-gen other outputs)
    ├── eda_fct_trips_20260429.ipynb           ← generated EDA notebook
    │
    ├── dim_zones_20260429.html
    ├── dim_zones_20260429.xlsx
    ├── dim_zones_20260429.json
    └── eda_dim_zones_20260429.ipynb
```

**Why flat?** Notebooks must be able to do `from eda_helpers import *` without any `sys.path` manipulation. Keeping helpers and notebooks at the same level inside `dq_eda/` means the import works as long as the analyst opens Jupyter from the `dq_eda/` directory (or adds it to the kernel path via the standard Jupyter mechanisms). Nesting helpers in a subdirectory would require modifying every generated notebook's setup cell — fragile and harder to explain.

**Naming conventions:**

| Output type | Pattern |
|---|---|
| HTML report | `{table}_{YYYYMMDD}.html` |
| Excel workbook | `{table}_{YYYYMMDD}.xlsx` |
| JSON export | `{table}_{YYYYMMDD}.json` |
| EDA notebook | `eda_{table}_{YYYYMMDD}.ipynb` |
| Helper files | `eda_helpers.py`, `eda_profile.py`, `eda_helpers_call_templates.py` (no date — one copy, versioned in place) |

### Helper copy behavior

On first run to a new `--project-dir`, dbprofile copies `eda_helpers.py`, `eda_profile.py`, and `eda_helpers_call_templates.py` from the package into `dq_eda/`. A version comment is injected at the top of each file:

```python
# eda_helpers.py — copied by dbprofile v1.4 on 2026-04-29
# Source: dbprofile.notebook.eda_helpers
# Re-run `dbprofile init --project-dir <path>` to update to the latest version.
```

On subsequent runs to the same `--project-dir`, the copy step checks the version comment. If the installed dbprofile version is newer, it prints a warning and offers to update:

```
[yellow]eda_helpers.py in dq_eda/ was copied from dbprofile v1.2.
Current version is v1.4. Run with --update-helpers to refresh.[/yellow]
```

It does **not** auto-overwrite — the analyst may have made project-specific customizations to their local copy.

### When `--project-dir` is omitted

For backward compatibility and dev/testing convenience, if `--project-dir` is not provided, outputs fall back to `./reports/` inside the dbprofile directory (current behavior). A deprecation notice is printed encouraging use of `--project-dir` for any real project work.

---

## 11. Notebook Write Safety — Hash-Based Change Detection

### Design decision

dbprofile **never silently overwrites a notebook the analyst may have modified.** On each generation it checks whether the existing file has been changed since dbprofile last wrote it. If it has, the analyst's copy is left untouched and the fresh baseline is written to a new date-stamped name. If it hasn't been touched, dbprofile overwrites silently — no backup clutter for files the analyst never modified.

### How the hash is stored

When dbprofile writes a notebook it computes a SHA-256 hash of the **cell sources only** (not outputs) and stores it in the notebook's own metadata:

```python
nb.metadata["dbprofile"] = {
    "generated_by":   "dbprofile",
    "version":        __version__,          # dbprofile package version
    "generated_at":   datetime.now().isoformat(),
    "table":          table,
    "source_hash":    _source_hash(nb),     # hash of cell sources at write time
}
```

```python
def _source_hash(nb: dict) -> str:
    """SHA-256 of all cell source strings, concatenated in order."""
    content = "\n".join(
        cell["source"]
        for cell in nb.get("cells", [])
        if cell.get("cell_type") in ("code", "markdown")
    )
    return hashlib.sha256(content.encode()).hexdigest()
```

**Why hash sources only, not outputs?** Running the notebook (executing cells) changes outputs — images, printed values, tracebacks — but not sources. An analyst who only ran the notebook without editing any cells should not trigger the "modified" path. Hashing sources means "has the analyst edited any cell text," which is the right question.

### Write behavior on re-generation

```
Existing file?
  └── No  → write to canonical name (eda_{table}_{YYYYMMDD}.ipynb), store hash. Done.
  └── Yes → read existing file
              └── No dbprofile metadata? → treat as analyst-modified (safe default)
              └── Has metadata?
                    └── source_hash matches current file? → analyst hasn't touched it
                    │     → overwrite in place, update metadata. Done.
                    └── source_hash differs → analyst modified the file
                          → write fresh baseline to new date-stamped name
                          → print notice (see below). Done.
```

### Console output when analyst has modified the file

```
[yellow]eda_fct_trips_20260429.ipynb has been modified since it was generated.
Leaving your file untouched.
Fresh baseline written to: dq_eda/eda_fct_trips_20260513.ipynb[/yellow]
```

The analyst ends up with both files — their customized version and the new baseline — and can diff them or manually cherry-pick new sections.

### Same-day re-run collision

If dbprofile generates a baseline, it is immediately overwritten on a same-day re-run (the hash matches because the analyst hasn't had time to touch it). If both the existing file and the new target name are the same date, and the hash indicates it was unmodified, overwrite in place. No time-suffix needed — the hash makes the decision, not the filename.

If for some reason the same-day file was already modified (analyst ran and edited quickly), the new baseline gets a `_HHMM` time suffix: `eda_fct_trips_20260429_1430.ipynb`.

### Force flag

Add `--force` to the `notebook` command for cases where the analyst explicitly wants to discard their version and start fresh:

```bash
dbprofile notebook --config ... --project-dir ~/projects/nyc_taxi \
  --force --tables fct_trips
```

With `--force`, dbprofile overwrites the existing file unconditionally but first saves a timestamped backup to a `dq_eda/.backups/` subfolder:

```
dq_eda/
  .backups/
    eda_fct_trips_20260429_backup_20260513_1430.ipynb
  eda_fct_trips_20260429.ipynb    ← fresh baseline (analyst's edits gone from main file)
```

The `.backups/` folder is gitignore-able and clearly signals "recovery only." The analyst's work is never permanently destroyed.

### Updated `notebook` command signature

```python
@cli.command("notebook")
@click.option("--config", "-c", required=True, type=click.Path(exists=True))
@click.option("--project-dir", "-p", default=None, type=click.Path())
@click.option("--json", "json_path", default=None, type=click.Path(exists=True),
              help="Re-generate from an existing JSON export (no DB connection needed).")
@click.option("--tables", default=None, multiple=True,
              help="Limit to specific tables (default: all tables in config scope).")
@click.option("--update-helpers", is_flag=True,
              help="Overwrite helper files in dq_eda/ with the current package version.")
@click.option("--force", is_flag=True,
              help="Overwrite existing notebooks even if analyst-modified. "
                   "Saves a timestamped backup to dq_eda/.backups/ first.")
def notebook_cmd(config, project_dir, json_path, tables, update_helpers, force):
    """Generate a Jupyter EDA notebook for each profiled table."""
    ...
```

### Implementation — `notebook_writer.py`

Centralise the write logic in a dedicated module so the hash check and backup behavior are tested independently of the generator:

```python
# dbprofile/notebook/notebook_writer.py

def write_notebook(
    nb: dict,
    out_dir: Path,
    table: str,
    force: bool = False,
) -> Path:
    """
    Write nb to out_dir, respecting hash-based change detection.

    Returns the path actually written.
    Raises nothing — handles all cases internally and prints status via rich.
    """
    canonical = out_dir / auto_name(table, "ipynb", prefix="eda_")

    if canonical.exists():
        existing = nbformat.read(canonical, as_version=4)
        if force:
            _backup(canonical, out_dir)
            _write(nb, canonical)
            console.print(f"[yellow]Overwrote {canonical.name} (backup saved to .backups/)[/yellow]")
        elif _analyst_modified(existing):
            dated = out_dir / _dated_name(table)
            _write(nb, dated)
            console.print(
                f"[yellow]{canonical.name} has been modified since it was generated.\n"
                f"Leaving your file untouched.\n"
                f"Fresh baseline written to: {dated.name}[/yellow]"
            )
            return dated
        else:
            _write(nb, canonical)   # silent overwrite — analyst hasn't touched it
    else:
        _write(nb, canonical)

    return canonical


def _analyst_modified(existing_nb: dict) -> bool:
    """True if the notebook's stored hash doesn't match its current sources."""
    meta = existing_nb.get("metadata", {}).get("dbprofile", {})
    stored_hash = meta.get("source_hash")
    if not stored_hash:
        return True   # no metadata = not generated by us = treat as modified
    return stored_hash != _source_hash(existing_nb)


def _backup(path: Path, out_dir: Path) -> None:
    backup_dir = out_dir / ".backups"
    backup_dir.mkdir(exist_ok=True)
    stem = path.stem
    ts   = datetime.now().strftime("%Y%m%d_%H%M")
    shutil.copy2(path, backup_dir / f"{stem}_backup_{ts}.ipynb")
```

---

## 12. nb2report Compatibility Requirements

The generated notebook must satisfy all nb2report rendering rules:

| Rule | Implementation |
|---|---|
| All narrative in markdown cells | Every section starts with a markdown cell explaining what the following code shows |
| `##` opens a new TOC section | Section headings use exactly `##` level |
| `###` is a sub-section | Sub-section headings use `###` |
| Figures from code cells → nearest preceding section | All `plot_*()` calls are in code cells that immediately follow the relevant markdown section header |
| `<!-- caption: ... -->` in a markdown cell captions the next figure | Add caption directives to markdown cells before `plot_distribution()` calls |
| `<!-- no-chart -->` suppresses the next figure | Not needed in generated notebooks |
| `> [!WARNING]` / `> [!CAUTION]` → callout box | Use exactly this syntax for DQ flags |
| Code cell source is always hidden | Not a concern — nb2report always hides code |
| DataFrame HTML tables are rendered | `display(df)` and `display(df.style...)` both work |
| stdout/print() is excluded by default | Use `display()` not `print()` for DataFrames shown in the report; use `print()` only for diagnostic output the analyst needs but stakeholders don't |

---

## 12. Configuration (`config.py` additions)

Add an optional `notebook` section to the YAML config:

```yaml
notebook:
  enabled: true
  max_continuous_fields: 12      # cap on plot_distribution() calls per table
  include_bivariate: true
  include_temporal: true
  include_dq_followup: true
  top_cat_column: null           # override auto-selected group field (e.g. "pickup_borough")
  grain_columns: []              # hint the expected grain columns

  # ── Sampling ────────────────────────────────────────────────────────────────
  sample_target_rows: 50000      # target row count for sample_df (BERNOULLI adjusted to hit this)
  sample_floor_pct: 0.1          # minimum BERNOULLI % — never go below this even for huge tables
  # Tables with row_count <= sample_target_rows are queried in full (no SAMPLE clause)

  # ── Column classification ────────────────────────────────────────────────────
  low_cardinality_threshold: 15  # string columns with n_unique <= this → LOW_CAT (plot_string_profile)
                                 # string columns with n_unique >  this → HIGH_CAT (plot_string_profile_hc)

# ── Project output directory ─────────────────────────────────────────────────
# Can also be passed as --project-dir on the CLI (CLI takes precedence).
# When set, ALL outputs (HTML, Excel, JSON, notebooks, helpers) go to
# <project_dir>/dq_eda/ instead of ./reports/.
project_dir: null               # e.g. ~/projects/nyc_taxi_analysis
```

All keys optional with sensible defaults.

---

## 13. Dependencies

Add to `pyproject.toml`:
- `nbformat>=5.0` — already used by nb2report but not dbprofile; add here
- `seaborn>=0.12` — for the correlation heatmap (Section 5a)

No new runtime deps for the analyst (they already have matplotlib, pandas, etc. to run the notebook).

---

## 14. Implementation Order (Suggested Phases)

### Phase 1 — Output Directory & Scaffolding
1. Add `dbprofile/notebook/output_dir.py` — `resolve_output_dir()` and `auto_name()` helpers
2. Add `--project-dir` flag to existing `run` and `excel` commands; wire all output writes through `resolve_output_dir()`
3. Create `dbprofile/notebook/` module with `__init__.py`
4. Implement `cells.py` (cell factories)
5. Implement `classify.py` (column classification)
6. Add `notebook` CLI command skeleton — resolves output dir, copies helpers, writes an empty notebook as a smoke test

### Phase 2 — Core Notebook Sections
7. `s01_setup.py` — full setup cell for Snowflake connector
8. `s02_data_gather.py` — BERNOULLI sampling logic, sample_df query + profile() calls
9. `s03_grain.py` — schema summary + boundary conditions
10. `s04_univariate.py` — all four sub-sections

### Phase 3 — Analytical Sections
11. `s05_bivariate.py` — correlation heatmap + scatter pairs
12. `s06_temporal.py` — time series (gated on date column detection)
13. `s07_dq_followup.py` — per-check deep-dives for flagged results

### Phase 4 — Header & Polish
14. `s00_header.py` — title cell + DQ callout summary
15. `notebook_writer.py` — `write_notebook()` with hash detection, backup, and `--force`
16. Wire `--export-notebook auto` into the existing `run` command
17. Config additions (`notebook:` and `project_dir:` in YAML, Pydantic models)
18. Helper version-check warning on subsequent runs

### Phase 5 — Testing & BigQuery/DuckDB
19. Unit tests for `classify.py` (most testable unit)
20. Unit tests for `cells.py` and `output_dir.py`
21. Unit tests for `notebook_writer.py` — cover all four branches: new file, unmodified overwrite, analyst-modified (new dated file), force-overwrite with backup
22. Integration test: run full `run` command on DuckDB dev fixture with `--project-dir /tmp/test_project`, assert `dq_eda/` structure and helper files are present
23. BigQuery connector variant for setup cell
24. DuckDB connector variant for setup cell

---

## 15. Out of Scope (Future)

- Auto-execution of the generated notebook (could add `--execute` flag using nbconvert later)
- Direct HTML output without going through nb2report (redundant with existing HTML report)
- LLM-generated narrative/summary cells (separate feature)
- Target variable analysis (Section 5 in EDA best practices — requires user to specify which column is the target)
- Sankey / from-to matrix (niche; add only when a from/to column pair is detected by name heuristic)

---

## 16. Key Design Principles (Non-Negotiable)

1. **Always use helpers** — never raw matplotlib in generated cells. Every chart must go through `eda_helpers.*`. This is what gives the notebook its consistent look and makes it compatible with nb2report.

2. **Pair charts with markdown** — every code cell that produces a chart must be preceded by a markdown cell (at least a `###` heading plus one line of context). The markdown is what nb2report renders; charts with no preceding narrative are invisible to stakeholders.

3. **Group columns into multi-panel helpers** — don't emit one `plot_distribution()` per column; use `plot_histograms([col_a, col_b, col_c])` for binary/ordinal groups, and `plot_field_aggregates([col_a, col_b, col_c])` for count columns. Individual `plot_distribution()` calls are reserved for the most analytically interesting continuous columns.

4. **DQ results drive the narrative** — the header section and the DQ follow-up section exist specifically to bridge the automated profiler output with the exploratory notebook. The analyst should not have to re-run the profiler to understand what to look at.

5. **FORCE_RELOAD always present** — this is the most important QoL pattern. Every query must be guarded. Analysts will Run All repeatedly while iterating on chart parameters; they must not wait for Snowflake on every run.

6. **Connector-agnostic section bodies** — sections 3–7 work on pandas DataFrames and are completely connector-agnostic. Only sections 0–2 (header, setup, data gather) are connector-specific. This keeps the analytical sections clean and testable.
