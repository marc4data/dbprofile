# dbprofile

Automated SQL database profiling with interactive HTML reports, quality scoring, Excel workbooks, and **runnable Jupyter EDA notebooks**.

---

## Setup

```bash
cd /Users/marcalexander/projects/ai_orchestrator_claude/dbprofile
source .venv/bin/activate
pip install -e ".[snowflake]"
```

Credentials live in `dbprofile/.env` (gitignored). Full paths required — no `~` expansion:

```
SNOWFLAKE_ACCOUNT=...
SNOWFLAKE_USER=...
SNOWFLAKE_PRIVATE_KEY_PATH=/Users/marcalexander/.ssh/snowflake/rsa_key.p8
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_ROLE=TRANSFORMER
```

---

## Project-based workflow (recommended)

dbprofile is a **utility tool**: its outputs (HTML, Excel, JSON, notebooks) are **analyst artifacts** that belong with the project the analyst is working on, not inside the dbprofile package directory. Pass `--project-dir <path>` and every output lands in `<path>/dq_eda/`, alongside the helper files the generated notebooks need.

```bash
dbprofile run \
  --config examples/config_snowflake.yaml \
  --project-dir ~/projects/portfolio_nyc_tlc \
  --export-json auto \
  --export-excel auto
```

After that one command, your project folder looks like this:

```
~/projects/portfolio_nyc_tlc/
└── dq_eda/
    ├── .dbprofile_state.json                          ← helper version tracking
    ├── .gitignore                                     ← ignores .backups/
    │
    ├── eda_helpers.py                                 ← chart helpers (copied from package)
    ├── eda_profile.py                                 ← profile / peek / schema / describe_by_type
    ├── eda_helpers_call_templates.py                  ← reference doc for helper signatures
    │
    ├── snowflake_analytics_dbt_malex_marts_20260430.html    ← one per run (DQ report)
    ├── snowflake_analytics_dbt_malex_marts_20260430.json    ← one per run (results snapshot)
    └── snowflake_analytics_dbt_malex_marts_20260430.xlsx    ← one per run (DQ workbook)
```

Add the `notebook` command and you also get one runnable Jupyter notebook per table:

```bash
dbprofile notebook \
  --config examples/config_snowflake.yaml \
  --project-dir ~/projects/portfolio_nyc_tlc
```

```
~/projects/portfolio_nyc_tlc/dq_eda/
    ├── eda_fct_daily_demand_20260430.ipynb            ← one notebook per table
    ├── eda_fct_trips_20260430.ipynb
    ├── eda_dim_zones_20260430.ipynb
    └── ...
```

The `dq_eda/` folder is meant to be checked into git alongside the rest of your project — runs are reproducible and shareable.

**Without `--project-dir`** the legacy fallback `./reports/` still works (with a one-line deprecation hint). Useful for quick local checks that don't belong to a specific project.

---

## What's in a generated notebook?

Each `eda_<table>_<date>.ipynb` is a **runnable EDA starting point** wired to the same connector dbprofile uses, with the DQ profiler results pre-baked:

| Section | Contents |
|---|---|
| **Header + DQ summary** | Title, table metadata, callout per `(check_name, severity)` bucket — critical first, then warn |
| **Setup** | Imports, theme, DataFrame CSS, connector wiring (`.env` + key-pair for Snowflake / ADC for BigQuery / read-only for DuckDB), `sql()` helper, `FORCE_RELOAD` guard |
| **Data Gathering** | `sample_df` (BERNOULLI sample targeting ~50K rows) + `daily_df` (per-day row count, gated on a date column) |
| **Schema & Grain** | Boundary conditions (nunique/min/max), `schema()` and `describe_by_type()` summaries |
| **Univariate Analysis** | `plot_histograms` (binary/ordinal), `plot_string_profile[_hc]` (categorical), `plot_field_aggregates` (counts), `plot_distribution` (continuous, capped at 12) |
| **Bivariate Analysis** | Correlation heatmap + top scatter pairs picked at runtime |
| **Temporal Analysis** | Daily volume line chart |
| **DQ Follow-up** | One sub-section per flagged column with a callout + an investigation cell (e.g. `sample_df[sample_df['email'].isna()].head(20)`) |

A typical notebook has 50–100 cells in canonical order so the analyst can run top-to-bottom and immediately see the table's quirks.

---

## Use Cases and Commands

### 1. Full daily profile (recommended)

```bash
dbprofile run \
  --config examples/config_snowflake.yaml \
  --project-dir ~/projects/portfolio_nyc_tlc \
  --export-json auto \
  --export-excel auto
```

HTML report + JSON snapshot + Excel workbook + helper files seeded into `dq_eda/`. The JSON snapshot is your safety net — it lets you regenerate Excel, HTML, or notebooks without re-querying Snowflake.

### 2. Generate / refresh notebooks from a fresh run

```bash
dbprofile notebook \
  --config examples/config_snowflake.yaml \
  --project-dir ~/projects/portfolio_nyc_tlc
```

Profiles + writes one notebook per table in scope. On first run, helper files are seeded too. On re-run, **edited notebooks are protected** — dbprofile detects analyst edits via a SHA-256 hash of cell sources and writes a fresh baseline to a date-stamped name instead of overwriting.

### 3. Generate notebooks from a saved JSON (no DB connection)

```bash
dbprofile notebook \
  --config examples/config_snowflake.yaml \
  --project-dir ~/projects/portfolio_nyc_tlc \
  --json ~/projects/portfolio_nyc_tlc/dq_eda/snowflake_analytics_dbt_malex_marts_20260430.json
```

Reuses an existing run's JSON to rebuild the notebooks in seconds. Use after pulling a teammate's `dq_eda/` folder, or when iterating on the notebook templates locally.

### 4. Notebook generation, scoped to specific tables

```bash
dbprofile notebook \
  --config examples/config_snowflake.yaml \
  --project-dir ~/projects/portfolio_nyc_tlc \
  --tables fct_trips --tables dim_zones
```

Pass `--tables` once per table. Useful for iterating on a single notebook without re-generating siblings.

### 5. Force-overwrite an analyst-modified notebook

```bash
dbprofile notebook \
  --config examples/config_snowflake.yaml \
  --project-dir ~/projects/portfolio_nyc_tlc \
  --force --tables fct_trips
```

`--force` overwrites the notebook anyway and saves a timestamped backup to `dq_eda/.backups/eda_fct_trips_20260430_backup_20260430_1430.ipynb` first. Use when you've made a mess of a notebook and want to start fresh from the generator's baseline.

### 6. Refresh helper files when dbprofile updates

```bash
dbprofile notebook \
  --config examples/config_snowflake.yaml \
  --project-dir ~/projects/portfolio_nyc_tlc \
  --update-helpers
```

Replaces the helper files in `dq_eda/` even if you've customized them locally (originals saved to `.backups/`). Use after upgrading dbprofile to a newer version.

### 7. Regenerate Excel from a previous run (no DB)

```bash
dbprofile excel \
  --json ~/projects/portfolio_nyc_tlc/dq_eda/snowflake_analytics_dbt_malex_marts_20260430.json \
  --config examples/config_snowflake.yaml \
  --project-dir ~/projects/portfolio_nyc_tlc
```

Reads the saved JSON, rebuilds scoring and EDA classification locally in ~1 second.

### 8. Regenerate HTML from a previous run (no DB)

```bash
dbprofile html \
  --json ~/projects/portfolio_nyc_tlc/dq_eda/snowflake_analytics_dbt_malex_marts_20260430.json \
  --config examples/config_snowflake.yaml \
  --project-dir ~/projects/portfolio_nyc_tlc
```

Same idea as `excel` but for the HTML report. Useful after template changes.

### 9. Override sampling for faster runs on large tables

```bash
dbprofile run \
  --config examples/config_snowflake.yaml \
  --project-dir ~/projects/portfolio_nyc_tlc \
  --sample-rate 0.05 --sample-method system \
  --export-json auto
```

`system` sampling is faster than `bernoulli` (samples whole micro-partitions instead of rows). 5% sample + system method cuts runtime significantly on large tables.

### 10. Dry run — preview queries without executing

```bash
dbprofile run --config examples/config_snowflake.yaml --dry-run
```

Shows every SQL query that would run. Useful for estimating BigQuery cost or verifying scope before a full run.

### 11. Verbose logging — debug connection or query issues

```bash
dbprofile run --config examples/config_snowflake.yaml --verbose
```

Turns on `DEBUG` logging. Shows each query as it executes, timing, and connector details.

### 12. BigQuery profiling

```bash
dbprofile run \
  --config examples/config.yaml \
  --project-dir ~/projects/portfolio_nyc_tlc \
  --export-json auto --export-excel auto
```

Targets `bigquery-public-data.new_york_taxi_trips`. Requires `BQ_PROJECT` and `GOOGLE_APPLICATION_CREDENTIALS` in `.env`.

### 13. Local DuckDB development

```bash
python scripts/seed_dev.py                                        # seed sample data
dbprofile run --config examples/config_dev.yaml --project-dir /tmp/dev_project
dbprofile notebook --config examples/config_dev.yaml --project-dir /tmp/dev_project
```

No cloud credentials needed. Good for testing template or check changes.

---

## Output File Naming Convention

| Output type | Pattern | Example |
|---|---|---|
| HTML report | `{connector}_{db}_{schema}_{date}.html` | `snowflake_analytics_dbt_malex_marts_20260430.html` |
| JSON snapshot | `{connector}_{db}_{schema}_{date}.json` | `snowflake_analytics_dbt_malex_marts_20260430.json` |
| Excel workbook | `{connector}_{db}_{schema}_{date}.xlsx` | `snowflake_analytics_dbt_malex_marts_20260430.xlsx` |
| EDA notebook | `eda_{table}_{date}.ipynb` | `eda_fct_trips_20260430.ipynb` |
| Helper files | fixed names (no date) | `eda_helpers.py`, `eda_profile.py`, `eda_helpers_call_templates.py` |

| Segment | Source | Example |
|---|---|---|
| Connector | `connection.dialect` | `snowflake` |
| Database | `scope.database` or `scope.dataset` | `analytics` |
| Schema | `scope.schemas` joined by `_` | `dbt_malex_marts` |
| Date | Run date UTC, `YYYYMMDD` | `20260430` |

This convention:
- **Groups by source** when sorted alphabetically (all Snowflake together, all BigQuery together)
- **Overwrites within the same day** for HTML / JSON / Excel (idempotent, no clutter)
- **Protects edited notebooks** — same-day re-runs of `dbprofile notebook` write a date-stamped baseline if the analyst has edited the canonical file
- **Creates history across days** — one set of artifacts per date, ready for diffing

---

## Notebook write safety (hash-based change detection)

The `notebook` command **never silently overwrites a notebook the analyst may have modified**. On each run it computes a SHA-256 hash of cell sources (not outputs — running a notebook does NOT count as modification) and stores it in `nb.metadata.dbprofile.source_hash` at write time.

| Scenario | Outcome |
|---|---|
| Canonical file doesn't exist | Write it. Done. |
| Canonical exists, hash matches stored | Silent overwrite — analyst hasn't touched it; safe to refresh |
| Canonical exists, hash differs | Analyst-modified; leave file alone, write fresh baseline to `eda_<table>_<date>_<HHMM>.ipynb` |
| `--force` after analyst edit | Backup original to `dq_eda/.backups/`, then overwrite |

Helper files (`eda_helpers.py`, `eda_profile.py`, `eda_helpers_call_templates.py`) follow the same protection rules — analyst edits survive re-runs, `--update-helpers` is the explicit "I want the package version" escape hatch.

---

## Config Files

| Config | Connector | Target | Notes |
|---|---|---|---|
| `examples/config_snowflake.yaml` | Snowflake | `Analytics` (all schemas) | **Preferred for Snowflake** — key-pair auth |
| `examples/config.yaml` | BigQuery | NYC Taxi public dataset | Requires GCP credentials |
| `examples/config_dev.yaml` | DuckDB | Local `dev.duckdb` | Run `seed_dev.py` first |

---

## The 9 Checks

| # | Check | Scope | What it finds |
|---|-------|-------|---------------|
| 1 | Schema Audit | Table | Column inventory, all-null columns, missing contract columns |
| 2 | Row Count | Table | Empty table detection, daily volume, partition skew |
| 3 | Sample Rows | Table | Top-N row preview for the report |
| 4 | Null Density | Column | Null %, empty strings, sentinel values (N/A, NONE, 9999-12-31) |
| 5 | Uniqueness | Column | Duplicate %, identifier vs. attribute classification |
| 6 | Numeric Distribution | Numeric cols | p25/p50/p75/p95/p99, IQR outliers, histogram + box-whisker |
| 7 | Frequency Distribution | Low-cardinality cols | Top-30 values, dominant-value flag, binary pill display |
| 8 | Temporal Consistency | Date/timestamp cols | Gap days, volume anomalies, daily time series chart |
| 9 | Format Validation | String cols | Email, phone, UUID, ISO country/currency pattern matching |

---

## Quality Scoring

Each table receives a 0–100 quality score based on weighted check results:

| Check | Weight | Score per result |
|---|---|---|
| Null Density | 25% | ok/info = 100, warn = 50, critical = 0 |
| Uniqueness | 20% | |
| Schema Audit | 15% | |
| Row Count | 15% | |
| Numeric Distribution | 10% | |
| Frequency Distribution | 5% | |
| Temporal Consistency | 5% | |
| Format Validation | 5% | |

Overall score is the weighted average across tables (weighted by row count).

---

## EDA Classification

Every column is classified for exploratory data analysis ordering:

| Code | Label | Detection Rule |
|---|---|---|
| A1 | Low-cardinality string | String with ≤ 200 distinct values |
| A2 | High-cardinality string | String with > 200 distinct values |
| B1 | Dates | DATE type |
| B2 | Datetimes | DATETIME type |
| B3 | Timestamps | TIMESTAMP type |
| C1 | Indicators | BOOL, or INT with ≤ 5 distinct values |
| C2 | Integers | INT with > 5 distinct values |
| C3 | Decimals | DECIMAL / NUMERIC type |
| C4 | Scientific Notation | FLOAT / DOUBLE type |

The 5-character EDA sort key (e.g., `A1003`) lets you sort columns in your preferred analysis order — categorical strings first, then dates, then numerics.

---

## Supported Databases

| Dialect | Status |
|---------|--------|
| Snowflake | Full support (primary) |
| BigQuery | Full support |
| DuckDB | Full support (local dev/test) |
| PostgreSQL | Planned |

---

## CLI Reference

```
dbprofile run            Profile a database and produce reports
  -c, --config           Path to YAML config file (required)
  -p, --project-dir      Project folder. Outputs go to <project-dir>/dq_eda/.
                         Falls back to ./reports/ when omitted.
  -o, --output           Override HTML output path
  --sample-rate          Override sample rate (0.0–1.0)
  --sample-method        Override sampling: bernoulli | system
  --dry-run              Preview queries without executing
  --export-json          Write JSON results ('auto' or a path)
  --export-excel         Write Excel workbook ('auto' or a path)
  -v, --verbose          Enable debug logging

dbprofile notebook       Generate a Jupyter EDA notebook for each profiled table
  -c, --config           Path to YAML config file (required)
  -p, --project-dir      Project folder (notebooks land in <project-dir>/dq_eda/)
  --json                 Re-generate from an existing JSON export (no DB needed)
  --tables               Limit to specific tables (pass once per table)
  --update-helpers       Refresh helpers in dq_eda/ even if analyst-modified
  --force                Overwrite analyst-modified notebooks (originals → .backups/)

dbprofile excel          Build Excel from saved JSON (no database needed)
  --json                 Path to JSON file from a previous run (required)
  -c, --config           Path to YAML config (required)
  -p, --project-dir      Project folder
  -o, --output           Override Excel output path

dbprofile html           Rebuild HTML from saved JSON (no database needed)
  --json                 Path to JSON file from a previous run (required)
  -c, --config           Path to YAML config (required)
  -p, --project-dir      Project folder
  -o, --output           Override HTML output path

dbprofile compare        Compare two profiling runs (coming soon)
```

---

## Development

```bash
pip install -e ".[dev]"
pytest tests/ -v
ruff check dbprofile tests
```

---

## License

MIT
