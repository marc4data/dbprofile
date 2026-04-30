# dbprofile

Automated SQL database profiling with interactive HTML reports, quality scoring, and Excel workbooks for EDA.

---

## Setup

```bash
cd /Users/marcalexander/projects/ai_orchestrator_claude/dbprofile
source .venv/bin/activate
pip install -e ".[snowflake]"
```

Credentials live in `dbprofile/.env` (gitignored). Full paths required — no `~` expansion:

```
SNOWFLAKE_ACCOUNT=***REDACTED***
SNOWFLAKE_USER=***REDACTED***
SNOWFLAKE_PRIVATE_KEY_PATH=/Users/marcalexander/.ssh/snowflake/rsa_key.p8
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_ROLE=TRANSFORMER
```

---

## Standard Run — Snowflake

This is the primary command. It profiles your Snowflake marts and generates all three outputs with auto-named files:

```bash
dbprofile run \
  --config examples/config_snowflake.yaml \
  --export-json auto \
  --export-excel auto
```

**Output files** — auto-named from `dbtype_db_schema_YYYYMMDD`:

```
reports/snowflake_analytics_dbt_malex_marts_20260418.html
reports/snowflake_analytics_dbt_malex_marts_20260418.json
reports/snowflake_analytics_dbt_malex_marts_20260418.xlsx
```

Running again on the same day overwrites the files. Running on a different day creates new files — building up a history for future profile comparison.

Open the report:

```bash
open reports/snowflake_analytics_dbt_malex_marts_*.html
```

---

## Use Cases and Commands

### 1. Full run with all exports (recommended daily workflow)

```bash
dbprofile run \
  --config examples/config_snowflake.yaml \
  --export-json auto \
  --export-excel auto
```

Generates HTML report + JSON snapshot + Excel workbook. The JSON snapshot is your safety net — it lets you regenerate the Excel or (future) run diffs without re-querying Snowflake.

### 2. HTML report only (quick check)

```bash
dbprofile run --config examples/config_snowflake.yaml
```

Fastest option — just the HTML report. Use when you only need a visual scan and don't need the Excel workbook for note-taking.

### 3. Regenerate Excel from a previous run (no Snowflake needed)

```bash
dbprofile excel \
  --json reports/snowflake_analytics_dbt_malex_marts_20260418.json \
  --config examples/config_snowflake.yaml \
  --output reports/snowflake_analytics_dbt_malex_marts_20260418.xlsx
```

Reads the saved JSON, rebuilds scoring and EDA classification locally in ~1 second. Use when you want a fresh workbook after template changes without waiting 15 minutes for the profiler.

### 4. Override sampling for faster runs on large tables

```bash
dbprofile run \
  --config examples/config_snowflake.yaml \
  --sample-rate 0.05 \
  --sample-method system \
  --export-json auto
```

`system` sampling is faster than `bernoulli` (samples whole micro-partitions instead of individual rows). 5% sample + system method cuts runtime significantly on large tables.

### 5. Override the output filename

```bash
dbprofile run \
  --config examples/config_snowflake.yaml \
  --output reports/my_custom_name.html \
  --export-json reports/my_custom_name.json \
  --export-excel reports/my_custom_name.xlsx
```

Pass explicit paths when you want to control the exact filenames instead of auto-naming.

### 6. Dry run — preview queries without executing

```bash
dbprofile run --config examples/config_snowflake.yaml --dry-run
```

Shows every SQL query that would run. Useful for estimating BigQuery cost or verifying scope before a full run.

### 7. Verbose logging — debug connection or query issues

```bash
dbprofile run --config examples/config_snowflake.yaml --verbose
```

Turns on `DEBUG` logging. Shows each query as it executes, timing, and connector details.

### 8. BigQuery profiling

```bash
dbprofile run \
  --config examples/config.yaml \
  --export-json auto \
  --export-excel auto
```

Uses the BigQuery config targeting `bigquery-public-data.new_york_taxi_trips`. Requires `BQ_PROJECT` and `GOOGLE_APPLICATION_CREDENTIALS` in `.env`.

### 9. Local DuckDB development

```bash
python scripts/seed_dev.py                          # seed sample data
dbprofile run --config examples/config_dev.yaml      # profile locally
```

No cloud credentials needed. Good for testing template or check changes.

---

## Output File Naming Convention

All output files follow `dbtype_db_schema_YYYYMMDD.<ext>`:

```
{connector}_{database}_{schema}_{date}.{html|json|xlsx}
```

| Segment | Source | Example |
|---|---|---|
| Connector | `connection.dialect` | `snowflake` |
| Database | `scope.database` or `scope.dataset` | `analytics` |
| Schema | `scope.schemas` joined by `_` | `dbt_malex_marts` |
| Date | Run date UTC | `20260418` |

This convention:
- **Groups by source** when sorted alphabetically (all Snowflake together, all BigQuery together)
- **Overwrites within the same day** (idempotent, no clutter)
- **Creates history across days** (one file per date, ready for diffing)
- **Enables future comparison** — match files by prefix, diff by date suffix

---

## Config Files

| Config | Connector | Target | Notes |
|---|---|---|---|
| `examples/config_snowflake.yaml` | Snowflake | `Analytics` (all schemas) | **Preferred for Snowflake** — key-pair auth |
| `examples/config.yaml` | BigQuery | NYC Taxi public dataset | Requires GCP credentials |
| `examples/config_dev.yaml` | DuckDB | Local `dev.duckdb` | Run `seed_dev.py` first |

---

## The 8 Checks

| # | Check | Scope | What it finds |
|---|-------|-------|---------------|
| 1 | Schema Audit | Table | Column inventory, all-null columns, missing contract columns |
| 2 | Row Count | Table | Empty table detection, daily volume, partition skew |
| 3 | Null Density | Column | Null %, empty strings, sentinel values (N/A, NONE, 9999-12-31) |
| 4 | Uniqueness | Column | Duplicate %, identifier vs. attribute classification |
| 5 | Numeric Distribution | Numeric cols | p25/p50/p75/p95/p99, IQR outliers, histogram + box-whisker |
| 6 | Frequency Distribution | Low-cardinality cols | Top-30 values, dominant-value flag, binary pill display |
| 7 | Temporal Consistency | Date/timestamp cols | Gap days, volume anomalies, daily time series chart |
| 8 | Format Validation | String cols | Email, phone, UUID, ISO country/currency pattern matching |

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
dbprofile run       Profile a database and produce reports
  -c, --config      Path to YAML config file (required)
  -o, --output      Override HTML output path
  --sample-rate     Override sample rate (0.0–1.0)
  --sample-method   Override sampling: bernoulli | system
  --dry-run         Preview queries without executing
  --export-json     Write JSON results ('auto' or a path)
  --export-excel    Write Excel workbook ('auto' or a path)
  -v, --verbose     Enable debug logging

dbprofile excel     Build Excel from saved JSON (no database needed)
  --json            Path to JSON file from a previous run (required)
  -c, --config      Path to YAML config (required)
  -o, --output      Output .xlsx path

dbprofile compare   Compare two profiling runs (coming soon)
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
