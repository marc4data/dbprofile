# sqlprofile — project build spec

Build a complete, publishable Python package called `dbprofile` that runs automated data profiling against a live SQL database and produces a navigable single-file HTML report. This spec is complete and self-contained — scaffold the full project from it.

---

## Goal

A CLI tool + importable library that:
1. Connects to a SQL database via SQLAlchemy
2. Discovers tables/columns per a YAML config
3. Runs 8 profiling checks against each applicable column
4. Writes a self-contained HTML report with table of contents, navigation, charts and/or tables, and RAG (red/amber/green) severity coloring

---

## Project structure

```
dbprofile/
├── dbprofile/
│   ├── __init__.py
│   ├── cli.py                  # click CLI: `dbprofile run --config config.yaml`
│   ├── config.py               # pydantic config loader + validator
│   ├── orchestrator.py         # discovers tables/columns, fans out checks, collects results
│   ├── connectors/
│   │   ├── __init__.py
│   │   └── base.py             # SQLAlchemy-based connector; dialect-aware info schema queries
│   ├── checks/
│   │   ├── __init__.py
│   │   ├── base.py             # CheckResult dataclass + BaseCheck ABC
│   │   ├── schema_audit.py         # check 1
│   │   ├── row_count.py            # check 2
│   │   ├── null_density.py         # check 3
│   │   ├── uniqueness.py           # check 4
│   │   ├── numeric_distribution.py # check 5
│   │   ├── frequency_distribution.py # check 6
│   │   ├── temporal_consistency.py # check 7
│   │   └── format_validation.py    # check 8
│   └── report/
│       ├── renderer.py         # Jinja2 renderer → single HTML file
│       └── template.html.j2    # full report template
├── tests/
│   ├── conftest.py             # in-memory DuckDB fixture
│   └── test_checks.py          # one test per check
├── examples/
│   └── config.yaml             # annotated example config
├── pyproject.toml
├── README.md
├── .env.example                # committed — template with empty values
├── .env                        # gitignored — actual credentials
├── .gitignore
└── .github/
    └── workflows/
        └── ci.yml              # pytest + ruff on push
```

---

## Default development target — NYC Taxi dataset on BigQuery

The default configuration targets the public NYC Taxi & Limousine Commission dataset on BigQuery. This is the primary development and tuning target before expanding to other data sources. It is large (hundreds of millions of rows), messy, has real nulls, numeric outliers, date gaps, and format-checkable columns — ideal for exercising all 8 checks end-to-end.

### Why this dataset
- `bigquery-public-data.new_york_taxi_trips` contains multiple tables: `tlc_yellow_trips_*`, `tlc_green_trips_*`, `tlc_fhv_trips`, `tlc_fhvhv_trips`
- Real-world null rates (e.g. `passenger_count`, `store_and_fwd_flag`)
- Numeric distributions worth profiling: `fare_amount`, `trip_distance`, `tip_amount` — all have outliers and negative values (refunds/errors)
- Temporal columns: `pickup_datetime`, `dropoff_datetime` — good for gap detection and volume anomaly checks
- Low-cardinality columns: `payment_type`, `rate_code`, `vendor_id` — good for frequency distribution and domain validation
- High row counts require `sample_rate` to be set to 0.01–0.05 during development

### Default config file: `config.yaml`

```yaml
# dbprofile default development config — NYC Taxi on BigQuery
# Credentials are loaded from environment variables — never hardcode them here.
# See .env.example for required variables.

connection:
  dialect: bigquery
  project: "${BQ_PROJECT}"           # resolved from environment at runtime
  credentials_path: "${GOOGLE_APPLICATION_CREDENTIALS}"  # path to service account JSON

scope:
  project: bigquery-public-data
  dataset: new_york_taxi_trips        # BigQuery uses "dataset" not "schema"
  tables:
    - tlc_yellow_trips_2022
    - tlc_green_trips_2022
  exclude_tables: []
  column_overrides:
    tlc_yellow_trips_2022:
      include:
        - vendor_id
        - pickup_datetime
        - dropoff_datetime
        - passenger_count
        - trip_distance
        - rate_code
        - store_and_fwd_flag
        - payment_type
        - fare_amount
        - extra
        - mta_tax
        - tip_amount
        - tolls_amount
        - total_amount

checks:
  enabled: [all]
  disabled: []
  sample_rate: 0.02                  # 2% sample — ~6M rows from 2022 table; increase for final runs

report:
  output: ./reports/nyc_taxi_profile.html
  include: [tables, charts]
  thresholds:
    null_pct_warn: 5
    null_pct_critical: 20
    duplicate_pct_warn: 0.001
    duplicate_pct_critical: 0.01
    outlier_pct_warn: 1
    outlier_pct_critical: 5
```

---

## Credential management — best practices

**Never commit credentials to git.** Use the following layered approach:

### 1. `.env` file (local development)

Create a `.env` file at the project root. It is gitignored. Use `python-dotenv` to load it at startup.

```bash
# .env  — never commit this file
BQ_PROJECT=your-gcp-project-id
GOOGLE_APPLICATION_CREDENTIALS=/path/to/your/service-account-key.json
```

### 2. `.env.example` (committed to git)

```bash
# .env.example — copy to .env and fill in values
BQ_PROJECT=
GOOGLE_APPLICATION_CREDENTIALS=
```

### 3. `.gitignore` entries to add

```
.env
*.json            # catches accidentally committed service account key files
reports/          # don't commit generated reports
results.json
results.parquet
```

### 4. Config value resolution

In `config.py`, resolve `${VAR_NAME}` placeholders in any config string value using `os.environ`. Load `.env` automatically using `python-dotenv` if present:

```python
from dotenv import load_dotenv
load_dotenv()  # loads .env if present, no-op if not

import re, os

def resolve_env_vars(value: str) -> str:
    """Replace ${VAR} placeholders with environment variable values."""
    return re.sub(
        r'\$\{(\w+)\}',
        lambda m: os.environ[m.group(1)],  # raises KeyError with clear name if missing
        value
    )
```

Apply `resolve_env_vars` to all string config values during loading.

### 5. BigQuery authentication options (in order of preference)

The connector should support three auth methods, tried in this order:

1. **Service account key file** — `GOOGLE_APPLICATION_CREDENTIALS` env var points to a JSON key file. Used for CI and local dev.
2. **Application Default Credentials (ADC)** — `gcloud auth application-default login`. Used for interactive local dev without a key file.
3. **Workload Identity / metadata server** — automatic when running on GCP (Cloud Run, GCE, etc.). No config needed.

In `connectors/base.py`, detect which method is available:

```python
from google.oauth2 import service_account
from google.auth import default as google_auth_default

def get_bigquery_credentials(credentials_path: str | None):
    if credentials_path and Path(credentials_path).exists():
        return service_account.Credentials.from_service_account_file(
            credentials_path,
            scopes=["https://www.googleapis.com/auth/bigquery.readonly"]
        )
    # fall back to ADC
    creds, _ = google_auth_default(
        scopes=["https://www.googleapis.com/auth/bigquery.readonly"]
    )
    return creds
```

Always request **read-only** BigQuery scope — `bigquery.readonly`. This is the principle of least privilege and reassures DBAs when granting access.

---

## Dependencies

Keep them minimal:
- `sqlalchemy>=2.0`
- `sqlalchemy-bigquery>=1.9`       ← BigQuery SQLAlchemy dialect
- `google-cloud-bigquery>=3.0`     ← BigQuery client
- `google-auth>=2.0`               ← GCP authentication
- `pandas>=2.0`
- `jinja2>=3.0`
- `pyyaml>=6.0`
- `click>=8.0`
- `pydantic>=2.0`
- `pydantic-settings>=2.0`         ← env var binding for settings
- `python-dotenv>=1.0`             ← .env file loading
- `rich>=13.0`                     ← CLI progress bars and console output
- `duckdb` (dev/test only)

No Spark, no dbt required.

---

## Configuration (YAML)

```yaml
connection:
  dialect: postgres          # postgres | bigquery | snowflake | duckdb | redshift | mysql
  dsn: "postgresql://user:pass@host:5432/dbname"

scope:
  schemas: [public]          # omit = all schemas
  tables: [orders, customers] # omit = all tables in schema
  exclude_tables: [audit_log, django_migrations]
  column_overrides:
    orders:
      include: [id, status, amount, created_at]  # only profile these columns

checks:
  enabled: [all]             # or list specific check names
  disabled: []               # opt-out by name, e.g. [temporal_consistency]
  sample_rate: 1.0           # 0.0–1.0; use 0.1 for large tables

report:
  output: ./dbprofile_report.html
  include: [tables, charts]  # tables | charts | both
  thresholds:
    null_pct_warn: 10
    null_pct_critical: 50
    duplicate_pct_warn: 0.001
    duplicate_pct_critical: 0.01
    outlier_pct_warn: 1
    outlier_pct_critical: 5
```

---

## The 8 profiling checks

Each check is a class inheriting `BaseCheck` with a `run(table, columns, conn, config) -> list[CheckResult]` method. Results include: `table`, `column` (None for table-level checks), `check_name`, `metric`, `value`, `severity` (ok/warn/critical), `detail` (dict of supporting data for charts/tables).

### Check 1 — Schema & metadata audit (table-level)
Query `information_schema.columns` for the table. Return column name, ordinal position, declared data type, `is_nullable`, `column_default`. Flag columns present in the table but absent from any schema contract if one is provided in config. No severity thresholds — informational.

### Check 2 — Row count & partition skew (table-level)
Total row count. If a date/timestamp column exists, group by `DATE_TRUNC('day', col)` and compute daily counts + percentage of total. Flag days with zero rows as gaps. Flag any single day exceeding 50% of total rows as skew (warn). Store the time series in `detail` for charting.

```sql
SELECT DATE_TRUNC('day', created_at) AS d, COUNT(*) AS n
FROM {table}
GROUP BY 1 ORDER BY 1;
```

### Check 3 — Null density & completeness (per column)
For every column: null count, null %, empty-string count (varchar only), and sentinel null count for known sentinels: `''`, `'N/A'`, `'n/a'`, `'NULL'`, `'none'`, `-1`, `0` (numeric only when column name suggests optional), `9999-12-31` (date only). Severity uses `null_pct_warn` / `null_pct_critical` thresholds.

```sql
SELECT
  COUNT(*) AS total,
  SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) AS null_count,
  ROUND(100.0 * AVG(CASE WHEN {col} IS NULL THEN 1.0 ELSE 0 END), 4) AS null_pct
FROM {table};
```

### Check 4 — Uniqueness & duplicate detection (per column + table-level composite)
Single-column: distinct count, distinct %, duplicate count. Table-level: duplicate row count across all columns. Flag if duplicate % exceeds `duplicate_pct_warn` / `duplicate_pct_critical`. Also run a window-function near-duplicate check grouped by business key columns if identified in config.

```sql
SELECT {col}, COUNT(*) AS n
FROM {table}
GROUP BY {col}
HAVING COUNT(*) > 1
ORDER BY n DESC
LIMIT 50;
```

### Check 5 — Numeric distribution & outlier detection (numeric columns only)
Mean, p25, median (p50), p75, p95, p99, stddev. IQR outlier count (values outside 1.5×IQR from quartiles). Store the percentile series in `detail` for a box-plot style chart. Severity based on `outlier_pct_warn` / `outlier_pct_critical`.

```sql
SELECT
  AVG({col}) AS mean,
  PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY {col}) AS p25,
  PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY {col}) AS p50,
  PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY {col}) AS p75,
  PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY {col}) AS p95,
  PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY {col}) AS p99,
  STDDEV({col}) AS stddev
FROM {table};
```

### Check 6 — Frequency distribution & cardinality (low-to-medium cardinality columns)
Only run on columns with fewer than 200 distinct values (configurable). Top-30 values by frequency with count and cumulative %. Flag if any single value exceeds 90% of rows (warn) — indicates a default that was never overridden. Store top-N series in `detail` for a bar chart.

```sql
SELECT {col}, COUNT(*) AS freq,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) AS pct
FROM {table}
GROUP BY {col}
ORDER BY freq DESC
LIMIT 30;
```

### Check 7 — Temporal consistency & load watermark (date/timestamp columns only)
Daily row counts with gap detection (calendar spine vs actual data). Flag missing days as critical. Flag days with zero rows in the middle of the series as gaps. Compute trailing-30-day average row count and flag days deviating by more than 2 stddev as anomalies. Store the full series in `detail` for a line chart.

```sql
-- gap detection using generate_series (Postgres) or equivalent
-- fall back to simple daily counts for dialects without generate_series
SELECT DATE_TRUNC('day', {col}) AS d, COUNT(*) AS n
FROM {table}
GROUP BY 1 ORDER BY 1;
```

### Check 8 — Format & domain validation (varchar/text columns)
Detect column intent from name patterns and apply relevant regex or domain checks:
- `*email*` → RFC-5322 simplified regex
- `*phone*` → digits/dashes/parens, 7–15 chars
- `*zip*` / `*postal*` → 5-digit or postal format
- `*url*` / `*website*` → starts with http/https
- `*uuid*` / `*guid*` → UUID v4 pattern
- `*country*` → check against ISO 3166-1 alpha-2 list
- `*currency*` / `*iso_currency*` → ISO 4217 3-letter code
- `*status*` / `*type*` / `*category*` → flag if distinct count > 50 (possible free-text in enum field)

Return violation count and violation % per pattern matched. Severity: warn if > 0.1%, critical if > 1%.

```sql
SELECT COUNT(*) AS violations
FROM {table}
WHERE {col} IS NOT NULL
  AND {col} !~ '{pattern}';
```

---

## CheckResult dataclass

```python
@dataclass
class CheckResult:
    table: str
    schema: str
    column: str | None        # None for table-level checks
    check_name: str           # e.g. "null_density"
    metric: str               # e.g. "null_pct"
    value: float | int | str
    severity: str             # "ok" | "warn" | "critical" | "info"
    detail: dict              # raw data for charts/tables, e.g. {"series": [...]}
    sql: str                  # the actual query that was run
    run_at: datetime
```

---

## Report design

Single self-contained `.html` file. All CSS and JS inlined. Uses **Chart.js** (CDN, pinned version) for charts. No server required — opens directly in a browser.

### Layout
- Fixed left sidebar (240px) with collapsible tree: Schema → Table → Check
- Main content area with smooth scroll and `id`-anchored sections
- Top sticky header with: project name, run timestamp, summary counts (total tables, total columns, critical issues, warnings)

### Executive summary section (top of main content)
- 4 metric cards: Tables Profiled, Columns Profiled, Critical Issues, Warnings
- Issues summary table: table name | check | column | severity | value — sorted critical first, clickable rows that jump to the relevant section

### Per-table section
- Table health scorecard: a CSS grid heatmap — rows = columns, columns = checks, cells colored by severity (green/amber/red/gray=not applicable)
- Collapsible sub-sections per check

### Per-check detail
- Result summary sentence (e.g. "3.4% null values — above warn threshold of 10%")
- If `include: tables` or `both`: an HTML data table of the raw results
- If `include: charts` or `both`: a Chart.js chart appropriate to the check type:
  - Check 2, 7: line chart (time series)
  - Check 5: horizontal bar chart showing p25/p50/p75/p95/p99
  - Check 6: bar chart of top-N value frequencies
  - Check 3, 4, 8: simple stat cards or small table (no chart needed)
  - Check 1: column metadata table

### "Issues only" view
A button in the header toggles the report to show only warn/critical findings. This is the auditor view.

### Deep links
Every section has a stable anchor: `#schema.table.check_name.column` — shareable URLs.

---

## CLI

```bash
# basic usage
dbprofile run --config config.yaml

# with overrides
dbprofile run --config config.yaml --output ./reports/run1.html --sample-rate 0.1

# dry run: print queries without executing
dbprofile run --config config.yaml --dry-run

# compare two runs
dbprofile compare baseline.json current.json --output diff_report.html

# export raw results as JSON or parquet alongside HTML
dbprofile run --config config.yaml --export-json results.json
```

---

## Output files

Primary: `dbprofile_report.html` — the navigable HTML report  
Optional: `results.json` — raw `CheckResult` records as JSON array  
Optional: `results.parquet` — same data as Parquet for downstream use  

---

## Testing

Use an **in-memory DuckDB** database as the test fixture. Seed it with synthetic tables covering:
- A table with known null rates
- A table with known duplicates
- A table with date gaps
- A table with format violations (bad emails, invalid statuses)

Write one test per check asserting that:
1. The check runs without error
2. The severity is correct for seeded bad data
3. The `detail` dict is populated for chart-producing checks

---

## README requirements

Include:
- One-line description and badges (PyPI, license, CI)
- Installation: `pip install dbprofile`
- Quickstart (5 lines: install → write config → run → open report)
- Supported databases table
- Config reference (all keys documented)
- Screenshot placeholder for the report
- Contributing guide
- License: MIT

---

## pyproject.toml

Use `[build-system] requires = ["hatchling"]`. Include console script entry point: `dbprofile = dbprofile.cli:main`. Python >= 3.10. Include `[project.optional-dependencies] dev = [pytest, ruff, duckdb]`.

---

## Dialect notes for the connector

`information_schema.columns` works across Postgres, MySQL, Snowflake, BigQuery, and DuckDB with minor differences. Abstract these into the connector:
- `PERCENTILE_CONT` syntax differs: Postgres/DuckDB use `WITHIN GROUP (ORDER BY col)`, Snowflake uses `PERCENTILE_CONT(col, 0.5)`, BigQuery uses `PERCENTILE_CONT(col, 0.5) OVER ()`
- `generate_series` for gap detection: available in Postgres and DuckDB; fall back to simple daily counts for other dialects
- `REGEXP_LIKE` vs `~` vs `REGEXP`: abstract into a `dialect_regex(col, pattern)` helper on the connector

### BigQuery-specific dialect notes (primary dialect — implement first)

BigQuery differs from standard SQL in several important ways. Handle all of these in the BigQuery connector:

**Table references** — BigQuery uses backtick-quoted fully-qualified names:
```sql
SELECT * FROM `bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2022`
```
The connector must render table refs as `` `{project}.{dataset}.{table}` ``.

**Sampling** — BigQuery has native TABLESAMPLE:
```sql
SELECT * FROM `project.dataset.table` TABLESAMPLE SYSTEM (2 PERCENT)
```
Use this instead of `WHERE RAND() < 0.02` — it is far cheaper on slot usage.

**information_schema** — BigQuery's information schema is dataset-scoped:
```sql
SELECT column_name, data_type, is_nullable
FROM `project.dataset`.INFORMATION_SCHEMA.COLUMNS
WHERE table_name = 'tlc_yellow_trips_2022'
```

**PERCENTILE_CONT** — BigQuery requires a window function form:
```sql
SELECT PERCENTILE_CONT(fare_amount, 0.5) OVER () AS median_fare
FROM `project.dataset.table` LIMIT 1
```
Wrap in a subquery to get a scalar result.

**DATE_TRUNC** — BigQuery uses `DATE_TRUNC(col, DAY)` not `DATE_TRUNC('day', col)`:
```sql
SELECT DATE_TRUNC(pickup_datetime, DAY) AS d, COUNT(*) AS n
FROM `project.dataset.table`
GROUP BY 1 ORDER BY 1
```

**No generate_series** — use a date range CTE with `UNNEST(GENERATE_DATE_ARRAY(...))` for gap detection:
```sql
WITH date_spine AS (
  SELECT d FROM UNNEST(
    GENERATE_DATE_ARRAY(DATE '2022-01-01', DATE '2022-12-31', INTERVAL 1 DAY)
  ) AS d
)
SELECT ds.d, COALESCE(t.n, 0) AS n
FROM date_spine ds
LEFT JOIN (
  SELECT DATE(pickup_datetime) AS d, COUNT(*) AS n
  FROM `project.dataset.table`
  GROUP BY 1
) t ON ds.d = t.d
ORDER BY 1
```

**Regex** — BigQuery uses `REGEXP_CONTAINS(col, pattern)`:
```sql
WHERE NOT REGEXP_CONTAINS(email, r'^[^@\s]+@[^@\s]+\.[^@\s]+')
```

**Cost awareness** — BigQuery charges per bytes scanned. The connector should:
1. Always use `TABLESAMPLE` when `sample_rate < 1.0`
2. Log estimated bytes processed per query using the BigQuery job statistics API
3. Print a cumulative estimated cost summary at the end of a run (at $6.25/TB scanned)
4. Support `--dry-run` mode using BigQuery's dry-run job API to estimate cost before executing

**BigQuery job stats** — after each query, retrieve and log:
```python
job = client.query(sql)
results = job.result()
bytes_processed = job.total_bytes_processed
print(f"  {bytes_processed / 1e9:.2f} GB processed")
```

---

## What to build first (suggested order)

1. `pyproject.toml` + package skeleton + `.gitignore` + `.env.example`
2. `config.py` — pydantic model, loader, env var resolver
3. `connectors/base.py` — BigQuery connector first (primary dialect), then abstract for others
4. `checks/base.py` — `CheckResult` dataclass, `BaseCheck` ABC
5. Checks 1–8 in order, validating each against the NYC Taxi dataset
6. `orchestrator.py` — table/column discovery, fan-out, result collection, cost tracking
7. `report/template.html.j2` + `renderer.py`
8. `cli.py`
9. Tests (DuckDB fixture for unit tests; BigQuery integration test gated behind `--integration` flag)
10. README + `examples/config.yaml` (pre-filled for NYC Taxi as the default example)

## Development workflow against NYC Taxi

After scaffolding, the recommended iteration loop is:

```bash
# 1. Set up credentials
cp .env.example .env
# edit .env with your GCP project ID and service account key path

# 2. Run a dry run first to see what queries will execute and estimated cost
dbprofile run --config config.yaml --dry-run

# 3. Run against a single table with 2% sample to verify all 8 checks work
dbprofile run --config config.yaml --sample-rate 0.02

# 4. Open report and iterate on layout/content
open reports/nyc_taxi_profile.html

# 5. Once satisfied, run full tables at higher sample rate
dbprofile run --config config.yaml --sample-rate 0.10

# 6. Export raw results for downstream comparison
dbprofile run --config config.yaml --export-json results_baseline.json
```

Expected findings in the NYC Taxi dataset to validate checks are working:
- Check 3 (nulls): `passenger_count` has ~1–3% nulls; `store_and_fwd_flag` has nulls for non-store-forward trips
- Check 5 (distribution): `fare_amount` and `total_amount` have negative values (refunds) — these should flag as outliers
- Check 5 (distribution): `trip_distance` has extreme outliers (0.0 and values > 100 miles)
- Check 6 (frequency): `payment_type` should show ~67% credit card, ~30% cash, with a long tail
- Check 7 (temporal): volume should show a clear weekly seasonality pattern; COVID period (2020) shows dramatic drop
- Check 8 (format): `vendor_id` only has values "1", "2", "CMT", "VTS" — domain validation catches strays
