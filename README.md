# dbprofile

Automated SQL database profiling with a navigable, self-contained HTML report.

## Quickstart

```bash
pip install -e ".[dev]"
cp .env.example .env        # fill in BQ_PROJECT and GOOGLE_APPLICATION_CREDENTIALS
dbprofile run --config examples/config.yaml --dry-run   # preview queries + cost
dbprofile run --config examples/config.yaml             # run and open report
open reports/nyc_taxi_profile.html
```

## The 8 checks

| # | Check | Scope | What it finds |
|---|-------|-------|---------------|
| 1 | Schema audit | Table | Column names, types, nullability |
| 2 | Row count | Table | Total rows, daily volume, partition skew |
| 3 | Null density | Column | Null %, empty strings, sentinel values |
| 4 | Uniqueness | Column | Duplicate %, top repeated values |
| 5 | Numeric distribution | Numeric cols | p25/p50/p75/p95/p99, IQR outliers |
| 6 | Frequency distribution | Low-cardinality cols | Top-30 values, dominant-value flag |
| 7 | Temporal consistency | Date/timestamp cols | Gap days, volume anomalies |
| 8 | Format validation | String cols | Email, URL, UUID, enum cardinality |

## Supported databases

| Dialect | Status |
|---------|--------|
| BigQuery | Full support (primary) |
| DuckDB | Full support (used for testing) |
| PostgreSQL | Planned |
| Snowflake | Planned |

## Config reference

```yaml
connection:
  dialect: bigquery           # bigquery | duckdb
  project: "${BQ_PROJECT}"   # your GCP billing project
  credentials_path: "${GOOGLE_APPLICATION_CREDENTIALS}"

scope:
  project: bigquery-public-data   # source project (BQ only)
  dataset: my_dataset
  tables: [orders, customers]     # omit to discover all
  exclude_tables: [audit_log]
  column_overrides:
    orders:
      include: [id, status, amount]  # only profile these columns

checks:
  enabled: [all]              # or list specific check names
  disabled: []                # opt-out specific checks
  sample_rate: 0.02           # 1.0 = full table; 0.02 = 2%

report:
  output: ./reports/report.html
  include: [tables, charts]
  thresholds:
    null_pct_warn: 10
    null_pct_critical: 50
    duplicate_pct_warn: 0.001
    duplicate_pct_critical: 0.01
    outlier_pct_warn: 1
    outlier_pct_critical: 5
```

## CLI reference

```bash
dbprofile run --config config.yaml                    # basic run
dbprofile run --config config.yaml --sample-rate 0.1  # override sample rate
dbprofile run --config config.yaml --dry-run          # show queries, estimate BQ cost
dbprofile run --config config.yaml --export-json results.json
dbprofile compare baseline.json current.json          # diff two runs (coming soon)
```

## Development

```bash
pip install -e ".[dev]"
pytest tests/ -v
ruff check dbprofile tests
```

## License

MIT
