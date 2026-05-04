"""Configuration loader — YAML + Pydantic validation, resolves ${ENV_VAR} placeholders."""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any, Literal

import yaml
from dotenv import load_dotenv
from pydantic import BaseModel, field_validator, model_validator

# Load .env file if present. No-op if the file doesn't exist.
load_dotenv()


# ---------------------------------------------------------------------------
# Environment variable resolution
# ---------------------------------------------------------------------------

def resolve_env_vars(value: str) -> str:
    """Replace ${VAR_NAME} placeholders with values from the environment.

    Raises KeyError with the variable name if a placeholder has no matching
    environment variable — fails loudly rather than silently using an empty string.
    """
    return re.sub(
        r"\$\{(\w+)\}",
        lambda m: os.environ[m.group(1)],
        value,
    )


def resolve_recursive(obj: Any) -> Any:
    """Recursively resolve ${VAR} placeholders in any string values in a dict/list."""
    if isinstance(obj, str):
        return resolve_env_vars(obj)
    if isinstance(obj, dict):
        return {k: resolve_recursive(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [resolve_recursive(item) for item in obj]
    return obj


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class ConnectionConfig(BaseModel):
    dialect: str  # "bigquery" | "duckdb" | "snowflake"
    # BigQuery-specific
    project: str | None = None
    credentials_path: str | None = None
    # Snowflake-specific
    account: str | None = None
    user: str | None = None
    password: str | None = None
    private_key_path: str | None = None
    private_key_passphrase: str | None = None
    warehouse: str | None = None
    role: str | None = None
    # DuckDB-specific
    database_path: str | None = None   # path to .duckdb file; omit for in-memory
    # Generic DSN (postgres, mysql, etc.)
    dsn: str | None = None

    @model_validator(mode="after")
    def check_dialect_requirements(self) -> "ConnectionConfig":
        if self.dialect == "bigquery" and not self.project:
            raise ValueError("connection.project is required for dialect=bigquery")
        if self.dialect == "snowflake":
            if not self.account:
                raise ValueError("connection.account is required for dialect=snowflake")
            if not self.user:
                raise ValueError("connection.user is required for dialect=snowflake")
            if not self.private_key_path and not self.password:
                raise ValueError(
                    "connection.private_key_path or connection.password required for snowflake"
                )
        return self


class ColumnOverride(BaseModel):
    include: list[str] | None = None  # if set, only profile these columns
    exclude: list[str] | None = None  # columns to skip


class ScopeConfig(BaseModel):
    # BigQuery uses project + dataset; Snowflake uses database + schemas
    project: str | None = None       # BQ source project (may differ from billing project)
    dataset: str | None = None       # BigQuery dataset name
    database: str | None = None      # Snowflake database name
    schemas: list[str] | None = None # explicit schema list; None = discover all
    tables: list[str] | None = None  # if None, discover all tables
    exclude_tables: list[str] = []
    column_overrides: dict[str, ColumnOverride] = {}


class CheckThresholds(BaseModel):
    null_pct_warn: float = 10.0
    null_pct_critical: float = 50.0
    duplicate_pct_warn: float = 0.001
    duplicate_pct_critical: float = 0.01
    outlier_pct_warn: float = 1.0
    outlier_pct_critical: float = 5.0
    frequency_cardinality_limit: int = 200  # skip freq check above this distinct count
    skew_day_pct: float = 50.0              # flag a single day > this % of total rows


class ChecksConfig(BaseModel):
    enabled: list[str] = ["all"]
    disabled: list[str] = []
    sample_rate: float = 1.0
    sample_method: Literal["bernoulli", "system"] = "bernoulli"
    """
    Sampling method when sample_rate < 1.0:
      bernoulli — row-level probability sampling; statistically uniform, slower on large tables
      system    — block-level sampling; much faster on large tables, slightly less uniform
    """

    @field_validator("sample_rate")
    @classmethod
    def validate_sample_rate(cls, v: float) -> float:
        if not 0.0 < v <= 1.0:
            raise ValueError("sample_rate must be between 0 (exclusive) and 1 (inclusive)")
        return v


class ReportConfig(BaseModel):
    output: str = "./dbprofile_report.html"
    include: list[str] = ["tables", "charts"]
    preview_rows: int = 25                      # max rows in the Data Preview table (1–1000)
    thresholds: CheckThresholds = CheckThresholds()


# ---------------------------------------------------------------------------
# Notebook generation config (per-section + per-column knobs)
#
# Mirrors the ColumnKind enum values as a Literal type so we don't have to
# import from dbprofile.notebook.classify (circular-import safe). The
# classifier will translate these strings back into the enum at apply time.
# ---------------------------------------------------------------------------

ColumnKindName = Literal[
    "date", "binary", "ordinal_cat", "low_cat", "high_cat",
    "string_id", "count_metric", "continuous", "unknown",
]


class ColumnNotebookOverride(BaseModel):
    """Per-column overrides applied during notebook generation.

    Currently `kind` only — wins over the auto-classifier when set.
    Future: per-column chart-param overrides (bin_cnt, top_n, etc.).
    """
    kind: ColumnKindName | None = None


class HeaderSectionConfig(BaseModel):
    enabled: bool = True


class SetupSectionConfig(BaseModel):
    enabled: bool = True


class DataGatherSectionConfig(BaseModel):
    enabled: bool = True
    sample_target_rows: int = 50_000
    sample_floor_pct: float = 0.1


class GrainSectionConfig(BaseModel):
    enabled: bool = True
    include_boundary: bool = True
    include_cardinality: bool = True


class FlagPanelConfig(BaseModel):
    enabled: bool = True
    label_threshold: int = 12


class CategoricalPanelConfig(BaseModel):
    enabled: bool = True
    low_cat_threshold: int = 15
    hc_top_n: int = 20


class CountMetricsPanelConfig(BaseModel):
    enabled: bool = True


class DistributionsPanelConfig(BaseModel):
    enabled: bool = True


class UnivariateSectionConfig(BaseModel):
    enabled: bool = True
    max_continuous_panels: int = 12
    flag_panel: FlagPanelConfig = FlagPanelConfig()
    categorical: CategoricalPanelConfig = CategoricalPanelConfig()
    count_metrics: CountMetricsPanelConfig = CountMetricsPanelConfig()
    distributions: DistributionsPanelConfig = DistributionsPanelConfig()


class BivariateSectionConfig(BaseModel):
    enabled: bool = True
    top_pairs: int = 4
    corr_floor: float = 0.10
    corr_ceiling: float = 0.98


class TemporalSectionConfig(BaseModel):
    enabled: bool = True


class DQFollowupSectionConfig(BaseModel):
    enabled: bool = True
    max_subsections: int = 20            # cap; rest go into a summary table
    skip_checks: list[str] = []          # check_names to drop entirely
    skip_columns: list[str] = []         # exact-match column blocklist


class NotebookSectionsConfig(BaseModel):
    header:      HeaderSectionConfig      = HeaderSectionConfig()
    setup:       SetupSectionConfig       = SetupSectionConfig()
    data_gather: DataGatherSectionConfig  = DataGatherSectionConfig()
    grain:       GrainSectionConfig       = GrainSectionConfig()
    univariate:  UnivariateSectionConfig  = UnivariateSectionConfig()
    bivariate:   BivariateSectionConfig   = BivariateSectionConfig()
    temporal:    TemporalSectionConfig    = TemporalSectionConfig()
    dq_followup: DQFollowupSectionConfig  = DQFollowupSectionConfig()


class NotebookConfig(BaseModel):
    columns:  dict[str, ColumnNotebookOverride] = {}
    sections: NotebookSectionsConfig = NotebookSectionsConfig()


class ProfileConfig(BaseModel):
    connection: ConnectionConfig
    scope: ScopeConfig
    checks: ChecksConfig = ChecksConfig()
    report: ReportConfig = ReportConfig()
    notebook: NotebookConfig = NotebookConfig()


# ---------------------------------------------------------------------------
# Loader
# ---------------------------------------------------------------------------

def load_config(path: str | Path) -> ProfileConfig:
    """Load, resolve env vars, and validate a YAML config file."""
    raw = Path(path).read_text()
    data = yaml.safe_load(raw)
    data = resolve_recursive(data)
    return ProfileConfig.model_validate(data)
