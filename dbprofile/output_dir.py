"""Output directory and filename resolution.

Single source for output path/naming logic shared by every CLI command
(run, excel, html, notebook). Three rules:

  * If --project-dir is given, outputs go to <project_dir>/dq_eda/.
    Created on first use; reused on subsequent runs.

  * If --project-dir is omitted, outputs fall back to ./reports/ — the
    legacy behavior. A one-line hint nudges users toward the new flag.

  * Filenames carry exactly one YYYYMMDD stamp, added by auto_name().
    Stem builders (e.g. run_stem) intentionally do NOT include a date.
"""

from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path

from rich.console import Console

console = Console()

DQ_EDA_SUBDIR = "dq_eda"
LEGACY_OUTPUT_DIR = "reports"


def resolve_output_dir(project_dir: str | Path | None) -> Path:
    """Return the directory where outputs should be written.

    project_dir given → <project_dir>/dq_eda/   (created if absent)
    project_dir None  → ./reports/              (legacy fallback, with hint)
    """
    if project_dir:
        out = Path(project_dir).expanduser() / DQ_EDA_SUBDIR
    else:
        console.print(
            "[yellow]No --project-dir specified — writing to "
            f"./{LEGACY_OUTPUT_DIR}/. Pass --project-dir <path> to "
            f"write into <path>/{DQ_EDA_SUBDIR}/ instead.[/yellow]"
        )
        out = Path(LEGACY_OUTPUT_DIR)
    out.mkdir(parents=True, exist_ok=True)
    return out


def auto_name(
    stem: str,
    ext: str,
    *,
    prefix: str = "",
    run_at: datetime | None = None,
) -> str:
    """Compose an auto-named filename: <prefix><stem>_<YYYYMMDD>.<ext>.

    Examples
    --------
    >>> auto_name("snowflake_analytics_marts", "html")
    'snowflake_analytics_marts_20260430.html'
    >>> auto_name("fct_trips", "ipynb", prefix="eda_")
    'eda_fct_trips_20260430.ipynb'
    """
    when = run_at or datetime.utcnow()
    stamp = when.strftime("%Y%m%d")
    return f"{prefix}{stem}_{stamp}.{ext}"


def run_stem(cfg) -> str:
    """Build a per-run filename stem from a profiler config (no date).

    Format: <dialect>_<db>_<schema(s)>
    Example: snowflake_analytics_dbt_malex_marts

    The date is added separately by auto_name().
    """
    parts = []
    parts.append(cfg.connection.dialect or "db")
    db = cfg.scope.database or cfg.scope.dataset or cfg.scope.project or ""
    if db:
        parts.append(db)
    schemas = cfg.scope.schemas or []
    if schemas:
        parts.append("_".join(schemas))
    stem = "_".join(p.lower() for p in parts if p)
    return re.sub(r"[^a-z0-9_]+", "_", stem).strip("_")
