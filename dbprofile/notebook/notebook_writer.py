"""Write a generated notebook to disk respecting analyst edits.

Behavior on every call to write_notebook():

  1. Canonical path doesn't exist          → write it. Stamp source_hash.
  2. Canonical exists, hash matches stored → silent overwrite (analyst
                                            hasn't touched it; pick up
                                            any package-side updates).
  3. Canonical exists, hash differs        → analyst modified the file.
                                            Write fresh baseline to a
                                            different name (HHMM suffix)
                                            and leave the original alone.
  4. force=True                            → back up original to
                                            .backups/, then overwrite.

The "modified" check is a SHA-256 of cell sources only (not outputs),
embedded in nb.metadata.dbprofile.source_hash at write time. Hashing
sources means "did the analyst edit cell text?" — running the notebook
(which mutates outputs/exec counts) does NOT count as modification.
"""

from __future__ import annotations

import hashlib
from datetime import datetime
from pathlib import Path
from typing import Literal

import nbformat
from rich.console import Console

from dbprofile import __version__ as DBPROFILE_VERSION
from dbprofile.notebook.backup import backup_file
from dbprofile.output_dir import auto_name

console = Console()

WriteOutcome = Literal[
    "written_new",                  # canonical didn't exist
    "overwritten_unchanged",        # hash matched → silent refresh
    "dated_baseline_after_edit",    # analyst modified → new HHMM-suffixed file
    "force_overwrite_with_backup",  # --force after analyst edit
]


# ── Public API ───────────────────────────────────────────────────────────────


def write_notebook(
    nb: nbformat.NotebookNode,
    out_dir: Path,
    table: str,
    *,
    force: bool = False,
    run_at: datetime | None = None,
) -> tuple[Path, WriteOutcome]:
    """Write `nb` to `out_dir/eda_<table>_<date>.ipynb` with safety rails.

    Returns (path_actually_written, outcome). Always returns — never raises
    on collisions. Console messages are emitted via rich for analyst-modified
    and force paths; the new-file and silent-refresh paths are quiet.
    """
    when = run_at or datetime.utcnow()
    # Filenames are always lowercase even when the source table is uppercase
    # (e.g. Snowflake). The table identity inside the notebook (title,
    # metadata, SQL) keeps the original case.
    fname_table = table.lower()
    canonical = out_dir / auto_name(fname_table, "ipynb", prefix="eda_", run_at=when)

    # Stamp metadata + hash before writing so the next run has something
    # to compare against. We mutate the nb in place.
    _embed_metadata(nb, table=table, run_at=when)

    if not canonical.exists():
        nbformat.write(nb, canonical)
        return canonical, "written_new"

    existing = nbformat.read(canonical, as_version=4)

    if force:
        backup_file(canonical, out_dir)
        nbformat.write(nb, canonical)
        console.print(
            f"[yellow]{canonical.name} overwritten "
            f"(original backed up to .backups/).[/yellow]"
        )
        return canonical, "force_overwrite_with_backup"

    if _analyst_modified(existing):
        # Don't touch the analyst's file. Write the new baseline to an
        # HHMM-suffixed filename in the same dir.
        dated = out_dir / auto_name(
            fname_table, "ipynb", prefix="eda_", run_at=when, hhmm=True,
        )
        nbformat.write(nb, dated)
        console.print(
            f"[yellow]{canonical.name} has been modified since it was generated.\n"
            f"Leaving your file untouched. "
            f"Fresh baseline written to: {dated.name}[/yellow]"
        )
        return dated, "dated_baseline_after_edit"

    # Hash matched — analyst hasn't touched it. Safe to refresh silently.
    nbformat.write(nb, canonical)
    return canonical, "overwritten_unchanged"


# ── Internal helpers ─────────────────────────────────────────────────────────


def _source_hash(nb) -> str:
    """SHA-256 hex of all cell source strings, concatenated in order.

    Excludes outputs, execution counts, and metadata so the hash is stable
    across notebook executions.
    """
    pieces: list[str] = []
    for cell in nb.get("cells", []):
        if cell.get("cell_type") not in ("code", "markdown"):
            continue
        src = cell.get("source", "")
        # nbformat stores source as either a string or a list of lines.
        if isinstance(src, list):
            src = "".join(src)
        pieces.append(src)
    return hashlib.sha256("\n".join(pieces).encode("utf-8")).hexdigest()


def _embed_metadata(nb, *, table: str, run_at: datetime) -> None:
    """Stamp nb.metadata.dbprofile with version + source_hash."""
    meta = nb.setdefault("metadata", {})
    meta["dbprofile"] = {
        "generated_by": "dbprofile",
        "version":      DBPROFILE_VERSION,
        "generated_at": run_at.isoformat(timespec="seconds"),
        "table":        table,
        "source_hash":  _source_hash(nb),
    }


def _analyst_modified(existing) -> bool:
    """True when existing's stored hash doesn't match its current sources.

    Missing metadata is treated as modified — we won't clobber a notebook
    we don't have provenance for. Forces the user to use --force if they
    really want to overwrite.
    """
    meta = existing.get("metadata", {}).get("dbprofile", {})
    stored = meta.get("source_hash")
    if not stored:
        return True
    return stored != _source_hash(existing)
