"""Copy notebook helpers from the package into <project>/dq_eda/.

Behavior on each call to copy_helpers():

  * First time (no state file)  → write all helpers, drop a .gitignore
                                  for .backups/, record hashes.
  * Re-run, helpers unchanged   → silent no-op.
  * Re-run, package has a newer → silently overwrite (no analyst work
    version, analyst hasn't       at risk).
    touched their copies
  * Re-run, analyst edited their → leave their copies alone, print a
    helper(s)                     hint pointing at --update-helpers.
  * Re-run with force=True       → back up analyst's copy to .backups/,
                                  then overwrite. Used by --update-helpers.

The "edited vs unchanged" check is hash-based, not comment-based:
state.py stores SHA-256 of each helper's shipped bytes the last time we
wrote them. We compare the current dq_eda/<helper> hash against that
recorded hash to decide whether the analyst has modified the file.
"""

from __future__ import annotations

import shutil
from pathlib import Path

from rich.console import Console

from dbprofile.notebook import state
from dbprofile.notebook import templates as templates_pkg
from dbprofile.notebook.backup import BACKUPS_DIRNAME, backup_file

console = Console()

HELPERS = (
    "eda_helpers.py",
    "eda_profile.py",
    "eda_helpers_call_templates.py",
)

GITIGNORE_FILENAME = ".gitignore"
REQUIREMENTS_FILENAME = "requirements.txt"

# Runtime deps the generated EDA notebooks need on the analyst's side.
# Includes every dialect's connector — the user can trim what they don't use.
# We only write this file if it's not already there, so the analyst can
# customize it freely.
_REQUIREMENTS_TEMPLATE = """\
# dbprofile-generated EDA notebook runtime deps.
# Install with:  pip install -r requirements.txt
#
# Always required:
matplotlib>=3.7
pandas>=2.0
numpy>=1.24
ipython>=8.0
seaborn>=0.12
mplcursors>=0.5

# Connector-specific (uncomment what you need; leave the rest commented to
# avoid pulling in deps you don't use):
# snowflake-connector-python>=3.0
# cryptography>=41.0
# google-cloud-bigquery>=3.0
# google-auth>=2.0
# duckdb>=0.9

# Jupyter — install separately if not already present:
#   pip install jupyterlab
"""


def _templates_dir() -> Path:
    """Directory containing the packaged helper source files."""
    return Path(templates_pkg.__file__).parent


def _ensure_gitignore(out_dir: Path) -> None:
    """Drop a .gitignore in dq_eda/ on first copy listing .backups/."""
    gi = out_dir / GITIGNORE_FILENAME
    if gi.exists():
        return
    gi.write_text(
        "# Created by dbprofile — keeps backup snapshots out of version control.\n"
        f"{BACKUPS_DIRNAME}/\n",
        encoding="utf-8",
    )


def _ensure_requirements(out_dir: Path) -> None:
    """Drop requirements.txt in dq_eda/ on first copy. Never overwrites —
    the analyst owns the file once it exists (they may have customized
    versions, removed unused deps, etc.).
    """
    req = out_dir / REQUIREMENTS_FILENAME
    if req.exists():
        return
    req.write_text(_REQUIREMENTS_TEMPLATE, encoding="utf-8")


def _classify(helper_name: str, out_dir: Path) -> str:
    """Classify the analyst's current copy of one helper.

    Returns one of:
      'absent'           — no copy in dq_eda/ yet (first run)
      'unchanged'        — analyst's copy matches the recorded hash
      'analyst_modified' — analyst's copy differs from recorded hash
    """
    target = out_dir / helper_name
    if not target.exists():
        return "absent"

    recorded = state.read_state(out_dir).get("helper_versions", {}).get(helper_name)
    current_hash = state.file_hash(target)

    if recorded is None:
        # No state record — be safe and treat as analyst_modified so we don't
        # clobber. --update-helpers can force the overwrite.
        return "analyst_modified"
    if current_hash == recorded:
        return "unchanged"
    return "analyst_modified"


def copy_helpers(out_dir: Path, *, force: bool = False) -> dict[str, str]:
    """Copy helpers from the package into out_dir, respecting analyst edits.

    Parameters
    ----------
    out_dir : Path
        Target dq_eda/ directory (must already exist; resolve_output_dir
        creates it).
    force : bool
        When True, overwrite even analyst-modified files, backing up the
        existing copy to dq_eda/.backups/ first. Wired to --update-helpers.

    Returns
    -------
    dict[str, str]
        Per-helper outcome: 'written', 'skipped_unchanged',
        'skipped_analyst_modified', or 'overwritten_with_backup'.
    """
    src_dir = _templates_dir()
    new_hashes: dict[str, str] = {}
    outcomes: dict[str, str] = {}
    skipped_modified: list[str] = []

    for helper in HELPERS:
        src = src_dir / helper
        dst = out_dir / helper
        status = _classify(helper, out_dir)

        if status == "absent":
            shutil.copy2(src, dst)
            new_hashes[helper] = state.file_hash(src)
            outcomes[helper] = "written"

        elif status == "unchanged":
            # Always refresh — silent overwrite is safe since hash matches.
            # Picks up any package-side updates without nagging the analyst.
            shutil.copy2(src, dst)
            new_hashes[helper] = state.file_hash(src)
            outcomes[helper] = "skipped_unchanged"

        elif status == "analyst_modified":
            if force:
                backup_file(dst, out_dir)
                shutil.copy2(src, dst)
                new_hashes[helper] = state.file_hash(src)
                outcomes[helper] = "overwritten_with_backup"
            else:
                outcomes[helper] = "skipped_analyst_modified"
                skipped_modified.append(helper)

    if new_hashes:
        state.update_helper_versions(out_dir, new_hashes)
        _ensure_gitignore(out_dir)
        _ensure_requirements(out_dir)

    if skipped_modified:
        console.print(
            f"[yellow]Helpers not updated (analyst-modified): "
            f"{', '.join(skipped_modified)}.\n"
            f"Run with --update-helpers to refresh "
            f"(originals will be saved to {BACKUPS_DIRNAME}/).[/yellow]"
        )

    return outcomes
