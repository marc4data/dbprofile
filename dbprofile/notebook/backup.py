"""Shared backup helper for dq_eda/.backups/.

helper_copy.py and notebook_writer.py both need to back up an analyst's
file before overwriting it. The pattern is identical, so the logic lives
here once.
"""

from __future__ import annotations

import shutil
from datetime import datetime
from pathlib import Path

BACKUPS_DIRNAME = ".backups"


def backup_dir(out_dir: Path) -> Path:
    return out_dir / BACKUPS_DIRNAME


def backup_file(target: Path, out_dir: Path) -> Path:
    """Copy `target` into `out_dir/.backups/<stem>_backup_<YYYYMMDD_HHMM><ext>`.

    Creates the backups directory if it doesn't exist. Returns the path
    written so callers can include it in console messages.
    """
    bd = backup_dir(out_dir)
    bd.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M")
    backup_name = f"{target.stem}_backup_{ts}{target.suffix}"
    backup_path = bd / backup_name
    shutil.copy2(target, backup_path)
    return backup_path
