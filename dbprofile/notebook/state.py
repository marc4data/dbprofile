"""Out-of-band state file for a dq_eda/ folder.

Stores a JSON sidecar (.dbprofile_state.json) recording which version of
each helper file is currently in dq_eda/ and when it was last copied.
We track this out-of-band — not via comments at the top of the helpers —
so the analyst can edit helper files freely without breaking the
update-detection logic.

State shape:
    {
      "dbprofile_version": "0.1.0",
      "last_helper_copy_at": "2026-04-30T18:42:00",
      "helper_versions": {
          "eda_helpers.py":                "<sha256 of source file as shipped>",
          "eda_profile.py":                "<sha256>",
          "eda_helpers_call_templates.py": "<sha256>"
      }
    }
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from pathlib import Path

from dbprofile import __version__ as DBPROFILE_VERSION

STATE_FILENAME = ".dbprofile_state.json"


def state_path(out_dir: Path) -> Path:
    return out_dir / STATE_FILENAME


def read_state(out_dir: Path) -> dict:
    """Return the state dict, or empty dict if no state file exists."""
    p = state_path(out_dir)
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}


def write_state(out_dir: Path, state: dict) -> None:
    state_path(out_dir).write_text(
        json.dumps(state, indent=2, default=str),
        encoding="utf-8",
    )


def file_hash(path: Path) -> str:
    """SHA-256 hex digest of a file's bytes."""
    return hashlib.sha256(path.read_bytes()).hexdigest()


def update_helper_versions(out_dir: Path, helper_hashes: dict[str, str]) -> None:
    """Merge new helper hashes into state, preserving any other keys."""
    state = read_state(out_dir)
    state.setdefault("helper_versions", {}).update(helper_hashes)
    state["dbprofile_version"] = DBPROFILE_VERSION
    state["last_helper_copy_at"] = datetime.utcnow().isoformat(timespec="seconds")
    write_state(out_dir, state)
