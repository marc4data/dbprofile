"""Tests for dbprofile.notebook.helper_copy — the four hash-detected branches.

The four cases we cover:

  1. First copy (absent)              → all helpers written, state + .gitignore created
  2. Re-copy on unmodified files      → silent overwrite (picks up package updates)
  3. Re-copy with analyst-modified    → preserved, state untouched, hint printed
  4. force=True with analyst-modified → backup created, then overwrite
"""

from __future__ import annotations

from pathlib import Path

from dbprofile.notebook import state
from dbprofile.notebook.helper_copy import (
    BACKUPS_DIRNAME,
    GITIGNORE_FILENAME,
    HELPERS,
    copy_helpers,
)

# Helpers ---------------------------------------------------------------------

def _read(p: Path) -> str:
    return p.read_text(encoding="utf-8")


def _modify(p: Path, suffix: str = "\n# analyst's custom edit\n") -> None:
    p.write_text(_read(p) + suffix, encoding="utf-8")


# ── Case 1: first copy ───────────────────────────────────────────────────────

class TestFirstCopy:
    def test_writes_all_helpers(self, tmp_path):
        outcomes = copy_helpers(tmp_path)
        for h in HELPERS:
            assert (tmp_path / h).is_file()
            assert outcomes[h] == "written"

    def test_creates_state_file_with_hashes(self, tmp_path):
        copy_helpers(tmp_path)
        s = state.read_state(tmp_path)
        assert "helper_versions" in s
        for h in HELPERS:
            assert h in s["helper_versions"]
            assert len(s["helper_versions"][h]) == 64   # sha256 hex

    def test_drops_gitignore_with_backups_entry(self, tmp_path):
        copy_helpers(tmp_path)
        gi = tmp_path / GITIGNORE_FILENAME
        assert gi.is_file()
        assert f"{BACKUPS_DIRNAME}/" in _read(gi)


# ── Case 2: re-copy on unmodified files ──────────────────────────────────────

class TestRecopyUnmodified:
    def test_silent_refresh(self, tmp_path):
        copy_helpers(tmp_path)
        outcomes = copy_helpers(tmp_path)
        for h in HELPERS:
            assert outcomes[h] == "skipped_unchanged"

    def test_no_backup_dir_created(self, tmp_path):
        copy_helpers(tmp_path)
        copy_helpers(tmp_path)
        assert not (tmp_path / BACKUPS_DIRNAME).exists()


# ── Case 3: analyst modification preserved ──────────────────────────────────

class TestAnalystModified:
    def test_preserves_analyst_edits(self, tmp_path):
        copy_helpers(tmp_path)
        target = tmp_path / "eda_helpers.py"
        _modify(target)
        modified_content = _read(target)

        outcomes = copy_helpers(tmp_path)

        assert outcomes["eda_helpers.py"] == "skipped_analyst_modified"
        assert _read(target) == modified_content   # untouched
        assert not (tmp_path / BACKUPS_DIRNAME).exists()

    def test_other_helpers_still_refresh(self, tmp_path):
        copy_helpers(tmp_path)
        _modify(tmp_path / "eda_helpers.py")

        outcomes = copy_helpers(tmp_path)

        assert outcomes["eda_helpers.py"] == "skipped_analyst_modified"
        # The other helpers, still unchanged, get the silent refresh.
        assert outcomes["eda_profile.py"] == "skipped_unchanged"


# ── Case 4: force=True backs up then overwrites ──────────────────────────────

class TestForceOverwrite:
    def test_creates_backup(self, tmp_path):
        copy_helpers(tmp_path)
        target = tmp_path / "eda_helpers.py"
        _modify(target, "\n# analyst signature\n")

        outcomes = copy_helpers(tmp_path, force=True)

        assert outcomes["eda_helpers.py"] == "overwritten_with_backup"
        backups = list((tmp_path / BACKUPS_DIRNAME).glob("eda_helpers_backup_*.py"))
        assert len(backups) == 1
        assert "# analyst signature" in _read(backups[0])

    def test_target_replaced_with_package_version(self, tmp_path):
        copy_helpers(tmp_path)
        target = tmp_path / "eda_helpers.py"
        _modify(target, "\n# analyst signature\n")

        copy_helpers(tmp_path, force=True)

        assert "# analyst signature" not in _read(target)

    def test_state_updated_after_force(self, tmp_path):
        copy_helpers(tmp_path)
        target = tmp_path / "eda_helpers.py"
        original_hash = state.read_state(tmp_path)["helper_versions"]["eda_helpers.py"]
        _modify(target)
        copy_helpers(tmp_path, force=True)

        # Hash should match the package source again (analyst edits gone).
        new_hash = state.read_state(tmp_path)["helper_versions"]["eda_helpers.py"]
        assert new_hash == original_hash
        assert new_hash == state.file_hash(target)


# ── Edge case: state file missing but helpers present ────────────────────────

class TestMissingState:
    def test_treats_files_as_analyst_modified(self, tmp_path):
        # Helpers present but no state file → can't prove they're untouched.
        # Safe default: treat as analyst-modified, leave alone.
        from shutil import copy2

        from dbprofile.notebook import templates as templates_pkg
        src_dir = Path(templates_pkg.__file__).parent
        for h in HELPERS:
            copy2(src_dir / h, tmp_path / h)

        outcomes = copy_helpers(tmp_path)
        for h in HELPERS:
            assert outcomes[h] == "skipped_analyst_modified"
