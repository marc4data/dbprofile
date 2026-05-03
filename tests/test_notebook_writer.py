"""Tests for dbprofile.notebook.notebook_writer — the four write branches."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import nbformat
import pytest

from dbprofile.notebook.backup import BACKUPS_DIRNAME
from dbprofile.notebook.cells import code_cell, md_cell
from dbprofile.notebook.notebook_writer import write_notebook

# ── Fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture
def fixed_run_at() -> datetime:
    """A deterministic run_at so filenames are predictable across tests."""
    return datetime(2026, 4, 30, 12, 0, 0)


def _new_nb(*sources: str) -> nbformat.NotebookNode:
    """Build a notebook with one markdown cell per source."""
    nb = nbformat.v4.new_notebook()
    nb.cells = [md_cell(s) for s in sources] or [code_cell("pass")]
    nb.metadata["kernelspec"] = {
        "display_name": "Python 3", "language": "python", "name": "python3",
    }
    return nb


def _read(path: Path):
    return nbformat.read(path, as_version=4)


# ── Branch 1: written_new ────────────────────────────────────────────────────

class TestWrittenNew:
    def test_writes_canonical_when_absent(self, tmp_path, fixed_run_at):
        nb = _new_nb("Hello")
        path, outcome = write_notebook(nb, tmp_path, "fct_trips", run_at=fixed_run_at)

        assert outcome == "written_new"
        assert path.name == "eda_fct_trips_20260430.ipynb"
        assert path.exists()

    def test_filename_lowercased_even_when_table_is_uppercase(self, tmp_path, fixed_run_at):
        """Snowflake returns table names UPPERCASE. The filename should
        still be lowercase for usability — the table identity inside
        the notebook keeps the original case."""
        nb = _new_nb("Hello")
        path, outcome = write_notebook(nb, tmp_path, "FCT_TRIPS", run_at=fixed_run_at)

        assert outcome == "written_new"
        assert path.name == "eda_fct_trips_20260430.ipynb"
        # The notebook's metadata still records the source table case
        import nbformat as _nb
        meta = _nb.read(path, as_version=4)["metadata"]["dbprofile"]
        assert meta["table"] == "FCT_TRIPS"

    def test_embeds_metadata_with_source_hash(self, tmp_path, fixed_run_at):
        nb = _new_nb("Hello")
        write_notebook(nb, tmp_path, "fct_trips", run_at=fixed_run_at)

        written = _read(tmp_path / "eda_fct_trips_20260430.ipynb")
        meta = written["metadata"]["dbprofile"]
        assert meta["table"] == "fct_trips"
        assert "source_hash" in meta
        assert len(meta["source_hash"]) == 64   # sha256 hex


# ── Branch 2: overwritten_unchanged ──────────────────────────────────────────

class TestOverwrittenUnchanged:
    def test_silent_overwrite_when_hash_matches(self, tmp_path, fixed_run_at):
        write_notebook(_new_nb("v1"), tmp_path, "tbl", run_at=fixed_run_at)
        # Second run with identical content — hash matches → silent refresh.
        path, outcome = write_notebook(_new_nb("v1"), tmp_path, "tbl", run_at=fixed_run_at)

        assert outcome == "overwritten_unchanged"
        assert not (tmp_path / BACKUPS_DIRNAME).exists()


# ── Branch 3: dated_baseline_after_edit ──────────────────────────────────────

class TestDatedBaseline:
    def test_writes_to_hhmm_suffixed_file(self, tmp_path, fixed_run_at):
        # First write: canonical file, hash recorded.
        write_notebook(_new_nb("v1"), tmp_path, "tbl", run_at=fixed_run_at)

        # Simulate analyst editing a cell — append to source.
        canonical = tmp_path / "eda_tbl_20260430.ipynb"
        existing = _read(canonical)
        existing.cells.append(md_cell("# analyst added this"))
        nbformat.write(existing, canonical)

        # Second run (same minute) — should NOT overwrite, should write a new
        # baseline at <name>_HHMM<ext>.
        path, outcome = write_notebook(_new_nb("v1"), tmp_path, "tbl", run_at=fixed_run_at)

        assert outcome == "dated_baseline_after_edit"
        assert path.name == "eda_tbl_20260430_1200.ipynb"
        assert path.exists()
        # Original analyst version is untouched.
        original = _read(canonical)
        assert any("# analyst added this" in str(c.source) for c in original.cells)
        # No backup was made — analyst's file is preserved in place.
        assert not (tmp_path / BACKUPS_DIRNAME).exists()

    def test_no_metadata_treated_as_modified(self, tmp_path, fixed_run_at):
        # Write a notebook directly (bypassing write_notebook so no metadata).
        canonical = tmp_path / "eda_tbl_20260430.ipynb"
        nbformat.write(_new_nb("hand-written"), canonical)

        path, outcome = write_notebook(_new_nb("v1"), tmp_path, "tbl", run_at=fixed_run_at)
        assert outcome == "dated_baseline_after_edit"
        assert path != canonical


# ── Branch 4: force_overwrite_with_backup ────────────────────────────────────

class TestForceOverwrite:
    def test_force_creates_backup_then_overwrites(self, tmp_path, fixed_run_at):
        write_notebook(_new_nb("v1"), tmp_path, "tbl", run_at=fixed_run_at)

        # Analyst edits canonical
        canonical = tmp_path / "eda_tbl_20260430.ipynb"
        existing = _read(canonical)
        existing.cells.append(md_cell("# analyst signature"))
        nbformat.write(existing, canonical)

        path, outcome = write_notebook(
            _new_nb("v2"), tmp_path, "tbl", force=True, run_at=fixed_run_at,
        )

        assert outcome == "force_overwrite_with_backup"
        assert path == canonical
        # Backup contains the analyst's edit.
        backups = list((tmp_path / BACKUPS_DIRNAME).glob("eda_tbl_20260430_backup_*.ipynb"))
        assert len(backups) == 1
        backed = _read(backups[0])
        assert any("# analyst signature" in str(c.source) for c in backed.cells)
        # Canonical now has v2 (no analyst signature).
        new = _read(canonical)
        assert not any("# analyst signature" in str(c.source) for c in new.cells)

    def test_force_when_canonical_absent_just_writes(self, tmp_path, fixed_run_at):
        path, outcome = write_notebook(
            _new_nb("v1"), tmp_path, "tbl", force=True, run_at=fixed_run_at,
        )
        # Force is harmless when there's nothing to overwrite.
        assert outcome == "written_new"
        assert path.exists()


# ── Hash stability ───────────────────────────────────────────────────────────

class TestHashStability:
    def test_running_notebook_doesnt_trigger_modified(self, tmp_path, fixed_run_at):
        """Source hash should be unchanged after adding outputs/exec counts."""
        write_notebook(_new_nb("only source matters"), tmp_path, "tbl", run_at=fixed_run_at)

        canonical = tmp_path / "eda_tbl_20260430.ipynb"
        existing = _read(canonical)
        # Simulate execution: add output + execution_count to a code cell.
        for cell in existing.cells:
            if cell.cell_type == "code":
                cell["outputs"] = [{"output_type": "stream", "text": "hi", "name": "stdout"}]
                cell["execution_count"] = 1
        nbformat.write(existing, canonical)

        path, outcome = write_notebook(_new_nb("only source matters"), tmp_path, "tbl",
                                        run_at=fixed_run_at)
        # Hash should still match — only outputs changed, not source.
        assert outcome == "overwritten_unchanged"
        assert path == canonical
