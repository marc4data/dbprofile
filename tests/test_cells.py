"""Tests for dbprofile.notebook.cells — factory functions."""

from __future__ import annotations

import pytest

from dbprofile.notebook.cells import (
    callout_cell,
    code_cell,
    md_cell,
    section_header,
)


class TestMdCell:
    def test_returns_markdown_cell(self):
        cell = md_cell("Hello")
        assert cell["cell_type"] == "markdown"
        assert cell["source"] == "Hello"


class TestCodeCell:
    def test_returns_code_cell(self):
        cell = code_cell("print('hi')")
        assert cell["cell_type"] == "code"
        assert cell["source"] == "print('hi')"

    def test_has_empty_outputs(self):
        cell = code_cell("x = 1")
        assert cell["outputs"] == []


class TestCalloutCell:
    @pytest.mark.parametrize(
        "severity,expected_tag",
        [
            ("critical", "[!CAUTION]"),
            ("warn",     "[!WARNING]"),
            ("info",     "[!NOTE]"),
            ("ok",       "[!NOTE]"),
            ("unknown",  "[!NOTE]"),  # fallback
        ],
    )
    def test_severity_to_tag(self, severity, expected_tag):
        cell = callout_cell(severity, "msg body")
        assert cell["cell_type"] == "markdown"
        assert expected_tag in cell["source"]

    def test_body_lines_prefixed_with_quote(self):
        cell = callout_cell("warn", "line one\nline two")
        # Each body line must start with `> ` so it stays inside the alert
        assert "> line one" in cell["source"]
        assert "> line two" in cell["source"]


class TestSectionHeader:
    @pytest.mark.parametrize("level,prefix", [(1, "#"), (2, "##"), (3, "###"), (6, "######")])
    def test_correct_hash_count(self, level, prefix):
        cell = section_header(level, "Title")
        assert cell["source"] == f"{prefix} Title"

    @pytest.mark.parametrize("bad_level", [0, 7, -1])
    def test_rejects_invalid_level(self, bad_level):
        with pytest.raises(ValueError):
            section_header(bad_level, "Title")
