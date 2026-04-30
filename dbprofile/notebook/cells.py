"""Cell factory functions for building Jupyter notebooks.

Section builders (s00_header.py, s01_setup.py, ...) call these to produce
nbformat cell dicts, then return a list that the generator concatenates
into the final notebook.

Why a thin wrapper around nbformat?

  * Centralises the callout syntax that nb2report renders.
  * Keeps section builders free of nbformat imports — they read like
    declarative cell sequences.
  * Makes it trivial to test: each factory returns a dict that can be
    inspected without running a notebook kernel.
"""

from __future__ import annotations

import nbformat

# Mapping from CheckResult severity → GitHub-style alert tag that nb2report
# renders as a styled callout box. "ok" and any unrecognised value collapse
# to a neutral note.
_CALLOUT_TAGS = {
    "critical": "[!CAUTION]",
    "warn":     "[!WARNING]",
    "info":     "[!NOTE]",
    "ok":       "[!NOTE]",
}


def md_cell(text: str) -> nbformat.NotebookNode:
    """Markdown cell."""
    return nbformat.v4.new_markdown_cell(text)


def code_cell(source: str) -> nbformat.NotebookNode:
    """Code cell with no outputs."""
    return nbformat.v4.new_code_cell(source)


def callout_cell(severity: str, message: str) -> nbformat.NotebookNode:
    """Markdown callout for nb2report rendering.

    Severity → tag mapping
        critical → [!CAUTION]
        warn     → [!WARNING]
        info/ok  → [!NOTE]
    """
    tag = _CALLOUT_TAGS.get(severity, "[!NOTE]")
    # Each line of the body must be prefixed with `> ` so the entire block
    # stays inside the GitHub-flavored alert.
    body = "\n".join(f"> {line}" for line in message.splitlines() or [""])
    return md_cell(f"> {tag}\n{body}")


def section_header(level: int, title: str) -> nbformat.NotebookNode:
    """Markdown heading at the given level (1=#, 2=##, 3=###, …)."""
    if not 1 <= level <= 6:
        raise ValueError(f"Heading level must be 1..6, got {level}")
    return md_cell(f"{'#' * level} {title}")
