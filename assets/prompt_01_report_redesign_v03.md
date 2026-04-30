# dbprofile Report Redesign Prompt — Iteration 03 (Phased)

This iteration applies refinements **on top of the v01 redesign** (see
`prompt_01_report_redesign.md`). It is based on reviewing the generated report and
identifying specific bugs, missing context, and sections that still need work.

The work is split into **five phases**, ordered so that each phase can be shipped and
reviewed independently. Early phases are fast bug-fix / cleanup passes; later phases
introduce new visual elements and interactive features. Complete each phase end-to-end
before moving to the next.

Before starting, re-read the current report generation code and the most recent HTML
output so you understand what has and hasn't been implemented from v01.

---

## v01 Requirements Still in Effect

Do not revert any of these — they remain required:

- Per-table summary card with metadata, issue scoreboard, and plain-English interpretation (v01 §D)
- Data-driven detection for key/attribute columns and for binary columns (v01 §E)
- LLM-generated interpretations with `use_llm_interpretation` feature flag and graceful fallback (v01 §I)
- Left nav: table-level status circles + expand/collapse sub-rows in canonical order (v01 §A)
- Navigation & linking: sticky TOC, back-to links, anchor jumps (v01 §G)

---

# PHASE 1 — Bug Fixes & Cleanup

**Goal:** eliminate obvious errors, remove low-value sections, and get the baseline
cleaner before any new work. Should be the fastest phase.

## 1.1. Remove unneeded sections

- **Schema Audit** — remove from left nav and remove the detail section entirely
- **Row Count** — remove from left nav and remove the detail section entirely
- Essential info from these (total row count, column count) moves into the per-table
  summary card (v01 §D) if not already there

Update the canonical check order throughout (nav, heatmap columns, detail section
sequence) to:

1. Null Density
2. Uniqueness
3. Numeric Distribution
4. Frequency Distribution
5. Temporal Consistency
6. Format Validation

## 1.2. Bug fixes

- **Anchor positioning**: clicking a link to a table section must land with the **table
  name header** as the topmost visible element — currently the header scrolls past and
  the chart lands at the top. Fix with `scroll-margin-top` sized to the sticky header /
  nav height, or equivalent.
- **Null Density blank space**: remove the large empty area at the top of the Null
  Density section.
- **Null Density calculation**: when a column has `NULL COUNT = 0`, the `NULL %` cell
  currently renders an error/blank value. It must render `0.00%`. Example from the
  current output: `DROPOFF_DATETIME` has `NULL COUNT = 0` but `NULL %` is shown
  incorrectly. Verify the calculation/render path.
- **Numeric Distribution percentile correctness**: MIN / P25 / P50 / P75 / P95 / P99 /
  MAX values appear broken across multiple columns — they should all be valid observed
  data points from the column's actual value distribution. No interpolation artifacts,
  no nulls included, no sentinel values polluting the percentile set. Verify the
  computation path and fix.

## 1.3. Minor polish

- **Uniqueness subgroup differentiation**: add a subtle background color to the
  Identifier subgroup header row (pale blue tint) and the Attribute subgroup header row
  (pale gray tint) to make the grouping visually obvious. Keep the group headers
  text-forward; the color is only for differentiation.
- **Sentinel Count explanation**: add an inline note or tooltip on the `Sentinel Count`
  column header in Null Density explaining what it captures (e.g., empty strings, `-1`
  placeholders, `'N/A'` / `'UNKNOWN'` string sentinels, or other nullish values that
  aren't technically `NULL`). Document which sentinels the profiler detects.

## Phase 1 quality bar

Report output has no empty sections, no obvious calculation errors, anchor jumps land
correctly, and Schema Audit / Row Count are gone. This is the "nothing looks broken"
baseline.

---

# PHASE 2 — Profiling Context & Executive Summary

**Goal:** make it immediately clear **what is being profiled**, and turn the Executive
Summary from a wall of rows into a compact scoreboard.

## 2.1. Report header (new)

The report currently gives no clue what data source is being profiled. A reader opening
the HTML cannot answer "what am I actually looking at?" — the data could be Snowflake,
BigQuery, Postgres, or something else, and could be sourced from any of several
schemas. This is a blocker.

Add a **header panel** at the very top of the report (above the Executive Summary)
containing:

- **Connector / platform** (Snowflake, BigQuery, Postgres, etc.)
- **Account** (if applicable to the platform)
- **Database**
- **Schema**
- **Role** used for the profiling session
- **Data time coverage**: the min and max date observed in the profiled data (not the
  profiling run timestamp) — this directly answers the "why am I seeing 2022 rows?"
  question by making the actual data coverage explicit
- **Profiling run timestamp** and **sample %**

Layout: a compact metadata banner / card. Visually distinct from the Executive Summary
below it. This header is part of the page chrome and remains visible (or at minimum,
sits above the first anchor target) regardless of which section a link jumps to.

## 2.2. Executive Summary trim-down

The current Executive Summary is too dense — reduce it to:

1. **Top Actions**: 3–5 prioritized plain-English action items with links to detail.
   Keep this — it's the most valuable part of the summary. Continues to use the
   LLM-generated interpretation path from v01 §I (respect the feature flag).
2. **Issue scoreboard by check type**: a compact grid showing severity counts per
   check, e.g.:
   ```
   Null Density          12 critical   3 warn
   Uniqueness             1 critical   0 warn   (+31 expected attribute duplicates)
   Numeric Distribution   8 critical   4 warn
   Frequency Distribution …
   Temporal Consistency   0 critical   2 warn
   Format Validation      …
   ```
3. **Remove the flat issue table entirely** from the Executive Summary. Detailed
   issue browsing belongs in the per-table sections below. The Executive Summary is a
   scoreboard + action list, not an audit log.

## Phase 2 quality bar

A reader opening the report can answer "what am I looking at, when was it profiled, and
what's the health at a glance" without scrolling — all from the header + trimmed
Executive Summary.

---

# PHASE 3 — Standardization & Heatmap v2

**Goal:** make every test-level section share a consistent structure and interaction
pattern, and upgrade the heatmap with richer column metadata.

## 3.1. Standard leading column structure

Every test-level detail table must share this leading column set:

`# | Column Name | Type | Nullable | Shape | [test-specific columns…]`

Where `#` / `Column Name` / `Type` / `Nullable` / `Shape` are rendered identically in
every section — a reader moving between sections doesn't have to re-learn the layout.

The test-specific columns append to the right — e.g., Null Density adds `Null %`,
`Null Count`, `Sentinel Count`, `Severity`; Uniqueness adds `Distinct %`, `Duplicate %`,
`Cardinality`, `Severity`; etc.

## 3.2. Type, Nullable, and column ordering in the heatmap

- **Reorder**: `#` becomes column 1, `Column Name` becomes column 2 (currently it's
  first), followed by `Type`, `Nullable`, `Shape`, then the check columns in canonical
  order.
- **Full data type**: show the complete type string from the source schema
  (`VARCHAR(255)`, `NUMBER(10,2)`, `TIMESTAMP_NTZ`, `FLOAT64`, etc.), not abbreviations
  like `STR` or `INT`.
- **Nullable column (new)**:
  - `FALSE` renders with strong emphasis — bold weight, full-opacity dark text
  - `TRUE` renders muted — approximately 25% opacity of the normal text color
  - Rationale: `TRUE` (nullable) is the default and rarely interesting; `FALSE` (NOT
    NULL constraint) is the informative case and should stand out.

## 3.3. Heatmap legend (required)

Add a visible, always-present legend near the heatmap explaining:

- **Color bands**: Critical / Warn / OK / N/A (with the color swatches)
- **Shape column**: what the mini-histogram represents for numeric columns vs. the
  cardinality bar for non-numeric columns
- **Check column abbreviations**: spell out what `ND`, `UN`, `FR`, etc. mean. Currently
  `FR` / the blue color is unexplained.
- **Cell angle indicator** (introduced in Phase 4, but reserve the legend slot now)

## 3.4. Consistent test-level section behaviors

Every test-level section must:

- Start with a `↑ Back to [TABLE]` hyperlink that jumps to the table's summary card /
  heatmap at the top of the table section
- Support **sortable column headers**: clicking any column header toggles ascending /
  descending sort. Show an active-sort caret in the header.

## Phase 3 quality bar

Opening any test-level section, the first five columns look and behave identically, the
back-link is present, and column headers are clickable for sorting. The heatmap has a
legend so every visual encoding is explained.

---

# PHASE 4 — Section Rebuilds & Visual Enhancements

**Goal:** rebuild the weakest sections (Frequency Distribution, Temporal Consistency,
Numeric Distribution expand) and introduce the angled value indicator on the heatmap.

## 4.1. Angled value indicator on heatmap cells

Overlay a **pie-chart-style indicator** on each heatmap cell:

- Think of it as a pie chart whose filled slice is **muted / nearly invisible** —
  only the **radial line at the edge of the slice** is drawn visibly. Like a clock
  hand with no clock face.
- The angle of that line encodes the check's primary metric value:
  - Scale the metric `0 → 1` and map to angle `0° → 360°`
  - `0°` points straight up (12 o'clock); rotates clockwise
  - Example: `outlier_pct = 0.12` → line at ~43° (slightly past 12 o'clock toward 3);
    `null_pct = 1.00` → full rotation back to 12 o'clock
- Use a thin dark stroke (1px–1.5px) centered in the cell. Length should be ~40–50% of
  the cell dimension.
- Combined with the existing color band, this gives a two-channel reading:
  color = severity band, angle = continuous metric value.
- Update the legend (from Phase 3) with a small example / clock diagram so readers can
  decode it.

## 4.2. Numeric Distribution — composite expand panel

The per-row expand currently shows empty space. Replace with a **composite chart** per
column:

1. **Histogram** (top): binned value distribution on x-axis, count on y-axis. Overlay a
   cumulative running-total line showing count **and %** at each bin. Choose a sensible
   default bin count (20 or Sturges / Freedman–Diaconis).
2. **Box-whisker plot** (bottom, sharing the x-axis with the histogram above): min, Q1,
   median, Q3, max, plus outlier points plotted individually.
3. **Critical constraint**: the boxplot's x-axis must share the **exact same scale**
   as the histogram above it, so percentile positions line up vertically between the
   two plots. The simplest guarantee is to render them as a single composite figure.

## 4.3. Frequency Distribution — full rebuild

The current implementation is effectively empty. Rebuild following v01 §F rules, with
column-type dispatch determined from **actual data values** (not column names):

- **Binary columns** — data type is boolean, OR exactly 2 distinct values within
  `{0, 1, True, False, 'Y', 'N'}` → compact pill display: `X% = 1 | Y% = 0`. No chart.
- **Low-cardinality categorical** — ≤ 20 distinct values, non-binary → horizontal bar
  chart with value labels and percentages.
- **High-cardinality** — > 20 distinct values → top 10 values with a
  "Showing top 10 of N distinct values" note.
- **Date / timestamp columns** → skip entirely (covered by Temporal Consistency).

Validate across multiple column types — not just one — before calling this done.

## 4.4. Temporal Consistency — convert to table

Replace the card-per-column layout with a single table using the standard leading
columns:

`# | Column Name | Type | Nullable | Shape | From | To | Gap Days | Anomaly Days | Severity`

Additional requirements:

- `Gap Days` and `Anomaly Days` cells each include an **inline horizontal bar** whose
  length encodes the value magnitude. Scale the bar relative to the max value across
  all rows in that column (or a fixed reasonable max — pick one approach and be
  consistent).
- Bar color follows severity.
- Keep the from / to date text alongside the bars.

## Phase 4 quality bar

Every check section has a proper visualization. No empty expand panels, no unexplained
visual encodings. The angled indicators in the heatmap let a reader eyeball metric
magnitudes across a whole table at once.

---

# PHASE 5 — Exception SQL Access

**Goal:** give a data engineer one-click access to the SQL that returns exception
records for any flagged violation. Scoped last because it's an interactive feature that
depends on the other sections being stable.

This should be **simple**: every check rule already has a distinct, clear SQL test.
The exception-records SQL is just that test inverted (return the rows that **fail**
the check) with a `LIMIT` clause appended.

## 5.1. Violation affordance

On every row flagged as Critical or Warning across all test sections, append a small
icon (e.g., `{}` or `⋯`) at the end of the row. Clicking it opens a lightweight
popover or modal showing:

- The **exception SQL** for that specific check + column, ready to copy
- A **limit selector** (10 / 100 / 1000) — radio buttons or a small segmented control
  that re-renders the SQL with the chosen `LIMIT`
- A **copy-to-clipboard** button for the SQL text

## 5.2. SQL generation pattern

For each check type, define the exception-records SQL template. Examples:

- **Null Density**: `SELECT * FROM {table} WHERE {column} IS NULL LIMIT {n}`
- **Uniqueness** (find duplicate values):
  `SELECT {column}, COUNT(*) AS n FROM {table} GROUP BY {column} HAVING COUNT(*) > 1 ORDER BY n DESC LIMIT {n}`
- **Numeric Distribution** (outliers): `SELECT * FROM {table} WHERE {column} > {p99_upper_bound} OR {column} < {p01_lower_bound} LIMIT {n}`
- **Frequency Distribution** (sentinel / dominant value overreach): depends on the
  rule that fired — reuse the rule's own SQL form
- **Temporal Consistency** (anomaly days): return the anomaly dates themselves
- **Format Validation**: `SELECT * FROM {table} WHERE NOT {format_check_expression} LIMIT {n}`

Keep the SQL readable — qualified table name, explicit column list where it matters,
clear `WHERE` clause. Don't overbuild; a simple parameterized template per check type
is enough.

## Phase 5 quality bar

Clicking the icon on any violation shows a copyable SQL that, run against the source,
returns example exception rows. Switching the limit updates the SQL instantly. No
other section regressions.

---

# Cross-Phase Quality Bar

By the end of Phase 5:

- Header answers "what am I looking at, when, from where"
- Executive Summary is a compact scoreboard, not a wall of rows
- Every test section shares a consistent leading column layout and back-link
- Heatmap carries three layers of information per cell: color (severity), angle (metric
  magnitude), legend-explained abbreviations
- No empty sections, no calculation bugs, no unexplained visual encodings
- A data engineer can one-click from any violation to runnable SQL

**Do not change the underlying profiling logic or data collection — only the HTML
report generation, presentation layer, and the exception-SQL template generation.**
