# dbprofile Report Redesign Prompt — Iteration 01

Review the existing report generation code for `analytics_profile.html` in this project.
Understand the current architecture completely before making any changes. Then implement
the following improvements. The goal is to significantly upgrade information architecture,
scannability, and interpretive quality while keeping the report self-contained.

---

## A. Left Navigation Panel

**Current problem:** Each table expands into 6–8 sub-rows of text (one per check), making
the nav very tall and hard to scan across tables.

**Required changes:**

1. **Table header row**: Each table entry in the nav shows the table name on the left, and
   a row of small colored status circles on the right, one per check in canonical order
   (see check order below). Circles are color-coded: 🔴 red = critical, 🟠 orange =
   warning, 🟢 green = ok, ⚫ gray = not applicable/not run. Make circles clearly visible
   — at least 12–14px diameter.

2. **Sub-rows remain** (expand/collapse behavior preserved), but they must:
   - Follow the canonical check order (not alphabetical)
   - Be consistent across all tables — every table shows exactly the same set of sub-rows,
     regardless of which checks were run (gray/disabled for checks that don't apply)
   - Each sub-row is a clickable anchor link to that section within the table

3. **Visual polish**: Use light gridlines between nav rows. Remove rounded cell corners.
   Keep the nav panel compact vertically — reduce padding/margins so more tables are
   visible without scrolling.

---

## B. Canonical Check Order

All ordering of checks throughout the report — nav sub-rows, heatmap columns, detail
section sequence — must follow this order:

| # | Check               | Scope                |
|---|---------------------|----------------------|
| 1 | Schema Audit        | Table                |
| 2 | Row Count           | Table                |
| 3 | Null Density        | Column               |
| 4 | Uniqueness          | Column               |
| 5 | Numeric Distribution| Numeric cols         |
| 6 | Frequency Distribution | Low-cardinality cols |
| 7 | Temporal Consistency| Date/timestamp cols  |
| 8 | Format Validation   | String cols          |

---

## C. Column × Check Heatmap (per table)

**Current problem:** Heatmap only shows column name and check severity cells. Columns are
listed alphabetically. No metadata visible at a glance.

**Required changes:**

1. **Column ordering**: Show columns in their original schema/ordinal position, not
   alphabetically. Use the column sequence number from the schema.

2. **Add metadata columns to the LEFT of the column name:**
   - **#** — column sequence number
   - **Data type** — short type label (e.g., `INT`, `VARCHAR`, `DATE`, `BOOL`, `FLOAT`)
   - **Mini-histogram** — a 10-bin sparkline inline in the row:
     - For numeric columns: 10 equal-width bins of the value distribution, rendered as
       tiny inline bars
     - For non-numeric columns (string, boolean, etc.): a single cardinality bar — a
       filled bar proportional to `distinct_count / row_count` indicating how unique
       the column is
   - **Column name** — with a hover tooltip showing data type, null %, distinct count,
     and a one-line description if available from schema metadata

3. **Check columns** (right side of heatmap): Follow canonical order (#1–8). Each cell
   shows severity color. Checks not run for a column type show as gray/empty.

4. **Visual polish**: Light gridlines between all rows and columns. Remove rounded corners
   on cells. The heatmap should be compact — tight row height.

---

## D. Per-Table Summary Card (new section, appears before heatmap)

Add a new summary card at the top of each table section containing:

- **Metadata row**: table name, row count, column count, profiling timestamp, sample %
- **Issue scoreboard**: `X critical · Y warnings · Z ok` (color-coded)
- **Plain-English interpretation**: 2–3 sentences summarizing data quality state.
  Highlight the most significant findings. Explicitly note when issue types are expected
  given the data context (e.g., attribute columns having high duplicate % in a dimension
  table, or columns that are all-null because the source join didn't populate them).
  Generate this interpretation from the actual profiling results.
- **Quick-jump links**: one link per check section below (e.g., "→ Null Density",
  "→ Uniqueness") — only show links for checks that were run

---

## E. Detail Sections — Redesign

Sections must appear in canonical order (#1–8). Each section header should include a
one-line finding summary (e.g., *"9 of 32 columns critical — all weather columns are
100% null"* or *"Uniqueness: 1 confirmed key column; 31 attribute columns with expected
duplicates"*).

### Section 3 — Null Density

- Lead with a compact horizontal bar chart: one bar per column, ordered by null %
  descending, color-coded by severity
- The full detail table (column / null % / null count / sentinel count / severity) follows,
  but columns with OK severity should be collapsed by default into a "Show N OK columns"
  toggle. Only flagged columns expand automatically.

### Section 4 — Uniqueness

- Split the uniqueness table into two groups: **Identifier columns** and **Attribute columns**
- Classification is **data-driven, not name-based**: if `distinct_count / row_count ≥ 0.95`,
  classify as Identifier. Otherwise, classify as Attribute.
- For Attribute columns, suppress or visually de-emphasize the Critical severity with a
  note: *"High duplicate % is expected for attribute/descriptor columns — this is
  informational, not an action item."*
- Add a **Cardinality** column showing the distinct value count (e.g., "2 distinct",
  "265 distinct") for immediate context

### Section 5 — Numeric Distribution

- Replace individual per-column cards with a **single consolidated comparison table** as
  the default view
- Table columns: `#` | `Column Name` | `Severity` | `Min` | `P25` | `P50` | `P75` |
  `P95` | `Max` | `Outlier %`
- `Outlier %` cell is color-coded by severity
- Sort order: Critical first, then Warning, then OK
- Each row is expandable (click to expand) to reveal the full percentile bar chart for
  that column

### Section 6 — Frequency Distribution

Apply column-type-appropriate treatment, determined from **actual data values** (not
column names):

- **Binary columns** — detected when: data type is boolean, OR the column has exactly
  2 distinct values and both are within `{0, 1, True, False, 'Y', 'N'}` → Show a compact
  pill display: `X% = 1 | Y% = 0`. No bar chart.
- **Low-cardinality categorical** (≤ 20 distinct values, non-binary) → Show a horizontal
  bar chart with value labels and percentages. This is the appropriate treatment.
- **High-cardinality** (> 20 distinct values) → Show only the **top 10 values by
  frequency** with a note: *"Showing top 10 of N distinct values."*
- **Date / timestamp columns** → Skip entirely (covered by Temporal Consistency)

### Section 7 — Temporal Consistency

- Keep the existing FROM / TO / GAP DAYS / ANOMALY DAYS card layout per date column
- Add a one-sentence summary at the top of the section, e.g.: *"DATE has complete
  coverage 2019-01-29 → 2025-12-13 with no gaps or anomalies. WEEK_START_DATE has 13
  anomaly days worth investigating."*

---

## F. Executive Summary Redesign

Replace the current flat issue table with a structured summary:

1. **Top Actions** (appears first): A prioritized list of the 3–5 most important findings
   across all tables, in plain English, with links to the relevant section. Focus on
   genuinely actionable issues, not expected behavior.

2. **Issue table** (grouped, not flat):
   - Group by check type (Null Density, Uniqueness, Numeric Distribution, etc.)
   - Within Null Density: cluster columns sharing a root cause into a single grouped row
     (e.g., *"9 weather columns — 100% null"* instead of 9 separate rows)
   - Within Uniqueness: tag attribute-column duplicate flags as *"Expected — attribute
     column"* and visually de-emphasize them
   - All rows link directly to the relevant table + check section

---

## G. Navigation & Linking

- Sticky left-side table of contents with anchor links to each table and to each check
  subsection within a table
- The Executive Summary issues list should have clickable rows that jump to the relevant
  table + check section
- Each table section should have a "↑ Back to Executive Summary" link at the top
- Each check subsection should have a "↑ Back to [TABLE]" link

---

## H. Quality Bar

The redesigned report should be significantly more scannable than the current version:

- A user should understand a table's overall data quality state in under 30 seconds from
  the per-table summary card
- The left nav should show health status for all tables without scrolling (or with minimal
  scroll)
- The frequency distribution section should not render full charts for binary or boolean
  columns
- Numeric distribution should not require scrolling through N full-page cards to see all
  columns

**Do not change the underlying profiling logic or data collection — only the HTML report
generation and presentation layer.**

---

## I. LLM-Generated Interpretations (Sections D and F.1)

The plain-English summary in **Section D** (per-table summary card) and the **Top Actions**
block in **Section F.1** (Executive Summary) must be generated via an Anthropic API call
at report generation time — not with hard-coded rules. These require the kind of
cross-metric synthesis and contextual judgment that rule-based logic cannot reliably
produce.

### What to pass to the API

For each per-table summary (Section D), construct a JSON payload containing:
- Table name, row count, column count, sample %, profiling timestamp
- All issues grouped by check type and severity, with column names and metric values
- For null density: which columns are null and by how much
- For uniqueness: cardinality ratios per column (to support identifier vs. attribute context)
- For numeric distribution: outlier % per column
- For temporal consistency: date range, gap count, anomaly count
- A system prompt explaining what each check type means and what each severity level
  indicates, so the model has sufficient context to interpret the data correctly

For the Top Actions block (Section F.1), make a single additional API call after all
per-table profiles are complete, passing a consolidated summary of findings across all
tables. This call should produce 3–5 prioritized plain-English action items that focus
on genuinely actionable issues, not expected behavior.

### Architecture

- API calls happen inside the Python report generation script, **not** in the browser
- If multiple tables are being profiled, make the per-table calls in parallel to minimize
  latency
- The returned interpretation text is embedded as a **static string** in the HTML output
  — the final HTML file has no runtime API dependency and remains fully self-contained
- Use `anthropic.Anthropic()` with `claude-sonnet-4-5` or latest available model; keep
  `max_tokens` modest (300–500 per summary) since these are short interpretive passages

### Feature flag

Add a boolean configuration flag — `use_llm_interpretation` — that can be set at
report generation time to enable or disable the API calls independently of whether an
API key is present. This flag exists specifically to support side-by-side evaluation of
LLM-generated vs. rule-based interpretation quality.

The flag should be controllable via:
- A CLI argument (e.g., `--llm-interpretation / --no-llm-interpretation`)
- An environment variable (e.g., `DBPROFILE_LLM_INTERPRETATION=true/false`)
- A config file entry if the project already uses one

When `use_llm_interpretation=True`: make the API calls and embed the returned text.
When `use_llm_interpretation=False`: run the rule-based fallback path (see below) and
embed its output instead. The HTML report should be visually identical in both cases —
same card layout, same section structure — so the only difference is the interpretation
text itself. This makes it straightforward to run the report generator twice (once with
the flag on, once off) and compare the two HTML outputs directly.

### Fallback behavior

If `use_llm_interpretation=False`, or if `ANTHROPIC_API_KEY` is not set, or if the API
call fails, fall back gracefully to a rule-based template that surfaces the top issue
counts per check type without interpretation. The report should still render completely
— the summary card shows a structured but non-interpretive summary (e.g., *"3 critical
issues in Null Density · 12 critical issues in Uniqueness · 0 temporal gaps"*) in place
of the generated text. Do not fail the entire report generation if the API call fails.
