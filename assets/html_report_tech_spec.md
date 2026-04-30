# HTML Report — Technical Specification

**Project:** dbprofile  
**Document purpose:** Methodology and implementation reference for the self-contained HTML report. Intended as a reusable pattern for other Python-generated HTML reporting projects.

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Report Generation Pipeline](#2-report-generation-pipeline)
3. [File Structure — Single File Output](#3-file-structure--single-file-output)
4. [Layout and Navigation](#4-layout-and-navigation)
5. [Sidebar Navigation System](#5-sidebar-navigation-system)
6. [Smooth Scrolling and Jump Navigation](#6-smooth-scrolling-and-jump-navigation)
7. [Expand / Collapse System](#7-expand--collapse-system)
8. [Tooltips](#8-tooltips)
9. [Sortable Tables](#9-sortable-tables)
10. [Charts — Chart.js Integration](#10-charts--chartjs-integration)
11. [Custom Chart.js Plugins](#11-custom-chartjs-plugins)
12. [Severity and Color System](#12-severity-and-color-system)
13. [Issues-Only Filter](#13-issues-only-filter)
14. [Templating Patterns](#14-templating-patterns)
15. [Adapting This Pattern to a Jupyter Notebook Report](#15-adapting-this-pattern-to-a-jupyter-notebook-report)

---

## 1. Architecture Overview

The report is a **single self-contained HTML file**. No server, no external stylesheets, no JavaScript files, no images — everything the browser needs is embedded inline.

| Component | Technology | Notes |
|---|---|---|
| Templating | Jinja2 | Python-side rendering; produces final HTML |
| Styling | Inline CSS (`<style>`) | No external stylesheet dependency |
| Charts | Chart.js 4.4 (CDN) | Only external dependency; falls back gracefully if offline |
| Interactivity | Vanilla JS (`<script>`) | No jQuery, no frameworks |
| Data pipeline | Python renderer | Builds context dict → Jinja2 renders → writes `.html` |

The one external CDN call is Chart.js:

```html
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
```

If you need a fully offline report, download `chart.umd.min.js` and embed it inline with `<script>{{ chart_js_source }}</script>`.

---

## 2. Report Generation Pipeline

```
ProfileConfig (YAML)
        │
        ▼
   Orchestrator
   runs checks → list[CheckResult]
        │
        ▼
   renderer.py
   _build_report_context()     ← pure data processing, no I/O
        │  builds: tables_ctx, scorecard, quality scores,
        │          column profiles, severity aggregates
        ▼
   render_report()             ← calls _build_report_context + Jinja2
        │  passes context dict to template.html.j2
        ▼
   template.html.j2            ← Jinja2 template
        │  loops tables, checks, columns; emits HTML + inline JS
        ▼
   Single .html file           ← written to disk
```

**Key design decision — context is separated from rendering.**  
`_build_report_context()` is a pure function: it takes results and config, returns a plain Python dict. `render_report()` calls it, then passes the dict to Jinja2. This means the same context can feed the HTML renderer, the Excel exporter, or any future format without repeating the processing logic.

---

## 3. File Structure — Single File Output

The output file has this structure:

```
<!DOCTYPE html>
<html>
<head>
  <script src="chart.js CDN" />        ← only external dependency
  <script>
    var _chartCfg = {};               ← chart config registry (populated later)
    var _chartInst = {};              ← chart instance registry
    var _bwPlugin = { ... };          ← box-whisker custom plugin
    var _barLabelPlugin = { ... };    ← bar-label custom plugin
    Chart.register(_bwPlugin, _barLabelPlugin);
  </script>
  <style> ... all CSS ... </style>   ← complete stylesheet inline
</head>
<body>
  <aside id="sidebar"> ... </aside>   ← fixed left navigation
  <div id="main">
    <div id="topbar"> ... </div>      ← sticky top bar
    <div class="profile-header"> ... </div>  ← run metadata banner
    <div id="summary"> ... </div>     ← executive summary section
    <!-- per-table sections -->
    <div id="table-{name}"> ... </div>
    ...
  </div>
  <script> ... all JS ... </script>   ← interactivity at bottom of body
</body>
```

**Why chart config before chart instances?**  
Each chart's `<canvas>` is rendered inline as part of the Jinja2 loop, and each emits a small `<script>` block that populates `_chartCfg['chart-id'] = { ... }`. Because the canvas scripts run at parse time (before the bottom `<script>` block), the config registry must exist first. The actual `new Chart(...)` calls happen lazily — only when a section is expanded — via `initSectionCharts()`.

---

## 4. Layout and Navigation

The report uses a classic **fixed sidebar + scrollable main** layout:

```css
body { display: flex; min-height: 100vh; }

#sidebar {
  width: 240px;
  min-width: 240px;
  position: fixed;           /* stays put while main scrolls */
  top: 0; left: 0; bottom: 0;
  overflow-y: auto;
  z-index: 100;
}

#main {
  margin-left: 240px;        /* clears the fixed sidebar */
  flex: 1;
  min-width: 0;              /* prevents flex overflow */
}
```

Within `#main`, the topbar is **sticky** (not fixed), so it scrolls away with its section when content above it is taller than the viewport:

```css
#topbar {
  position: sticky;
  top: 0;
  z-index: 90;               /* below sidebar (100) but above content */
}
```

Per-table sections use `scroll-margin-top: 60px` to compensate for the sticky topbar height when `scrollIntoView()` is called:

```css
.table-section { scroll-margin-top: 60px; }
.check-section { scroll-margin-top: 60px; }
```

Without `scroll-margin-top`, the section header slides under the sticky topbar when you navigate to it.

---

## 5. Sidebar Navigation System

### Structure

```html
<aside id="sidebar">
  <!-- Column-label header row (rotated -90°) -->
  <div class="nav-col-header-row">
    <span style="flex:1"></span>
    <span class="nav-col-labels">
      <span class="nav-col-label"><span>SCHEMA</span></span>
      <span class="nav-col-label"><span>ROW CT</span></span>
      <!-- ... one per canonical check ... -->
    </span>
  </div>

  <!-- One row per table -->
  <div class="nav-table-row">
    <div class="nav-table-header" onclick="jumpTo('table-tablename')">
      <span class="nav-table-name">
        <a href="#table-tablename">tablename</a>
      </span>
      <span class="nav-circles">
        <span class="nc critical" title="Schema Audit: critical"
              onclick="event.stopPropagation(); jumpTo('tablename-schema_audit')">
        </span>
        <!-- ... one circle per canonical check ... -->
      </span>
    </div>
  </div>
</aside>
```

### Rotated Column Labels

The abbreviated check names (SCHEMA, ROW CT, NULL, UNIQUE, etc.) are rotated -90° and positioned above the circle columns:

```css
.nav-col-label {
  width: 10px;               /* matches circle diameter */
  display: flex;
  justify-content: center;
  overflow: visible;         /* text overflows the 10px box */
}

.nav-col-label span {
  display: inline-block;
  transform: rotate(-90deg);
  transform-origin: center;
  font-size: 7px;
  font-weight: 700;
  white-space: nowrap;       /* prevents line-wrapping during rotation */
  line-height: 1;
}
```

Each label cell is the same width as its corresponding circle (10px + 3px gap), so the columns align perfectly. `overflow: visible` lets the rotated text extend beyond the cell without clipping.

### Status Circles

Each circle is a `10px × 10px` `border-radius: 50%` span. Severity drives the background color via CSS class:

```css
.nc { width:10px; height:10px; border-radius:50%; border:1.5px solid rgba(255,255,255,0.42); }
.nc.critical { background: #f38ba8; }   /* pink-red */
.nc.warn     { background: #f9e2af; }   /* yellow */
.nc.ok       { background: #a6e3a1; }   /* green */
.nc.info     { background: #89dceb; }   /* teal */
.nc.na       { background: #52526e; }   /* muted gray */
```

The white semi-transparent border (`rgba(255,255,255,0.42)`) gives each circle a visible outline against the dark sidebar, making them legible even when the colors are similar.

---

## 6. Smooth Scrolling and Jump Navigation

### The `jumpTo()` Function

All internal navigation — sidebar circles, quick-links, home/up icons — routes through one function:

```javascript
function jumpTo(id) {
  var el = document.getElementById(id);
  if (!el) return;
  el.scrollIntoView({ behavior: 'smooth', block: 'start' });

  // If the target is a collapsed check section, auto-expand it
  var header = el.querySelector('.check-header');
  var body = el.querySelector('.check-body');
  if (body && !body.classList.contains('open') && header) {
    toggleCheck(header, id);
  }
}
```

Key behaviors:
- `behavior: 'smooth'` — browser-native smooth scroll, no library needed
- `block: 'start'` — aligns the element's top edge to the viewport top (offset by `scroll-margin-top`)
- Auto-expands collapsed sections so you always land on visible content

### Anchor Links vs. `jumpTo()`

Most links use both an `href` anchor (for right-click → open in tab, bookmarking) and an `onclick` that calls `jumpTo()` for smooth scrolling within the page:

```html
<a href="#table-orders"
   onclick="jumpTo('table-orders'); return false;">
  orders
</a>
```

`return false` prevents the default anchor jump (which would be instant, not smooth).

### Home and Up Arrow Icons

Every check section header has two icon buttons:

```html
<a class="hdr-icon-btn" href="#summary" 
   onclick="event.stopPropagation()" 
   title="Home — top of report">&#8962;</a>   <!-- ⌂ -->

<a class="hdr-icon-btn" href="#table-{name}" 
   onclick="event.stopPropagation()" 
   title="Up — {tablename}">&#8593;</a>        <!-- ↑ -->
```

`event.stopPropagation()` prevents the click from bubbling to the check header's `onclick` (which would toggle expand/collapse).

---

## 7. Expand / Collapse System

### Check Sections

Each check has a header (always visible) and a body (hidden by default, shown when open):

```css
.check-body         { display: none; }   /* collapsed */
.check-body.open    { display: block; }  /* expanded */
```

The header's `onclick` calls `toggleCheck()`:

```javascript
function toggleCheck(header, sectionId) {
  var body = document.getElementById('body-' + sectionId);
  var arrow = header.querySelector('.check-toggle');
  if (!body) return;
  var opening = !body.classList.contains('open');
  body.classList.toggle('open');
  if (arrow) arrow.classList.toggle('open');
  if (opening) initSectionCharts(body);   // lazy chart initialization
}
```

**Lazy chart initialization** is the critical detail: `initSectionCharts()` is only called when a section is opened, not on page load. This means:
- Page load is fast regardless of how many charts exist
- Charts are only instantiated for sections the user actually opens
- The `_chartInst` registry prevents double-initialization

### Expand / Collapse All (Per Table)

Each table header has an "Expand All / Collapse All" button:

```html
<button class="expand-all-btn" id="xall-{table}"
        onclick="toggleAllChecks('{table}', this)">
  + Expand All
</button>
```

```javascript
function toggleAllChecks(tableId, btn) {
  var section = document.getElementById('table-' + tableId);
  if (!section) return;
  var bodies = section.querySelectorAll('.check-body');
  var anyOpen = Array.from(bodies).some(function(b) {
    return b.classList.contains('open');
  });
  bodies.forEach(function(b) {
    var header = b.previousElementSibling;
    if (anyOpen) {
      b.classList.remove('open');
      var arrow = header ? header.querySelector('.expand-icon') : null;
      if (arrow) arrow.textContent = '▶';
    } else {
      b.classList.add('open');
      var arrow = header ? header.querySelector('.expand-icon') : null;
      if (arrow) arrow.textContent = '▼';
    }
  });
  if (btn) btn.textContent = anyOpen ? '+ Expand All' : '– Collapse All';
}
```

The logic: if **any** body is open → collapse all; otherwise → expand all. This handles the mixed state gracefully.

### Auto-Expand on Page Load

On DOMContentLoaded, sections with critical or warn badges are automatically expanded so the most important findings are immediately visible:

```javascript
document.addEventListener('DOMContentLoaded', function() {
  document.querySelectorAll('.check-section').forEach(function(section) {
    var badge = section.querySelector('.check-header .badge');
    if (badge && (badge.classList.contains('critical') || badge.classList.contains('warn'))) {
      var header = section.querySelector('.check-header');
      var id = section.id;
      if (header && id) toggleCheck(header, id);
    }
  });
  // Sync expand-all button labels
  document.querySelectorAll('.table-section').forEach(function(ts) {
    var tableId = ts.id.replace('table-', '');
    var btn = document.getElementById('xall-' + tableId);
    if (!btn) return;
    if (ts.querySelector('.check-body.open')) btn.textContent = '– Collapse All';
  });
  initSortableTables();
});
```

### Subgroup Expand/Collapse (Within a Check)

Some checks (Null Density) split results into subgroups (Violators / OK columns). These use the same CSS pattern but a separate toggle function:

```javascript
function toggleSubgroup(header, bodyId) {
  var body = document.getElementById(bodyId);
  if (!body) return;
  body.classList.toggle('open');
  var arrow = header.querySelector('.subgroup-toggle');
  if (arrow) arrow.style.transform =
    body.classList.contains('open') ? 'rotate(90deg)' : '';
}
```

The arrow indicator rotates 90° on expand (▶ becomes ▼ via CSS transform, not a character swap).

---

## 8. Tooltips

The report uses **native HTML `title` attributes** for tooltips — no JavaScript tooltip library:

```html
<th title="Null Density — % of SQL NULL values per column
SQL: SUM(CASE WHEN col IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*)">
  ND
</th>
```

Advantages:
- Zero JavaScript, zero CSS
- Works everywhere (desktop browsers show on hover; mobile shows on long-press)
- Multi-line tooltips work with embedded `\n` or `&#10;` in the attribute value
- Rendered by the browser's native tooltip engine — consistent, accessible

Limitation: styling is browser-controlled (no custom font, background, or animation). If styled tooltips are needed, a CSS-only approach using `::after` pseudo-elements is the next step up without adding a library.

**Where tooltips are used:**
- Column headers in every data table — explains abbreviations and the underlying SQL
- Nav sidebar circles — `title="Check Name: severity"`
- Shape column — describes sparkline encoding
- Per-cell heatmap cells — `title="Check: severity"`
- Quick-link pills — `title="Full check name"` when abbreviated text is shown

---

## 9. Sortable Tables

All data tables with an `id` attribute and `data-sort` on their `<th>` elements are automatically wired for click-to-sort on page load.

### Column Declaration

```html
<table class="heatmap" id="scoreboard-heatmap">
  <thead>
    <tr>
      <th data-sort="str">Table</th>
      <th data-sort="num">Rows</th>
      <th data-sort="num">Score</th>
      <th data-sort="str">SA</th>
      <!-- no data-sort = not sortable (e.g. Shape column with SVG) -->
    </tr>
  </thead>
```

Sort types: `"str"` (locale-aware string), `"num"` (float), `"sev"` (severity rank order).

### Sort Data Attribute

Where the display text and sort value differ (e.g., formatted numbers like "1,234" vs. raw `1234`), cells carry a `data-val` attribute:

```html
<td class="hm-num" data-val="1234">1,234</td>
```

The sort function reads `data-val` first, falling back to `textContent`:

```javascript
var aVal = (aCell.getAttribute('data-val') || aCell.textContent || '').trim();
```

### Sort Implementation

```javascript
function sortTable(th) {
  var table = th.closest('table');
  var id = table.id;
  var ths = Array.from(th.closest('tr').querySelectorAll('th'));
  var colIdx = ths.indexOf(th);
  var sortType = th.getAttribute('data-sort') || 'str';

  var prev = _sortState[id] || { col: -1, dir: 0 };
  var dir = (prev.col === colIdx && prev.dir === 1) ? -1 : 1;   // toggle
  _sortState[id] = { col: colIdx, dir: dir };

  // Visual indicator
  ths.forEach(function(h) { h.classList.remove('sort-asc', 'sort-desc'); });
  th.classList.add(dir === 1 ? 'sort-asc' : 'sort-desc');

  // Sort and re-append rows
  var tbody = table.querySelector('tbody');
  var rows = Array.from(tbody.querySelectorAll('tr'));
  rows.sort(function(a, b) {
    var aVal = ...; var bVal = ...;
    var cmp;
    if (sortType === 'num') cmp = parseFloat(aVal) - parseFloat(bVal);
    else cmp = aVal.localeCompare(bVal);
    return cmp * dir;
  });
  rows.forEach(function(r) { tbody.appendChild(r); });
}
```

`tbody.appendChild(r)` on an existing row **moves** it (doesn't copy) — this is the standard DOM trick for reordering table rows without creating new elements.

---

## 10. Charts — Chart.js Integration

### Deferred Initialization Pattern

Chart.js is loaded from CDN in `<head>`. Chart configurations are registered into `_chartCfg` by inline `<script>` blocks emitted by the Jinja2 template for each chart. Actual `new Chart(...)` calls happen only when the containing section is opened.

```javascript
// In <head> — registry declared before any chart scripts run
var _chartCfg = {};
var _chartInst = {};

// Emitted by Jinja2 inside each check section (runs at parse time)
_chartCfg['c-orders-nd-amount'] = (function() {
  var labels = ["label1", "label2", ...];
  var data   = [10, 20, ...];
  return { type: 'bar', data: {...}, options: {...} };
})();

// Called when user expands the section
function initSectionCharts(bodyEl) {
  var canvases = bodyEl.querySelectorAll('canvas[data-chart-id]');
  canvases.forEach(function(c) {
    var id = c.getAttribute('data-chart-id');
    if (_chartCfg[id] && !_chartInst[id]) {   // guard: don't double-init
      _chartInst[id] = new Chart(c, _chartCfg[id]);
    }
  });
}
```

### Chart ID Naming Convention

Each chart ID is built from `table-checktype-column` to guarantee uniqueness across the document:

```
c-{table}-{check_short}-{column_name}
c-orders-nd-order_amount          (null density histogram)
c-orders-fr-status                (frequency distribution bar)
c-orders-tc-created_at            (temporal line chart)
c-orders-rc                       (row count time series)
```

### Chart Canvas Declaration

```html
<div class="chart-wrap" style="height:200px">
  <canvas id="c-orders-nd-amount" 
          data-chart-id="c-orders-nd-amount">
  </canvas>
</div>
```

`data-chart-id` is what `initSectionCharts()` queries. The `id` attribute is redundant but useful for direct DOM selection.

### Chart Types Used

| Check | Chart Type | Key Config |
|---|---|---|
| Null Density | Horizontal bar | Bars colored by severity; `indexAxis: 'y'` |
| Numeric Distribution | Vertical bar (histogram) | Custom `_bwPlugin` draws box-whisker below |
| Frequency Distribution | Horizontal bar | Custom `_barLabelPlugin` draws % labels at bar end |
| Temporal Consistency | Line | Filled area under line; `fill: true` |
| Row Count Time Series | Line | Same as temporal |

---

## 11. Custom Chart.js Plugins

Both plugins are registered globally in `<head>` so every chart on the page can opt into them.

### Box-Whisker Plugin (`_bwPlugin`)

Draws a box-and-whisker diagram below a histogram chart using Chart.js's Canvas 2D API, synchronized to the chart's own x-axis scale.

**Activation:** chart must include `options.plugins.boxWhisker: { p25, p50, p75, lf, uf, histLow, histHigh }`.

**How it works:**
1. `afterDraw` hook fires after Chart.js has finished drawing
2. Reads `chart.chartArea` for bounds and `chart.scales['x']` for the value→pixel mapping
3. Uses `xScale.getPixelForValue(v)` to position whisker endpoints and box edges at the exact same pixel as the histogram bars above
4. All drawing is Canvas 2D primitives: `moveTo/lineTo/stroke` for lines, `fillRect/strokeRect` for the IQR box

**Key geometry:**
```javascript
var yTop  = ca.bottom + 24;   // box top
var yMid  = ca.bottom + 36;   // whisker centerline
var yBot  = ca.bottom + 48;   // box bottom
var yAxis = ca.bottom + 72;   // labeled x-axis line
var yTickLbl = ca.bottom + 88; // axis value labels
```

### Bar-Label Plugin (`_barLabelPlugin`)

Draws percentage values at the right end of horizontal bars.

**Activation:** `options.plugins.barLabels: { decimals: 1 }`.

**How it works:**
```javascript
afterDraw: function(chart) {
  chart.data.datasets.forEach(function(ds, i) {
    var meta = chart.getDatasetMeta(i);
    meta.data.forEach(function(bar, j) {
      var x = bar.x + 4;     // 4px padding after bar end
      var y = bar.y;
      ctx.fillText(val.toFixed(dec) + '%', x, y);
    });
  });
}
```

`bar.x` is the right edge of the rendered bar in pixel space. Adding 4px gives a small gap before the label.

### Dynamic X-Axis Maximum with Label Headroom

For frequency distribution charts, the x-axis max is set to `maxVal * 1.15` (15% headroom) so bar labels don't overflow the chart area:

```javascript
var maxVal = Math.max.apply(null, data);
scales: { x: { max: maxVal * 1.15 } }
```

---

## 12. Severity and Color System

A five-level severity palette flows from Python through the template into CSS:

| Level | Meaning | Background | Text | CSS Class |
|---|---|---|---|---|
| `critical` | Threshold exceeded — action required | `#f38ba8` (pink-red) | `#d20f39` / `#6b0020` | `.critical` |
| `warn` | Threshold exceeded — review recommended | `#f9e2af` (yellow) | `#b45309` / `#7a4000` | `.warn` |
| `ok` | Within normal range | `#a6e3a1` (green) | `#40a02b` / `#1a5c1a` | `.ok` |
| `info` | Informational — expected pattern | `#89dceb` (teal) | `#0077a8` / `#005f78` | `.info` |
| `na` | Check did not apply to this column | `#e8eaf0` (light gray) | `#6c7086` / `#8a8fb0` | `.na` |

The same classes drive multiple UI elements:
- **Nav sidebar circles** — `.nc.critical`, `.nc.warn`, etc.
- **Heatmap cells** — `.hm-cell.critical`, `.hm-cell.warn`, etc.
- **Badges** — `.badge.critical`, `.badge.warn`, etc.
- **Severity pills** in table headers — `.sev-pill.critical`, etc.

This unified class naming means a single source of truth for the color palette. Changing a color in one place changes it everywhere.

### Quality Score Color Mapping (Python side)

The 0–100 quality score is mapped to a color in Python, passed as a callable into the template:

```python
def _score_color(score: int) -> str:
    if score >= 90: return "#a6e3a1"    # green
    if score >= 75: return "#f9e2af"    # yellow
    if score >= 60: return "#fab387"    # orange
    return "#f38ba8"                    # red
```

Usage in template:

```jinja2
{% set sc_color = score_color(overall_quality_score) %}
<div class="score-circle" style="color:{{ sc_color }};border-color:{{ sc_color }}">
  {{ overall_quality_score }}
</div>
```

---

## 13. Issues-Only Filter

The topbar "Issues only" button toggles a class on `<body>` and hides all-ok sections via CSS:

```javascript
function toggleIssuesOnly(btn) {
  document.body.classList.toggle('issues-only');
  btn.classList.toggle('active');
  btn.textContent = btn.classList.contains('active') ? 'Show all' : 'Issues only';
}
```

```css
body.issues-only .table-section.all-ok  { display: none; }
body.issues-only .check-section.all-ok  { display: none; }
```

Sections are marked `all-ok` by the Jinja2 template based on their worst severity:

```jinja2
<div class="check-section {% if worst_sev in ('ok','info','na') %}all-ok{% endif %}">
```

This is CSS-only filtering — no DOM manipulation, no re-rendering, instant toggle.

---

## 14. Templating Patterns

### Jinja2 Configuration

```python
env = Environment(
    loader=FileSystemLoader(str(template_dir)),
    autoescape=False    # SVG sparklines are embedded as raw markup
)
env.filters["tojson"] = lambda v: json.dumps(v, default=str)
```

`autoescape=False` is intentional — SVG is generated by trusted Python code and must not be HTML-escaped. If user data ever flows directly into template output, escape it explicitly with `{{ value | e }}`.

### The `tojson` Filter

Chart data is passed from Jinja2 to JavaScript via the `tojson` filter:

```jinja2
var s = {{ r.detail.series | tojson }};
```

This serializes a Python list/dict to valid JSON inline. `default=str` handles Python `Decimal`, `datetime`, and other non-JSON-native types gracefully.

### Namespace for Counters

Jinja2 variables are scoped to their block. To maintain a counter across a loop, use `namespace`:

```jinja2
{% set ns = namespace(count=0) %}
{% for item in items %}
  {% if item.severity == 'critical' %}
    {% set ns.count = ns.count + 1 %}
  {% endif %}
{% endfor %}
Total critical: {{ ns.count }}
```

### Inline Macros

Repeated HTML structures are defined as macros within the template:

```jinja2
{% macro null_row(r, bold) %}
  <td class="col-ord">{{ prof.ordinal }}</td>
  <td class="col-name">{% if bold %}<strong>{{ r.column }}</strong>{% endif %}</td>
  ...
{% endmacro %}

{% for r in flagged %}
<tr>{{ null_row(r, true) }}</tr>
{% endfor %}
```

### Inline SVG Sparklines

Sparklines (box plots and cardinality bars) are generated as SVG strings in Python and embedded directly:

```python
def _make_numeric_sparkline(p25, p50, p75, min_v, max_v) -> str:
    return f'<svg width="60" height="14">...<rect x="{x}" .../></svg>'
```

```jinja2
<td class="col-shape">{{ prof.get('sparkline_svg','') }}</td>
```

Because `autoescape=False`, the SVG renders as actual markup rather than escaped text.

---

## 15. Adapting This Pattern to a Jupyter Notebook Report

If you are building a report that consumes Jupyter notebook outputs (charts, tables, markdown cells) and renders them as a standalone HTML file, the core patterns from this project transfer directly.

### Recommended Architecture

```
notebook_runner.py
  → executes notebook cells → collects outputs
        │
        ▼
context_builder.py
  → processes outputs → builds context dict
        │
        ▼
Jinja2 template (report.html.j2)
  → renders single HTML file
```

### Transferable Patterns

**1. Deferred chart initialization**  
If notebook cells produce Chart.js configurations, write them to `_chartCfg` in `<head>` and initialize lazily on section open. Prevents slow page loads with many charts.

**2. Single-file output**  
Embed all CSS inline. The report stays portable — email it, put it on a drive, open it anywhere.

**3. Expand/collapse sections**  
Map each notebook section (H1/H2 heading) to a collapsible div. Use `display:none` / `.open` toggle. Auto-expand sections with flagged metrics.

**4. Smooth scroll navigation**  
Build a sidebar from notebook section headings. Route all navigation through `jumpTo()`. Use `scroll-margin-top` on all section anchors.

**5. Native HTML tooltips**  
Use `title` attributes on column headers, chart labels, and abbreviations. No library needed.

**6. Chart.js custom plugins**  
Register plugins in `<head>` as global objects before any chart scripts. Access them as `options.plugins.myPlugin = {...}` per chart.

**7. `tojson` filter**  
Pass Python lists/dicts to inline JavaScript cleanly. Handles dates and decimals via `default=str`.

**8. CSS class-driven severity**  
Define a 4–5 level severity palette as CSS classes once. Apply the same class names to circles, cells, badges, and pills throughout the report.

### Minimal Skeleton

```html
<!DOCTYPE html>
<html>
<head>
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
  <script>
    var _chartCfg = {};
    var _chartInst = {};
  </script>
  <style>
    /* sidebar + main layout, collapse CSS, severity palette */
  </style>
</head>
<body>
  <aside id="sidebar">
    <!-- generated from section headings -->
  </aside>
  <div id="main">
    <div id="topbar" style="position:sticky;top:0">Report Title</div>
    {% for section in sections %}
    <div id="section-{{ loop.index }}" class="report-section" style="scroll-margin-top:50px">
      <div class="section-header" onclick="toggleSection(this)">
        {{ section.title }}
      </div>
      <div class="section-body">
        {{ section.content }}   {# HTML from notebook cell output #}
        {% if section.chart_id %}
        <canvas id="{{ section.chart_id }}" data-chart-id="{{ section.chart_id }}"></canvas>
        <script>
          _chartCfg['{{ section.chart_id }}'] = {{ section.chart_config | tojson }};
        </script>
        {% endif %}
      </div>
    </div>
    {% endfor %}
  </div>
  <script>
    function toggleSection(header) {
      var body = header.nextElementSibling;
      var opening = !body.classList.contains('open');
      body.classList.toggle('open');
      if (opening) {
        body.querySelectorAll('canvas[data-chart-id]').forEach(function(c) {
          var id = c.getAttribute('data-chart-id');
          if (_chartCfg[id] && !_chartInst[id]) {
            _chartInst[id] = new Chart(c, _chartCfg[id]);
          }
        });
      }
    }
    function jumpTo(id) {
      var el = document.getElementById(id);
      if (el) el.scrollIntoView({ behavior: 'smooth', block: 'start' });
    }
  </script>
</body>
```

The three lines that matter most for the Jupyter use case:
1. `_chartCfg` registered before chart scripts → deferred initialization works
2. `autoescape=False` → notebook HTML output renders as markup, not escaped text  
3. `scroll-margin-top` on every section → navigation lands correctly under a sticky header
