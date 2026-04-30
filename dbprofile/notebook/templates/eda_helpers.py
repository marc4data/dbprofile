"""
eda_helpers.py — Reusable chart components for fct_trips EDA notebook.

Usage in notebook:
    %load_ext autoreload
    %autoreload 2
    from eda_helpers import *

Reload options:
    %autoreload 2          — auto-reload all modules before every cell (recommended)
    %autoreload 1          — only reload modules registered with %aimport
    importlib.reload(mod)  — manual one-shot reload

Architecture — Figure vs Axes
-----------------------------
    fig  (Figure) = the entire image canvas. Controls overall size (`figsize`),
                    the super-title (`fig.suptitle`), and spacing between panels.
                    Think of it as the piece of paper.

    ax   (Axes)   = one individual chart panel within the figure. Each ax has its
                    own x-axis, y-axis, title, gridlines, and plotted data.
                    Think of it as one rectangle drawn on the paper.

    plt.subplots(nrows, ncols) creates a Figure containing an nrows x ncols grid
    of Axes. Returns (fig, axes). All data calls (ax.plot, ax.bar, ax.scatter)
    and formatting calls (ax.set_title, ax.grid, ax.set_ylim) target a specific
    Axes — never the Figure. The Figure only controls the container.

    GridSpec — for complex layouts (e.g. one panel spanning multiple rows), use
    fig.add_gridspec() to define a flexible grid, then fig.add_subplot(gs[row, col])
    to place each Axes. gs[:, 0] means "all rows in column 0" — lets one panel
    span the full height while others are stacked in an adjacent column.

    Example:
        fig = plt.figure(figsize=(20, 9))
        gs  = fig.add_gridspec(2, 2, width_ratios=[4, 1])
        ax_line = fig.add_subplot(gs[:, 0])   # left col, spans both rows
        ax_bar  = fig.add_subplot(gs[0, 1])   # right col, top half
        ax_dow  = fig.add_subplot(gs[1, 1])   # right col, bottom half
"""

import math
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.lines as mlines
import matplotlib.ticker as mticker
import matplotlib.dates as mdates

try:
    import mplcursors
    _HAS_MPLCURSORS = True
except ImportError:
    _HAS_MPLCURSORS = False

# ── Constants ──────────────────────────────────────────────────────────────────

# ── Chart sizing ───────────────────────────────────────────────────────────────
# Single source of truth for figure dimensions. All functions reference these
# instead of hardcoding figsize. Override from the notebook if needed:
#     import eda_helpers; eda_helpers.CHART_WIDTH = 14
#
# Actual pixel size on screen = figsize_inches * CHART_DPI
# Example: CHART_WIDTH=12, CHART_DPI=72 → 864px wide
CHART_WIDTH      = 12     # inches — standard width for all charts
CHART_ROW_HEIGHT = 5.0    # inches — height per borough row in plot_borough_detail
CHART_DPI        = 100    # dots per inch — controls rendered pixel size

# ── Cardinality thresholds ────────────────────────────────────────────────────
# Fields with distinct values ≤ threshold are "low cardinality" — can show
# individual values on an axis. Above threshold → need binning/sampling.
CARDINALITY_THRESHOLD_STR = 15    # string fields
CARDINALITY_THRESHOLD_NUM = 50    # numeric fields
TITLE_SM         = 10     # font size for titles on small/secondary panels (bar, DoW)


# ── Font sizes — single source of truth for chart typography ──────────────────
# Module-level constants used by chart helpers. Override at notebook level
# to apply across every chart in a report:
#     import eda_helpers
#     eda_helpers.FONT_TITLE = 14
# Goal: uniform typography across all charts in an EDA report (nb2report HTML).
FONT_TITLE       = 17     # ax.set_title for single-chart helpers
FONT_SUPTITLE    = 18     # fig.suptitle for multi-panel grids
FONT_AXIS_LABEL  = 15     # x/y axis names
FONT_TICK        = 13     # tick labels (numbers/categories on axes)
FONT_BAR_VALUE   = 12     # value annotations on bars
FONT_PANEL_TITLE = 15     # individual panel titles in small-multiples
FONT_BADGE       = 12     # r-value pills, n=... stats subtitles
FONT_LEGEND      = 10     # legend text within panels


BOROUGH_COLORS = {
    'manhattan':      '#1f77b4',  # blue   — dominant borough
    'queens':         '#ff7f0e',  # orange
    'brooklyn':       '#2ca02c',  # green
    'bronx':          '#d62728',  # red
    'staten island':  '#9467bd',  # purple
    'ewr':            '#8c564b',  # brown  — Newark Airport
    '(null borough)': '#bbbbbb',  # grey   — no dim_zones match
    'n/a':            '#aaaaaa',  # light grey
    'unknown':        '#cccccc',  # very light grey
}


# ── Expected DataFrame schemas ────────────────────────────────────────────────
# pickup_df / dropoff_df : columns = [date_col, 'borough', 'trip_cnt']
# hist_df                : columns = ['trip_duration_minutes', 'fare_amount',
#                                     'passenger_count', 'borough']

# ── Number formatting ─────────────────────────────────────────────────────────

def fmt_num(x):
    """Dynamically scale a number to M (millions) or K (thousands) for labels."""
    if abs(x) >= 1_000_000:
        return f'{x / 1_000_000:.1f}M'
    elif abs(x) >= 1_000:
        return f'{x / 1_000:.0f}K'
    return f'{x:,.0f}'


def _infer_label(date_col):
    """Derive a human-readable label from a date column name.
    'pickup_date' -> 'Pickup', 'dropoff_date' -> 'Dropoff'."""
    return date_col.replace('_date', '').replace('_', ' ').title()


def _darken(hex_color, factor=0.5):
    """Darken a hex color by blending it toward black.
    factor=0.5 means halfway between the original and pure black."""
    h = hex_color.lstrip('#')
    r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
    return f'#{int(r*factor):02x}{int(g*factor):02x}{int(b*factor):02x}'


# ── Hover tooltips ────────────────────────────────────────────────────────────
# Requires %matplotlib widget (interactive backend) + mplcursors.
# With %matplotlib inline (static PNGs), tooltips silently do nothing.
#
# mplcursors attaches to specific matplotlib "artists" (Line2D, BarContainer, etc.)
# and fires a callback when the cursor hovers over one. The callback receives a
# Selection object (sel) with:
#   sel.target       — (x, y) coordinates of the hover point
#   sel.artist       — the matplotlib object being hovered (the line, bar, etc.)
#   sel.annotation   — a text annotation you can customize
#   sel.index        — which data point within the artist
#
# We format the annotation text to show context (borough, date, value) and style
# the tooltip box with a white background and light border.
#
# Reference: https://mplcursors.readthedocs.io/en/stable/

def _add_line_tooltips(ax, date_axis=True, pct=False, suffix='trips', x_labels=None):
    """
    Add hover tooltips to all labeled Line2D artists on the given Axes.

    Filters out internal matplotlib lines (labels starting with '_') so only
    data lines and trend lines get tooltips.

    Parameters
    ----------
    ax         : Axes containing Line2D artists
    date_axis  : if True, format x-values as dates (for time-series charts)
    pct        : if True, format y-values as percentages
    suffix     : unit label appended to y-values when pct=False (default 'trips')
    x_labels   : list of strings for categorical x-axis (e.g. day names for DoW).
                 The x-value is rounded to an index into this list.
    """
    if not _HAS_MPLCURSORS:
        return
    # ax.lines contains only Line2D objects — fill_between (PolyCollection) is excluded
    lines = [l for l in ax.lines
             if l.get_label() and not l.get_label().startswith('_')]
    if not lines:
        return
    cursor = mplcursors.cursor(lines, hover=True)

    @cursor.connect("add")
    def on_add(sel):
        label = sel.artist.get_label()
        y = sel.target[1]
        parts = [label]
        if date_axis:
            dt = mdates.num2date(sel.target[0])
            # Line 141-142: date + day-of-week in the tooltip
            parts.append(dt.strftime('%a, %b %d, %Y'))   # e.g. "Mon, Jan 15, 2025"
        elif x_labels:
            idx = int(round(sel.target[0]))
            if 0 <= idx < len(x_labels):
                parts.append(x_labels[idx])
        if pct:
            parts.append(f'{y:.1f}%')
        else:
            parts.append(f'{fmt_num(y)} {suffix}')
        sel.annotation.set_text('\n'.join(parts))
        sel.annotation.get_bbox_patch().set(
            alpha=0.92, facecolor='white', edgecolor='#cccccc'
        )


# ── Axis formatting helpers ───────────────────────────────────────────────────

def fmt_date_axis(ax, interval=1):
    """Simple monthly date format — used by standalone chart cells.
    For the full year/month grid treatment, use _fmt_time_xaxis instead."""
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=interval))
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')


def fmt_yaxis(ax):
    """Simple y-axis with comma format and basic grid — used by standalone chart cells.
    For the clean 4-division treatment, use _fmt_clean_yaxis instead."""
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f'{x:,.0f}'))
    ax.grid(axis='y', linestyle='--', alpha=0.4)


# ── Gridline presets ──────────────────────────────────────────────────────────
# String shortcuts for matplotlib date locators. Passed as `major=` / `minor=`
# to _fmt_time_xaxis and the chart functions that forward to it.
#
# Supports interval syntax:  'month:3' = every 3 months (quarterly)
#
# Preset table:
#   Preset       Locator                        Default format   Use case
#   'year'       YearLocator()                  '%Y'             Multi-year view
#   'quarter'    MonthLocator(interval=3)       '%b %Y'          Annual view
#   'month'      MonthLocator()                 '%b'             Quarterly view
#   'week'       WeekdayLocator(Monday)         'W%W'            Monthly view
#   'day'        DayLocator()                   '%-d'            Weekly view
#   'month:2'    MonthLocator(interval=2)       '%b'             Custom interval

# Default label formats — auto-applied when user doesn't pass an explicit format.
_PRESET_FMTS = {
    'year':    '%Y',
    'quarter': '%b %Y',
    'month':   '1',       # special: first letter of month name (handled in _fmt_time_xaxis)
    'week':    'W%W',
    'day':     '%-d',
}

def _get_locator(preset):
    """Convert a preset string to a matplotlib date locator.

    Parameters
    ----------
    preset : str or Locator
        String like 'year', 'month', 'week', 'day', 'quarter'.
        Append ':N' for intervals: 'month:3' = every 3 months.
        Or pass a raw matplotlib Locator object for full control.
    """
    if not isinstance(preset, str):
        return preset  # already a Locator object — pass through

    parts = preset.split(':')
    name  = parts[0]
    n     = int(parts[1]) if len(parts) > 1 else 1

    if name == 'year':
        return mdates.YearLocator(base=n)
    if name == 'quarter':
        return mdates.MonthLocator(interval=3 * n)
    if name == 'month':
        return mdates.MonthLocator(interval=n)
    if name == 'week':
        return mdates.WeekdayLocator(byweekday=0, interval=n)
    if name == 'day':
        return mdates.DayLocator(interval=n)
    raise ValueError(f"Unknown preset: '{name}'. Options: year, quarter, month, week, day")


def _nice_ylim(data_max):
    """
    Round data_max up to a clean number whose quarters are round, readable values.

    Returns (ylim, step) where ylim = 4 * step.

    Examples:
        107,000  ->  ylim=200,000  step=50,000   ticks: 0, 50K, 100K, 150K, 200K
          3,500  ->  ylim=  4,000  step= 1,000   ticks: 0, 1K, 2K, 3K, 4K
            690  ->  ylim=    800  step=   200   ticks: 0, 200, 400, 600, 800
             18  ->  ylim=     20  step=     5   ticks: 0, 5, 10, 15, 20
    """
    if data_max <= 0:
        return 4, 1
    raw_step = data_max / 4
    magnitude = 10 ** math.floor(math.log10(raw_step))
    for nice in [1, 1.5, 2, 2.5, 3, 4, 5, 8, 10]:
        step = nice * magnitude
        if step >= raw_step:
            ylim = step * 4
            if ylim == int(ylim):
                ylim = int(ylim)
            if step == int(step):
                step = int(step)
            return ylim, step
    return math.ceil(data_max), math.ceil(data_max) / 4


def _snap_date_range(df, date_col):
    """
    Compute tight x-axis limits snapped to whole-month boundaries.

    Returns (x_min, x_max) where:
      x_min = first day of the earliest month with data
      x_max = last day of the latest month with data

    Eliminates dead whitespace before/after the data range.
    """
    d_min = df[date_col].min()
    d_max = df[date_col].max()
    # First of the min month
    x_min = d_min.to_period('M').to_timestamp()
    # Last day of the max month
    x_max = (d_max.to_period('M') + 1).to_timestamp() - pd.Timedelta(days=1)
    return x_min, x_max


def _fmt_time_xaxis(ax, major='year', minor='month',
                    major_fmt=None, minor_fmt=None):
    """
    Configure a time-series x-axis with two levels of gridlines.

    Parameters
    ----------
    ax        : Axes to format
    major     : where to place major gridlines — preset string or Locator object.
                Presets: 'year', 'quarter', 'month', 'week', 'day'.
                Append ':N' for intervals: 'month:3' = every 3 months.
    minor     : where to place minor gridlines — same options as major.
    major_fmt : strftime format for major tick labels. If None, auto-set from
                the preset (e.g. 'year' → '%Y', 'month' → '%b').
                Override with any strftime string: '%b %Y', '%-m', etc.
    minor_fmt : strftime format for minor tick labels. Same auto/override logic.

    Full strftime reference: https://strftime.org
    """
    # Resolve preset names to default formats if user didn't pass explicit ones.
    # _PRESET_FMTS maps 'year' → '%Y', 'month' → '%b', etc.
    major_name = major.split(':')[0] if isinstance(major, str) else None
    minor_name = minor.split(':')[0] if isinstance(minor, str) else None
    if major_fmt is None:
        major_fmt = _PRESET_FMTS.get(major_name or 'year', '%Y')
    if minor_fmt is None:
        minor_fmt = _PRESET_FMTS.get(minor_name or 'month', '%b')

    # Minor ticks — set FIRST so we can measure label height for major padding.
    ax.xaxis.set_minor_locator(_get_locator(minor))
    # '1' is a special format: first letter of month name (J, F, M, A, ...).
    # strftime has no single-letter code, so we use a FuncFormatter instead.
    if minor_fmt == '1':
        ax.xaxis.set_minor_formatter(mticker.FuncFormatter(
            lambda x, _: mdates.num2date(x).strftime('%b')[0]
        ))
    else:
        ax.xaxis.set_minor_formatter(mdates.DateFormatter(minor_fmt))
    ax.grid(True, axis='x', which='minor', color='gray', alpha=0.2, linewidth=0.5)
    plt.setp(ax.xaxis.get_minorticklabels(), rotation=0, ha='left')

    # Major ticks — bold labels, prominent gridlines.
    # pad pushes the major label below the minor labels so they don't overlap.
    minor_fontsize = ax.xaxis.get_minorticklabels()[0].get_fontsize() if ax.xaxis.get_minorticklabels() else 10
    ax.xaxis.set_major_locator(_get_locator(major))
    # Same '1' handling for major if needed
    if major_fmt == '1':
        ax.xaxis.set_major_formatter(mticker.FuncFormatter(
            lambda x, _: mdates.num2date(x).strftime('%b')[0]
        ))
    else:
        ax.xaxis.set_major_formatter(mdates.DateFormatter(major_fmt))
    ax.grid(True, axis='x', which='major', color='gray', alpha=0.5, linewidth=1.3)
    ax.tick_params(axis='x', which='major', pad=minor_fontsize * 1.5)
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=0, ha='center', fontweight='bold')


def _fmt_clean_yaxis(ax, data_max, pct=False):
    """
    Set y-axis with exactly 3 interior gridlines at 25% / 50% / 75% of a rounded max.

    Parameters
    ----------
    ax       : the Axes to format
    data_max : the actual maximum value in the plotted data
    pct      : if True, format tick labels as percentages (e.g. "15%")
               if False, use fmt_num (e.g. "50K")
    """
    ylim, step = _nice_ylim(data_max)
    ax.set_ylim(0, ylim)
    ticks = [step * i for i in range(5)]
    ax.set_yticks(ticks)
    if pct:
        ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f'{x:.0f}%'))
    else:
        ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: fmt_num(x)))
    ax.grid(True, axis='y', which='major', color='gray', alpha=0.5, linewidth=1.3)
    ax.yaxis.set_minor_locator(mticker.NullLocator())


# ── Low-level chart components ─────────────────────────────────────────────────

def _add_trends(ax, dates, values, color='#333333', ma_window=7, band_sigmas=1):
    """
    Overlay trend lines on a single-borough time-series panel.

    Parameters
    ----------
    ax          : Axes to draw on
    dates       : DatetimeIndex
    values      : trip_cnt array
    color       : borough hex color — MA line uses a darkened shade
    ma_window   : days for rolling mean (default 7)
    band_sigmas : std devs for confidence band (default 1)
    """
    series = pd.Series(values, index=dates)
    ma      = series.rolling(ma_window, center=True).mean()
    ma_std  = series.rolling(ma_window, center=True).std()
    ma_upper = ma + band_sigmas * ma_std
    ma_lower = ma - band_sigmas * ma_std

    dark = _darken(color, factor=0.4)

    ax.plot(ma.index, ma.values,
            color=dark, linewidth=2.4, alpha=0.8,
            linestyle='--', label=f'{ma_window}-day MA')
    ax.fill_between(ma.index, ma_lower, ma_upper,
                    color=dark, alpha=0.40,
                    label=f'\u00b1{band_sigmas}\u03c3 band ({ma_window}d)')

    try:
        from statsmodels.nonparametric.smoothers_lowess import lowess
        smooth = lowess(values, mdates.date2num(dates), frac=0.15)
        ax.plot(mdates.num2date(smooth[:, 0]), smooth[:, 1],
                color='black', linewidth=2, alpha=0.7,
                linestyle='-', label='LOWESS')
    except ImportError:
        pass


def _draw_line_chart(ax, df, date_col, log_scale=False, highlight=None,
                     major='year', minor='month',
                     major_fmt=None, minor_fmt=None):
    """
    Draw borough time-series lines on the given Axes, then apply axis formatting.
    Snaps x-axis to whole-month boundaries to eliminate dead whitespace.

    major / minor  — gridline presets: 'year', 'quarter', 'month', 'week', 'day'.
                     Append ':N' for intervals (e.g. 'month:3').
    major_fmt / minor_fmt — label format override (auto-set from preset if None).
    """
    for borough, grp in df.groupby('borough'):
        grp = grp.set_index(date_col).sort_index()
        if highlight:
            lw    = 2.5 if borough == highlight else 0.8
            alpha = 0.95 if borough == highlight else 0.3
        else:
            lw, alpha = 1.5, 0.85

        ax.plot(grp.index, grp['trip_cnt'],
                label=borough,
                color=BOROUGH_COLORS.get(borough, '#999999'),
                linewidth=lw, alpha=alpha)

    # Snap x-axis to whole-month boundaries — no dead whitespace
    x_min, x_max = _snap_date_range(df, date_col)
    ax.set_xlim(x_min, x_max)

    _fmt_time_xaxis(ax, major=major, minor=minor,
                    major_fmt=major_fmt, minor_fmt=minor_fmt)

    if log_scale:
        ax.set_yscale('log')
        ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: fmt_num(x)))
        ax.grid(True, axis='y', which='major', color='gray', alpha=0.5, linewidth=1.3)
    else:
        _fmt_clean_yaxis(ax, df['trip_cnt'].max())

    # Hover tooltips — shows borough, date, and trip count on hover
    _add_line_tooltips(ax, date_axis=True)


def _borough_bar(ax, df, highlight=None, label_fontsize=16):
    """
    Horizontal bar chart of avg daily trips per borough.
    Bars have a 90% black border that stays visible even when the fill is muted.
    """
    avg = df.groupby('borough')['trip_cnt'].mean().sort_values(ascending=True)
    total = avg.sum()

    for i, (borough, val) in enumerate(avg.items()):
        bar_alpha = 0.85
        if highlight:
            bar_alpha = 0.95 if borough == highlight else 0.2
        # edgecolor always at full opacity — shows bar shape even when fill is muted
        ax.barh(borough, val,
                color=BOROUGH_COLORS.get(borough, '#999999'),
                alpha=bar_alpha,
                edgecolor='#1a1a1a', linewidth=0.8)

        pct = val / total * 100
        txt_alpha = 1.0
        if highlight:
            txt_alpha = 1.0 if borough == highlight else 0.3
        ax.text(val * 1.06, i,
                f'{fmt_num(val)} ({pct:.1f}%)',
                va='center', fontsize=label_fontsize,
                color=BOROUGH_COLORS.get(borough, '#444'),
                alpha=txt_alpha)

    ax.set_title('Avg Daily Trips', fontsize=TITLE_SM)
    ax.set_xlabel('Avg Trips / Day')
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: fmt_num(x)))
    ax.set_xlim(right=avg.max() * 1.75)
    ax.grid(False)
    ax.spines['right'].set_visible(False)
    ax.spines['top'].set_visible(False)


def _draw_dow_chart(ax, df, date_col, metric_field='trip_id',
                    group_by_field=None, aggr='count',
                    pct_of_group=True, chart_type='line',
                    first_day_of_week='monday', color=None):
    """
    Day-of-week distribution chart on a given Axes.

    Aggregates data by day of week, optionally grouped by a categorical field.
    Can show raw aggregated values or % of each group's total across the week.

    Two aggregation levels:
      Inner (aggr)       : how to combine the metric per group per day-of-week.
                           'count' = count non-null values (row count if metric is PK)
                           'sum'   = total (e.g. fare_amount)
                           'mean'  = average
      Outer (pct_of_group): if True, convert inner result to % of that group's
                           weekly total. Each group's 7 values sum to 100%.
                           If False, show the raw inner aggregation.

    Parameters
    ----------
    ax               : Axes to draw on
    df               : DataFrame with date_col and metric_field columns
    date_col         : date column to extract day-of-week from
    metric_field     : column to aggregate (default 'trip_id' for counting)
    group_by_field   : column to group by (e.g. 'borough'). None = whole population.
    aggr             : inner aggregation — 'count', 'sum', or 'mean'
    pct_of_group     : if True, convert to % of group total (outer calc).
                       If False, show raw aggregated values.
    chart_type       : 'line' (connected dots) or 'bar' (grouped bars)
    first_day_of_week: 'monday' (ISO, default) or 'sunday' (US convention)
    """
    # ── Day ordering ──────────────────────────────────────────────────
    if first_day_of_week == 'sunday':
        dow_remap = {6: 0, 0: 1, 1: 2, 2: 3, 3: 4, 4: 5, 5: 6}
        day_labels = ['S', 'M', 'T', 'W', 'T', 'F', 'S']
    else:
        dow_remap = {i: i for i in range(7)}
        day_labels = ['M', 'T', 'W', 'T', 'F', 'S', 'S']

    temp = df.copy()
    temp['_dow'] = temp[date_col].dt.dayofweek.map(dow_remap)

    # ── Inner aggregation ─────────────────────────────────────────────
    group_col = group_by_field if group_by_field else '_all'
    if not group_by_field:
        temp[group_col] = 'All'

    agged = (temp.groupby([group_col, '_dow'])[metric_field]
             .agg(aggr)
             .reset_index(name='_value'))

    # ── Outer calculation: % of group total ───────────────────────────
    if pct_of_group:
        totals = agged.groupby(group_col)['_value'].transform('sum')
        agged['_value'] = agged['_value'] / totals * 100

    # ── Plot ──────────────────────────────────────────────────────────
    groups = sorted(agged[group_col].unique())

    if chart_type == 'line':
        for group in groups:
            grp = agged[agged[group_col] == group].sort_values('_dow')
            # Use explicit color if provided (single-borough), else look up from BOROUGH_COLORS
            c = color if color else BOROUGH_COLORS.get(group, '#999999')
            ax.plot(grp['_dow'], grp['_value'],
                    marker='o', markersize=4,
                    color=c, linewidth=1.5, alpha=0.85,
                    label=group)
    elif chart_type == 'bar':
        n = len(groups)
        bar_w = 0.8 / n
        for j, group in enumerate(groups):
            grp = agged[agged[group_col] == group].sort_values('_dow')
            offset = (j - n / 2 + 0.5) * bar_w
            c = color if color else BOROUGH_COLORS.get(group, '#999999')
            ax.bar(grp['_dow'] + offset, grp['_value'], bar_w,
                   color=c, alpha=0.85, label=group,
                   edgecolor='#1a1a1a', linewidth=0.5)

    # ── Axis formatting ───────────────────────────────────────────────
    # DoW uses minor ticks only — no major ticks or gridlines on this chart.
    # Single-letter labels, flat, centered horizontally.
    ax.set_xticks([], minor=False)                          # clear major ticks
    ax.set_xticks(range(7), minor=True)                     # days as minor ticks
    ax.set_xticklabels(day_labels, minor=True, rotation=0, ha='center')
    ax.grid(True, axis='x', which='minor', color='gray', alpha=0.2, linewidth=0.5)
    ax.grid(False, axis='x', which='major')                 # no major x-gridlines
    ax.set_xlabel('Day of Week')

    # Y-axis: clean 25% divisions, percentage or raw format
    data_max = agged['_value'].max()
    _fmt_clean_yaxis(ax, data_max, pct=pct_of_group)

    if pct_of_group:
        ax.set_ylabel('% of Group Total')
        ax.set_title('Day-of-Week %', fontsize=TITLE_SM)
    else:
        label = metric_field.replace('_', ' ').title()
        ax.set_ylabel(label)
        ax.set_title(f'Day-of-Week {label}', fontsize=TITLE_SM)

    ax.spines['right'].set_visible(False)
    ax.spines['top'].set_visible(False)

    # Hover tooltips for line-mode DoW charts
    # (bar-mode tooltips are less reliable with mplcursors — the text labels suffice)
    if chart_type == 'line':
        _add_line_tooltips(ax, date_axis=False, pct=pct_of_group,
                           suffix=metric_field.replace('_', ' '),
                           x_labels=day_labels)


def _sort_group_values(df, group_field, hist_field, sort_order='desc', sort_by='num'):
    """Return sorted list of unique values for a group field.

    Parameters
    ----------
    df          : DataFrame
    group_field : column name to get unique values from
    hist_field  : column used for numeric sorting (sum of this field)
    sort_order  : 'asc' or 'desc'
    sort_by     : 'alpha' = sort on group names, 'num' = sort on sum(hist_field)
    """
    vals = df[group_field].unique()
    if sort_by == 'alpha':
        return sorted(vals, reverse=(sort_order == 'desc'))
    else:  # 'num'
        sums = df.groupby(group_field)[hist_field].sum()
        sums = sums.sort_values(ascending=(sort_order == 'asc'))
        return list(sums.index)


def _draw_histogram(ax, df, hist_field, bins, color='#888888',
                    color_field=None, pct=False, title=None,
                    clip_min=None, clip_max=None,
                    show_labels=False,
                    cumulative_line=False, cumulative_behind=True):
    """
    Draw a histogram on a single Axes, with optional out-of-range annotations,
    bar labels, and a cumulative % line.

    Parameters
    ----------
    ax                 : Axes to draw on
    df                 : DataFrame containing hist_field (and optionally color_field)
    hist_field         : numeric column to bin
    bins               : pre-computed bin edges (array)
    color              : bar fill color when color_field is None (default medium gray)
    color_field        : if provided, stack histograms by this field's unique values
    pct                : if True, y-axis shows % of total instead of raw counts
    title              : panel title (optional)
    clip_min           : annotate records below this value with count + boundary line
    clip_max           : annotate records above this value with count + boundary line
    show_labels        : if True, print the count/% on top of each bar
    cumulative_line    : if True, add a secondary y-axis (0–100%) with a running
                         cumulative % line (Pareto-style)
    cumulative_behind  : if True, cumulative line renders behind the bars (zorder=1).
                         If False, line renders in front (zorder=10).
    """
    if df.empty:
        ax.set_visible(False)
        return

    all_data = df[hist_field].dropna()
    n_total  = len(all_data)

    hist_kwargs = dict(bins=bins, edgecolor='#1a1a1a', linewidth=0.5)

    if pct:
        total = len(all_data)
        if total > 0:
            hist_kwargs['weights'] = np.ones(total) / total * 100

    if color_field and color_field in df.columns:
        groups = sorted(df[color_field].unique())
        data_list = [df[df[color_field] == g][hist_field].dropna() for g in groups]
        colors = [BOROUGH_COLORS.get(g, None) for g in groups]
        if pct:
            all_total = sum(len(d) for d in data_list)
            weights_list = [np.ones(len(d)) / all_total * 100 if len(d) > 0
                           else np.array([]) for d in data_list]
            hist_kwargs['weights'] = weights_list
        counts, _, patches = ax.hist(
            data_list, label=[str(g) for g in groups],
            color=colors, stacked=True, alpha=0.8, **hist_kwargs)
        ax.legend(fontsize=7, framealpha=0.6)
    else:
        counts, _, patches = ax.hist(
            all_data, color=color, alpha=0.8, **hist_kwargs)

    # ── Bar labels ────────────────────────────────────────────────────
    # Print count (or %) centered above each bar.
    if show_labels and n_total > 0:
        # counts is either a 1D array (single group) or 2D (stacked).
        # For stacked, use the last row (total height per bin).
        if isinstance(counts, list) or (hasattr(counts, 'ndim') and counts.ndim == 2):
            bar_heights = counts[-1] if hasattr(counts, 'ndim') else counts[-1]
        else:
            bar_heights = counts
        bin_centers = (bins[:-1] + bins[1:]) / 2
        for x, h in zip(bin_centers, bar_heights):
            if h > 0:
                label_txt = f'{h:.0f}%' if pct else fmt_num(h)
                ax.text(x, h, label_txt, ha='center', va='bottom', fontsize=9)

    # ── Cumulative % line (Pareto) ────────────────────────────────────
    # Secondary y-axis on the right, 0–100%. Running sum of bin counts as
    # a percentage of total, plotted at bin centers.
    if cumulative_line and n_total > 0:
        # Compute raw counts per bin (ignore pct weights — cumulative always uses raw)
        raw_counts, _ = np.histogram(all_data, bins=bins)
        cum_pct = np.cumsum(raw_counts) / raw_counts.sum() * 100
        bin_centers = (bins[:-1] + bins[1:]) / 2

        # ax.twinx() creates a secondary Axes sharing the same x-axis.
        # It has its own independent y-axis on the right side.
        ax2 = ax.twinx()
        z = 1 if cumulative_behind else 10
        ax2.plot(bin_centers, cum_pct, color='black', linewidth=1.5,
                 alpha=0.7, marker='.', markersize=4, zorder=z,
                 label='Cumulative %')
        ax2.set_ylim(0, 100)
        ax2.set_ylabel('Cumulative %')
        ax2.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f'{x:.0f}%'))
        ax2.yaxis.set_major_locator(mticker.MultipleLocator(25))  # ticks at 0, 25, 50, 75, 100
        # Make the bars render behind or in front of the line
        if cumulative_behind:
            ax.set_zorder(ax2.get_zorder() + 1)
            ax.patch.set_visible(False)  # make primary axes background transparent

    # ── Out-of-range overlay ──────────────────────────────────────────
    if clip_min is not None and n_total > 0:
        n_below = int((all_data < clip_min).sum())
        if n_below > 0:
            ax.axvline(x=clip_min, color='#d62728', linestyle='--',
                       linewidth=1.2, alpha=0.7)
            pct_below = n_below / n_total * 100
            ax.text(clip_min, ax.get_ylim()[1] * 0.95,
                    f'  \u2190 {n_below:,} ({pct_below:.1f}%) below',
                    ha='left', va='top', fontsize=10, color='#d62728',
                    fontweight='bold')

    if clip_max is not None and n_total > 0:
        n_above = int((all_data > clip_max).sum())
        if n_above > 0:
            ax.axvline(x=clip_max, color='#d62728', linestyle='--',
                       linewidth=1.2, alpha=0.7)
            pct_above = n_above / n_total * 100
            ax.text(clip_max, ax.get_ylim()[1] * 0.95,
                    f'{n_above:,} ({pct_above:.1f}%) above \u2192  ',
                    ha='right', va='top', fontsize=10, color='#d62728',
                    fontweight='bold')

    if title:
        ax.set_title(title, fontsize=TITLE_SM)
    ax.set_xlabel(hist_field.replace('_', ' ').title())
    if pct:
        ax.set_ylabel('% of Total')
    else:
        ax.set_ylabel('Count')

    # Clean y-axis with 25% divisions
    auto_ymax = ax.get_ylim()[1]
    _fmt_clean_yaxis(ax, auto_ymax, pct=pct)

    ax.spines['right'].set_visible(False)
    ax.spines['top'].set_visible(False)


def _fmt_box_label(val):
    """Round a box-plot stat value to a readable label.
    Large values use K/M scaling; small values get 1 decimal."""
    if abs(val) >= 1000:
        return fmt_num(val)
    return f'{val:.1f}'


def _draw_boxplot(ax, df, value_field, group_field=None,
                  orientation='vertical', color_field=None,
                  show_outliers=True, show_means=False, notch=False,
                  whis=1.5, show_labels=False, show_strip=False,
                  strip_max_points=5000, show_axis=True,
                  clip_min=None, clip_max=None,
                  view_min=None, view_max=None, title=None):
    """
    Draw box-whisker plot(s) on a single Axes.

    Parameters
    ----------
    ax               : Axes to draw on
    df               : DataFrame containing value_field
    value_field      : numeric column for the distribution
    group_field      : categorical column — one box per unique value, sorted by median desc
    orientation      : 'vertical' or 'horizontal'
    color_field      : field to color boxes by (default: uses group_field colors)
    show_outliers    : show individual outlier dots beyond whiskers
    show_means       : overlay a diamond marker for the mean
    notch            : draw notched boxes (confidence interval around median)
    whis             : whisker extent as IQR multiplier (default 1.5). Use (0, 100) for min/max.
    show_labels      : annotate boxes with rounded values for whisker_lo, Q1, median, Q3, whisker_hi
    show_strip       : overlay individual data points as jittered dots (auto-sampled above threshold)
    strip_max_points : max points per group for strip overlay (default 5000)
    show_axis        : if False, hide value axis ticks/labels (useful when show_labels is on)
    clip_min         : annotate records below this value + draw boundary line
    clip_max         : annotate records above this value + draw boundary line
    title            : panel title
    """
    if df.empty:
        ax.set_visible(False)
        return

    vert = orientation == 'vertical'
    all_data = df[value_field].dropna()

    # ── Group data and sort by median desc ────────────────────────────
    if group_field:
        medians = df.groupby(group_field)[value_field].median().sort_values(ascending=False)
        groups = list(medians.index)
        data_list = [df[df[group_field] == g][value_field].dropna().values for g in groups]
        labels = [str(g).title() for g in groups]
    else:
        groups = [None]
        data_list = [all_data.values]
        labels = [value_field.replace('_', ' ').title()]

    # ── Colors ────────────────────────────────────────────────────────
    cf = color_field or group_field
    colors = []
    for g in groups:
        if g and cf and g in BOROUGH_COLORS:
            colors.append(BOROUGH_COLORS[g])
        else:
            colors.append('#888888')

    # ── Draw boxes ────────────────────────────────────────────────────
    # patch_artist=True makes boxes fillable rectangles instead of line outlines.
    # Box width scales with the number of groups — wider when fewer groups
    # so the box fills the available space instead of being a narrow sliver.
    n_groups = len(data_list)
    box_width = min(0.8, max(0.4, 1.5 / n_groups))  # wider for fewer groups

    bp = ax.boxplot(data_list, vert=vert, patch_artist=True,
                    widths=box_width,
                    notch=notch, showfliers=show_outliers,
                    showmeans=show_means, whis=whis,
                    meanprops=dict(marker='D', markerfacecolor='black',
                                   markeredgecolor='black', markersize=5),
                    flierprops=dict(marker='.', markersize=2, alpha=0.3),
                    medianprops=dict(color='black', linewidth=1.5))

    # Tighten the group axis so boxes fill the space — no dead margins
    if vert:
        ax.set_xlim(0.25, n_groups + 0.75)
    else:
        ax.set_ylim(0.25, n_groups + 0.75)

    for i, patch in enumerate(bp['boxes']):
        patch.set_facecolor(colors[i])
        patch.set_alpha(0.7)
        patch.set_edgecolor('#1a1a1a')
        patch.set_linewidth(0.8)

    # ── Strip / jitter overlay ────────────────────────────────────────
    if show_strip:
        for i, data in enumerate(data_list):
            n = len(data)
            if n == 0:
                continue
            if n > strip_max_points:
                rng = np.random.default_rng(42)
                sample = rng.choice(data, strip_max_points, replace=False)
            else:
                sample = data
            jitter = np.random.default_rng(42).uniform(-0.15, 0.15, len(sample))
            pos = i + 1
            if vert:
                ax.scatter(pos + jitter, sample, alpha=0.1, s=2,
                           color='black', zorder=0)
            else:
                ax.scatter(sample, pos + jitter, alpha=0.1, s=2,
                           color='black', zorder=0)

    # ── Value labels on boxes ─────────────────────────────────────────
    if show_labels:
        for i, data in enumerate(data_list):
            if len(data) == 0:
                continue
            q1  = np.percentile(data, 25)
            med = np.percentile(data, 50)
            q3  = np.percentile(data, 75)
            iqr = q3 - q1
            # Whisker bounds: furthest data point within whis * IQR
            if isinstance(whis, (list, tuple)):
                w_lo = np.percentile(data, whis[0])
                w_hi = np.percentile(data, whis[1])
            else:
                w_lo = data[data >= q1 - whis * iqr].min() if len(data[data >= q1 - whis * iqr]) > 0 else q1
                w_hi = data[data <= q3 + whis * iqr].max() if len(data[data <= q3 + whis * iqr]) > 0 else q3

            pos = i + 1
            stats = [w_lo, q1, med, q3, w_hi]
            if vert:
                for val in stats:
                    ax.text(pos + 0.4, val, _fmt_box_label(val),
                            va='center', ha='left', fontsize=9, color='#333')
            else:
                for val in stats:
                    ax.text(val, pos + 0.45, _fmt_box_label(val),
                            va='bottom', ha='center', fontsize=9, color='#333')

    # ── Out-of-range overlay ──────────────────────────────────────────
    n_total = len(all_data)
    if clip_min is not None and n_total > 0:
        n_below = int((all_data < clip_min).sum())
        if n_below > 0:
            line_fn = ax.axhline if vert else ax.axvline
            line_fn(clip_min, color='#d62728', linestyle='--', linewidth=1.2, alpha=0.7)
            pct_below = n_below / n_total * 100
            if vert:
                ax.text(len(data_list) + 0.5, clip_min,
                        f'{n_below:,} ({pct_below:.1f}%) below',
                        va='center', fontsize=10, color='#d62728', fontweight='bold')
            else:
                ax.text(clip_min, len(data_list) + 0.5,
                        f'{n_below:,} ({pct_below:.1f}%) below',
                        ha='center', fontsize=10, color='#d62728', fontweight='bold')

    if clip_max is not None and n_total > 0:
        n_above = int((all_data > clip_max).sum())
        if n_above > 0:
            line_fn = ax.axhline if vert else ax.axvline
            line_fn(clip_max, color='#d62728', linestyle='--', linewidth=1.2, alpha=0.7)
            pct_above = n_above / n_total * 100
            if vert:
                ax.text(len(data_list) + 0.5, clip_max,
                        f'{n_above:,} ({pct_above:.1f}%) above',
                        va='center', fontsize=10, color='#d62728', fontweight='bold')
            else:
                ax.text(clip_max, len(data_list) + 0.5,
                        f'{n_above:,} ({pct_above:.1f}%) above',
                        ha='center', fontsize=10, color='#d62728', fontweight='bold')

    # ── Axis formatting ───────────────────────────────────────────────
    # view_min / view_max override the auto-computed axis limits AFTER
    # _fmt_clean_yaxis sets clean tick marks. Data is NOT filtered — box
    # stats (Q1, median, Q3, whiskers) still reflect the full dataset.
    # This just zooms the visible window to a reasonable range.
    #
    # Fallback: if view_max/view_min aren't set, use clip_max/clip_min.
    # Setting clip_max=60 almost always means "I want to see 0–60" — having
    # to also pass view_max=60 is redundant.
    eff_view_max = view_max if view_max is not None else clip_max
    eff_view_min = view_min if view_min is not None else clip_min

    if vert:
        ax.set_xticklabels(labels)
        ax.set_ylabel(value_field.replace('_', ' ').title())
        v_max = eff_view_max if eff_view_max is not None else ax.get_ylim()[1]
        v_min = eff_view_min if eff_view_min is not None else 0
        _fmt_clean_yaxis(ax, v_max)
        ax.set_ylim(v_min, ax.get_ylim()[1])  # respect _fmt_clean_yaxis ceiling
        if not show_axis:
            ax.yaxis.set_visible(False)
    else:
        ax.set_yticklabels(labels)
        ax.set_xlabel(value_field.replace('_', ' ').title())
        v_max = eff_view_max if eff_view_max is not None else ax.get_xlim()[1]
        v_min = eff_view_min if eff_view_min is not None else 0
        ylim, step = _nice_ylim(v_max)
        ax.set_xlim(v_min, ylim)
        ax.set_xticks([step * i for i in range(5)])
        ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: fmt_num(x)))
        if not show_axis:
            ax.xaxis.set_visible(False)

    if title:
        ax.set_title(title, fontsize=TITLE_SM)
    ax.spines['right'].set_visible(False)
    ax.spines['top'].set_visible(False)
    ax.grid(True, axis='y' if vert else 'x', which='major',
            color='gray', alpha=0.3, linewidth=0.5)


def plot_boxplot(df, value_field, group_field=None,
                 orientation='vertical', color_field=None,
                 show_outliers=True, show_means=False, notch=False,
                 whis=1.5, show_labels=False, show_strip=False,
                 strip_max_points=5000, show_axis=True,
                 row_group=None, row_sort='desc', row_sort_by='num',
                 col_group=None, col_sort='desc', col_sort_by='num',
                 shared_y=True, clip_min=None, clip_max=None,
                 view_min=None, view_max=None,
                 panel_width=None, panel_height=4.0):
    """
    Box-whisker plot with optional small-multiples via row/col grouping.

    Parameters
    ----------
    df               : DataFrame
    value_field      : numeric column for the distribution
    group_field      : categorical — one box per value, sorted by median descending
    orientation      : 'vertical' or 'horizontal'
    color_field      : field to color boxes by (default: group_field → BOROUGH_COLORS)
    show_outliers    : show outlier dots beyond whiskers (default True)
    show_means       : overlay diamond marker for the mean (default False)
    notch            : notched boxes — CI around median (default False)
    whis             : whisker extent as IQR multiplier (default 1.5). (0,100) for min/max.
    show_labels      : annotate whisker_lo, Q1, median, Q3, whisker_hi values (default False)
    show_strip       : overlay jittered data points (auto-sampled above threshold)
    strip_max_points : max points per group for strip (default 5000)
    show_axis        : show value axis ticks/labels (default True)
    row_group        : field for row-wise trellis
    row_sort         : 'asc' or 'desc'
    row_sort_by      : 'alpha' or 'num'
    col_group        : field for column-wise trellis
    col_sort         : 'asc' or 'desc'
    col_sort_by      : 'alpha' or 'num'
    shared_y         : same value-axis range across panels (default True)
    clip_min         : annotate + boundary line for records below this value
    clip_max         : annotate + boundary line for records above this value
    panel_width      : width per panel (inches). Default = CHART_WIDTH / n_cols
    panel_height     : height per panel (inches). Default = 4.0
    """
    # ── Determine row/col groups ──────────────────────────────────────
    if row_group:
        row_vals = _sort_group_values(df, row_group, value_field, row_sort, row_sort_by)
    else:
        row_vals = [None]
    if col_group:
        col_vals = _sort_group_values(df, col_group, value_field, col_sort, col_sort_by)
    else:
        col_vals = [None]

    n_rows = len(row_vals)
    n_cols = len(col_vals)

    # ── Figure sizing ─────────────────────────────────────────────────
    # Auto-scale panel width based on number of groups within each panel.
    # A single ungrouped box doesn't need 12 inches — scale proportionally.
    # ~1.5 inches per group, clamped between 3 and CHART_WIDTH.
    if panel_width:
        pw = panel_width
    else:
        n_boxes = len(df[group_field].unique()) if group_field else 1
        pw = min(CHART_WIDTH, max(3, n_boxes * 1.5)) / max(n_cols, 1)
    fig_w = pw * n_cols
    fig_h = panel_height * n_rows

    share_ax = 'sharey' if orientation == 'vertical' else 'sharex'
    fig, axes = plt.subplots(n_rows, n_cols,
                             figsize=(fig_w, fig_h), dpi=CHART_DPI,
                             squeeze=False,
                             **{share_ax: shared_y})

    # ── Draw per panel ────────────────────────────────────────────────
    for r, row_val in enumerate(row_vals):
        for c, col_val in enumerate(col_vals):
            ax = axes[r, c]
            mask = pd.Series(True, index=df.index)
            if row_group and row_val is not None:
                mask &= df[row_group] == row_val
            if col_group and col_val is not None:
                mask &= df[col_group] == col_val
            panel_df = df[mask]

            title_parts = []
            if row_val is not None:
                title_parts.append(str(row_val).title())
            if col_val is not None:
                title_parts.append(str(col_val).title())
            title = ' | '.join(title_parts) if title_parts else None

            _draw_boxplot(ax, panel_df, value_field,
                         group_field=group_field,
                         orientation=orientation,
                         color_field=color_field,
                         show_outliers=show_outliers,
                         show_means=show_means,
                         notch=notch, whis=whis,
                         show_labels=show_labels,
                         show_strip=show_strip,
                         strip_max_points=strip_max_points,
                         show_axis=show_axis,
                         clip_min=clip_min, clip_max=clip_max,
                         view_min=view_min, view_max=view_max,
                         title=title)

            if r < n_rows - 1:
                ax.set_xlabel('')
            if c > 0:
                ax.set_ylabel('')

    plt.tight_layout(pad=0.5)
    plt.show()


def _wrap_title(text, max_chars=15):
    """Word-wrap a title string, splitting on spaces to stay within max_chars per line."""
    words = text.split()
    lines, current = [], ''
    for w in words:
        if current and len(current) + 1 + len(w) > max_chars:
            lines.append(current)
            current = w
        else:
            current = f'{current} {w}'.strip() if current else w
    if current:
        lines.append(current)
    return '\n'.join(lines)


def plot_indicators(df, fields, max_cols=5, panel_width=2.0, panel_height=3.0):
    """
    Indicator field audit — small-multiples bar chart for 0/1 fields.

    For each field, shows a bar chart of distinct values as % of total.
    Expected values (0, 1) are gray; unexpected values (NULL, anything else)
    are red to draw immediate attention to data quality issues.

    Layout:
      - Columns = fields, rows = chart types (currently 1 row: bar chart)
      - Wraps to multiple rows of columns if len(fields) > max_cols
      - Stats (n, count distinct) annotated above each column

    Parameters
    ----------
    df           : DataFrame
    fields       : list of column names to audit (e.g. indicator _ind columns)
    max_cols     : max columns per row before wrapping (default 5)
    panel_width  : width per panel in inches (default 2.0)
    panel_height : height per panel in inches (default 3.0)
    """
    n_fields = len(fields)
    n_cols   = min(n_fields, max_cols)
    n_rows   = -(-n_fields // max_cols)   # ceiling division

    fig_w = panel_width * n_cols
    fig_h = panel_height * n_rows
    fig, axes = plt.subplots(n_rows, n_cols,
                             figsize=(fig_w, fig_h), dpi=CHART_DPI,
                             squeeze=False, sharey=True)

    # Shared y-axis: 0–110% — extra headroom so 100% bar labels don't clip.
    # Ticks only at 0/25/50/75/100 — the 100–110 zone is unlabeled padding.
    for ax_row in axes:
        for ax in ax_row:
            ax.set_ylim(0, 110)
            ax.set_yticks([0, 25, 50, 75, 100])
            ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f'{x:.0f}%'))
            ax.grid(True, axis='y', which='major', color='gray', alpha=0.3, linewidth=0.5)
            ax.grid(False, axis='x')            # kill x-gridlines from theme
            ax.spines['right'].set_visible(False)
            ax.spines['top'].set_visible(False)

    for idx, field in enumerate(fields):
        r = idx // max_cols
        c = idx % max_cols
        ax = axes[r, c]

        col_data = df[field]
        n_total = len(col_data)

        # ── Build value counts including NULLs ────────────────────────
        # Replace NaN with the string 'NULL' so it shows as a bar
        clean = col_data.fillna('NULL').astype(str)
        counts = clean.value_counts()

        # ── Sort: 0, 1 first (natural order), then anomalies by count desc ──
        expected = []
        anomalies = []
        for val in counts.index:
            if val in ('0', '0.0', '1', '1.0'):
                expected.append(val)
            else:
                anomalies.append(val)
        expected.sort()       # '0' before '1'
        ordered = expected + anomalies
        counts = counts.reindex(ordered)

        pcts = counts / n_total * 100
        n_distinct = len(counts)

        # ── Bar colors: gray for expected, red for anomalies ──────────
        colors = []
        edge_colors = []
        for val in ordered:
            if val in ('0', '0.0'):
                colors.append('#cccccc')
                edge_colors.append('#555555')
            elif val in ('1', '1.0'):
                colors.append('#888888')
                edge_colors.append('#555555')
            else:
                colors.append('#d62728')
                edge_colors.append('#a01c1c')

        # ── Draw bars ─────────────────────────────────────────────────
        x_pos = range(len(ordered))
        bars = ax.bar(x_pos, pcts.values, color=colors,
                       edgecolor=edge_colors, linewidth=0.8, alpha=0.85)

        # ── Mark labels: % on top of each bar, +1 font ───────────────
        for xi, (_, pct_val) in enumerate(zip(bars, pcts.values)):
            if pct_val > 0:
                ax.text(xi, pct_val + 1.5, f'{pct_val:.3f}%',
                        ha='center', va='bottom', fontsize=TITLE_SM - 1,
                        fontweight='bold' if ordered[xi] not in ('0', '0.0', '1', '1.0') else 'normal',
                        color='#d62728' if ordered[xi] not in ('0', '0.0', '1', '1.0') else '#333')

        # ── X-axis: distinct values as labels ─────────────────────────
        # Clean display: '0.0' → '0', '1.0' → '1'
        display_labels = []
        for val in ordered:
            if val == '0.0':
                display_labels.append('0')
            elif val == '1.0':
                display_labels.append('1')
            else:
                display_labels.append(val)
        ax.set_xticks(x_pos)
        ax.set_xticklabels(display_labels, rotation=0, ha='center')

        # ── Column header: word-wrapped field name ────────────────────
        clean_name = field.replace('_', ' ').title()
        wrapped    = _wrap_title(clean_name, max_chars=15)
        ax.set_title(wrapped, fontsize=TITLE_SM, fontweight='bold',
                     linespacing=1.1, pad=28)  # pad between title and subtitle

        # ── Stats annotation between title and plot ───────────────────
        stats_text = f'n={fmt_num(n_total)}  |  distinct={n_distinct}'
        ax.text(0.5, 1.08, stats_text, transform=ax.transAxes,
                ha='center', va='bottom', fontsize=TITLE_SM - 1, color='#666')

        # Only show y-label on leftmost column of each row
        if c == 0:
            ax.set_ylabel('% of Total')
        else:
            ax.set_ylabel('')

    # ── Hide unused panels ────────────────────────────────────────────
    for idx in range(n_fields, n_rows * n_cols):
        r = idx // max_cols
        c = idx % max_cols
        axes[r, c].set_visible(False)

    plt.tight_layout(pad=0.5)
    plt.show()


def _draw_string_bars(ax, df, field, top_n=None, show_cumulative=True,
                      show_pct=True, show_length=False, sort_by='freq',
                      color='#888888', title_lines=2):
    """
    Draw a horizontal bar chart of value frequencies for a single string field.
    If show_cumulative=True, adds a secondary y-axis with a cumulative % line.

    Parameters
    ----------
    ax              : Axes to draw on
    df              : DataFrame
    field           : column name
    top_n           : max values to show (default: CARDINALITY_THRESHOLD_STR)
    show_cumulative : add cumulative % line on secondary y-axis (0–100%)
    show_pct        : label bars with % (True) or raw count (False)
    show_length     : annotate string length next to each value label
    sort_by         : 'freq' (default), 'alpha', or 'length'
    color           : bar fill color
    """
    if top_n is None:
        top_n = CARDINALITY_THRESHOLD_STR

    col_data = df[field]
    n_total  = len(col_data)
    n_nulls  = int(col_data.isna().sum())
    n_distinct = col_data.nunique()

    # Include NULLs as a visible value
    clean = col_data.fillna('NULL').astype(str)
    counts = clean.value_counts()

    # Sort
    if sort_by == 'alpha':
        counts = counts.sort_index()
    elif sort_by == 'length':
        counts = counts.iloc[counts.index.str.len().argsort()]
    # else 'freq' — already sorted by value_counts

    # Take top N
    top = counts.head(top_n)
    tail_count = counts.iloc[top_n:].sum() if len(counts) > top_n else 0
    tail_distinct = max(0, len(counts) - top_n)

    pcts = top / n_total * 100
    cum_pcts = pcts.cumsum()

    # ── Draw horizontal bars ──────────────────────────────────────────
    y_pos = list(range(len(top) - 1, -1, -1))  # top value at the top
    values = list(top.index)
    bar_pcts = list(pcts.values)

    ax.barh(y_pos, bar_pcts, color=color, alpha=0.8,
            edgecolor='#1a1a1a', linewidth=0.5)

    # ── Value labels on y-axis ────────────────────────────────────────
    display_labels = []
    for v in values:
        lbl = str(v)
        if show_length:
            lbl = f'{v} ({len(str(v))} chars)'
        display_labels.append(lbl)
    ax.set_yticks(y_pos)
    ax.set_yticklabels(display_labels, fontsize=TITLE_SM - 1)

    # ── Bar-end labels: % or count (+1.5 font from previous) ─────────
    for yi, (pct_val, raw_count) in enumerate(zip(bar_pcts, top.values)):
        y = len(top) - 1 - yi
        if show_pct:
            label_txt = f'{pct_val:.1f}%'
        else:
            label_txt = fmt_num(raw_count)
        ax.text(pct_val + 0.5, y, label_txt,
                va='center', ha='left', fontsize=TITLE_SM + 0.5, color='#333')

    # ── Cumulative % as secondary y-axis line ─────────────────────────
    # ── X-axis: 0–108% — extra padding so bar labels don't clip
    ax.set_xlim(0, 108)
    ax.set_xticks([0, 25, 50, 75, 100])
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f'{x:.0f}%'))
    ax.set_xlabel('% of Total and Cumulative %')
    ax.grid(False)                                                          # clear all
    ax.grid(True, axis='x', which='major', color='gray', alpha=0.3, linewidth=0.5)  # vertical gridlines at 0/25/50/75/100%

    if show_cumulative and len(bar_pcts) > 1:
        ax2 = ax.twiny()  # secondary x-axis on top (shares y-axis)
        ax2.plot(list(cum_pcts.values), y_pos, color='black',
                 linewidth=1.5, alpha=0.6, marker='.', markersize=4)
        # Sync with bottom axis range — both 0–108
        ax2.set_xlim(0, 108)
        # Hide all top axis labels and ticks — bottom axis is the single source
        ax2.set_xticks([])
        ax2.tick_params(axis='x', length=0)

    # ── Title: field name — padded to uniform line count ─────────────
    # Pad with blank lines ABOVE the text so that:
    #   - tight_layout allocates the same space for every panel
    #   - the visible title text sits at the BOTTOM of that space
    #   - all titles are bottom-aligned (top edge varies, bottom edge is uniform)
    clean_name = field.replace('_', ' ').title()
    wrapped = _wrap_title(clean_name, max_chars=15)
    n_lines = wrapped.count('\n') + 1
    if n_lines < title_lines:
        wrapped = '\n' * (title_lines - n_lines) + wrapped
    ax.set_title(wrapped, fontsize=TITLE_SM, fontweight='bold', loc='left',
                 linespacing=1.2, pad=34)

    # Stats subtitle — pushed above the cumulative % top axis labels
    stats = f'n={fmt_num(n_total)}  |  distinct={n_distinct}  |  null={fmt_num(n_nulls)}'
    if tail_distinct > 0:
        stats += f'  |  tail={tail_distinct} values ({tail_count/n_total*100:.1f}%)'
    ax.text(0.0, 1.10, stats, transform=ax.transAxes,
            ha='left', va='bottom', fontsize=TITLE_SM - 1, color='#666')

    ax.spines['right'].set_visible(False)
    ax.spines['top'].set_visible(False)


def plot_string_profile(df, fields, top_n=None, show_cumulative=True,
                        show_pct=True, show_length=False, sort_by='freq',
                        max_cols=3, panel_width=4.0, panel_height=4.0,
                        dpi=240):
    """
    Low-cardinality string field profiler — horizontal bar chart per field.

    Uniform grid — every panel has the same height regardless of how many
    bars it contains. Bars are top-aligned within each panel, not stretched.

    Parameters
    ----------
    df              : DataFrame
    fields          : list of string column names to profile
    top_n           : max values per field (default: CARDINALITY_THRESHOLD_STR)
    show_cumulative : cumulative % line on secondary axis (default True)
    show_pct        : label bars with % (True) or raw count (False)
    show_length     : annotate string length next to each value label
    sort_by         : 'freq' (default), 'alpha', or 'length'
    max_cols        : chart columns in the grid (default 3)
    panel_width     : width per chart panel in inches (default 4.0)
    panel_height    : height per chart panel in inches (default 4.0 — fixed)
    """
    if top_n is None:
        top_n = CARDINALITY_THRESHOLD_STR

    n_fields   = len(fields)
    n_cols     = min(n_fields, max_cols)
    n_rows     = -(-n_fields // n_cols)

    # Pre-compute the max title line count so all panels get padded equally.
    # This ensures tight_layout allocates the same space above every panel.
    max_title_lines = 1
    for field in fields:
        clean = field.replace('_', ' ').title()
        wrapped = _wrap_title(clean, max_chars=15)
        n_lines = wrapped.count('\n') + 1
        max_title_lines = max(max_title_lines, n_lines)

    fig_w = panel_width * n_cols
    fig_h = panel_height * n_rows

    fig, axes = plt.subplots(n_rows, n_cols,
                             figsize=(fig_w, fig_h), dpi=dpi,
                             squeeze=False)

    for idx, field in enumerate(fields):
        r = idx // n_cols
        c = idx % n_cols
        ax = axes[r, c]

        _draw_string_bars(ax, df, field, top_n=top_n,
                         show_cumulative=show_cumulative,
                         title_lines=max_title_lines,
                         show_pct=show_pct, show_length=show_length,
                         sort_by=sort_by, color='#888888')

        # Let each panel auto-fit its y-axis to the actual number of bars.
        # Panels with fewer values get thicker bars but use their space well.

    # Hide unused panels
    for idx in range(n_fields, n_rows * n_cols):
        r = idx // n_cols
        c = idx % n_cols
        axes[r, c].set_visible(False)

    plt.tight_layout(pad=0.8, h_pad=4.0)
    plt.show()


def _draw_hc_string_panel(axes_triple, df, field, top_n=20, title_lines=2):
    """
    Draw a 3-panel profile for a single high-cardinality string field.

    Parameters
    ----------
    axes_triple : tuple of (ax_freq, ax_len, ax_topn) — three vertically stacked Axes
    df          : DataFrame
    field       : column name
    top_n       : number of most frequent values to show in the top-N bar chart
    title_lines : max title line count for uniform padding across columns
    """
    ax_freq, ax_len, ax_topn = axes_triple

    col_data   = df[field]
    n_total    = len(col_data)
    n_nulls    = int(col_data.isna().sum())
    clean      = col_data.fillna('NULL').astype(str)
    n_distinct = clean.nunique()
    dup_pct    = (1 - n_distinct / max(n_total, 1)) * 100

    # Value counts for frequency distribution
    counts = clean.value_counts()

    # ── Panel 1: Frequency distribution (log-scale bins) ──────────────
    # Bin the value counts: "how many values appear 1x, 2-5x, 6-10x, ..."
    # This reveals whether the field is power-law (few dominant values)
    # or uniform (many values with similar frequency).
    # Build bins dynamically — only include edges that are within the data range.
    # Each (low, high, label) defines one bin.
    bin_defs = [
        (0,     1,     '1'),
        (1,     5,     '2-5'),
        (5,     10,    '6-10'),
        (10,    50,    '11-50'),
        (50,    100,   '51-100'),
        (100,   1000,  '101-1K'),
        (1000,  10000, '1K-10K'),
        (10000, float('inf'), '10K+'),
    ]
    max_count = counts.max()
    # Keep only bins that could contain data
    active = [(lo, hi, lbl) for lo, hi, lbl in bin_defs if lo < max_count]
    if not active:
        active = [bin_defs[0]]
    # Cap the last bin at max_count + 1
    lo, _, lbl = active[-1]
    active[-1] = (lo, max_count + 1, lbl)

    freq_bins   = [active[0][0]] + [hi for _, hi, _ in active]
    freq_labels = [lbl for _, _, lbl in active]

    freq_hist = pd.cut(counts, bins=freq_bins, labels=freq_labels,
                       right=True, include_lowest=True)
    freq_counts = freq_hist.value_counts().reindex(freq_labels).fillna(0)

    x_pos = range(len(freq_counts))
    ax_freq.bar(x_pos, freq_counts.values, color='#888888', alpha=0.8,
                edgecolor='#1a1a1a', linewidth=0.5)

    # Mark labels on bars
    for xi, val in enumerate(freq_counts.values):
        if val > 0:
            ax_freq.text(xi, val + freq_counts.max() * 0.02,
                         fmt_num(val), ha='center', va='bottom',
                         fontsize=TITLE_SM, color='#333')

    ax_freq.set_xticks(x_pos)
    ax_freq.set_xticklabels(freq_counts.index, rotation=45, ha='right')
    ax_freq.set_ylabel('# of Values')
    ax_freq.set_xlabel('Frequency (times a value appears)')
    auto_max = ax_freq.get_ylim()[1]
    _fmt_clean_yaxis(ax_freq, auto_max)
    ax_freq.grid(False, axis='x')  # no vertical gridlines on frequency panel
    ax_freq.spines['right'].set_visible(False)
    ax_freq.spines['top'].set_visible(False)

    # ── Panel 2: String length distribution ───────────────────────────
    # Shows if values are consistently formatted or have anomalies.
    str_lengths = clean.str.len()
    len_min = int(str_lengths.min())
    len_max = int(str_lengths.max())
    len_range = len_max - len_min

    # Smart binning: 1-char bins if range ≤ 30, otherwise auto
    if len_range <= 30:
        len_bins = np.arange(len_min, len_max + 2) - 0.5  # center on integers
    else:
        len_bins = np.linspace(len_min, len_max, min(30, len_range) + 1)

    ax_len.hist(str_lengths, bins=len_bins, color='#888888', alpha=0.8,
                edgecolor='#1a1a1a', linewidth=0.5)

    ax_len.set_ylabel('# of Rows')
    ax_len.set_xlabel('String Length (characters)')
    auto_max = ax_len.get_ylim()[1]
    _fmt_clean_yaxis(ax_len, auto_max)
    ax_len.spines['right'].set_visible(False)
    ax_len.spines['top'].set_visible(False)

    # ── Title + stats (on the top panel) ──────────────────────────────
    clean_name = field.replace('_', ' ').title()
    wrapped = _wrap_title(clean_name, max_chars=15)
    n_lines = wrapped.count('\n') + 1
    if n_lines < title_lines:
        wrapped = '\n' * (title_lines - n_lines) + wrapped
    ax_freq.set_title(wrapped, fontsize=TITLE_SM, fontweight='bold',
                      loc='left', linespacing=1.2, pad=34)

    # Stats subtitle
    most_freq = counts.index[0]
    most_freq_n = counts.iloc[0]
    stats = (f'n={fmt_num(n_total)}  |  distinct={fmt_num(n_distinct)}  |  '
             f'null={fmt_num(n_nulls)}  |  dup={dup_pct:.1f}%')
    ax_freq.text(0.0, 1.10, stats, transform=ax_freq.transAxes,
                 ha='left', va='bottom', fontsize=TITLE_SM - 1, color='#666')

    # Most/least frequent as a second stats line
    least_freq = counts.index[-1]
    least_freq_n = counts.iloc[-1]
    stats2 = (f'top: "{most_freq}" ({fmt_num(most_freq_n)})  |  '
              f'bottom: "{least_freq}" ({fmt_num(least_freq_n)})')
    ax_freq.text(0.0, 1.04, stats2, transform=ax_freq.transAxes,
                 ha='left', va='bottom', fontsize=TITLE_SM - 2, color='#999')

    # Length stats on the bottom panel
    len_stats = (f'len: min={len_min}  med={int(str_lengths.median())}  '
                 f'max={len_max}')
    ax_len.text(0.98, 0.95, len_stats, transform=ax_len.transAxes,
                ha='right', va='top', fontsize=TITLE_SM - 2, color='#666',
                bbox=dict(facecolor='white', alpha=0.7, edgecolor='none'))

    # ── Panel 3: Top-N most frequent values (horizontal bar) ──────────
    # Same layout as plot_string_profile — horizontal bars, % labels,
    # cumulative % line on secondary axis.
    _draw_string_bars(ax_topn, df, field, top_n=top_n,
                      show_cumulative=True, show_pct=True,
                      sort_by='freq', color='#888888')
    ax_topn.set_title('')  # title is on the freq panel above — don't repeat


def plot_string_profile_hc(df, fields, top_n=20, max_cols=3, panel_width=4.0,
                           panel_height=3.0, dpi=240):
    """
    High-cardinality string field profiler — frequency + length + top-N.

    For each field, shows three stacked charts:
      Row 1: frequency distribution histogram — how many values appear N times
      Row 2: string length distribution — character count per value
      Row 3: top-N most frequent values — horizontal bar chart with cumulative %

    Parameters
    ----------
    df           : DataFrame
    fields       : list of high-cardinality string column names
    top_n        : number of most frequent values to show in bar chart (default 20)
    max_cols     : columns in the grid before wrapping (default 3)
    panel_width  : width per column in inches (default 4.0)
    panel_height : height per chart panel in inches (default 3.0 — 3 panels per field)
    dpi          : render resolution (default 240)
    """
    n_fields   = len(fields)
    n_cols     = min(n_fields, max_cols)
    n_field_rows = -(-n_fields // n_cols)

    # Each field gets 3 chart rows (freq + length + top-N)
    n_chart_rows = n_field_rows * 3

    # Pre-compute max title lines for uniform alignment
    max_title_lines = 1
    for field in fields:
        clean = field.replace('_', ' ').title()
        wrapped = _wrap_title(clean, max_chars=15)
        n_lines = wrapped.count('\n') + 1
        max_title_lines = max(max_title_lines, n_lines)

    fig_w = panel_width * n_cols
    fig_h = panel_height * n_chart_rows

    fig, axes = plt.subplots(n_chart_rows, n_cols,
                             figsize=(fig_w, fig_h), dpi=dpi,
                             squeeze=False)

    for idx, field in enumerate(fields):
        field_row = idx // n_cols
        c = idx % n_cols
        # Each field occupies 3 consecutive chart rows
        r_freq = field_row * 3
        r_len  = field_row * 3 + 1
        r_topn = field_row * 3 + 2

        ax_freq = axes[r_freq, c]
        ax_len  = axes[r_len, c]
        ax_topn = axes[r_topn, c]

        _draw_hc_string_panel((ax_freq, ax_len, ax_topn), df, field,
                              top_n=top_n, title_lines=max_title_lines)

    # Hide unused panels
    for idx in range(n_fields, n_field_rows * n_cols):
        field_row = idx // n_cols
        c = idx % n_cols
        for offset in range(3):
            r = field_row * 3 + offset
            if r < n_chart_rows:
                axes[r, c].set_visible(False)

    plt.tight_layout(pad=0.8, h_pad=3.0)
    plt.show()


# ── Flow analysis — From/To matrix and Sankey ─────────────────────────────────

def plot_from_to_matrix(df, from_field='pickup_borough', to_field='dropoff_borough',
                        show_pct=True, log_y=True,
                        show_yticks=True, show_ytick_labels=True,
                        show_gridlines=True,
                        show_row_axis_title=True, show_col_axis_title=True,
                        font_column_headers=0, font_row_labels=0,
                        font_row_axis_title=0, font_col_axis_title=0,
                        font_mark_labels=0, font_ytick_labels=0,
                        font_legend=0, font_title=0,
                        sep_linewidth=1.5, sep_alpha=0.5,
                        borough_sep_linewidth=2.0, borough_sep_alpha=0.7,
                        legend_loc='right',
                        dpi=200, cell_width=1.4, cell_height=0.9):
    """
    From/To bar matrix — one vertical bar per cell, colored by magnitude.

    Rows = From (pickup), Columns = To (dropoff).
    Sorted by most frequent values (highest volume top-left).
    Bar color steps by order of magnitude. Labels above bars.

    Parameters
    ----------
    df                   : DataFrame with from_field and to_field columns
    from_field           : column for rows (default 'pickup_borough')
    to_field             : column for columns (default 'dropoff_borough')
    show_pct             : annotate bars with % of total (default True)
    log_y                : True = log y-axis, False = linear with _nice_ylim
    show_yticks          : show y-axis tick marks (default True)
    show_ytick_labels    : show y-axis tick labels (default True)
    show_gridlines       : show horizontal gridlines (default True)
    show_row_axis_title  : show "From: <field>" label on left side (default True)
    show_col_axis_title  : show "To: <field>" label on top (default True)
    font_column_headers  : offset from TITLE_SM for column header labels
    font_row_labels      : offset from TITLE_SM for row labels
    font_row_axis_title  : offset from TITLE_SM for row axis title
    font_col_axis_title  : offset from TITLE_SM for column axis title
    font_mark_labels     : offset from TITLE_SM for bar value labels
    font_ytick_labels    : offset from TITLE_SM for y-axis tick labels
    font_legend          : offset from TITLE_SM for legend text
    font_title           : offset from TITLE_SM for suptitle
    sep_linewidth        : bottom spine width per cell (default 1.5)
    sep_alpha            : bottom spine alpha per cell (default 0.5)
    borough_sep_linewidth: horizontal separator between rows (default 2.0)
    borough_sep_alpha    : separator alpha (default 0.7)
    legend_loc           : 'right', 'bottom', or 'none' (default 'right')
    dpi                  : render resolution (default 200)
    cell_width           : width per cell in inches (default 1.4)
    cell_height          : height per cell in inches (default 0.9)
    """
    import matplotlib.cm as _cm

    def _fs(offset):
        """Font size helper — TITLE_SM + caller-supplied offset."""
        return TITLE_SM + offset

    # ── Build the cross-tabulation ────────────────────────────────────
    clean_from = df[from_field].fillna('(null)').astype(str)
    clean_to   = df[to_field].fillna('(null)').astype(str)

    ct = pd.crosstab(clean_from, clean_to)
    total = ct.values.sum()

    row_order = ct.sum(axis=1).sort_values(ascending=False).index
    col_order = ct.sum(axis=0).sort_values(ascending=False).index
    ct = ct.reindex(index=row_order, columns=col_order, fill_value=0)

    n_rows_ct, n_cols_ct = ct.shape
    from_labels = [str(r).title() for r in ct.index]
    to_labels   = [str(c).title() for c in ct.columns]
    data = ct.values.astype(float)
    data_max = data.max()

    # ── Stepped color scale by magnitude ──────────────────────────────
    boundaries = [0]
    v = 1
    while v <= data_max * 10:
        boundaries.append(v)
        v *= 10
    n_steps = len(boundaries) - 1
    colormap = _cm.get_cmap('Blues', n_steps)

    def _mag_color(val):
        if val <= 0:
            return '#f0f0f0'
        step = min(np.searchsorted(boundaries, val, side='right') - 1, n_steps - 1)
        return colormap(step / max(n_steps - 1, 1))

    # ── Figure ────────────────────────────────────────────────────────
    fig_w = cell_width * n_cols_ct + 1.8
    fig_h = cell_height * n_rows_ct + 2.0

    fig, axes = plt.subplots(n_rows_ct, n_cols_ct,
                             figsize=(fig_w, fig_h), dpi=dpi,
                             squeeze=False, sharex=True, sharey=True)

    from_label = from_field.replace('_', ' ').title()
    to_label   = to_field.replace('_', ' ').title()

    for i in range(n_rows_ct):
        for j in range(n_cols_ct):
            ax = axes[i, j]
            val = data[i, j]
            color = _mag_color(val)

            ax.bar(0, max(val, 0.5), width=0.8, color=color, alpha=0.85,
                   edgecolor='#1a1a1a', linewidth=0.5)

            # Bar value labels
            if val > 0:
                label = fmt_num(val)
                if show_pct:
                    label += f'\n{val / total * 100:.1f}%'
                if log_y:
                    label_y = val * 1.5
                else:
                    label_y = val + data_max * 0.02
                ax.text(0, label_y, label, ha='center', va='bottom',
                        fontsize=_fs(font_mark_labels), color='#333')

            # Y-axis scale
            if log_y:
                ax.set_yscale('log')
                ax.set_ylim(0.5, data_max * 5)
            else:
                ax.set_ylim(0, _nice_ylim(data_max))

            ax.set_xlim(-0.6, 0.6)
            ax.set_xticks([])

            # Y-axis ticks
            if log_y:
                mag_ticks = [b for b in boundaries if b >= 1 and b <= data_max * 5]
                ax.set_yticks(mag_ticks)
                ax.set_yticklabels([fmt_num(t) for t in mag_ticks],
                                   fontsize=_fs(font_ytick_labels))
            else:
                ax.tick_params(axis='y', labelsize=_fs(font_ytick_labels))

            ax.tick_params(left=show_yticks,
                          labelleft=(show_ytick_labels and j == 0))

            # Horizontal gridlines
            if show_gridlines:
                ax.grid(True, axis='y', which='major', color='gray',
                        alpha=0.51, linewidth=0.5)
            else:
                ax.grid(False)
            ax.grid(False, axis='x')

            ax.spines['top'].set_visible(False)
            ax.spines['right'].set_visible(False)
            ax.spines['bottom'].set_color((0, 0, 0, sep_alpha))
            ax.spines['bottom'].set_linewidth(sep_linewidth)

            # Row labels (left edge)
            if j == 0:
                ax.set_ylabel(from_labels[i], fontsize=_fs(font_row_labels),
                              rotation=0, ha='right', va='center', labelpad=10)
            else:
                ax.set_ylabel('')

            # Column headers (top row)
            if i == 0:
                ax.set_title(to_labels[j], fontsize=_fs(font_column_headers), pad=16)

    # ── Row axis title (rotated, left of row labels) ──────────────────
    if show_row_axis_title:
        fig.text(0.01, 0.5, f'From: {from_label}',
                 fontsize=_fs(font_row_axis_title), fontweight='bold',
                 rotation=90, ha='left', va='center')

    # ── Column axis title (centered, above column headers) ────────────
    if show_col_axis_title:
        fig.text(0.5, 0.99, f'To: {to_label}',
                 fontsize=_fs(font_col_axis_title), fontweight='bold',
                 ha='center', va='top')

    # ── Horizontal separator lines between borough rows ───────────────
    plt.tight_layout(pad=0.5)
    fig.canvas.draw()
    for i in range(n_rows_ct - 1):
        pos_above = axes[i, 0].get_position()
        pos_below = axes[i + 1, 0].get_position()
        y = (pos_above.y0 + pos_below.y1) / 2
        fig.add_artist(mlines.Line2D(
            [0.03, 0.97], [y, y], transform=fig.transFigure,
            color='#333', alpha=borough_sep_alpha,
            linewidth=borough_sep_linewidth, linestyle='-'
        ))

    # ── Legend — magnitude color steps ────────────────────────────────
    if legend_loc != 'none':
        from matplotlib.patches import Patch
        legend_patches = []
        for k in range(n_steps):
            lo = boundaries[k]
            hi = boundaries[k + 1]
            c  = colormap(k / max(n_steps - 1, 1))
            lo_label = fmt_num(lo) if lo > 0 else '0'
            hi_label = fmt_num(hi)
            legend_patches.append(Patch(facecolor=c, edgecolor='#333', linewidth=0.5,
                                        label=f'{lo_label} – {hi_label}'))
        legend_patches.insert(0, Patch(facecolor='#f0f0f0', edgecolor='#ccc',
                                       linewidth=0.5, label='0 trips'))

        if legend_loc == 'bottom':
            fig.legend(handles=legend_patches, title='Trip Count',
                       loc='lower center', bbox_to_anchor=(0.5, -0.08),
                       ncol=len(legend_patches),
                       fontsize=_fs(font_legend) - 2,
                       title_fontsize=_fs(font_legend) - 1)
        else:  # 'right' (default)
            fig.legend(handles=legend_patches, title='Trip Count',
                       loc='center left', bbox_to_anchor=(1.0, 0.5),
                       fontsize=_fs(font_legend) - 2,
                       title_fontsize=_fs(font_legend) - 1)

    fig.suptitle(f'{from_label} \u2192 {to_label} Trip Flow',
                 fontsize=_fs(font_title) + 2, fontweight='bold', y=1.04)

    plt.show()


def plot_sankey(df, from_field='pickup_zone', to_field='dropoff_zone',
                group_field='pickup_borough', to_group_field='dropoff_borough',
                top_n=10, n_cols=2, width=400, height=450):
    """
    Small-multiples Sankey — one diagram per group (borough), tiled 2-wide.

    Left nodes = pickup zones (colored by pickup borough).
    Right nodes = dropoff zones (colored by DROPOFF borough, grouped by borough).
    Shows top N routes per borough by volume.

    Parameters
    ----------
    df              : DataFrame
    from_field      : column for source nodes (default 'pickup_zone')
    to_field        : column for target nodes (default 'dropoff_zone')
    group_field     : column to split small multiples by (default 'pickup_borough')
    to_group_field  : column to color right-side nodes (default 'dropoff_borough')
    top_n           : top N routes per borough (default 10)
    n_cols          : columns in the tiled layout (default 2)
    width           : width per Sankey in pixels (default 400)
    height          : height per Sankey in pixels (default 450)
    """
    try:
        import plotly.graph_objects as go
    except ImportError:
        print('plotly is required. Install with: pip install plotly')
        return

    from IPython.display import display as ipy_display, HTML as ipy_HTML

    clean_from     = df[from_field].fillna('(null)').astype(str)
    clean_to       = df[to_field].fillna('(null)').astype(str)
    clean_group    = df[group_field].fillna('(null)').astype(str)
    clean_to_group = df[to_group_field].fillna('(null)').astype(str)

    # Build zone → borough mapping for coloring right-side nodes
    zone_to_borough = dict(zip(clean_to, clean_to_group))

    group_totals = clean_group.value_counts()
    groups = [g for g in group_totals.index
              if g not in ('(null)', 'n/a', 'unknown')]

    # ── Build HTML for each Sankey ────────────────────────────────────
    sankey_htmls = []
    first = True  # include plotly.js only once

    for group in groups:
        mask = clean_group == group
        flow_df = pd.DataFrame({
            'from': clean_from[mask],
            'to': clean_to[mask],
            'to_borough': clean_to_group[mask],
        })
        flow_counts = (flow_df.groupby(['from', 'to', 'to_borough'])
                       .size().reset_index(name='count'))
        flow_counts = flow_counts.sort_values('count', ascending=False)

        top_flows = flow_counts.head(top_n).copy()
        other_count = flow_counts.iloc[top_n:]['count'].sum() if len(flow_counts) > top_n else 0
        total_group = flow_counts['count'].sum()

        if len(top_flows) == 0:
            continue

        # Build nodes — from zones and to zones (with borough grouping)
        from_vals = list(top_flows['from'].unique())

        # Sort to-nodes by borough then by zone name for visual grouping
        to_with_boro = top_flows[['to', 'to_borough']].drop_duplicates()
        to_with_boro = to_with_boro.sort_values(['to_borough', 'to'])
        to_vals = list(to_with_boro['to'])

        from_nodes = [f'from_{v}' for v in from_vals]
        to_nodes   = [f'to_{v}' for v in to_vals]
        all_nodes  = from_nodes + to_nodes
        node_idx   = {name: i for i, name in enumerate(all_nodes)}

        # Node colors
        borough_color = BOROUGH_COLORS.get(group, '#cccccc')
        node_colors = []
        node_labels = []
        for v in from_vals:
            node_colors.append(borough_color)
            node_labels.append(v)
        for _, row in to_with_boro.iterrows():
            boro = row['to_borough']
            node_colors.append(BOROUGH_COLORS.get(boro, '#cccccc'))
            node_labels.append(f'{row["to"]}')

        # Links
        sources, targets, values, link_colors = [], [], [], []
        for _, r in top_flows.iterrows():
            src = node_idx.get(f'from_{r["from"]}')
            tgt = node_idx.get(f'to_{r["to"]}')
            if src is not None and tgt is not None:
                sources.append(src)
                targets.append(tgt)
                values.append(r['count'])
                # Link color = semi-transparent version of the from-borough color
                h = borough_color.lstrip('#')
                rgb = tuple(int(h[k:k+2], 16) for k in (0, 2, 4))
                link_colors.append(f'rgba({rgb[0]},{rgb[1]},{rgb[2]},0.25)')

        title = f'{group.title()} \u2014 Top {top_n} ({fmt_num(total_group)} trips)'
        if other_count > 0:
            other_pct = other_count / total_group * 100
            title += f'<br><span style="font-size:10px;color:#666">Other: {fmt_num(other_count)} ({other_pct:.1f}%)</span>'

        fig = go.Figure(go.Sankey(
            arrangement='snap',
            node=dict(
                pad=10, thickness=15,
                line=dict(color='#333', width=0.5),
                label=node_labels, color=node_colors,
            ),
            link=dict(
                source=sources, target=targets,
                value=values, color=link_colors,
            ),
        ))
        fig.update_layout(
            title_text=title, font_size=12,
            width=width, height=height,
            margin=dict(l=5, r=5, t=40, b=10),
        )

        js_include = 'cdn' if first else False
        first = False
        sankey_htmls.append(fig.to_html(full_html=False, include_plotlyjs=js_include))

    # ── Tile 2-wide using HTML flexbox ────────────────────────────────
    rows_html = []
    for i in range(0, len(sankey_htmls), n_cols):
        pair = sankey_htmls[i:i + n_cols]
        row = '<div style="display:flex; gap:5px; margin-bottom:10px;">'
        for h in pair:
            row += f'<div style="flex:1; min-width:0;">{h}</div>'
        row += '</div>'
        rows_html.append(row)

    ipy_display(ipy_HTML('\n'.join(rows_html)))


def plot_distribution(df, field, bin_cnt=20, bin_incr=None,
                      bin_min=None, bin_max=None, bin_on_int=False,
                      clip_min=None, clip_max=None,
                      pct=False, show_labels=False,
                      cumulative_line=False, cumulative_behind=True,
                      group_field=None, show_outliers=True,
                      show_means=False, whis=1.5, show_box_labels=False,
                      show_strip=False, strip_max_points=5000,
                      height_ratio=(4, 1),
                      panel_width=None, panel_height=6.0):
    """
    Combined histogram + box-whisker for a single numeric field.

    Layout (shared x-axis):
      ┌────────────────────────────┐
      │      Histogram             │  ← 4/5 of height (configurable)
      │      (vertical bars)       │
      ├────────────────────────────┤
      │  ▐████▌  Box-whisker       │  ← 1/5 of height
      └────────────────────────────┘

    The boxplot is forced horizontal so its value axis aligns with the
    histogram's x-axis. sharex links them — zooming one zooms both.

    Parameters
    ----------
    df               : DataFrame
    field            : numeric column (used by both histogram and boxplot)
    bin_cnt          : number of bins (ignored if bin_incr set)
    bin_incr         : fixed bin width — overrides bin_cnt
    bin_min / bin_max: clip range — constrains axis AND annotates out-of-range
    bin_on_int       : snap bin edges to integers
    clip_min/clip_max: alias for bin_min/bin_max (backward compat)
    pct              : histogram y-axis as % of total
    show_labels      : bar-top labels on histogram
    cumulative_line  : Pareto cumulative % line on histogram
    cumulative_behind: cumulative line behind (True) or in front (False)
    group_field      : categorical for boxplot grouping (one box per value)
    show_outliers    : boxplot outlier dots
    show_means       : boxplot mean diamond
    whis             : whisker extent (IQR multiplier)
    show_box_labels  : annotate Q1/median/Q3 values on boxes
    show_strip       : jittered data points on boxes
    strip_max_points : auto-sample threshold for strip
    height_ratio     : (histogram, boxplot) height ratio — default (4, 1)
    panel_width      : figure width (default auto from CHART_WIDTH)
    panel_height     : total figure height (default 6.0)
    """
    # ── Compute bins (same logic as plot_histogram) ───────────────────
    data_min = bin_min if bin_min is not None else df[field].min()
    data_max = bin_max if bin_max is not None else df[field].max()
    # Use clip values as fallback for bin range
    if clip_min is not None and bin_min is None:
        data_min = clip_min
    if clip_max is not None and bin_max is None:
        data_max = clip_max

    if bin_on_int:
        data_min = int(math.floor(data_min))
        data_max = int(math.ceil(data_max))

    if bin_incr is not None:
        bins = np.arange(data_min, data_max + bin_incr, bin_incr)
        if bin_on_int:
            bins = np.unique(np.round(bins).astype(int))
    elif bin_on_int:
        bins = np.linspace(data_min, data_max, bin_cnt + 1)
        bins = np.unique(np.round(bins).astype(int))
    else:
        bins = np.linspace(data_min, data_max, bin_cnt + 1)

    # ── Effective clip values ─────────────────────────────────────────
    eff_clip_min = clip_min if clip_min is not None else bin_min
    eff_clip_max = clip_max if clip_max is not None else bin_max

    # ── Figure with GridSpec ──────────────────────────────────────────
    pw = panel_width or CHART_WIDTH
    fig = plt.figure(figsize=(pw, panel_height), dpi=CHART_DPI)
    gs  = fig.add_gridspec(2, 1, height_ratios=list(height_ratio),
                           hspace=0.05)

    ax_hist = fig.add_subplot(gs[0])
    ax_box  = fig.add_subplot(gs[1], sharex=ax_hist)

    # ── Top: histogram ────────────────────────────────────────────────
    _draw_histogram(ax_hist, df, field, bins, color='#888888',
                    pct=pct, show_labels=show_labels,
                    cumulative_line=cumulative_line,
                    cumulative_behind=cumulative_behind,
                    clip_min=eff_clip_min, clip_max=eff_clip_max)

    # Title: field name.  Subtitle: bin config summary.
    title_str = field.replace('_', ' ').title()
    n_bins = len(bins) - 1
    incr_str = str(bin_incr) if bin_incr is not None else 'auto'
    sub_parts = [f'n={n_bins}', f'incr={incr_str}',
                 f'min={fmt_num(bins[0])}', f'max={fmt_num(bins[-1])}']
    subtitle_str = '  |  '.join(sub_parts)
    ax_hist.set_title(f'{title_str}\n', fontsize=TITLE_SM, pad=2)
    # Subtitle as smaller text just below the title
    ax_hist.text(0.5, 1.0, subtitle_str, transform=ax_hist.transAxes,
                 ha='center', va='top', fontsize=TITLE_SM - 2, color='#666')

    # Hide x-axis on histogram — the boxplot below shows it
    plt.setp(ax_hist.get_xticklabels(), visible=False)
    ax_hist.set_xlabel('')

    # ── Bottom: horizontal boxplot ────────────────────────────────────
    # No clip_min/clip_max here — the out-of-range annotations only show
    # on the histogram above. The boxplot just uses view_min/view_max to
    # constrain the axis without adding redundant boundary lines.
    _draw_boxplot(ax_box, df, field,
                  group_field=group_field,
                  orientation='horizontal',
                  show_outliers=show_outliers,
                  show_means=show_means,
                  whis=whis,
                  show_labels=show_box_labels,
                  show_strip=show_strip,
                  strip_max_points=strip_max_points,
                  view_min=data_min, view_max=data_max)
    # The boxplot's x-axis is shared — _draw_boxplot may have set xlim,
    # but sharex keeps it aligned with the histogram's bin range.
    ax_box.set_ylabel('')

    plt.tight_layout(pad=0.5)
    plt.show()


def plot_histogram(df, hist_field, bin_cnt=20, bin_incr=None,
                   bin_min=None, bin_max=None, bin_on_int=False,
                   row_group=None, row_sort='desc', row_sort_by='num',
                   col_group=None, col_sort='desc', col_sort_by='num',
                   color_field=None, pct=False, shared_y=True,
                   show_labels=False,
                   cumulative_line=False, cumulative_behind=True,
                   panel_width=None, panel_height=4.0):
    """
    Histogram with optional small-multiples via row/col grouping.

    Layout:
      - No groups        → single histogram
      - row_group only   → vertical stack (n_rows × 1)
      - col_group only   → horizontal strip (1 × n_cols)
      - both             → full grid (n_rows × n_cols)

    Parameters
    ----------
    df            : DataFrame
    hist_field    : numeric column to bin
    bin_cnt       : number of bins (default 20). Ignored if bin_incr is set.
    bin_incr      : fixed bin width. Overrides bin_cnt. E.g. bin_incr=5 with
                    range 0–50 creates 10 bins of width 5.
    bin_min       : override MIN(hist_field) for bin range. Records below this
                    value are excluded from bins and annotated on the chart
                    with a count + boundary line.
    bin_max       : override MAX(hist_field) for bin range. Same annotation
                    for records above.
    bin_on_int    : if True, snap bin edges to integers (no decimals)
    row_group     : field for row-wise small multiples (one row per unique value)
    row_sort      : 'asc' or 'desc' (default 'desc')
    row_sort_by   : 'alpha' = sort on group name, 'num' = sort on sum(hist_field)
    col_group     : field for column-wise small multiples
    col_sort      : 'asc' or 'desc' (default 'desc')
    col_sort_by   : 'alpha' or 'num'
    color_field   : if provided, stack bars within each panel by this field
    pct           : if True, y-axis shows % of total (default False = raw counts)
    shared_y      : if True, all panels share the same y-axis range (default True)
    panel_width   : width per panel in inches (default auto from CHART_WIDTH)
    panel_height  : height per panel in inches (default 4.0)
    """
    # ── 1. Compute bins from full dataset ─────────────────────────────
    data_min = bin_min if bin_min is not None else df[hist_field].min()
    data_max = bin_max if bin_max is not None else df[hist_field].max()

    if bin_on_int:
        data_min = int(math.floor(data_min))
        data_max = int(math.ceil(data_max))

    if bin_incr is not None:
        # Fixed-width bins: start at data_min, step by bin_incr up through data_max
        bins = np.arange(data_min, data_max + bin_incr, bin_incr)
        if bin_on_int:
            bins = np.unique(np.round(bins).astype(int))
    elif bin_on_int:
        bins = np.linspace(data_min, data_max, bin_cnt + 1)
        bins = np.unique(np.round(bins).astype(int))
    else:
        bins = np.linspace(data_min, data_max, bin_cnt + 1)

    # ── 2. Determine row/col groups with sorting ──────────────────────
    if row_group:
        row_vals = _sort_group_values(df, row_group, hist_field, row_sort, row_sort_by)
    else:
        row_vals = [None]

    if col_group:
        col_vals = _sort_group_values(df, col_group, hist_field, col_sort, col_sort_by)
    else:
        col_vals = [None]

    n_rows = len(row_vals)
    n_cols = len(col_vals)

    # ── 3. Figure sizing ──────────────────────────────────────────────
    pw = panel_width or (CHART_WIDTH / max(n_cols, 1))
    fig_w = pw * n_cols
    fig_h = panel_height * n_rows

    fig, axes = plt.subplots(n_rows, n_cols,
                             figsize=(fig_w, fig_h), dpi=CHART_DPI,
                             squeeze=False,   # always return 2D array
                             sharey=shared_y)

    # ── 4. Draw histogram per panel ───────────────────────────────────
    for r, row_val in enumerate(row_vals):
        for c, col_val in enumerate(col_vals):
            ax = axes[r, c]

            # Filter data for this panel
            mask = pd.Series(True, index=df.index)
            if row_group and row_val is not None:
                mask &= df[row_group] == row_val
            if col_group and col_val is not None:
                mask &= df[col_group] == col_val
            panel_df = df[mask]

            # Panel title: combine row and col values
            title_parts = []
            if row_val is not None:
                title_parts.append(str(row_val).title())
            if col_val is not None:
                title_parts.append(str(col_val).title())
            title = ' | '.join(title_parts) if title_parts else hist_field.replace('_', ' ').title()

            # Panel color: use borough color if group value matches, else gray
            panel_color = '#888888'
            if row_val and row_val in BOROUGH_COLORS:
                panel_color = BOROUGH_COLORS[row_val]
            elif col_val and col_val in BOROUGH_COLORS:
                panel_color = BOROUGH_COLORS[col_val]

            _draw_histogram(ax, panel_df, hist_field, bins,
                           color=panel_color, color_field=color_field,
                           pct=pct, title=title,
                           clip_min=bin_min, clip_max=bin_max,
                           show_labels=show_labels,
                           cumulative_line=cumulative_line,
                           cumulative_behind=cumulative_behind)

            # Column headers on top row only
            if r == 0 and col_group and col_val is not None:
                ax.set_title(str(col_val).title(), fontsize=TITLE_SM, fontweight='bold')

            # Hide x-label on non-bottom rows to reduce clutter
            if r < n_rows - 1:
                ax.set_xlabel('')

            # Hide y-label on non-left columns
            if c > 0:
                ax.set_ylabel('')

    plt.tight_layout(pad=0.5)
    plt.show()


def plot_histograms(df, fields, max_cols=3, panel_width=3.5, panel_height=3.0,
                    bin_incr=1, label_threshold=None):
    """
    Multi-field histogram grid — one panel per field, independent X-axis per panel.

    Built for discrete integer fields with very different ranges
    (e.g. is_holiday 0/1, day_of_week 1-7, pickup_month 1-12, pickup_year 2022).
    Each panel is binned and scaled to its own field. Y-axis is % of total.
    Bins are centered on integers so each integer value gets its own bar.

    Layout follows plot_indicators: wraps to multiple rows when
    len(fields) > max_cols. Y-axis is NOT shared (each panel scales to its own %).

    Parameters
    ----------
    df              : DataFrame containing all fields
    fields          : list of numeric column names — one panel per field
    max_cols        : max columns per row before wrapping (default 3)
    panel_width     : width per panel in inches (default 3.5)
    panel_height    : height per panel in inches (default 3.0)
    bin_incr        : bin width in integer units (default 1 = one bar per integer)
    label_threshold : if set to integer N, panels with fewer than N bars print
                      the % on top of each bar. Panels with >= N bars stay
                      unlabeled to avoid clutter. Default None = never label.
    """
    n_fields = len(fields)
    n_cols   = min(n_fields, max_cols)
    n_rows   = -(-n_fields // max_cols)   # ceiling division

    fig_w = panel_width * n_cols
    fig_h = panel_height * n_rows
    fig, axes = plt.subplots(n_rows, n_cols,
                             figsize=(fig_w, fig_h), dpi=CHART_DPI,
                             squeeze=False, sharey=False, sharex=False)

    for idx, field in enumerate(fields):
        r = idx // max_cols
        c = idx % max_cols
        ax = axes[r, c]

        col_data = df[field].dropna()
        n_total  = len(col_data)

        if n_total == 0:
            ax.set_visible(False)
            continue

        # Integer-centered bins (independent per field). Offset by half so each
        # integer value lands in the center of its own bar — e.g. for 0/1 data,
        # bins=[-0.5, 0.5, 1.5] → bar at x=0 captures 0s, bar at x=1 captures 1s.
        data_min = int(math.floor(col_data.min()))
        data_max = int(math.ceil(col_data.max()))
        half = bin_incr / 2.0
        bins = np.arange(data_min - half, data_max + half + bin_incr, bin_incr)

        n_bars = len(bins) - 1
        show_labels = label_threshold is not None and n_bars < label_threshold

        _draw_histogram(ax, df, field, bins,
                        color='#888888', pct=True,
                        title=None, show_labels=show_labels)

        ax.set_xlabel('')
        if c > 0:
            ax.set_ylabel('')

        # Integer x-ticks when range is small enough to label every value
        n_ticks = data_max - data_min + 1
        if n_ticks <= 15:
            ax.set_xticks(range(data_min, data_max + 1))

        clean_name = field.replace('_', ' ').title()
        wrapped    = _wrap_title(clean_name, max_chars=18)
        ax.set_title(wrapped, fontsize=FONT_PANEL_TITLE, fontweight='bold',
                     linespacing=1.1, pad=28)

        stats_text = f'n={fmt_num(n_total)}'
        ax.text(0.5, 1.08, stats_text, transform=ax.transAxes,
                ha='center', va='bottom', fontsize=FONT_BADGE, color='#666')

        ax.tick_params(axis='both', labelsize=FONT_TICK)

    # Hide unused panels in the trailing row
    for idx in range(n_fields, n_rows * n_cols):
        r = idx // max_cols
        c = idx % max_cols
        axes[r, c].set_visible(False)

    plt.tight_layout(pad=0.5)
    plt.show()


def plot_field_aggregates(df, fields, agg='sum', sort='desc',
                          orientation='horizontal', color='#888888',
                          show_labels=True, panel_width=None,
                          panel_height=None, title=None):
    """
    Bar chart comparing an aggregate (sum, mean, etc.) across multiple fields.

    Built for comparing related numeric/count columns at a glance — e.g.
    trip_count vs airport_pickup_count vs cash_trips vs credit_card_trips.
    Each bar is one field; bar value = agg(df[field]).

    Parameters
    ----------
    df           : DataFrame
    fields       : list of numeric column names — one bar per field
    agg          : aggregation (str or callable). Default 'sum'.
                   Common: 'sum', 'mean', 'median', 'min', 'max', 'count', 'std'.
                   Any callable returning a scalar from a Series also works.
    sort         : 'desc' (default), 'asc', or None to keep input order
    orientation  : 'horizontal' (default — best for long field names) or 'vertical'
    color        : single bar fill color (default medium gray)
    show_labels  : annotate each bar with its formatted value (default True)
    panel_width  : figure width in inches (default auto)
    panel_height : figure height in inches (default auto, scales with n_fields)
    title        : chart title (default: '<AGG> by Field')

    Typography is governed by the module-level FONT_* constants — change those
    once at the top of your notebook to apply across every chart in the report.
    """
    n = len(fields)

    # df[fields].agg(agg) returns a Series indexed by field name
    totals = df[fields].agg(agg)

    if sort == 'desc':
        totals = totals.sort_values(ascending=False)
    elif sort == 'asc':
        totals = totals.sort_values(ascending=True)

    # Auto figure sizing — height grows with field count for horizontal,
    # width grows for vertical
    if orientation == 'horizontal':
        fig_w = panel_width  or CHART_WIDTH
        fig_h = panel_height or max(2.5, n * 0.45)
    else:
        fig_w = panel_width  or max(6, n * 0.9)
        fig_h = panel_height or 4.5

    fig, ax = plt.subplots(figsize=(fig_w, fig_h), dpi=CHART_DPI)

    labels = [f.replace('_', ' ') for f in totals.index]
    agg_label = agg.upper() if isinstance(agg, str) else 'Aggregate'

    if orientation == 'horizontal':
        # matplotlib barh stacks bottom-up, so reverse to put first item at top
        plot_data = totals.iloc[::-1]
        plot_labels = labels[::-1]
        bars = ax.barh(plot_labels, plot_data.values, color=color, alpha=0.85,
                       edgecolor='#1a1a1a', linewidth=0.5)
        ax.set_xlabel(agg_label, fontsize=FONT_AXIS_LABEL)
        ax.xaxis.set_major_formatter(
            mticker.FuncFormatter(lambda x, _: fmt_num(x)))
        if show_labels:
            for bar, val in zip(bars, plot_data.values):
                ax.text(bar.get_width(), bar.get_y() + bar.get_height() / 2,
                        f'  {fmt_num(val)}',
                        va='center', ha='left', fontsize=FONT_BAR_VALUE)
    else:
        bars = ax.bar(labels, totals.values, color=color, alpha=0.85,
                      edgecolor='#1a1a1a', linewidth=0.5)
        ax.set_ylabel(agg_label, fontsize=FONT_AXIS_LABEL)
        ax.yaxis.set_major_formatter(
            mticker.FuncFormatter(lambda x, _: fmt_num(x)))
        plt.setp(ax.get_xticklabels(), rotation=45, ha='right')
        if show_labels:
            for bar, val in zip(bars, totals.values):
                ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height(),
                        f'{fmt_num(val)}',
                        va='bottom', ha='center', fontsize=FONT_BAR_VALUE)

    ax.tick_params(axis='both', labelsize=FONT_TICK)

    ax.set_title(title or f'{agg_label} by Field',
                 fontsize=FONT_TITLE, fontweight='bold')
    ax.spines['right'].set_visible(False)
    ax.spines['top'].set_visible(False)
    ax.grid(True, axis='x' if orientation == 'horizontal' else 'y',
            color='gray', alpha=0.3, linewidth=0.5)

    plt.tight_layout()
    plt.show()


def plot_field_aggregates_by_group(df, fields, group_field, agg='sum',
                                   sort='shared_desc', max_cols=3,
                                   panel_width=4.5, panel_height=None,
                                   shared_x=True, show_labels=True,
                                   color_map=None, default_color='#888888',
                                   suptitle=None):
    """
    Small-multiples bar chart — one panel per unique value of group_field,
    each panel shows the aggregate of every field for that group's subset.

    Built for cross-segment comparison (e.g. compare trip_count, cash_trips,
    credit_trips across boroughs). Shared X-axis by default so bar lengths
    are directly comparable across panels. Each panel is colored by group
    (uses BOROUGH_COLORS by default).

    Parameters
    ----------
    df            : DataFrame
    fields        : list of numeric column names
    group_field   : categorical column — one panel per unique value
    agg           : aggregation (str or callable). Default 'sum'.
                    Common: 'sum', 'mean', 'median', 'min', 'max', 'count', 'std'.
    sort          : 'shared_desc' (default): sort fields by overall agg desc,
                    use that order in every panel — best for cross-panel comparison.
                    'shared_asc' : same, ascending.
                    'panel_desc' : each panel sorts its own fields desc.
                    None         : keep input field order.
    max_cols      : max panels per row before wrapping (default 3)
    panel_width   : width per panel in inches (default 4.5)
    panel_height  : height per panel (default auto: scales with n_fields)
    shared_x      : if True, all panels share the same X-axis range so bar
                    lengths are directly comparable across groups (default True)
    show_labels   : annotate each bar with formatted value (default True)
    color_map     : dict mapping group value → color. None = use BOROUGH_COLORS.
                    Lookup is case-insensitive for string group values.
    default_color : color for group values not found in color_map
    suptitle      : figure-level title (default: '<AGG> of Fields by <Group>')
    """
    if color_map is None:
        color_map = BOROUGH_COLORS

    # Aggregate per group: DataFrame indexed by group, columns = fields
    per_group = df.groupby(group_field)[fields].agg(agg)

    # Order panels by overall total — biggest group first
    group_totals = per_group.sum(axis=1).sort_values(ascending=False)
    group_values = group_totals.index.tolist()
    n_groups = len(group_values)

    # Determine field ordering used inside each panel
    shared_modes = {'shared_desc', 'shared_asc', None}
    if sort == 'shared_desc':
        field_order = df[fields].agg(agg).sort_values(ascending=False).index.tolist()
    elif sort == 'shared_asc':
        field_order = df[fields].agg(agg).sort_values(ascending=True).index.tolist()
    else:
        field_order = list(fields)

    # Common X-axis upper bound (with 15% headroom for labels)
    x_max = per_group.values.max() * 1.15 if shared_x else None

    n_fields = len(fields)
    n_cols   = min(n_groups, max_cols)
    n_rows   = -(-n_groups // max_cols)

    panel_h  = panel_height or max(2.5, n_fields * 0.4)
    fig_w    = panel_width * n_cols
    fig_h    = panel_h * n_rows
    # Scale fonts so on-screen size matches helpers that render at CHART_WIDTH —
    # nb2report displays charts at uniform widths, so a narrower native render
    # gets stretched and its fonts otherwise look "punchy" relative to wider charts.
    scale = fig_w / CHART_WIDTH

    fig, axes = plt.subplots(n_rows, n_cols, figsize=(fig_w, fig_h),
                             dpi=CHART_DPI, squeeze=False, sharex=shared_x)

    agg_label = agg.upper() if isinstance(agg, str) else 'Aggregate'

    for idx, gv in enumerate(group_values):
        r = idx // max_cols
        c = idx % max_cols
        ax = axes[r, c]

        # Pick field order for this panel
        if sort == 'panel_desc':
            totals = per_group.loc[gv].sort_values(ascending=False)
        elif sort == 'panel_asc':
            totals = per_group.loc[gv].sort_values(ascending=True)
        else:
            totals = per_group.loc[gv].reindex(field_order)

        # barh stacks bottom-up; reverse so first item ends up at top
        plot_data = totals.iloc[::-1]
        plot_labels = [f.replace('_', ' ') for f in plot_data.index]

        # Color lookup — case-insensitive for strings
        gv_key = gv.lower() if isinstance(gv, str) else gv
        bar_color = color_map.get(gv_key, default_color)

        bars = ax.barh(plot_labels, plot_data.values, color=bar_color,
                       alpha=0.85, edgecolor='#1a1a1a', linewidth=0.5)

        ax.xaxis.set_major_formatter(
            mticker.FuncFormatter(lambda x, _: fmt_num(x)))
        if x_max is not None:
            ax.set_xlim(0, x_max)

        if show_labels:
            for bar, val in zip(bars, plot_data.values):
                ax.text(bar.get_width(), bar.get_y() + bar.get_height() / 2,
                        f'  {fmt_num(val)}',
                        va='center', ha='left', fontsize=FONT_BAR_VALUE * scale)

        # Panel title (group value) + subtitle (overall total for this group)
        title = str(gv).title() if isinstance(gv, str) else str(gv)
        ax.set_title(title, fontsize=FONT_PANEL_TITLE * scale,
                     fontweight='bold', pad=24)
        subtitle = f'total {agg_label.lower()}: {fmt_num(group_totals[gv])}'
        ax.text(0.5, 1.02, subtitle, transform=ax.transAxes,
                ha='center', va='bottom', fontsize=FONT_BADGE * scale, color='#666')

        ax.tick_params(axis='both', labelsize=FONT_TICK * scale)

        # In shared-order modes every panel shows the same field list, so
        # hide the y-tick labels on non-leftmost columns to reduce clutter
        if c > 0 and sort in shared_modes:
            ax.set_yticklabels([])

        ax.spines['right'].set_visible(False)
        ax.spines['top'].set_visible(False)
        ax.grid(True, axis='x', color='gray', alpha=0.3, linewidth=0.5)

    # Hide unused panels in the trailing row
    for idx in range(n_groups, n_rows * n_cols):
        r = idx // max_cols
        c = idx % max_cols
        axes[r, c].set_visible(False)

    if suptitle is None:
        suptitle = (f'{agg_label} of Fields by '
                    f'{group_field.replace("_", " ").title()}')
    fig.suptitle(suptitle, fontsize=FONT_SUPTITLE * scale, fontweight='bold')

    # Leave headroom at the top for the suptitle
    plt.tight_layout(rect=[0, 0, 1, 0.96])
    plt.show()


def _draw_scatter_panel(ax, df, x_field, y_field,
                        color_field=None, color='#888888',
                        color_map=None, default_color='#888888',
                        alpha=0.4, point_size=10,
                        trend=None, correlation=True,
                        font_scale=1.0):
    """
    Draw a single scatter panel on `ax`. Optional per-point coloring by
    color_field, linear trend line, and Pearson r badge in the corner.
    Pearson r is computed on the data passed in (post-sampling, post-filter).

    font_scale multiplies all FONT_* constants for fonts drawn in this panel —
    used by plot_scatter to compensate for its narrower default render width
    so on-screen fonts match the wider chart helpers in nb2report HTML.
    """
    x = df[x_field]
    y = df[y_field]

    if color_field and color_field in df.columns:
        unique_vals = df[color_field].dropna().unique()
        cmap = color_map if color_map is not None else BOROUGH_COLORS
        for v in unique_vals:
            mask = df[color_field] == v
            v_key = v.lower() if isinstance(v, str) else v
            c = cmap.get(v_key, default_color)
            ax.scatter(x[mask], y[mask], color=c, alpha=alpha,
                       s=point_size, edgecolors='none', label=str(v))
        # Skip legend if too many categories — would dominate the panel
        if len(unique_vals) <= 12:
            ax.legend(fontsize=FONT_LEGEND * font_scale, framealpha=0.7, loc='best')
    else:
        ax.scatter(x, y, color=color, alpha=alpha,
                   s=point_size, edgecolors='none')

    # Linear trend line via numpy polyfit (degree 1)
    valid = ~(x.isna() | y.isna())
    if trend == 'linear' and valid.sum() >= 2:
        xv = x[valid].values
        yv = y[valid].values
        coef = np.polyfit(xv, yv, 1)
        line_x = np.array([xv.min(), xv.max()])
        line_y = np.polyval(coef, line_x)
        ax.plot(line_x, line_y, color='#333333', linewidth=1.5,
                linestyle='--', alpha=0.7, zorder=10)

    # Pearson r badge (top-left of panel, white-bg pill)
    if correlation and valid.sum() >= 2:
        r = np.corrcoef(x[valid], y[valid])[0, 1]
        ax.text(0.02, 0.98, f'r = {r:.3f}', transform=ax.transAxes,
                ha='left', va='top', fontsize=FONT_BADGE * font_scale,
                bbox=dict(boxstyle='round,pad=0.3', facecolor='white',
                          edgecolor='#999', alpha=0.85))

    ax.tick_params(axis='both', labelsize=FONT_TICK * font_scale)
    ax.spines['right'].set_visible(False)
    ax.spines['top'].set_visible(False)
    ax.grid(True, color='gray', alpha=0.3, linewidth=0.5)


def plot_scatter(df, x_field, y_field,
                 group_field=None, color_field=None,
                 trend=None, correlation=True,
                 log_x=False, log_y=False,
                 x_max=None, y_max=None,
                 x_min=None, y_min=None,
                 alpha=0.4, point_size=10,
                 sample=None, sample_seed=42,
                 max_cols=3, shared_axes=True,
                 panel_width=4.0, panel_height=4.0,
                 color='#888888', color_map=None,
                 default_color='#888888', title=None):
    """
    Scatterplot of two numeric measures.

    Single-panel mode (group_field=None): one chart for the whole dataset.
    Grid mode (group_field='borough'): small multiples, one panel per group
    value, panels colored via BOROUGH_COLORS by default.

    Parameters
    ----------
    df            : DataFrame
    x_field       : numeric column on the X-axis
    y_field       : numeric column on the Y-axis
    group_field   : if given, render one panel per unique value (small multiples).
                    None = single chart for the whole frame.
    color_field   : if given, color individual POINTS by this categorical field
                    (uses BOROUGH_COLORS by default). Independent of group_field —
                    you can group by month and color by borough, for example.
    trend         : 'linear' = overlay a least-squares regression line. None = skip.
    correlation   : if True, show Pearson r in a corner badge
    log_x, log_y  : log-scale either axis (useful when measures span orders
                    of magnitude — fares, distances)
    x_max, y_max  : force upper bound on each axis (zoom past outliers).
                    None = auto. In grid mode applies to every panel.
    x_min, y_min  : force lower bound on each axis. None = auto.
    alpha         : point transparency (default 0.4 — let dense regions show through)
    point_size    : matplotlib `s` (points²) — default 10
    sample        : cap rendered points for performance. e.g. sample=10_000
                    on a 30M-row frame. r is computed on the sampled data.
    sample_seed   : random seed for the sample (so re-runs are stable)
    max_cols      : max panels per row in grid mode (default 3)
    shared_axes   : grid mode — share both X and Y axes across panels so cloud
                    shapes are directly comparable (default True)
    panel_width   : width per panel in inches (default 4.0)
    panel_height  : height per panel in inches (default 4.0)
    color         : single panel color when neither color_field nor group palette applies
    color_map     : dict mapping group/color value → color. None = BOROUGH_COLORS.
    default_color : color for values not found in color_map
    title         : chart title (default auto: '<Y> vs <X>' or with 'by <Group>')
    """
    if color_map is None:
        color_map = BOROUGH_COLORS

    # Sample BEFORE grouping so grid panels stay proportional
    if sample and len(df) > sample:
        df = df.sample(n=sample, random_state=sample_seed)

    # ── Single-panel mode ────────────────────────────────────────────────
    if group_field is None:
        fig_w = panel_width * 1.5
        fig_h = panel_height
        # Scale fonts down so on-screen size matches wider chart helpers
        # in nb2report HTML (which displays all images at uniform width)
        scale = fig_w / CHART_WIDTH

        fig, ax = plt.subplots(figsize=(fig_w, fig_h), dpi=CHART_DPI)
        _draw_scatter_panel(ax, df, x_field, y_field,
                            color_field=color_field, color=color,
                            color_map=color_map, default_color=default_color,
                            alpha=alpha, point_size=point_size,
                            trend=trend, correlation=correlation,
                            font_scale=scale)
        if log_x:
            ax.set_xscale('log')
        if log_y:
            ax.set_yscale('log')
        if x_min is not None or x_max is not None:
            ax.set_xlim(left=x_min, right=x_max)
        if y_min is not None or y_max is not None:
            ax.set_ylim(bottom=y_min, top=y_max)
        ax.set_xlabel(x_field.replace('_', ' ').title(),
                      fontsize=FONT_AXIS_LABEL * scale)
        ax.set_ylabel(y_field.replace('_', ' ').title(),
                      fontsize=FONT_AXIS_LABEL * scale)
        ax.set_title(title or
                     f'{y_field.replace("_", " ").title()} vs '
                     f'{x_field.replace("_", " ").title()}',
                     fontsize=FONT_TITLE * scale, fontweight='bold')
        plt.tight_layout()
        plt.show()
        return

    # ── Grid mode ────────────────────────────────────────────────────────
    # Largest group first (most visually important)
    groups   = df[group_field].value_counts().index.tolist()
    n_groups = len(groups)
    n_cols   = min(n_groups, max_cols)
    n_rows   = -(-n_groups // max_cols)

    fig_w = panel_width  * n_cols
    fig_h = panel_height * n_rows
    # Scale fonts to compensate for narrower-than-CHART_WIDTH renders
    # so on-screen text matches the wider helpers in nb2report HTML.
    scale = fig_w / CHART_WIDTH

    fig, axes = plt.subplots(n_rows, n_cols,
                             figsize=(fig_w, fig_h), dpi=CHART_DPI,
                             squeeze=False,
                             sharex=shared_axes, sharey=shared_axes)

    for idx, gv in enumerate(groups):
        r = idx // max_cols
        c = idx % max_cols
        ax = axes[r, c]

        gv_key = gv.lower() if isinstance(gv, str) else gv
        panel_color = color_map.get(gv_key, default_color)

        sub = df[df[group_field] == gv]
        _draw_scatter_panel(ax, sub, x_field, y_field,
                            color_field=color_field,
                            color=panel_color,
                            color_map=color_map,
                            default_color=default_color,
                            alpha=alpha, point_size=point_size,
                            trend=trend, correlation=correlation,
                            font_scale=scale)

        if log_x:
            ax.set_xscale('log')
        if log_y:
            ax.set_yscale('log')
        if x_min is not None or x_max is not None:
            ax.set_xlim(left=x_min, right=x_max)
        if y_min is not None or y_max is not None:
            ax.set_ylim(bottom=y_min, top=y_max)

        ax.set_title(str(gv).title() if isinstance(gv, str) else str(gv),
                     fontsize=FONT_PANEL_TITLE * scale, fontweight='bold', pad=22)
        ax.text(0.5, 1.02, f'n={fmt_num(len(sub))}',
                transform=ax.transAxes, ha='center', va='bottom',
                fontsize=FONT_BADGE * scale, color='#666')

        # Axis labels only on outer edges to reduce clutter
        if r == n_rows - 1:
            ax.set_xlabel(x_field.replace('_', ' ').title(),
                          fontsize=FONT_AXIS_LABEL * scale)
        else:
            ax.set_xlabel('')
        if c == 0:
            ax.set_ylabel(y_field.replace('_', ' ').title(),
                          fontsize=FONT_AXIS_LABEL * scale)
        else:
            ax.set_ylabel('')

    # Hide unused panels in the trailing row
    for idx in range(n_groups, n_rows * n_cols):
        r = idx // max_cols
        c = idx % max_cols
        axes[r, c].set_visible(False)

    suptitle = title or (
        f'{y_field.replace("_", " ").title()} vs '
        f'{x_field.replace("_", " ").title()} '
        f'by {group_field.replace("_", " ").title()}'
    )
    fig.suptitle(suptitle, fontsize=FONT_SUPTITLE * scale, fontweight='bold')
    plt.tight_layout(rect=[0, 0, 1, 0.96])
    plt.show()


# ── High-level chart functions ─────────────────────────────────────────────────
# These create complete Figure + Axes layouts using GridSpec and call the
# low-level components. Each function creates its own fig, configures all
# panels, then calls plt.show().

def plot_daily_trips(df, date_col, log_scale=False,
                     dow_chart_type='line', dow_aggr='sum',
                     dow_pct=True, dow_first_day='monday',
                     major='year', minor='month',
                     major_fmt=None, minor_fmt=None):
    """
    Three-panel chart using GridSpec:

      ┌───────────────────────┬──────────┐
      │                       │ Avg Bar  │  ← gs[0, 1]  (right col, top half)
      │   Line chart          │          │
      │   (spans both rows)   ├──────────┤
      │                       │ DoW dist │  ← gs[1, 1]  (right col, bottom half)
      │   gs[:, 0]            │          │
      └───────────────────────┴──────────┘

    The line chart uses gs[:, 0] — the colon means "all rows in column 0",
    so it spans the full height. The right column is split into two independent
    Axes via gs[0, 1] and gs[1, 1].

    Parameters
    ----------
    df             : DataFrame with [date_col, 'borough', 'trip_cnt']
    date_col       : column name for x-axis
    log_scale      : if True, y-axis uses log scale on the line chart
    dow_chart_type : 'line' or 'bar' for the day-of-week panel
    dow_aggr       : inner aggregation for DoW chart ('count', 'sum', 'mean')
    dow_pct        : if True, show % of group total; if False, raw values
    dow_first_day  : 'monday' or 'sunday'
    """
    label  = _infer_label(date_col)
    suffix = ' (log scale)' if log_scale else ''

    # GridSpec: 2 rows x 2 cols. Left column spans both rows (line chart).
    # Right column: top = bar, bottom = day-of-week.
    # width_ratios [3, 1] = 75/25 split — gives right panels enough room for labels.
    # Height = 60% of width — landscape aspect ratio for time-series readability.
    fig = plt.figure(figsize=(CHART_WIDTH, CHART_WIDTH * 0.6), dpi=CHART_DPI)
    gs  = fig.add_gridspec(2, 2, width_ratios=[3, 1], height_ratios=[1, 1],
                           hspace=0.55, wspace=0.3)

    ax_line = fig.add_subplot(gs[:, 0])    # left col — spans both rows
    ax_bar  = fig.add_subplot(gs[0, 1])    # right col, top half
    ax_dow  = fig.add_subplot(gs[1, 1])    # right col, bottom half

    # ── Left: time-series line chart ──────────────────────────────────
    _draw_line_chart(ax_line, df, date_col, log_scale=log_scale,
                     major=major, minor=minor,
                     major_fmt=major_fmt, minor_fmt=minor_fmt)
    ax_line.set_title(f'Daily Trip Count by {label} Borough{suffix}', fontsize=TITLE_SM)
    ax_line.set_xlabel(f'{label} Date')
    ax_line.set_ylabel(f'Trip Count{suffix}')

    # ── Right top: avg daily bar chart ────────────────────────────────
    _borough_bar(ax_bar, df, label_fontsize=10)

    # ── Right bottom: day-of-week distribution ────────────────────────
    _draw_dow_chart(ax_dow, df, date_col,
                    metric_field='trip_cnt',
                    group_by_field='borough',
                    aggr=dow_aggr,
                    pct_of_group=dow_pct,
                    chart_type=dow_chart_type,
                    first_day_of_week=dow_first_day)

    plt.tight_layout(pad=0.1)
    plt.show()


def plot_borough_detail(df, date_col, show_trend=False, ma_window=7, band_sigmas=1,
                        dow_chart_type='line', dow_aggr='sum',
                        dow_pct=True, dow_first_day='monday',
                        major='year', minor='month',
                        major_fmt=None, minor_fmt=None):
    """
    Per-borough detail: one row per real borough, sorted by avg daily trips desc.

    Layout per borough row (uses GridSpec):

      ┌───────────────────────┬──────────┐
      │                       │ Avg Bar  │  ← highlighted for this borough
      │   Single borough      │          │
      │   line chart          ├──────────┤
      │                       │ DoW dist │  ← filtered to this borough only
      │                       │          │
      └───────────────────────┴──────────┘

    The full figure uses a grid of (N_boroughs * 2) rows x 2 columns.
    Each borough occupies 2 consecutive rows. The left column spans both
    rows (line chart), the right column splits into bar (top) and DoW (bottom).

    Parameters
    ----------
    df             : DataFrame with [date_col, 'borough', 'trip_cnt']
    date_col       : column name for the x-axis
    show_trend     : overlay rolling mean + LOWESS on each line chart
    ma_window      : days for rolling mean (default 7)
    band_sigmas    : std devs for confidence band (default 1)
    dow_chart_type : 'line' or 'bar' for the day-of-week panel
    dow_aggr       : inner aggregation for DoW ('count', 'sum', 'mean')
    dow_pct        : if True, % of total; if False, raw values
    dow_first_day  : 'monday' or 'sunday'
    """
    label = _infer_label(date_col)

    avg_by_boro = df.groupby('borough')['trip_cnt'].mean()
    real = [b for b in avg_by_boro.sort_values(ascending=False).index
            if b not in ('(null borough)', 'n/a', 'unknown')]

    n = len(real)

    # GridSpec: (n * 2) rows x 2 cols. Each borough gets 2 rows.
    # height_ratios alternates [1, 1] for each borough pair.
    fig = plt.figure(figsize=(CHART_WIDTH, n * CHART_ROW_HEIGHT), dpi=CHART_DPI)
    # hspace=0.6 — controls vertical gap between ALL grid rows. Since each borough
    # gets 2 rows, this creates spacing both within a borough's pair (bar↔DoW) and
    # between borough groups. A higher value prevents titles from colliding.
    gs  = fig.add_gridspec(n * 2, 2,
                           width_ratios=[3, 1],
                           height_ratios=[1, 1] * n,
                           hspace=0.6, wspace=0.25)

    line_axes = []  # collect line chart axes for separator positioning later

    for i, borough in enumerate(real):
        row = i * 2  # starting row for this borough's pair
        color = BOROUGH_COLORS.get(borough, '#999')

        # ── Left: line chart spanning 2 rows ──────────────────────────
        ax_line = fig.add_subplot(gs[row:row + 2, 0])
        line_axes.append(ax_line)

        boro_df = df[df['borough'] == borough].set_index(date_col).sort_index()
        ax_line.plot(boro_df.index, boro_df['trip_cnt'],
                     color=color, linewidth=1.5, alpha=0.9,
                     label=borough)  # label needed for tooltip to show borough name

        if show_trend:
            _add_trends(ax_line, boro_df.index, boro_df['trip_cnt'].values,
                        color=color, ma_window=ma_window, band_sigmas=band_sigmas)

        ax_line.set_title(f'{borough.title()} \u2014 {label} Trips',
                          color=color, fontweight='bold', fontsize=TITLE_SM)
        ax_line.set_ylabel('Trip Count')

        # Snap x-axis to data range
        x_min, x_max = _snap_date_range(df, date_col)
        ax_line.set_xlim(x_min, x_max)

        _fmt_time_xaxis(ax_line, major=major, minor=minor,
                        major_fmt=major_fmt, minor_fmt=minor_fmt)
        _fmt_clean_yaxis(ax_line, boro_df['trip_cnt'].max())

        if show_trend:
            ax_line.legend(loc='upper left', framealpha=0.6)

        # Hover tooltips — works on the borough line AND trend lines (MA, LOWESS)
        _add_line_tooltips(ax_line, date_axis=True)

        if i == n - 1:
            ax_line.set_xlabel(f'{label} Date')

        # ── Right top: bar chart, this borough highlighted ────────────
        ax_bar = fig.add_subplot(gs[row, 1])
        _borough_bar(ax_bar, df, highlight=borough, label_fontsize=10)

        # ── Right bottom: DoW for this borough only ───────────────────
        ax_dow = fig.add_subplot(gs[row + 1, 1])
        boro_full = df[df['borough'] == borough]
        _draw_dow_chart(ax_dow, boro_full, date_col,
                        metric_field='trip_cnt',
                        group_by_field=None,  # single borough → one line/bar series
                        aggr=dow_aggr,
                        pct_of_group=dow_pct,
                        chart_type=dow_chart_type,
                        first_day_of_week=dow_first_day,
                        color=color)          # match the borough's line chart color
        ax_dow.set_title(f'{borough.title()} \u2014 Day of Week', fontsize=TITLE_SM)

    plt.tight_layout(pad=0.5)

    # ── Horizontal separators between borough groups ──────────────────
    # Drawn AFTER tight_layout so we can read the final axes positions.
    # get_position() returns a Bbox in figure-fraction coords (0–1).
    # The separator sits at the midpoint between the bottom of one borough's
    # axes (y0) and the top of the next borough's axes (y1).
    for i in range(len(line_axes) - 1):
        pos_above = line_axes[i].get_position()      # current borough
        pos_below = line_axes[i + 1].get_position()   # next borough
        y = (pos_above.y0 + pos_below.y1) / 2
        fig.add_artist(mlines.Line2D(
            [0.02, 0.98], [y, y],           # x: 2% to 98% of figure width
            transform=fig.transFigure,       # coords are in figure fractions, not data units
            color='gray', alpha=0.3, linewidth=0.8, linestyle='-'
        ))

    plt.show()
