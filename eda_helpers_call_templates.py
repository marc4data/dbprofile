"""
eda_helpers_call_templates.py — Copy-paste templates for all eda_helpers chart functions.

This file is NOT meant to be executed — it's a reference. Copy the function
body into a notebook cell, uncomment the params you need, and adjust values.

The VS Code Outline (Explorer panel, left side) shows each template as a
clickable entry organized by category using classes as section headers.

MAINTENANCE:
  - When a new helper function is added to eda_helpers.py, add a template here.
  - When parameters change on an existing helper, update the template.
  - Full rebuild: rewrite this entire file to match current eda_helpers.py signatures.
  - Incremental: add/update just the affected template function.
"""


class TIME_SERIES:
    """Daily trends, borough breakdowns, trend overlays."""

    def template_plot_daily_trips():
        """Three-panel: line chart + avg bar + day-of-week distribution."""
        plot_daily_trips(
            df       = pickup_df,             # DataFrame with [date_col, 'borough', 'trip_cnt']
            date_col = 'pickup_date',         # date column for x-axis

            # ── Y-axis ────────────────────────────────────────────────────
            # log_scale = False,              # True = log y-axis (compresses dominant borough)

            # ── Day-of-week panel (right bottom) ──────────────────────────
            # dow_chart_type = 'line',        # 'line' or 'bar'
            # dow_aggr       = 'sum',         # inner aggregation: 'count', 'sum', 'mean'
            # dow_pct        = True,          # True = % of group total, False = raw values
            # dow_first_day  = 'monday',      # 'monday' or 'sunday'

            # ── X-axis gridlines ──────────────────────────────────────────
            # major     = 'year',             # 'year', 'quarter', 'month', 'week', 'day'. ':N' for interval.
            # minor     = 'month',            # same options
            # major_fmt = None,               # strftime override (auto from preset if None)
            # minor_fmt = None,               # strftime override
        )

    def template_plot_borough_detail():
        """Per-borough detail: line chart + bar + DoW, one row per borough."""
        plot_borough_detail(
            df       = pickup_df,
            date_col = 'pickup_date',

            # ── Trend overlays ────────────────────────────────────────────
            # show_trend  = False,            # True = overlay rolling MA + LOWESS
            # ma_window   = 7,                # days for rolling mean (7=weekly, 14=biweekly, 30=monthly)
            # band_sigmas = 1,                # std devs for confidence band (1σ≈68%, 2σ≈95%)

            # ── Day-of-week panel ─────────────────────────────────────────
            # dow_chart_type = 'line',         # 'line' or 'bar'
            # dow_aggr       = 'sum',          # 'count', 'sum', 'mean'
            # dow_pct        = True,           # True = % of group total
            # dow_first_day  = 'monday',       # 'monday' or 'sunday'

            # ── X-axis gridlines ──────────────────────────────────────────
            # major     = 'year',
            # minor     = 'month',
            # major_fmt = None,
            # minor_fmt = None,
        )


class DISTRIBUTIONS:
    """Histograms, box-whisker, combined distribution views."""

    def template_plot_distribution():
        """Combined histogram (top) + horizontal boxplot (bottom), shared x-axis."""
        plot_distribution(
            df    = hist_df,                  # DataFrame to plot
            field = 'trip_distance_miles',    # numeric column (used by both hist + box)

            # ── Binning (shared) ──────────────────────────────────────────
            # bin_cnt    = 20,                # number of bins (ignored if bin_incr set)
            # bin_incr   = 0.5,              # fixed bin width — overrides bin_cnt
            # bin_min    = 0,                 # left boundary — constrains axis + annotates excluded
            # bin_max    = 15,                # right boundary — constrains axis + annotates excluded
            # bin_on_int = False,             # True = snap bin edges to integers

            # ── Clipping (histogram only — red boundary lines + count) ────
            # clip_min   = None,              # annotate records below (if different from bin_min)
            # clip_max   = None,              # annotate records above (if different from bin_max)

            # ── Histogram options ─────────────────────────────────────────
            # pct               = False,      # True = y-axis shows % of total
            # show_labels       = False,      # True = count/% on top of each bar
            # cumulative_line   = False,      # True = Pareto-style cumulative % (0–100%)
            # cumulative_behind = True,       # True = line behind bars, False = in front

            # ── Boxplot options ───────────────────────────────────────────
            # group_field       = 'borough',  # one box per value, sorted by median desc
            # show_outliers     = True,       # dots beyond whiskers
            # show_means        = False,      # diamond marker for mean
            # whis              = 1.5,        # whisker extent (IQR multiplier). (0,100) for min/max.
            # show_box_labels   = False,      # annotate Q1, median, Q3, whisker values
            # show_strip        = False,      # jittered data points on boxes
            # strip_max_points  = 5000,       # auto-sample threshold for strip

            # ── Layout ────────────────────────────────────────────────────
            # height_ratio = (4, 1),          # (histogram, boxplot) vertical split
            # panel_width  = None,            # figure width (default = CHART_WIDTH)
            # panel_height = 6.0,             # total figure height
        )

    def template_plot_histogram():
        """Histogram with optional small-multiples, cumulative line, and bar labels."""
        plot_histogram(
            df         = hist_df,             # DataFrame to plot
            hist_field = 'trip_duration_minutes',  # numeric column to bin

            # ── Binning ───────────────────────────────────────────────────
            bin_cnt    = 20,                  # number of bins (ignored if bin_incr is set)
            # bin_incr = 5,                   # fixed bin width — overrides bin_cnt
            # bin_min  = 0,                   # clip left  — annotates excluded records below
            # bin_max  = 60,                  # clip right — annotates excluded records above
            # bin_on_int = False,             # True = snap bin edges to integers

            # ── Small multiples ───────────────────────────────────────────
            # row_group    = 'borough',       # one row per unique value of this field
            # row_sort     = 'desc',          # 'asc' or 'desc'
            # row_sort_by  = 'num',           # 'alpha' = sort by name, 'num' = sort by sum(hist_field)
            # col_group    = None,            # one column per unique value of this field
            # col_sort     = 'desc',
            # col_sort_by  = 'num',

            # ── Appearance ────────────────────────────────────────────────
            # color_field       = None,       # stack bars by this field's values (within each panel)
            # pct               = False,      # True = y-axis shows % of total, False = raw counts
            # shared_y          = True,       # True = same y-range across all panels
            # show_labels       = False,      # True = print count/% on top of each bar
            # cumulative_line   = False,      # True = add Pareto-style cumulative % line (0–100%)
            # cumulative_behind = True,       # True = line behind bars, False = line in front

            # ── Size ──────────────────────────────────────────────────────
            # panel_width  = None,            # width per panel (inches). Default = CHART_WIDTH / n_cols
            # panel_height = 4.0,             # height per panel (inches)
        )

    def template_plot_histograms():
        """Multi-field histogram grid — one panel per field, independent X-axis,
        bins centered on integers, Y-axis as % of total."""
        plot_histograms(
            df     = hist_df,                     # DataFrame containing all fields
            fields = [                            # list of numeric columns — one panel each
                'is_holiday',
                'is_weekend',
                'day_of_week',
                'pickup_month',
                'pickup_year',
            ],

            # ── Layout ────────────────────────────────────────────────────
            # max_cols     = 3,                   # max panels per row before wrapping
            # panel_width  = 3.5,                 # width per panel (inches)
            # panel_height = 3.0,                 # height per panel (inches)

            # ── Bins / labels ─────────────────────────────────────────────
            # bin_incr        = 1,                # bin width (1 = one bar per integer value)
            # label_threshold = None,             # int N: panels w/ < N bars get % labels (None = never)
        )

    def template_plot_field_aggregates():
        """Bar chart comparing an aggregate (sum, mean, etc.) across multiple fields."""
        plot_field_aggregates(
            df     = sample_df,                   # DataFrame containing all fields
            fields = [                            # list of numeric columns — one bar per field
                'trip_count',
                'adjusted_trip_count',
                'airport_pickup_count',
                'cross_borough_count',
                'cash_trips',
                'evening_rush_trips',
                'morning_rush_trips',
                'credit_card_trips',
                'overnight_trips',
            ],

            # ── Aggregation ───────────────────────────────────────────────
            # agg          = 'sum',               # 'sum', 'mean', 'median', 'min', 'max', 'count', 'std', or callable
            # sort         = 'desc',              # 'desc', 'asc', or None (keep input order)

            # ── Layout ────────────────────────────────────────────────────
            # orientation  = 'horizontal',        # 'horizontal' (best for long names) or 'vertical'
            # color        = '#888888',           # bar fill color
            # show_labels  = True,                # annotate each bar with formatted value
            # panel_width  = None,                # figure width (default auto)
            # panel_height = None,                # figure height (default auto, scales w/ n_fields)
            # title        = None,                # default = '<AGG> by Field'
        )

    def template_plot_field_aggregates_by_group():
        """Small-multiples horizontal bar chart — one panel per group value
        (e.g. one per borough), each panel shows the aggregate of every field
        for that group's subset. Auto-colors panels via BOROUGH_COLORS."""
        plot_field_aggregates_by_group(
            df          = sample_df,              # DataFrame containing all fields
            group_field = 'borough',              # categorical column — one panel per unique value
            fields      = [                       # list of numeric columns
                'trip_count',
                'adjusted_trip_count',
                'airport_pickup_count',
                'cross_borough_count',
                'cash_trips',
                'evening_rush_trips',
                'morning_rush_trips',
                'credit_card_trips',
                'overnight_trips',
            ],

            # ── Aggregation / ordering ────────────────────────────────────
            # agg          = 'sum',               # 'sum', 'mean', 'median', 'min', 'max', 'count', 'std', or callable
            # sort         = 'shared_desc',       # 'shared_desc' (default — best for cross-panel compare),
            #                                     # 'shared_asc', 'panel_desc', 'panel_asc', None (input order)

            # ── Layout ────────────────────────────────────────────────────
            # max_cols     = 3,                   # max panels per row before wrapping
            # panel_width  = 4.5,                 # width per panel (inches)
            # panel_height = None,                # default auto, scales w/ n_fields
            # shared_x     = True,                # True = shared X-axis range (bar lengths comparable)

            # ── Appearance ────────────────────────────────────────────────
            # show_labels   = True,               # annotate each bar with formatted value
            # color_map     = None,               # dict group→color. None = BOROUGH_COLORS
            # default_color = '#888888',          # color for groups not in color_map
            # suptitle      = None,               # default = '<AGG> of Fields by <Group>'
        )

    def template_plot_boxplot():
        """Box-whisker with grouping, labels, strip overlay, and view constraints."""
        plot_boxplot(
            df          = hist_df,            # DataFrame to plot
            value_field = 'trip_duration_minutes',  # numeric column for distribution

            # ── Grouping ──────────────────────────────────────────────────
            # group_field  = 'borough',       # one box per unique value, sorted by median desc
            # orientation  = 'vertical',      # 'vertical' or 'horizontal'

            # ── Small multiples ───────────────────────────────────────────
            # row_group    = None,            # one row per unique value of this field
            # row_sort     = 'desc',
            # row_sort_by  = 'num',           # 'alpha' or 'num' (sort by sum of value_field)
            # col_group    = None,            # one column per unique value
            # col_sort     = 'desc',
            # col_sort_by  = 'num',

            # ── Box appearance ────────────────────────────────────────────
            # color_field   = None,           # color boxes by this field (default: group_field)
            # show_outliers = True,           # dots beyond whiskers
            # show_means    = False,          # diamond marker for mean (in addition to median line)
            # notch         = False,          # notched boxes (CI around median)
            # whis          = 1.5,            # whisker extent as IQR multiplier. (0,100) for min/max.

            # ── Overlays ──────────────────────────────────────────────────
            # show_labels       = False,      # annotate Q1, median, Q3, whisker values on each box
            # show_strip        = False,      # jittered data points over the boxes
            # strip_max_points  = 5000,       # auto-sample above this threshold

            # ── Axis ──────────────────────────────────────────────────────
            # show_axis    = True,            # False = hide value axis ticks (useful with show_labels)
            # shared_y     = True,            # same value-axis range across all panels
            # clip_min     = None,            # boundary line + count for records below
            # clip_max     = None,            # boundary line + count for records above
            # view_min     = None,            # zoom axis min (stats still use full data)
            # view_max     = None,            # zoom axis max (stats still use full data)

            # ── Size ──────────────────────────────────────────────────────
            # panel_width  = None,            # width per panel (inches). Default = CHART_WIDTH / n_cols
            # panel_height = 4.0,             # height per panel (inches)
        )


class CORRELATIONS:
    """Bivariate analysis — scatter and other two-measure relationships."""

    def template_plot_scatter():
        """Scatterplot of two numeric measures. Single-panel by default;
        pass group_field='borough' to render small multiples (one panel per group)."""
        plot_scatter(
            df      = sample_df,              # DataFrame
            x_field = 'trip_distance_miles',  # numeric column for X
            y_field = 'fare_amount',          # numeric column for Y

            # ── Mode ──────────────────────────────────────────────────────
            # group_field   = None,           # None = single panel; e.g. 'borough' = grid
            # color_field   = None,           # color individual points by this categorical field
            #                                 # (e.g. color_field='borough' on a single-panel chart)

            # ── Decorations ───────────────────────────────────────────────
            # trend         = None,           # 'linear' = overlay regression line; None = skip
            # correlation   = True,           # True = show Pearson r badge in corner
            # log_x         = False,          # log-scale X
            # log_y         = False,          # log-scale Y

            # ── Manual axis limits (zoom past outliers) ──────────────────
            # x_max         = None,           # force upper bound on X (None = auto)
            # y_max         = None,           # force upper bound on Y (None = auto)
            # x_min         = None,           # force lower bound on X (None = auto)
            # y_min         = None,           # force lower bound on Y (None = auto)

            # ── Point appearance ──────────────────────────────────────────
            # alpha         = 0.4,            # point transparency (lower = denser overplot OK)
            # point_size    = 10,             # matplotlib `s` (points²)

            # ── Performance ───────────────────────────────────────────────
            # sample        = None,           # cap rendered points (e.g. 10_000) for huge frames
            # sample_seed   = 42,             # random seed for reproducible sampling

            # ── Grid mode layout ──────────────────────────────────────────
            # max_cols      = 3,              # max panels per row before wrapping
            # shared_axes   = True,           # True = shared X+Y across panels (for cross-compare)
            # panel_width   = 4.0,            # width per panel (inches)
            # panel_height  = 4.0,            # height per panel (inches)

            # ── Color ─────────────────────────────────────────────────────
            # color         = '#888888',      # single color when no palette applies
            # color_map     = None,           # dict value→color. None = BOROUGH_COLORS
            # default_color = '#888888',      # fallback for values not in color_map

            # ── Title ─────────────────────────────────────────────────────
            # title         = None,           # default auto: '<Y> vs <X>' or with 'by <Group>'
        )


class STRING_PROFILING:
    """Low and high cardinality string/categorical field profiling."""

    def template_plot_string_profile():
        """Low-cardinality: horizontal bar chart per field, cumulative % line."""
        plot_string_profile(
            df     = str_df,                  # DataFrame with string columns
            fields = [                        # list of column names to profile
                'payment_type_label',
                'rate_code_label',
                'store_and_fwd_flag',
                'pickup_borough',
                'pickup_service_zone',
                'dropoff_borough',
                'dropoff_service_zone',
                'vendor_id',
                'day_of_week',
            ],
            # top_n           = 20,           # max values per field (default: CARDINALITY_THRESHOLD_STR)
            # show_cumulative = True,         # cumulative % line on secondary axis
            # show_pct        = True,         # label bars with % (False = raw count)
            # show_length     = False,        # annotate string length next to each value
            # sort_by         = 'freq',       # 'freq', 'alpha', or 'length'
            # max_cols        = 3,            # chart columns in the grid
            # panel_width     = 4.0,          # width per chart panel (inches)
            # panel_height    = 4.0,          # height per chart panel (inches)
            # dpi             = 240,          # render resolution
        )

    def template_plot_string_profile_hc():
        """High-cardinality: frequency distribution + string length histogram per field."""
        plot_string_profile_hc(
            df     = str_df,                  # DataFrame with string columns
            fields = [                        # list of high-cardinality column names
                'pickup_zone',
                'dropoff_zone',
            ],
            # top_n       = 20,               # values in the top-N bar chart (3rd row)
            # max_cols     = 3,               # columns in the grid before wrapping
            # panel_width  = 4.0,             # width per column (inches)
            # panel_height = 3.0,             # height per chart panel (3 panels per field)
            # dpi          = 240,             # render resolution
        )


class FLOW_ANALYSIS:
    """From/To matrices, Sankey diagrams, route analysis."""

    def template_plot_from_to_matrix():
        """Bar matrix: rows = From, cols = To, bar height = trip count, color by magnitude."""
        plot_from_to_matrix(
            df         = str_df,              # DataFrame with from/to columns
            from_field = 'pickup_borough',    # column for rows
            to_field   = 'dropoff_borough',   # column for columns

            # ── Cell content ──────────────────────────────────────────────
            # show_pct     = True,            # annotate % of total on each bar
            # log_y        = True,            # True = log y-axis, False = linear

            # ── Y-axis controls ───────────────────────────────────────────
            # show_yticks       = True,       # show y-axis tick marks
            # show_ytick_labels = True,       # show y-axis tick labels
            # show_gridlines    = True,       # horizontal gridlines

            # ── Axis titles ───────────────────────────────────────────────
            # show_row_axis_title = True,     # "From: <field>" rotated on left
            # show_col_axis_title = True,     # "To: <field>" centered on top

            # ── Font offsets (from TITLE_SM base) ─────────────────────────
            # font_column_headers = 0,        # column header labels
            # font_row_labels     = 0,        # row labels (left edge)
            # font_row_axis_title = 0,        # "From:" axis title
            # font_col_axis_title = 0,        # "To:" axis title
            # font_mark_labels    = 0,        # bar value/% labels
            # font_ytick_labels   = 0,        # y-axis tick labels
            # font_legend         = 0,        # legend text
            # font_title          = 0,        # suptitle

            # ── Separators ────────────────────────────────────────────────
            # sep_linewidth        = 1.5,     # bottom spine per cell
            # sep_alpha            = 0.5,     # bottom spine alpha
            # borough_sep_linewidth = 2.0,    # horizontal line between rows
            # borough_sep_alpha     = 0.7,    # separator alpha

            # ── Legend ────────────────────────────────────────────────────
            # legend_loc   = 'right',         # 'right', 'bottom', or 'none'

            # ── Size ──────────────────────────────────────────────────────
            # dpi          = 200,
            # cell_width   = 1.4,             # width per cell (inches)
            # cell_height  = 0.9,             # height per cell (inches)
        )

    def template_plot_sankey():
        """Interactive Plotly Sankey: flow diagram between zones, colored by borough."""
        plot_sankey(
            df          = str_df,             # DataFrame with from/to columns
            from_field  = 'pickup_zone',      # column for source nodes
            to_field    = 'dropoff_zone',     # column for target nodes
            color_field = 'pickup_borough',   # column to color source nodes by

            # ── Filtering ─────────────────────────────────────────────────
            # top_n   = 20,                   # top N routes by volume; rest → "Other"

            # ── Layout ────────────────────────────────────────────────────
            # title   = None,                 # auto-generated if None
            # width   = 900,                  # pixels
            # height  = 600,                  # pixels
        )


class DATA_QUALITY:
    """Indicator field audits, DQ checks."""

    def template_plot_indicators():
        """Small-multiples bar chart audit for 0/1 indicator fields."""
        plot_indicators(
            df     = hist_df,
            fields = [
                'is_null_batch_ind',
                'jfk_flat_rate_ind',
                'long_duration_ind',
                'negative_duration_ind',
                'negative_fare_ind',
                'passenger_count_missing_ind',
                'weather_freezing_day_ind',
                'weather_rain_day_ind',
                'weather_snow_day_ind',
                'zero_distance_ind',
            ],
            # max_cols     = 5,               # columns per row before wrapping
            # panel_width  = 2.0,             # width per panel (inches)
            # panel_height = 3.0,             # height per panel (inches)
        )
