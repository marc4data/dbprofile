"""Per-section cell builders.

Each module returns a list of nbformat cells for one part of the notebook.
The generator concatenates them into the final document. Splitting the
notebook this way keeps each section's logic small, testable, and easy
to swap.

Sections (referenced from the feature plan §6):
  s00_header       — title + DQ summary callouts            (Phase 4)
  s01_setup        — imports, theme, connector, sql()      (this PR)
  s02_data_gather  — BERNOULLI sampling + sample_df         (Phase 4+)
  s03_grain        — schema summary + boundary conditions  (Phase 4+)
  s04_univariate   — distributions per column kind         (Phase 5)
  s05_bivariate    — correlation + scatter pairs           (Phase 5)
  s06_temporal     — time series                           (Phase 5)
  s07_dq_followup  — per-flagged-check deep-dives          (Phase 5)
"""
