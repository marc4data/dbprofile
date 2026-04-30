"""Packaged source-of-truth helpers shipped with dbprofile.

These files are copied into <project_dir>/dq_eda/ on first run via
helper_copy.copy_helpers(). The project-root files at
eda_helpers.py / eda_profile.py / eda_helpers_call_templates.py are
re-export shims pointing here so the dbprofile sample notebook keeps
working with `from eda_helpers import *`.
"""
