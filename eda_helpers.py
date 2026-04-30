"""Re-export shim — source of truth lives in the dbprofile package.

This file exists so notebooks at the project root can do
`from eda_helpers import *` exactly as before. The actual helper code
lives at dbprofile/notebook/templates/eda_helpers.py and is what gets
copied into <project_dir>/dq_eda/ when dbprofile generates a notebook.

Edit the package version, not this file.
"""

from dbprofile.notebook.templates.eda_helpers import *  # noqa: F401, F403
