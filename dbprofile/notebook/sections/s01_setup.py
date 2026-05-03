"""Setup section — imports, theme, and connector wiring.

Emits two cells per the feature plan §6 (Section 1):

  Cell 1  Markdown intro for the section
  Cell 2  One large code cell — imports, matplotlib defaults, DataFrame
          CSS, connector setup, sql() helper, FORCE_RELOAD guard.

The connector setup branches on `cfg.connection.dialect`:

  snowflake  Reads creds from env vars (.env loaded via python-dotenv).
             Mirrors dbprofile.connectors.base.SnowflakeConnector — same
             key-pair → DER-PKCS8 conversion, same env var names.

  bigquery   Service account file via GOOGLE_APPLICATION_CREDENTIALS or
             Application Default Credentials. PROJECT/DATASET hardcoded
             from cfg (not secret).

  duckdb     Hardcoded database_path from cfg. Read-only connection so
             the analyst can't accidentally mutate the dev seed.

Why env vars (not the YAML values directly)?
  - notebooks get checked into git alongside other project artifacts.
    Hardcoded creds would leak. Env vars + .env keep secrets out of
    the notebook itself.
  - the analyst can swap accounts (dev vs prod) by editing .env, no
    notebook regeneration required.
"""

from __future__ import annotations

import nbformat

from dbprofile.notebook.cells import code_cell, md_cell, section_header


def build_setup_cells(
    *,
    cfg,
    schema_name: str,
    connector_type: str,
) -> list[nbformat.NotebookNode]:
    """Return the cells for the Setup section.

    Parameters
    ----------
    cfg            ProfileConfig — connection + scope info needed to
                   build the connector cell.
    schema_name    Schema/dataset name for this notebook's table.
    connector_type 'snowflake' | 'bigquery' | 'duckdb' (anything else
                    falls back to a documented placeholder).
    """
    return [
        section_header(2, "Setup"),
        md_cell(
            "Imports, theme, and connector wiring. Run this cell once when "
            "the kernel starts. Re-running is cheap — it just re-imports."
        ),
        code_cell(_build_setup_source(
            cfg=cfg,
            schema_name=schema_name,
            connector_type=connector_type,
        )),
    ]


# ── Setup cell composition ───────────────────────────────────────────────────


def _build_setup_source(
    *,
    cfg,
    schema_name: str,
    connector_type: str,
) -> str:
    """Compose the single large setup cell."""
    parts = [
        _imports_block(),
        _theme_block(),
        _dataframe_css_block(),
        _connector_block(cfg=cfg, schema_name=schema_name, connector_type=connector_type),
        _sql_helper_block(connector_type=connector_type),
        _reload_guard_block(),
    ]
    return "\n\n".join(parts)


# ── Universal blocks ─────────────────────────────────────────────────────────


def _imports_block() -> str:
    return (
        "# ── Imports ──────────────────────────────────────────────────────────\n"
        "import warnings\n"
        "warnings.filterwarnings('ignore')\n"
        "\n"
        "import numpy as np\n"
        "import pandas as pd\n"
        "import matplotlib.pyplot as plt\n"
        "from IPython.display import HTML, display\n"
        "\n"
        "from eda_helpers import *\n"
        "from eda_profile import profile, peek, summarize, schema, describe_by_type\n"
        "\n"
        "%load_ext autoreload\n"
        "%autoreload 2\n"
        "%matplotlib inline"
    )


def _theme_block() -> str:
    return (
        "# ── Theme & global chart defaults ────────────────────────────────────\n"
        "plt.style.use('seaborn-v0_8-whitegrid')\n"
        "plt.rcParams.update({\n"
        "    'axes.titlesize':    24,\n"
        "    'axes.labelsize':    12,\n"
        "    'xtick.labelsize':   10,\n"
        "    'ytick.labelsize':   10,\n"
        "    'legend.fontsize':   10,\n"
        "    'figure.titlesize':  16,\n"
        "    'axes.spines.top':   False,\n"
        "    'axes.spines.right': False,\n"
        "})"
    )


def _dataframe_css_block() -> str:
    return (
        '# ── DataFrame table styling ──────────────────────────────────────────\n'
        'display(HTML("""\n'
        '<style>\n'
        'table.dataframe {\n'
        '    border-collapse: collapse !important;\n'
        '    border: 2px solid rgba(0,0,0,0.2) !important;\n'
        '}\n'
        'table.dataframe td, table.dataframe th {\n'
        '    border: 1.5px solid rgba(0,0,0,0.15) !important;\n'
        '}\n'
        'table.dataframe thead th {\n'
        '    background-color: #f0f0f0 !important;\n'
        '    font-weight: bold !important;\n'
        '}\n'
        '</style>\n'
        '"""))'
    )


def _reload_guard_block() -> str:
    return (
        "# ── Reload guard for cached DataFrames ───────────────────────────────\n"
        "# Flip to True to re-run all SQL queries on the next 'Run All'.\n"
        "FORCE_RELOAD = False"
    )


# ── Connector-specific blocks ────────────────────────────────────────────────


_INLINE_DOTENV = (
    "# ── .env loader (inlined — no python-dotenv dependency) ──────────────\n"
    "import os\n"
    "from pathlib import Path\n"
    "\n"
    "def _load_env_file():\n"
    "    \"\"\"Walk from cwd upward, parse the first .env we find into "
    "os.environ.\n"
    "    Skips comments + blank lines; respects pre-set env vars.\"\"\"\n"
    "    for _parent in [Path.cwd(), *Path.cwd().parents]:\n"
    "        _env = _parent / '.env'\n"
    "        if _env.is_file():\n"
    "            for _line in _env.read_text().splitlines():\n"
    "                _line = _line.strip()\n"
    "                if not _line or _line.startswith('#') or '=' not in _line:\n"
    "                    continue\n"
    "                _k, _, _v = _line.partition('=')\n"
    '                os.environ.setdefault(_k.strip(), _v.strip().strip("\\"\'"))\n'
    "            return\n"
    "_load_env_file()"
)


def _connector_block(*, cfg, schema_name: str, connector_type: str) -> str:
    if connector_type == "snowflake":
        return _snowflake_connector(cfg=cfg, schema_name=schema_name)
    if connector_type == "bigquery":
        return _bigquery_connector(cfg=cfg, schema_name=schema_name)
    if connector_type == "duckdb":
        return _duckdb_connector(cfg=cfg)
    return _generic_placeholder(connector_type=connector_type)


def _snowflake_connector(*, cfg, schema_name: str) -> str:
    """Snowflake: env-var-driven, key-pair via the same logic dbprofile uses."""
    database = (cfg.scope.database or "").upper()
    schema = (schema_name or "").upper()
    return (
        f"{_INLINE_DOTENV}\n"
        "\n"
        "# ── Connection (Snowflake — key-pair via .env) ───────────────────────\n"
        "import snowflake.connector\n"
        "from cryptography.hazmat.backends import default_backend\n"
        "from cryptography.hazmat.primitives import serialization\n"
        "\n"
        f"DATABASE = '{database}'\n"
        f"SCHEMA   = '{schema}'\n"
        "\n"
        "def _load_private_key(path: str, passphrase: str | None = None) -> bytes:\n"
        "    with open(path, 'rb') as f:\n"
        "        pk = serialization.load_pem_private_key(\n"
        "            f.read(),\n"
        "            password=passphrase.encode() if passphrase else None,\n"
        "            backend=default_backend(),\n"
        "        )\n"
        "    return pk.private_bytes(\n"
        "        encoding=serialization.Encoding.DER,\n"
        "        format=serialization.PrivateFormat.PKCS8,\n"
        "        encryption_algorithm=serialization.NoEncryption(),\n"
        "    )\n"
        "\n"
        "_conn = snowflake.connector.connect(\n"
        "    account     = os.environ['SNOWFLAKE_ACCOUNT'],\n"
        "    user        = os.environ['SNOWFLAKE_USER'],\n"
        "    warehouse   = os.environ.get('SNOWFLAKE_WAREHOUSE'),\n"
        "    role        = os.environ.get('SNOWFLAKE_ROLE'),\n"
        "    database    = DATABASE,\n"
        "    schema      = SCHEMA,\n"
        "    private_key = _load_private_key(os.environ['SNOWFLAKE_PRIVATE_KEY_PATH']),\n"
        ")"
    )


def _bigquery_connector(*, cfg, schema_name: str) -> str:
    """BigQuery: project + dataset baked in (not secret); creds via ADC or env."""
    project = cfg.connection.project or ""
    # In BigQuery terminology, the "dataset" plays the role of schema.
    dataset = schema_name or cfg.scope.dataset or ""
    return (
        f"{_INLINE_DOTENV}\n"
        "\n"
        "# ── Connection (BigQuery — ADC or GOOGLE_APPLICATION_CREDENTIALS) ────\n"
        "from google.cloud import bigquery\n"
        "\n"
        f"PROJECT = '{project}'\n"
        f"DATASET = '{dataset}'\n"
        "\n"
        "_client = bigquery.Client(project=PROJECT)"
    )


def _duckdb_connector(*, cfg) -> str:
    """DuckDB: path from config, read-only so the analyst can't mutate the seed."""
    db_path = cfg.connection.database_path or ""
    return (
        "# ── Connection (DuckDB — read-only) ──────────────────────────────────\n"
        "import duckdb\n"
        "\n"
        f"DATABASE_PATH = '{db_path}'\n"
        "\n"
        "_conn = duckdb.connect(DATABASE_PATH, read_only=True)"
    )


def _generic_placeholder(*, connector_type: str) -> str:
    return (
        f"# ── Connection ({connector_type}) ──────────────────────────────────\n"
        f"# TODO: dbprofile does not yet ship a setup cell template for the\n"
        f"# '{connector_type}' dialect. Wire your own connection here and\n"
        f"# define a sql(query) -> pd.DataFrame helper.\n"
        "_conn = None"
    )


# ── sql() helper ─────────────────────────────────────────────────────────────


def _sql_helper_block(*, connector_type: str) -> str:
    """Pick the sql() implementation that matches the connector."""
    if connector_type == "snowflake":
        body = (
            "    cur = _conn.cursor()\n"
            "    try:\n"
            "        cur.execute(query)\n"
            "        cols = [d[0] for d in cur.description]\n"
            "        return pd.DataFrame(cur.fetchall(), columns=cols)\n"
            "    finally:\n"
            "        cur.close()"
        )
    elif connector_type == "bigquery":
        body = "    return _client.query(query).to_dataframe()"
    elif connector_type == "duckdb":
        body = "    return _conn.execute(query).fetchdf()"
    else:
        body = "    raise NotImplementedError('sql() helper not wired')"

    return (
        "# ── sql() helper ────────────────────────────────────────────────────\n"
        "def sql(query: str) -> pd.DataFrame:\n"
        '    """Run a query and return the result as a pandas DataFrame."""\n'
        f"{body}"
    )
