"""Shared DuckDB fixtures for all tests."""

from __future__ import annotations

import duckdb
import pytest

from dbprofile.connectors.base import DuckDBConnector


@pytest.fixture(scope="session")
def duck_conn():
    """In-memory DuckDB connection, shared across the test session."""
    conn = duckdb.connect(":memory:")
    _seed(conn)
    yield conn
    conn.close()


@pytest.fixture(scope="session")
def connector(duck_conn):
    """DuckDBConnector wrapping the shared in-memory connection."""
    return DuckDBConnector(conn=duck_conn)


def _seed(conn: duckdb.DuckDBPyConnection) -> None:
    """Create synthetic tables covering all check scenarios."""

    # ── Table 1: known null rates ─────────────────────────────────────
    conn.execute("DROP TABLE IF EXISTS test_nulls")
    conn.execute("""
    CREATE TABLE test_nulls AS
    SELECT
      i                                    AS id,
      CASE WHEN i % 5 = 0 THEN NULL ELSE 'user_' || i END AS email,
      CASE WHEN i % 10 = 0 THEN NULL ELSE i * 1.5 END      AS amount,
      CASE WHEN i % 3 = 0 THEN NULL ELSE current_date - CAST(i % 365 AS INTEGER) END AS created_at
    FROM range(1, 1001) t(i)
    """)

    # ── Table 2: known duplicates ─────────────────────────────────────
    conn.execute("DROP TABLE IF EXISTS test_dupes")
    conn.execute("""
    CREATE TABLE test_dupes AS
    SELECT
      (i % 10) AS id,
      'category_' || (i % 3) AS category
    FROM range(1, 101) t(i)
    """)

    # ── Table 3: date gaps ────────────────────────────────────────────
    conn.execute("DROP TABLE IF EXISTS test_gaps")
    conn.execute("""
    CREATE TABLE test_gaps AS
    SELECT
      (DATE '2023-01-01' + INTERVAL (i) DAY)::date AS event_date,
      i AS value
    FROM range(0, 30) t(i)
    WHERE i NOT IN (5, 6, 7, 15)   -- deliberate gaps
    """)

    # ── Table 4: format violations ────────────────────────────────────
    conn.execute("DROP TABLE IF EXISTS test_formats")
    conn.execute("""
    CREATE TABLE test_formats AS
    SELECT
      i AS id,
      CASE
        WHEN i <= 80  THEN 'user' || i || '@example.com'
        ELSE 'not-an-email-' || i          -- 20% violations
      END AS email,
      CASE
        WHEN i <= 90 THEN 'active'
        WHEN i <= 95 THEN 'inactive'
        ELSE 'pending'
      END AS status
    FROM range(1, 101) t(i)
    """)

    # ── Table 5: numeric outliers ─────────────────────────────────────
    conn.execute("DROP TABLE IF EXISTS test_numeric")
    conn.execute("""
    CREATE TABLE test_numeric AS
    SELECT
      i AS id,
      CASE
        WHEN i = 1   THEN -9999.0   -- low outlier
        WHEN i = 100 THEN  9999.0   -- high outlier
        ELSE CAST(50 + (i % 20) - 10 AS DOUBLE)
      END AS score
    FROM range(1, 101) t(i)
    """)
