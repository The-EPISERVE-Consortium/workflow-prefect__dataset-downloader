"""Unit tests for tasks/store_to_mariadb.py."""

import math
import os
from unittest.mock import MagicMock, patch

import pandas as pd

from tasks.store_to_mariadb import store_to_mariadb, _to_rows


SAMPLE_DF = pd.DataFrame(
    {
        "Kalenderwoche": ["2024-W01", "2024-W02"],
        "Inzidenz": [12.3, 14.7],
    }
)


@patch.dict(
    os.environ,
    {
        "MARIADB_HOST": "localhost",
        "MARIADB_USER": "user",
        "MARIADB_PASSWORD": "pass",
    },
)
def test_store_to_mariadb_writes_and_commits():
    """store_to_mariadb should create the table, insert rows, and commit."""
    mock_cursor = MagicMock()
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    with patch("tasks._mariadb.pymysql.connect", return_value=mock_conn):
        store_to_mariadb.fn(SAMPLE_DF, "grippeweb", "db")

    assert mock_cursor.execute.call_count == 4  # CREATE DATABASE, USE, DROP TABLE, CREATE TABLE
    mock_cursor.executemany.assert_called_once()
    call_args = mock_cursor.executemany.call_args
    assert "grippeweb" in call_args.args[0]
    assert len(call_args.args[1]) == len(SAMPLE_DF)
    mock_conn.commit.assert_called_once()
    mock_conn.close.assert_called_once()


def test_to_rows_converts_nan_and_inf_to_none():
    """_to_rows should replace NaN, inf, and -inf with None."""
    df = pd.DataFrame({"a": [1.0, float("nan"), float("inf"), float("-inf")]})
    rows = _to_rows(df)
    assert rows == [(1.0,), (None,), (None,), (None,)]


def test_to_rows_preserves_finite_floats():
    """_to_rows should leave normal float values unchanged."""
    df = pd.DataFrame({"a": [0.0, -1.5, 3.14]})
    rows = _to_rows(df)
    assert all(math.isfinite(r[0]) for r in rows)


@patch.dict(
    os.environ,
    {
        "MARIADB_HOST": "localhost",
        "MARIADB_USER": "user",
        "MARIADB_PASSWORD": "pass",
    },
)
def test_store_to_mariadb_upserts_when_primary_key_given():
    """store_to_mariadb should use INSERT ... ON DUPLICATE KEY UPDATE when primary_key is set."""
    mock_cursor = MagicMock()
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    with patch("tasks._mariadb.pymysql.connect", return_value=mock_conn):
        store_to_mariadb.fn(SAMPLE_DF, "weather", "db", primary_key="Kalenderwoche")

    assert mock_cursor.execute.call_count == 3  # CREATE DATABASE, USE, CREATE TABLE IF NOT EXISTS
    mock_cursor.executemany.assert_called_once()
    sql = mock_cursor.executemany.call_args.args[0]
    assert "ON DUPLICATE KEY UPDATE" in sql
    assert len(mock_cursor.executemany.call_args.args[1]) == len(SAMPLE_DF)
