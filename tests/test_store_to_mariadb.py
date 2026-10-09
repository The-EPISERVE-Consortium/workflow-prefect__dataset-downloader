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


# --- batched inserts ---------------------------------------------------------

ENV = {"MARIADB_HOST": "localhost", "MARIADB_USER": "user", "MARIADB_PASSWORD": "pass"}


def _run_store(df, batch_rows, **kwargs):
    """Run store_to_mariadb against a mocked connection with a small batch size; return (cursor, conn)."""
    mock_cursor = MagicMock()
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    with patch.dict(os.environ, ENV), \
            patch("tasks._mariadb.pymysql.connect", return_value=mock_conn), \
            patch("tasks.store_to_mariadb._BATCH_ROWS", batch_rows):
        store_to_mariadb.fn(df, "t", "db", **kwargs)
    return mock_cursor, mock_conn


def _big_df(n=1000):
    return pd.DataFrame(
        {
            "id": [f"{i:05d}" for i in range(n)],
            "value": [float("nan") if i % 7 == 0 else i / 3 for i in range(n)],
            "label": [None if i % 11 == 0 else f"row {i}" for i in range(n)],
        }
    )


def test_large_frame_is_inserted_in_batches_with_all_rows_in_order():
    df = _big_df(1000)

    cursor, conn = _run_store(df, batch_rows=300)

    batches = [call.args[1] for call in cursor.executemany.call_args_list]
    assert [len(b) for b in batches] == [300, 300, 300, 100]
    assert [row for b in batches for row in b] == _to_rows(df)  # same rows, same order, NaN -> None
    conn.commit.assert_called_once()  # still one transaction


def test_upsert_is_batched_too():
    df = _big_df(500)

    cursor, conn = _run_store(df, batch_rows=200, primary_key="id")

    sizes = [len(call.args[1]) for call in cursor.executemany.call_args_list]
    assert sizes == [200, 200, 100]
    assert all("ON DUPLICATE KEY UPDATE" in call.args[0] for call in cursor.executemany.call_args_list)
    conn.commit.assert_called_once()


def test_frame_smaller_than_a_batch_is_one_call():
    cursor, _ = _run_store(_big_df(10), batch_rows=300)

    cursor.executemany.assert_called_once()


def test_frame_exactly_one_batch_is_one_call():
    cursor, _ = _run_store(_big_df(300), batch_rows=300)

    cursor.executemany.assert_called_once()
    assert len(cursor.executemany.call_args.args[1]) == 300


def test_empty_frame_creates_the_table_and_inserts_nothing():
    cursor, conn = _run_store(_big_df(0), batch_rows=300)

    cursor.executemany.assert_not_called()
    executed = " ".join(call.args[0] for call in cursor.execute.call_args_list)
    assert "CREATE TABLE `t`" in executed
    conn.commit.assert_called_once()


def test_nan_in_a_later_batch_still_becomes_none():
    df = pd.DataFrame({"v": [1.0, 2.0, 3.0, float("nan"), float("inf")]})

    cursor, _ = _run_store(df, batch_rows=2)

    last = cursor.executemany.call_args_list[-1].args[1]
    assert last == [(None,)]  # the last batch holds only the inf row
    assert cursor.executemany.call_args_list[1].args[1] == [(3.0,), (None,)]
