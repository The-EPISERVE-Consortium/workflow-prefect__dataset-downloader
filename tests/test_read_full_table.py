"""Unit tests for tasks/read_full_table.py."""

from unittest.mock import MagicMock, patch

import pandas as pd

from tasks.read_full_table import read_full_table, _match_schema


def _mock_conn(rows, columns):
    cursor = MagicMock()
    cursor.fetchall.return_value = rows
    cursor.description = [(c,) for c in columns]
    conn = MagicMock()
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    return conn, cursor


SCHEMA_DF = pd.DataFrame(
    {"time": ["2026-01-01"], "temperature_2m_max (°C)": [1.5], "temperature_2m_min (°C)": [-2.0]}
)


def test_read_full_table_orders_by_primary_key_and_returns_all_rows():
    rows = [
        ("1940-01-01", "-5.2", "-11.8"),
        ("1940-01-02", "-7.9", "-14.3"),
        ("2026-01-01", "3.1", "0.4"),
    ]
    conn, cursor = _mock_conn(rows, list(SCHEMA_DF.columns))

    with patch("tasks.read_full_table.connect", return_value=conn):
        result = read_full_table.fn("weather_berlin_daily", "episerve-raw-data", "time", SCHEMA_DF)

    assert cursor.execute.call_args.args[0] == "SELECT * FROM `weather_berlin_daily` ORDER BY `time`"
    assert len(result) == 3
    conn.close.assert_called_once()


def test_read_full_table_casts_numeric_columns_to_schema_dtype():
    rows = [("1940-01-01", "-5.2", "-11.8"), ("1940-01-02", "-7.9", "")]
    conn, _ = _mock_conn(rows, list(SCHEMA_DF.columns))

    with patch("tasks.read_full_table.connect", return_value=conn):
        result = read_full_table.fn("weather_berlin_daily", "episerve-raw-data", "time", SCHEMA_DF)

    assert result["temperature_2m_max (°C)"].dtype == SCHEMA_DF["temperature_2m_max (°C)"].dtype
    assert result["temperature_2m_max (°C)"].tolist() == [-5.2, -7.9]
    assert pd.isna(result["temperature_2m_min (°C)"].iloc[1])  # "" -> NaN
    assert result["time"].tolist() == ["1940-01-01", "1940-01-02"]  # object column left alone


def test_match_schema_orders_columns_and_keeps_extras_last():
    df = pd.DataFrame({"b": ["2"], "extra": ["x"], "a": ["1"]})
    schema = pd.DataFrame({"a": [1], "b": [2]})
    out = _match_schema(df, schema)
    assert list(out.columns) == ["a", "b", "extra"]


def test_match_schema_leaves_uncastable_column_as_read():
    df = pd.DataFrame({"a": ["not-a-number", "also-not"]})
    schema = pd.DataFrame({"a": [1.0, 2.0]})
    out = _match_schema(df, schema)
    # to_numeric(errors="coerce") -> all NaN, still float, does not raise
    assert out["a"].isna().all()
