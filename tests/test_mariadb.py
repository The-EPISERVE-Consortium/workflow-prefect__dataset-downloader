"""Unit tests for tasks/_mariadb.py."""

import os
from unittest.mock import MagicMock, patch

from tasks._mariadb import connect


ENV = {
    "MARIADB_HOST": "db.example",
    "MARIADB_USER": "user",
    "MARIADB_PASSWORD": "pass",
}


@patch.dict(os.environ, ENV)
def test_connect_reads_env_and_defaults_database_to_none():
    with patch("tasks._mariadb.pymysql.connect", return_value=MagicMock()) as mock_connect:
        connect()

    mock_connect.assert_called_once_with(
        host="db.example", user="user", password="pass", database=None
    )


@patch.dict(os.environ, ENV)
def test_connect_passes_database_when_given():
    with patch("tasks._mariadb.pymysql.connect", return_value=MagicMock()) as mock_connect:
        connect(database="episerve-raw-data")

    assert mock_connect.call_args.kwargs["database"] == "episerve-raw-data"
