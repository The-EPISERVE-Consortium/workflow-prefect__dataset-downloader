"""Shared MariaDB connection helper for the dataset-downloader tasks."""

import os

import pymysql


def connect(database: str | None = None) -> pymysql.connections.Connection:
    """Open a MariaDB connection from the MARIADB_* environment variables.

    Args:
        database: Optional database to connect into. Left unset by
            ``store_to_mariadb`` (which issues its own ``CREATE DATABASE`` /
            ``USE``); passed by readers that know the database already exists.

    Returns:
        An open pymysql connection. The caller is responsible for closing it.
    """
    return pymysql.connect(
        host=os.environ["MARIADB_HOST"],
        user=os.environ["MARIADB_USER"],
        password=os.environ["MARIADB_PASSWORD"],
        database=database,
    )
