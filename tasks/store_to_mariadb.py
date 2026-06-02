"""Prefect task for writing the downloaded TSV data into MariaDB."""

import math
import os

import pymysql
from prefect import task

from tasks._logging import get_logger


@task
def store_to_mariadb(df, table: str, database: str, primary_key: str | None = None) -> None:
    """Write a DataFrame to a MariaDB table.

    Without primary_key: replaces the table on each run.
    With primary_key: upserts rows so repeated runs with overlapping data are idempotent.
    """
    logger = get_logger(__name__)
    host = os.environ["MARIADB_HOST"]
    logger.info("Connecting to %s/%s", host, database)

    conn = pymysql.connect(
        host=host,
        user=os.environ["MARIADB_USER"],
        password=os.environ["MARIADB_PASSWORD"],
    )
    with conn.cursor() as cursor:
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{database}`")
        cursor.execute(f"USE `{database}`")
    try:
        with conn.cursor() as cursor:
            if primary_key:
                _upsert(cursor, df, table, primary_key, logger)
            else:
                _replace(cursor, df, table, logger)
        conn.commit()
        logger.info("Done. Table `%s` now has %d rows and %d columns.", table, len(df), len(df.columns))
    finally:
        conn.close()


def _to_rows(df) -> list[tuple]:
    return [
        tuple(None if isinstance(v, float) and not math.isfinite(v) else v for v in row)
        for row in df.itertuples(index=False, name=None)
    ]


def _replace(cursor, df, table: str, logger) -> None:
    cols = ", ".join(f"`{col}` TEXT" for col in df.columns)
    cursor.execute(f"DROP TABLE IF EXISTS `{table}`")
    cursor.execute(f"CREATE TABLE `{table}` ({cols})")
    logger.info("Inserting %d rows into `%s`", len(df), table)
    placeholders = ", ".join(["%s"] * len(df.columns))
    cursor.executemany(f"INSERT INTO `{table}` VALUES ({placeholders})", _to_rows(df))


def _upsert(cursor, df, table: str, primary_key: str, logger) -> None:
    cols = ", ".join(
        f"`{col}` VARCHAR(255) PRIMARY KEY" if col == primary_key else f"`{col}` TEXT"
        for col in df.columns
    )
    cursor.execute(f"CREATE TABLE IF NOT EXISTS `{table}` ({cols})")
    logger.info("Upserting %d rows into `%s`", len(df), table)
    placeholders = ", ".join(["%s"] * len(df.columns))
    updates = ", ".join(f"`{col}`=VALUES(`{col}`)" for col in df.columns if col != primary_key)
    sql = f"INSERT INTO `{table}` VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {updates}"
    cursor.executemany(sql, _to_rows(df))
