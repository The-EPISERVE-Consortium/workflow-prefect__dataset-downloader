"""Prefect task for writing the downloaded TSV data into MariaDB."""

import math

from prefect import task

from tasks._logging import get_logger
from tasks._mariadb import connect

# Rows per INSERT batch. Keeps memory flat for large tables: only one batch of row tuples
# exists at a time, instead of a tuple for every row of the DataFrame.
_BATCH_ROWS = 50_000


@task
def store_to_mariadb(df, table: str, database: str, primary_key: str | None = None) -> None:
    """Write a DataFrame to a MariaDB table.

    Without primary_key: replaces the table on each run.
    With primary_key: upserts rows so repeated runs with overlapping data are idempotent.
    """
    logger = get_logger(__name__)
    logger.info("Connecting to %s", database)

    conn = connect()
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


def _insert_in_batches(cursor, sql: str, df, logger, batch_size: int | None = None) -> None:
    """Run ``executemany`` for ``sql`` over ``df`` in batches, keeping the row order.

    A DataFrame with at most ``batch_size`` rows is sent in one call, exactly as before; an
    empty DataFrame sends nothing.

    Args:
        cursor: Open database cursor.
        sql: INSERT statement with one placeholder per column.
        df: DataFrame whose rows are inserted.
        logger: Logger for progress messages on multi-batch loads.
        batch_size: Rows per call; defaults to ``_BATCH_ROWS``.
    """
    size = batch_size or _BATCH_ROWS
    total = len(df)
    for start in range(0, total, size):
        cursor.executemany(sql, _to_rows(df.iloc[start:start + size]))
        if total > size:
            logger.info("Inserted %d/%d rows", min(start + size, total), total)


def _replace(cursor, df, table: str, logger) -> None:
    cols = ", ".join(f"`{col}` TEXT" for col in df.columns)
    cursor.execute(f"DROP TABLE IF EXISTS `{table}`")
    cursor.execute(f"CREATE TABLE `{table}` ({cols})")
    logger.info("Inserting %d rows into `%s`", len(df), table)
    placeholders = ", ".join(["%s"] * len(df.columns))
    _insert_in_batches(cursor, f"INSERT INTO `{table}` VALUES ({placeholders})", df, logger)


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
    _insert_in_batches(cursor, sql, df, logger)
