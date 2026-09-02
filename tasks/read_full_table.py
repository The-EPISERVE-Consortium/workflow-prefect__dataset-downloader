"""Prefect task: read a whole MariaDB table back as a DataFrame.

Used to publish the *full accumulated history* of an upsert dataset to lakeFS /
CKAN, rather than only the most recent downloaded window. Every MariaDB column
is stored as TEXT, so columns are re-typed from the freshly parsed download
frame before the table is handed on to ``convert_to_parquet``.
"""

import pandas as pd
from prefect import task

from tasks._logging import get_logger
from tasks._mariadb import connect


@task
def read_full_table(
    table: str,
    database: str,
    primary_key: str,
    schema_df: pd.DataFrame,
) -> pd.DataFrame:
    """Return the entire ``table`` as a DataFrame, typed like ``schema_df``.

    Args:
        table: Table name.
        database: Database name (already provisioned by ``store_to_mariadb``).
        primary_key: Column to ``ORDER BY`` -- chronological for time-keyed tables.
        schema_df: The just-downloaded delta frame; used only for column order
            and dtypes, so the published Parquet keeps the same schema it has
            today instead of all-string columns.

    Returns:
        The full table as a DataFrame.
    """
    logger = get_logger(__name__)

    conn = connect(database=database)
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"SELECT * FROM `{table}` ORDER BY `{primary_key}`")
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
    finally:
        conn.close()

    df = _match_schema(pd.DataFrame(rows, columns=columns), schema_df)
    logger.info("Read %d rows / %d columns from `%s`", len(df), len(df.columns), table)
    return df


def _match_schema(df: pd.DataFrame, schema_df: pd.DataFrame) -> pd.DataFrame:
    """Reorder ``df`` to ``schema_df``'s columns and cast each to its dtype.

    Best effort: a column that will not cast is left as read rather than
    failing the whole publish (the flow's row-count check is the hard guard).
    """
    ordered = [c for c in schema_df.columns if c in df.columns]
    extra = [c for c in df.columns if c not in schema_df.columns]
    df = df[ordered + extra].copy()

    for col in ordered:
        target = schema_df[col].dtype
        try:
            if pd.api.types.is_numeric_dtype(target):
                df[col] = pd.to_numeric(df[col], errors="coerce").astype(target, copy=False)
            else:
                df[col] = df[col].astype(target, copy=False)
        except (ValueError, TypeError):
            pass
    return df
