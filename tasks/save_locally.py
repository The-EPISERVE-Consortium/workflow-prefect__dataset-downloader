"""Prefect task for parsing a downloaded dataset file into a DataFrame."""

import pandas as pd
from prefect import task


@task
def parse_dataset(path: str, delimiter: str, skiprows: int = 0) -> pd.DataFrame:
    """Read a delimited file from the local filesystem into a DataFrame."""
    return pd.read_csv(path, sep=delimiter, skiprows=skiprows)
