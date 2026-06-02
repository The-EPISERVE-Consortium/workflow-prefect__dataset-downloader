"""Generic Prefect flow for downloading, storing, and publishing datasets."""

import tempfile
from pathlib import Path

from prefect import flow

from tasks.commit_to_lakefs import commit_to_lakefs
from tasks.convert_to_parquet import convert_to_parquet
from tasks.create_fdo_metadata import create_fdo_metadata
from tasks.download_tsv import download_file
from tasks.save_locally import parse_dataset
from tasks.store_to_mariadb import store_to_mariadb

_EXTENSION_DELIMITERS = {
    ".tsv": "\t",
    ".csv": ",",
}


def _resolve_delimiter(lakefs_object_path: str, override: str | None) -> str:
    if override is not None:
        return override
    ext = Path(lakefs_object_path).suffix.lower()
    if ext not in _EXTENSION_DELIMITERS:
        raise ValueError(
            f"Cannot infer delimiter for extension '{ext}'. "
            "Set source_delimiter explicitly in datasets.yaml."
        )
    return _EXTENSION_DELIMITERS[ext]


def _validate_required_parameters(params: dict[str, str]) -> None:
    """Raise a clear error if a required flow parameter is missing or blank."""
    missing = [name for name, value in params.items() if value is None or value == ""]
    if missing:
        missing_list = ", ".join(sorted(missing))
        raise ValueError(
            f"Missing required flow parameter(s): {missing_list}. "
            "Provide all dataset-specific parameters in the deployment configuration or run request."
        )


@flow
def run_dataset(
    dataset_name: str,
    source_url: str,
    lakefs_repo: str,
    lakefs_branch: str,
    lakefs_object_path: str,
    lakefs_commit_message: str,
    mariadb_table: str,
    mariadb_database: str,
    lakefs_processed_repo: str,
    source_delimiter: str | None = None,
    source_skiprows: int = 0,
    mariadb_primary_key: str | None = None,
) -> None:
    """Download a dataset, commit it to lakeFS with FDO metadata, then load it into MariaDB."""
    _validate_required_parameters(
        {
            "dataset_name": dataset_name,
            "source_url": source_url,
            "lakefs_repo": lakefs_repo,
            "lakefs_branch": lakefs_branch,
            "lakefs_object_path": lakefs_object_path,
            "lakefs_commit_message": lakefs_commit_message,
            "mariadb_table": mariadb_table,
            "mariadb_database": mariadb_database,
            "lakefs_processed_repo": lakefs_processed_repo,
        }
    )
    delimiter = _resolve_delimiter(lakefs_object_path, source_delimiter)
    local_path = str(Path(tempfile.gettempdir()) / Path(lakefs_object_path).name)
    download_file(source_url, local_path)
    fdo = create_fdo_metadata(dataset_name, source_url, lakefs_object_path)
    commit_to_lakefs(local_path, lakefs_repo, lakefs_branch, lakefs_object_path, lakefs_commit_message, fdo)
    df = parse_dataset(local_path, delimiter, source_skiprows)
    store_to_mariadb(df, mariadb_table, mariadb_database, mariadb_primary_key)
    convert_to_parquet(df, fdo["@id"], fdo, source_url, lakefs_processed_repo)
