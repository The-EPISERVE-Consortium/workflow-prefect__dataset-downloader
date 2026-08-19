"""Generic Prefect flow for downloading, storing, and publishing datasets."""

import tempfile
from pathlib import Path

from prefect import flow

from tasks.commit_to_lakefs import commit_to_lakefs
from tasks.convert_to_parquet import convert_to_parquet
from tasks.create_fdo_metadata import create_fdo_metadata
from tasks.detect_content_change import resolve_content_changed_at
from tasks.download_tsv import download_file
from tasks.save_locally import parse_dataset
from tasks.store_to_mariadb import store_to_mariadb

_EXTENSION_DELIMITERS = {
    ".tsv": "\t",
    ".csv": ",",
}


def _resolve_delimiter(lakefs_object_path: str, override: str | None) -> str:
    """Resolve the source file delimiter from configuration or object path.

    Args:
        lakefs_object_path: Target lakeFS object path for the source file.
        override: Explicit delimiter from configuration, when provided.

    Returns:
        The delimiter to use when parsing the downloaded dataset.

    Raises:
        ValueError: If no override is set and the object extension is unsupported.
    """
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
    """Raise a clear error if a required flow parameter is missing or blank.

    Args:
        params: Mapping of required parameter names to provided values.

    Raises:
        ValueError: If any required parameter is missing or blank.
    """
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
    display_name: str | None = None,
    description: str | None = None,
) -> None:
    """Download a dataset, publish metadata, and load it into storage targets.

    Args:
        dataset_name: Dataset key or name used in generated metadata.
        source_url: URL used to download the source dataset.
        lakefs_repo: lakeFS repository for raw data.
        lakefs_branch: lakeFS branch for raw data commits.
        lakefs_object_path: Target path for the raw dataset object in lakeFS.
        lakefs_commit_message: Commit message for the raw lakeFS commit.
        mariadb_table: MariaDB table that receives the parsed dataset.
        mariadb_database: MariaDB database that contains the destination table.
        lakefs_processed_repo: lakeFS repository for converted Parquet data.
        source_delimiter: Optional delimiter override for parsing the source file.
        source_skiprows: Number of leading source rows to skip while parsing.
        mariadb_primary_key: Optional primary key column for MariaDB writes.
        display_name: Optional display name for generated FDO metadata.
        description: Optional dataset description for generated FDO metadata.

    Raises:
        ValueError: If required parameters are missing or the delimiter cannot be inferred.
    """
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
    content_hash, content_changed_at = resolve_content_changed_at(
        local_path, lakefs_repo, lakefs_branch, lakefs_object_path
    )
    fdo = create_fdo_metadata(
        dataset_name, source_url, lakefs_object_path, description, display_name,
        content_hash, content_changed_at,
    )
    commit_to_lakefs(local_path, lakefs_repo, lakefs_branch, lakefs_object_path, lakefs_commit_message, fdo)
    df = parse_dataset(local_path, delimiter, source_skiprows)
    store_to_mariadb(df, mariadb_table, mariadb_database, mariadb_primary_key)
    convert_to_parquet(df, fdo, source_url, lakefs_processed_repo)
