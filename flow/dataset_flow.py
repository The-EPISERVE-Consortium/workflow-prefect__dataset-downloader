"""Generic Prefect flow for downloading, storing, and publishing TSV datasets."""

from prefect import flow

from tasks.commit_to_lakefs import commit_to_lakefs
from tasks.create_fdo_metadata import create_fdo_metadata
from tasks.download_tsv import download_tsv
from tasks.save_locally import save_locally
from tasks.store_to_mariadb import store_to_mariadb


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
    source_delimiter: str,
    local_path: str,
    lakefs_repo: str,
    lakefs_branch: str,
    lakefs_object_path: str,
    lakefs_commit_message: str,
    mariadb_table: str,
    mariadb_database: str,
    source_skiprows: int = 0,
    mariadb_primary_key: str | None = None,
) -> None:
    """Fetch a TSV dataset, persist it locally, and publish it downstream."""
    _validate_required_parameters(
        {
            "dataset_name": dataset_name,
            "source_url": source_url,
            "source_delimiter": source_delimiter,
            "local_path": local_path,
            "lakefs_repo": lakefs_repo,
            "lakefs_branch": lakefs_branch,
            "lakefs_object_path": lakefs_object_path,
            "lakefs_commit_message": lakefs_commit_message,
            "mariadb_table": mariadb_table,
            "mariadb_database": mariadb_database,
        }
    )
    df = download_tsv(source_url, source_delimiter, source_skiprows)
    save_locally(df, local_path)
    fdo = create_fdo_metadata(dataset_name, source_url, lakefs_object_path)
    commit_to_lakefs(local_path, lakefs_repo, lakefs_branch, lakefs_object_path, lakefs_commit_message, fdo)
    store_to_mariadb(df, mariadb_table, mariadb_database, mariadb_primary_key)
