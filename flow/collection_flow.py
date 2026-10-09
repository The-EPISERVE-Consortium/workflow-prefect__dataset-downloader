"""Prefect flow for a dataset that consists of several source files and is published as one dataset."""

import tempfile
from pathlib import Path

from prefect import flow

from flow.dataset_flow import _resolve_delimiter, _validate_required_parameters
from tasks.commit_to_lakefs import commit_to_lakefs
from tasks.convert_to_parquet import publish_collection, write_parquet_component
from tasks.create_fdo_metadata import create_collection_fdo, create_fdo_metadata
from tasks.detect_content_change import resolve_source_changed_at
from tasks.download_tsv import download_file
from tasks.save_locally import parse_dataset
from tasks.store_to_mariadb import store_to_mariadb

# The processed repository is always written on `main` (as run_dataset does); `lakefs_branch` is the raw branch.
PROCESSED_BRANCH = "main"

REQUIRED_SOURCE_KEYS = ("name", "source_url", "lakefs_object_path", "mariadb_table")


def _validate_sources(sources: list[dict]) -> None:
    """Check the ``sources`` list of a collection.

    Args:
        sources: One mapping per source file. Required keys: ``name``, ``source_url``,
            ``lakefs_object_path`` and ``mariadb_table``; optional: ``display_name``,
            ``description``, ``source_delimiter`` and ``source_skiprows``.

    Raises:
        ValueError: If the list is empty, a source lacks a required key, or two sources would
            write to the same raw object, MariaDB table or parquet file.
    """
    if not sources:
        raise ValueError("A collection needs at least one source.")
    for index, source in enumerate(sources):
        missing = [key for key in REQUIRED_SOURCE_KEYS if not source.get(key)]
        if missing:
            raise ValueError(f"Source #{index + 1} is missing required key(s): {', '.join(missing)}.")
    for label, values in {
        "lakefs_object_path": [s["lakefs_object_path"] for s in sources],
        "mariadb_table": [s["mariadb_table"] for s in sources],
        "parquet file name": [Path(s["source_url"].split("?")[0]).stem for s in sources],
    }.items():
        duplicates = sorted({v for v in values if values.count(v) > 1})
        if duplicates:
            raise ValueError(f"Sources must have distinct {label} values, duplicated: {', '.join(duplicates)}.")


@flow
def run_dataset_collection(
    dataset_name: str,
    sources: list[dict],
    lakefs_repo: str,
    lakefs_branch: str,
    lakefs_commit_message: str,
    mariadb_database: str,
    lakefs_processed_repo: str,
    qid_seed: str,
    display_name: str | None = None,
    description: str | None = None,
    dataset_url: str | None = None,
    license_id: str | None = None,
    attribution: str | None = None,
) -> None:
    """Download several source files and publish them as one dataset.

    Each source file is handled like a ``run_dataset`` dataset up to the MariaDB step: it is
    downloaded, committed to lakeFS as a raw object with its own FDO sidecar, and loaded into its
    own MariaDB table. The sources are processed one after the other, so only one DataFrame is in
    memory at a time. Afterwards the Parquet files of all sources are published to
    ``data-processed`` as the components of one dataset, with one FDO and one commit.

    Args:
        dataset_name: Dataset key, used as the name of the combined dataset.
        sources: One mapping per source file (see ``_validate_sources``).
        lakefs_repo: lakeFS repository for raw data.
        lakefs_branch: lakeFS branch for raw data commits.
        lakefs_commit_message: Commit message for the raw lakeFS commits.
        mariadb_database: MariaDB database that contains the destination tables.
        lakefs_processed_repo: lakeFS repository for converted Parquet data.
        qid_seed: String hashed into the QID of the combined dataset (required; the QIDs of the raw
            files come from the source file names).
        display_name: Optional human-facing name of the combined dataset.
        description: Optional description of the combined dataset.
        dataset_url: Optional page of the dataset as a whole; defaults to the first source URL.
        license_id: Optional licence identifier (CKAN licence list value) for all FDOs.
        attribution: Optional credit line for all FDOs.

    Raises:
        ValueError: If required parameters are missing or the sources are invalid.
    """
    _validate_required_parameters(
        {
            "dataset_name": dataset_name,
            "lakefs_repo": lakefs_repo,
            "lakefs_branch": lakefs_branch,
            "lakefs_commit_message": lakefs_commit_message,
            "mariadb_database": mariadb_database,
            "lakefs_processed_repo": lakefs_processed_repo,
            "qid_seed": qid_seed,
        }
    )
    _validate_sources(sources)
    delimiters = [_resolve_delimiter(s["lakefs_object_path"], s.get("source_delimiter")) for s in sources]

    raw_fdos: list[dict] = []
    components: list[dict] = []
    with tempfile.TemporaryDirectory() as tmp:
        for source, delimiter in zip(sources, delimiters):
            object_path = source["lakefs_object_path"]
            local_path = str(Path(tmp) / Path(object_path).name)

            download_file(source["source_url"], local_path)
            source_changed_at = resolve_source_changed_at(local_path, lakefs_repo, lakefs_branch, object_path)
            raw_fdo = create_fdo_metadata(
                source["name"], source["source_url"], object_path, source.get("description"),
                source.get("display_name"), source_changed_at, license_id=license_id, attribution=attribution,
            )
            commit_to_lakefs(local_path, lakefs_repo, lakefs_branch, object_path, lakefs_commit_message, raw_fdo)

            df = parse_dataset(local_path, delimiter, source.get("source_skiprows", 0))
            store_to_mariadb(df, source["mariadb_table"], mariadb_database, None)
            components.append(write_parquet_component(df, source["source_url"], tmp))
            raw_fdos.append(raw_fdo)
            del df  # free the memory before the next (possibly large) source is parsed

        fdo = create_collection_fdo(
            dataset_name, raw_fdos, components, lakefs_processed_repo, PROCESSED_BRANCH, qid_seed,
            display_name=display_name, description=description, dataset_url=dataset_url,
            license_id=license_id, attribution=attribution,
        )
        publish_collection(fdo, components, lakefs_processed_repo, PROCESSED_BRANCH)
