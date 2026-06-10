"""Prefect task for creating an FDO (FAIR Digital Object) metadata record."""

import hashlib
from datetime import datetime, timezone
from pathlib import Path

from prefect import task


def mint_qid(source_url: str) -> str:
    """Create a stable QID-like identifier from a source URL filename.

    Args:
        source_url: URL of the source dataset.

    Returns:
        Stable QID-like identifier derived from the source filename.
    """
    filename = Path(source_url.split("?")[0]).name
    digest = hashlib.sha256(filename.encode()).hexdigest()
    return f"Q{int(digest, 16) % 10**13:013d}"


def _media_type(object_path: str) -> str:
    """Infer the media type for a dataset object path.

    Args:
        object_path: lakeFS object path or filename.

    Returns:
        Media type matching the object extension, or a binary fallback.
    """
    return {
        ".tsv": "text/tab-separated-values",
        ".csv": "text/csv",
        ".json": "application/json",
        ".parquet": "application/vnd.apache.parquet",
    }.get(Path(object_path).suffix.lower(), "application/octet-stream")


@task
def create_fdo_metadata(
    dataset_name: str,
    source_url: str,
    lakefs_object_path: str,
    description: str | None = None,
    display_name: str | None = None,
) -> dict:
    """Build an FDO metadata record for the downloaded dataset.

    Args:
        dataset_name: Human-readable dataset name.
        source_url: URL used to download the dataset.
        lakefs_object_path: Target object path for the raw dataset in lakeFS.
        description: Optional dataset description for the schema.org profile.
        display_name: Optional human-facing display name for the schema.org profile.

    Returns:
        FDO metadata record for the downloaded dataset.
    """
    qid = mint_qid(source_url) + "-raw"
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    filename = Path(lakefs_object_path).name
    profile_description = description or f"Dataset {dataset_name}"
    profile_display_name = display_name or dataset_name

    return {
        "@context": [
            "https://w3id.org/fdo/context/v1",
            {
                "schema": "https://schema.org/",
                "prov": "http://www.w3.org/ns/prov#",
                "fdo": "https://w3id.org/fdo/vocabulary/",
            },
        ],
        "@id": qid,
        "@type": "DigitalObject",
        "kernel": {
            "@id": qid,
            "digitalObjectType": "https://schema.org/Dataset",
            "primaryIdentifier": qid,
            "kernelVersion": "v1",
            "immutable": False,
            "modified": now,
            "fdo:hasComponent": [
                {
                    "@id": f"#{filename}",
                    "componentId": filename,
                    "mediaType": _media_type(lakefs_object_path),
                }
            ],
        },
        "profile": {
            "@context": "https://schema.org/",
            "@type": "Dataset",
            "@id": qid,
            "name": dataset_name,
            "display_name": profile_display_name,
            "description": profile_description,
            "url": source_url,
            "additionalType": lakefs_object_path.split("/")[0],
            "distribution": [
                {
                    "@type": "DataDownload",
                    "contentUrl": source_url,
                }
            ],
        },
        "provenance": {
            "prov:generatedAtTime": now,
            "prov:wasAttributedTo": "EPISERVE Consortium dataset downloader",
        },
    }
