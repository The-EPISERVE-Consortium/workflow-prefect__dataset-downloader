"""Prefect task for creating an FDO (FAIR Digital Object) metadata record."""

import hashlib
from datetime import datetime, timezone
from pathlib import Path

from prefect import task


def mint_qid(source_url: str) -> str:
    filename = Path(source_url.split("?")[0]).name
    digest = hashlib.sha256(filename.encode()).hexdigest()
    return f"Q{int(digest, 16) % 10**13:013d}"


def _media_type(object_path: str) -> str:
    return {
        ".tsv": "text/tab-separated-values",
        ".csv": "text/csv",
        ".json": "application/json",
        ".parquet": "application/vnd.apache.parquet",
    }.get(Path(object_path).suffix.lower(), "application/octet-stream")


@task
def create_fdo_metadata(dataset_name: str, source_url: str, lakefs_object_path: str) -> dict:
    """Build an FDO metadata record for the downloaded dataset."""
    qid = mint_qid(source_url)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    filename = Path(lakefs_object_path).name

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
            "description": f"Dataset {dataset_name} downloaded from {source_url}",
            "url": source_url,
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
