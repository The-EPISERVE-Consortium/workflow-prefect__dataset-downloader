"""Prefect task for creating an FDO (FAIR Digital Object) metadata record."""

import hashlib
from datetime import datetime, timezone
from pathlib import Path

from prefect import task


def mint_qid(source_url: str, seed_override: str | None = None) -> str:
    """Create a stable QID-like identifier for a source dataset.

    By default the seed is the source URL's filename (query string ignored),
    which keeps the QID stable across runs even when query parameters change.
    Pass ``seed_override`` when that filename is not unique per dataset -- e.g.
    two Open-Meteo endpoints that both resolve to ``forecast``.

    Args:
        source_url: URL of the source dataset.
        seed_override: Explicit seed string to hash instead of the URL filename.

    Returns:
        Stable QID-like identifier derived from the seed.
    """
    seed = seed_override or Path(source_url.split("?")[0]).name
    digest = hashlib.sha256(seed.encode()).hexdigest()
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
    source_changed_at: str | None = None,
    license_id: str | None = None,
    attribution: str | None = None,
    qid_seed: str | None = None,
) -> dict:
    """Build an FDO metadata record for the downloaded dataset.

    Args:
        dataset_name: Human-readable dataset name.
        source_url: URL used to download the dataset.
        lakefs_object_path: Target object path for the raw dataset in lakeFS.
        description: Optional dataset description for the schema.org profile.
        display_name: Optional human-facing display name for the schema.org profile.
        source_changed_at: Optional timestamp of the run in which the source
            data last actually changed (as opposed to `kernel.modified`, which
            is stamped on every run regardless of whether the source changed).
            Change detection itself compares against the checksum lakeFS already
            reports for the previously committed raw object, so no content hash
            needs to be stored here.
        license_id: Optional licence identifier for the schema.org profile
            (`profile.license`). Passed straight through to CKAN's native
            `license_id` field by sync-lakefs-ckan, so use a value from CKAN's
            licence list, e.g. `cc-by`.
        attribution: Optional credit line for the schema.org profile
            (`profile.creditText`), surfaced as an `attribution` extra in CKAN.
        qid_seed: Optional explicit seed for `mint_qid`, for datasets whose
            source URL filename is not unique (e.g. two Open-Meteo endpoints).

    Returns:
        FDO metadata record for the downloaded dataset.
    """
    qid = mint_qid(source_url, qid_seed) + "-raw"
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    filename = Path(lakefs_object_path).name
    profile_description = description or f"Dataset {dataset_name}"
    profile_display_name = display_name or dataset_name

    licence_fields = {}
    if license_id:
        licence_fields["license"] = license_id
    if attribution:
        licence_fields["creditText"] = attribution

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
            **licence_fields,
        },
        "provenance": {
            "prov:generatedAtTime": now,
            "prov:wasAttributedTo": "EPISERVE Consortium dataset downloader",
            "source_changed_at": source_changed_at or now,
        },
    }
