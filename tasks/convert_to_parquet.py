"""Prefect task for converting a dataset DataFrame to Parquet and storing it in lakeFS."""

import io
import json
import os
import random
import time
from pathlib import Path

import pandas as pd
from prefect import task

from tasks._logging import get_logger
from tasks.commit_to_lakefs import _get_lakefs_repository
from tasks._sharding import shard_qid  # noqa: F401  (re-exported: tests and callers import it from here)
from tasks.create_fdo_metadata import mint_qid


def _commit_changes(branch, repo: str, branch_name: str, message: str, logger) -> None:
    """Commit everything uncommitted on a lakeFS branch, retrying on concurrent-commit conflicts.

    Does nothing when there are no uncommitted changes. Retries up to three times with a random
    delay when lakeFS reports a ``predicate failed`` conflict (another flow committed meanwhile).

    Raises:
        RuntimeError: If the conflict persists after three attempts.
    """
    changes = list(branch.uncommitted())
    if not changes:
        logger.info("No uncommitted lakeFS changes detected on %s/%s", repo, branch_name)
        return

    for attempt in range(1, 4):
        try:
            ref = branch.commit(message=message)
            logger.info("Committed parquet %s on %s/%s", getattr(ref, "id", "<unknown>"), repo, branch_name)
            break
        except Exception as e:
            if "no changes" in str(e).lower():
                logger.info("No changes to commit on %s/%s", repo, branch_name)
                break
            elif "predicate failed" in str(e).lower():
                if attempt < 3:
                    delay = random.uniform(3, 10)
                    logger.warning(
                        "Commit conflict on %s/%s (predicate failed), retrying in %.1fs (attempt %d/3)",
                        repo, branch_name, delay, attempt,
                    )
                    time.sleep(delay)
                else:
                    raise RuntimeError(
                        f"Commit to {repo}/{branch_name} failed after 3 attempts due to concurrent modifications"
                    ) from e
            else:
                raise


@task
def convert_to_parquet(
    df: pd.DataFrame,
    fdo_metadata: dict,
    source_url: str,
    lakefs_repo: str,
    lakefs_branch: str = "main",
    qid_seed: str | None = None,
) -> None:
    """Convert a DataFrame to Parquet and commit it alongside its FDO metadata to the processed lakeFS repo.

    ``qid_seed`` must match the value passed to ``create_fdo_metadata`` so the
    processed FDO's QID lines up with the raw one.
    """
    logger = get_logger(__name__)

    canonical_qid = mint_qid(source_url, qid_seed)
    qid_upper = canonical_qid.upper()
    shard_prefix = shard_qid(canonical_qid)
    source_stem = Path(source_url.split("?")[0]).stem
    parquet_filename = source_stem + ".parquet"
    parquet_path = f"{shard_prefix}/components/{parquet_filename}"
    fdo_path = f"{shard_prefix}/{qid_upper}.fdo.json"

    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    parquet_bytes = buf.getvalue()
    parquet_size = len(parquet_bytes)

    raw_qid = fdo_metadata["@id"]
    lakefs_content_url = f"lakefs://{lakefs_repo}/{lakefs_branch}/{parquet_path}"

    processed_fdo = {
        **fdo_metadata,
        "@id": canonical_qid,
        "kernel": {
            **fdo_metadata["kernel"],
            "@id": canonical_qid,
            "primaryIdentifier": canonical_qid,
            "fdo:hasComponent": [
                {
                    "@id": f"components/{parquet_filename}",
                    "componentId": parquet_filename,
                    "mediaType": "application/vnd.apache.parquet",
                }
            ],
        },
        "profile": {
            **fdo_metadata.get("profile", {}),
            "@id": canonical_qid,
        },
        "provenance": {
            **fdo_metadata.get("provenance", {}),
            "prov:wasDerivedFrom": {
                "@id": raw_qid,
                "prov:hadPrimarySource": source_url,
            },
        },
    }

    if "profile" in processed_fdo and processed_fdo["profile"].get("distribution"):
        dist = processed_fdo["profile"]["distribution"]
        processed_fdo = {
            **processed_fdo,
            "profile": {
                **processed_fdo["profile"],
                "distribution": [
                    {**dist[0], "contentUrl": lakefs_content_url, "contentSize": parquet_size},
                    *dist[1:],
                ],
            },
        }

    branch = _get_lakefs_repository(lakefs_repo).branch(lakefs_branch)

    logger.info("Uploading parquet to lakeFS %s/%s/%s", lakefs_repo, lakefs_branch, parquet_path)
    # pre_sign=False: upload via the lakeFS API, not a presigned PUT. The SDK
    # omits Content-Type from the presigned SigV4 signature and Ceph RGW (Squid)
    # rejects the mismatch with 403 AccessDenied.
    branch.object(parquet_path).upload(
        data=parquet_bytes, content_type="application/vnd.apache.parquet", pre_sign=False
    )

    logger.info("Uploading FDO metadata to lakeFS %s/%s/%s", lakefs_repo, lakefs_branch, fdo_path)
    branch.object(fdo_path).upload(
        data=json.dumps(processed_fdo, indent=2).encode(),
        content_type="application/json",
        pre_sign=False,
    )

    _commit_changes(branch, lakefs_repo, lakefs_branch, f"Parquet conversion of {canonical_qid}", logger)


@task
def write_parquet_component(df: pd.DataFrame, source_url: str, directory: str) -> dict:
    """Write one source's DataFrame to a local Parquet file, named after the source file.

    Writing locally (not to lakeFS) lets a multi-source flow release each DataFrame before the
    next source is processed, and publish all files in one quick step at the end, so the
    branch never holds a half-finished set of files.

    Args:
        df: DataFrame to store.
        source_url: URL of the source file; its filename stem names the parquet file.
        directory: Local directory to write into.

    Returns:
        ``{"componentId": <file name>, "local_path": <path>, "size": <bytes>}``.
    """
    name = Path(source_url.split("?")[0]).stem + ".parquet"
    local_path = os.path.join(directory, name)
    df.to_parquet(local_path, index=False)
    return {"componentId": name, "local_path": local_path, "size": os.path.getsize(local_path)}


@task
def publish_collection(
    fdo: dict,
    components: list[dict],
    lakefs_repo: str,
    lakefs_branch: str = "main",
) -> None:
    """Upload the Parquet files and the FDO of a multi-source dataset and commit them together.

    Args:
        fdo: Processed FDO from ``create_collection_fdo``; its ``@id`` determines the sharded path.
        components: Files from ``write_parquet_component`` (``componentId`` and ``local_path``).
        lakefs_repo: lakeFS repository for processed data.
        lakefs_branch: Branch to commit to.
    """
    logger = get_logger(__name__)
    qid = fdo["@id"]
    shard = shard_qid(qid)
    branch = _get_lakefs_repository(lakefs_repo).branch(lakefs_branch)

    for comp in components:
        path = f"{shard}/components/{comp['componentId']}"
        logger.info("Uploading parquet to lakeFS %s/%s/%s", lakefs_repo, lakefs_branch, path)
        with open(comp["local_path"], "rb") as f:
            # pre_sign=False: see convert_to_parquet (presigned PUTs are rejected by the object store)
            branch.object(path).upload(data=f.read(), content_type="application/vnd.apache.parquet", pre_sign=False)

    fdo_path = f"{shard}/{qid}.fdo.json"
    logger.info("Uploading FDO metadata to lakeFS %s/%s/%s", lakefs_repo, lakefs_branch, fdo_path)
    branch.object(fdo_path).upload(
        data=json.dumps(fdo, indent=2).encode(), content_type="application/json", pre_sign=False
    )

    _commit_changes(branch, lakefs_repo, lakefs_branch, f"Parquet conversion of {qid}", logger)
