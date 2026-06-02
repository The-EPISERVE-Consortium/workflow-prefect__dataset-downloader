"""Prefect task for converting a dataset DataFrame to Parquet and storing it in lakeFS."""

import io
import json
from pathlib import Path

import pandas as pd
from prefect import task

from tasks._logging import get_logger
from tasks.commit_to_lakefs import _get_lakefs_repository


def shard_qid(qid: str) -> str:
    """Return the sharded directory prefix for a QID using 2-2-2 padding.

    Args:
        qid: Identifier beginning with ``Q`` followed by digits.

    Returns:
        str: Sharded prefix in the form ``pp/qq/rr/Qxxxx``.
    """
    normalized = qid.upper()
    if not normalized.startswith("Q"):
        raise ValueError("QID must start with 'Q'")
    digits = normalized[1:]
    if not digits.isdigit():
        raise ValueError("QID must contain digits after 'Q'")
    padded = digits.zfill(6)
    return f"{padded[0:2]}/{padded[2:4]}/{padded[4:6]}/{normalized}"


@task
def convert_to_parquet(
    df: pd.DataFrame,
    qid: str,
    fdo_metadata: dict,
    source_url: str,
    lakefs_repo: str,
    lakefs_branch: str = "main",
) -> None:
    """Convert a DataFrame to Parquet and commit it alongside its FDO metadata to the processed lakeFS repo."""
    logger = get_logger(__name__)

    qid_upper = qid.upper()
    shard_prefix = shard_qid(qid)
    source_stem = Path(source_url.split("?")[0]).stem
    parquet_filename = source_stem + ".parquet"
    parquet_path = f"{shard_prefix}/components/{parquet_filename}"
    fdo_path = f"{shard_prefix}/{qid_upper}.fdo.json"

    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    parquet_bytes = buf.getvalue()

    processed_fdo = {
        **fdo_metadata,
        "kernel": {
            **fdo_metadata["kernel"],
            "fdo:hasComponent": [
                {
                    "@id": f"components/{parquet_filename}",
                    "componentId": parquet_filename,
                    "mediaType": "application/vnd.apache.parquet",
                }
            ],
        },
    }

    branch = _get_lakefs_repository(lakefs_repo).branch(lakefs_branch)

    logger.info("Uploading parquet to lakeFS %s/%s/%s", lakefs_repo, lakefs_branch, parquet_path)
    branch.object(parquet_path).upload(data=parquet_bytes, content_type="application/vnd.apache.parquet")

    logger.info("Uploading FDO metadata to lakeFS %s/%s/%s", lakefs_repo, lakefs_branch, fdo_path)
    branch.object(fdo_path).upload(data=json.dumps(processed_fdo, indent=2).encode(), content_type="application/json")

    changes = list(branch.uncommitted())
    if not changes:
        logger.info("No uncommitted lakeFS changes detected on %s/%s", lakefs_repo, lakefs_branch)
        return

    try:
        ref = branch.commit(message=f"Parquet conversion of {qid}")
        logger.info("Committed parquet %s on %s/%s", getattr(ref, "id", "<unknown>"), lakefs_repo, lakefs_branch)
    except Exception as e:
        if "no changes" in str(e).lower():
            logger.info("No changes to commit on %s/%s", lakefs_repo, lakefs_branch)
        else:
            raise
