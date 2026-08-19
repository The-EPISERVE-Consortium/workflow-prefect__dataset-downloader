"""Prefect task for converting a dataset DataFrame to Parquet and storing it in lakeFS."""

import io
import json
import random
import time
from pathlib import Path

import pandas as pd
from prefect import task

from tasks._logging import get_logger
from tasks.commit_to_lakefs import _get_lakefs_repository
from tasks.create_fdo_metadata import mint_qid


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
    fdo_metadata: dict,
    source_url: str,
    lakefs_repo: str,
    lakefs_branch: str = "main",
) -> None:
    """Convert a DataFrame to Parquet and commit it alongside its FDO metadata to the processed lakeFS repo."""
    logger = get_logger(__name__)

    canonical_qid = mint_qid(source_url)
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
    branch.object(parquet_path).upload(data=parquet_bytes, content_type="application/vnd.apache.parquet")

    logger.info("Uploading FDO metadata to lakeFS %s/%s/%s", lakefs_repo, lakefs_branch, fdo_path)
    branch.object(fdo_path).upload(data=json.dumps(processed_fdo, indent=2).encode(), content_type="application/json")

    changes = list(branch.uncommitted())
    if not changes:
        logger.info("No uncommitted lakeFS changes detected on %s/%s", lakefs_repo, lakefs_branch)
        return

    for attempt in range(1, 4):
        try:
            ref = branch.commit(message=f"Parquet conversion of {canonical_qid}")
            logger.info("Committed parquet %s on %s/%s", getattr(ref, "id", "<unknown>"), lakefs_repo, lakefs_branch)
            break
        except Exception as e:
            if "no changes" in str(e).lower():
                logger.info("No changes to commit on %s/%s", lakefs_repo, lakefs_branch)
                break
            elif "predicate failed" in str(e).lower():
                if attempt < 3:
                    delay = random.uniform(3, 10)
                    logger.warning(
                        "Commit conflict on %s/%s (predicate failed), retrying in %.1fs (attempt %d/3)",
                        lakefs_repo, lakefs_branch, delay, attempt,
                    )
                    time.sleep(delay)
                else:
                    raise RuntimeError(
                        f"Commit to {lakefs_repo}/{lakefs_branch} failed after 3 attempts due to concurrent modifications"
                    ) from e
            else:
                raise
