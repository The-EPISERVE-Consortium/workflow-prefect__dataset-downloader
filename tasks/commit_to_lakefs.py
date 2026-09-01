"""Prefect task for uploading the saved TSV to lakeFS and committing it."""

import json
import os
import random
import time
from pathlib import Path

from prefect import task

from tasks._logging import get_logger

def _get_lakefs_repository(repo: str):
    """Create a lakeFS repository handle from environment-based connection settings."""
    try:
        import lakefs
        from lakefs.client import Client
    except ImportError as exc:
        raise RuntimeError("The 'lakefs' package must be installed to upload to lakeFS.") from exc

    client = Client(
        host=os.environ["LAKEFS_HOST"],
        username=os.environ["LAKEFS_ACCESS_KEY"],
        password=os.environ["LAKEFS_SECRET_KEY"],
    )
    return lakefs.repository(repo, client=client)


@task
def commit_to_lakefs(
    path: str,
    repo: str,
    branch: str,
    object_path: str,
    commit_message: str,
    fdo_metadata: dict | None = None,
) -> None:
    """Upload the saved file to lakeFS and create a commit on the target branch.

    If fdo_metadata is provided, uploads it as <stem>.fdo.json alongside the data file
    in the same commit.
    """
    logger = get_logger(__name__)
    lakefs_branch = _get_lakefs_repository(repo).branch(branch)
    local_path = Path(path)

    logger.info("Uploading %s to lakeFS %s/%s/%s", local_path, repo, branch, object_path)
    with local_path.open("rb") as infile:
        lakefs_branch.object(object_path).upload(
            data=infile.read(),
            content_type="text/tab-separated-values",
            # Upload via the lakeFS API rather than a presigned PUT: the SDK
            # omits Content-Type from the presigned SigV4 signature and Ceph
            # RGW (Squid) rejects the mismatch with 403 AccessDenied.
            pre_sign=False,
        )

    if fdo_metadata is not None:
        fdo_object_path = str(Path(object_path).parent / (Path(object_path).stem + ".fdo.json"))
        logger.info("Uploading FDO metadata to lakeFS %s/%s/%s", repo, branch, fdo_object_path)
        lakefs_branch.object(fdo_object_path).upload(
            data=json.dumps(fdo_metadata, indent=2).encode(),
            content_type="application/json",
            pre_sign=False,
        )

    changes = list(lakefs_branch.uncommitted())
    if not changes:
        logger.info("No uncommitted lakeFS changes detected on %s/%s", repo, branch)
        return

    for attempt in range(1, 4):
        try:
            ref = lakefs_branch.commit(message=commit_message)
            logger.info("Committed lakeFS change %s on %s/%s", getattr(ref, "id", "<unknown>"), repo, branch)
            break
        except Exception as e:
            if "no changes" in str(e).lower():
                logger.info("No changes to commit on %s/%s", repo, branch)
                break
            elif "predicate failed" in str(e).lower():
                if attempt < 3:
                    delay = random.uniform(3, 10)
                    logger.warning(
                        "Commit conflict on %s/%s (predicate failed), retrying in %.1fs (attempt %d/3)",
                        repo, branch, delay, attempt,
                    )
                    time.sleep(delay)
                else:
                    raise RuntimeError(
                        f"Commit to {repo}/{branch} failed after 3 attempts due to concurrent modifications"
                    ) from e
            else:
                raise
