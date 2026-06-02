"""Prefect task for uploading the saved TSV to lakeFS and committing it."""

import json
import os
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
        )

    if fdo_metadata is not None:
        fdo_object_path = str(Path(object_path).parent / (Path(object_path).stem + ".fdo.json"))
        logger.info("Uploading FDO metadata to lakeFS %s/%s/%s", repo, branch, fdo_object_path)
        lakefs_branch.object(fdo_object_path).upload(
            data=json.dumps(fdo_metadata, indent=2).encode(),
            content_type="application/json",
        )

    changes = list(lakefs_branch.uncommitted())
    if not changes:
        logger.info("No uncommitted lakeFS changes detected on %s/%s", repo, branch)
        return

    ref = lakefs_branch.commit(message=commit_message)
    logger.info("Committed lakeFS change %s on %s/%s", getattr(ref, "id", "<unknown>"), repo, branch)
