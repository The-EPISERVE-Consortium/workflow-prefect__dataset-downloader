"""Prefect task for determining when a dataset's content last actually changed."""

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

from prefect import task

from tasks._logging import get_logger
from tasks.commit_to_lakefs import _get_lakefs_repository


def _sha256_file(path: str) -> str:
    """Return the hex sha256 digest of a local file's contents."""
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_previous_kernel(lakefs_repo: str, lakefs_branch: str, lakefs_object_path: str) -> dict:
    """Return the kernel block of the previously committed FDO sidecar, or {} if none exists."""
    fdo_object_path = str(Path(lakefs_object_path).parent / (Path(lakefs_object_path).stem + ".fdo.json"))
    branch = _get_lakefs_repository(lakefs_repo).branch(lakefs_branch)
    try:
        with branch.object(fdo_object_path).reader() as f:
            fdo = json.loads(f.read())
        return fdo.get("kernel", {})
    except Exception:
        return {}


@task
def resolve_content_changed_at(
    local_path: str,
    lakefs_repo: str,
    lakefs_branch: str,
    lakefs_object_path: str,
) -> tuple[str, str]:
    """Determine the content hash of the newly downloaded file and the timestamp
    of the run in which the content last actually changed.

    Compares the new file's sha256 against the content_hash recorded in the
    previously committed FDO sidecar. If they match, the previous
    content_changed_at is carried forward; otherwise (including on the first
    run) content_changed_at is stamped with the current time.

    Returns:
        (content_hash, content_changed_at)
    """
    logger = get_logger(__name__)
    content_hash = _sha256_file(local_path)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    previous_kernel = _read_previous_kernel(lakefs_repo, lakefs_branch, lakefs_object_path)
    previous_hash = previous_kernel.get("content_hash", "")
    previous_changed_at = previous_kernel.get("content_changed_at", "")

    if previous_hash and previous_changed_at and previous_hash == content_hash:
        logger.info("Content unchanged, carrying forward content_changed_at=%s", previous_changed_at)
        return content_hash, previous_changed_at

    logger.info("Content changed (or first run), stamping content_changed_at=%s", now)
    return content_hash, now
