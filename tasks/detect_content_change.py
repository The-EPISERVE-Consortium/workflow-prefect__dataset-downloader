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


def _read_previous_provenance(lakefs_repo: str, lakefs_branch: str, lakefs_object_path: str) -> dict:
    """Return the provenance block of the previously committed FDO sidecar, or {} if none exists."""
    fdo_object_path = str(Path(lakefs_object_path).parent / (Path(lakefs_object_path).stem + ".fdo.json"))
    branch = _get_lakefs_repository(lakefs_repo).branch(lakefs_branch)
    try:
        with branch.object(fdo_object_path).reader() as f:
            fdo = json.loads(f.read())
        return fdo.get("provenance", {})
    except Exception:
        return {}


@task
def resolve_source_changed_at(
    local_path: str,
    lakefs_repo: str,
    lakefs_branch: str,
    lakefs_object_path: str,
) -> tuple[str, str]:
    """Determine the content hash of the newly downloaded source file and the
    timestamp of the run in which the source data last actually changed.

    Compares the new file's sha256 against the source_content_hash recorded in
    the previously committed FDO sidecar's provenance block. If they match,
    the previous source_changed_at is carried forward; otherwise (including on
    the first run) source_changed_at is stamped with the current time.

    Returns:
        (source_content_hash, source_changed_at)
    """
    logger = get_logger(__name__)
    source_content_hash = _sha256_file(local_path)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    previous_provenance = _read_previous_provenance(lakefs_repo, lakefs_branch, lakefs_object_path)
    previous_hash = previous_provenance.get("source_content_hash", "")
    previous_changed_at = previous_provenance.get("source_changed_at", "")

    if previous_hash and previous_changed_at and previous_hash == source_content_hash:
        logger.info("Source content unchanged, carrying forward source_changed_at=%s", previous_changed_at)
        return source_content_hash, previous_changed_at

    logger.info("Source content changed (or first run), stamping source_changed_at=%s", now)
    return source_content_hash, now
