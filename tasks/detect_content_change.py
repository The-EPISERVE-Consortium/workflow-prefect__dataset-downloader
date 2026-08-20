"""Prefect task for determining when a dataset's content last actually changed."""

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

from prefect import task

from lakefs.exceptions import ObjectNotFoundException
from tasks._logging import get_logger
from tasks.commit_to_lakefs import _get_lakefs_repository


def _md5_file(path: str) -> str:
    """Return the hex md5 digest of a local file's contents.

    md5 (not sha256) so this is directly comparable to the checksum lakeFS
    itself reports for the previously committed raw object via ObjectInfo.checksum.
    """
    digest = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _previous_raw_checksum(lakefs_repo: str, lakefs_branch: str, lakefs_object_path: str) -> str | None:
    """Return the lakeFS-reported checksum of the currently committed raw object, or None if absent."""
    branch = _get_lakefs_repository(lakefs_repo).branch(lakefs_branch)
    try:
        return branch.object(lakefs_object_path).stat().checksum.strip('"').lower()
    except ObjectNotFoundException:
        return None


def _previous_source_changed_at(lakefs_repo: str, lakefs_branch: str, lakefs_object_path: str) -> str:
    """Return the source_changed_at recorded in the previously committed FDO sidecar, or "" if none exists."""
    fdo_object_path = str(Path(lakefs_object_path).parent / (Path(lakefs_object_path).stem + ".fdo.json"))
    branch = _get_lakefs_repository(lakefs_repo).branch(lakefs_branch)
    try:
        with branch.object(fdo_object_path).reader() as f:
            fdo = json.loads(f.read())
        return fdo.get("provenance", {}).get("source_changed_at", "")
    except Exception:
        return ""


@task
def resolve_source_changed_at(
    local_path: str,
    lakefs_repo: str,
    lakefs_branch: str,
    lakefs_object_path: str,
) -> str:
    """Determine the timestamp of the run in which the source data last actually changed.

    Compares the newly downloaded file's md5 against the checksum lakeFS reports for the
    raw object currently committed at lakefs_object_path (i.e. the previous run's file) —
    lakeFS already stores this, so we don't need to keep our own copy of the hash anywhere.
    If they match, the previous source_changed_at (read from the FDO sidecar) is carried
    forward; otherwise (including on the first run) source_changed_at is stamped with the
    current time.

    Returns:
        source_changed_at
    """
    logger = get_logger(__name__)
    local_checksum = _md5_file(local_path)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    previous_checksum = _previous_raw_checksum(lakefs_repo, lakefs_branch, lakefs_object_path)
    if previous_checksum is None:
        logger.info("No previous raw object found, stamping source_changed_at=%s", now)
        return now

    if previous_checksum == local_checksum:
        previous_changed_at = _previous_source_changed_at(lakefs_repo, lakefs_branch, lakefs_object_path)
        if previous_changed_at:
            logger.info("Source content unchanged, carrying forward source_changed_at=%s", previous_changed_at)
            return previous_changed_at

    logger.info("Source content changed, stamping source_changed_at=%s", now)
    return now
