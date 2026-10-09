"""Prefect task for downloading a dataset file to local storage."""

import hashlib
import os
import urllib.request
from urllib.parse import urlsplit, urlunsplit

from prefect import task

from tasks._logging import get_logger

_LFS_SIGNATURE = b"version https://git-lfs.github.com/spec/v1"
_GITHUB_RAW_HOST = "raw.githubusercontent.com"
_GITHUB_MEDIA_HOST = "media.githubusercontent.com"


def _read_lfs_pointer(path: str) -> dict | None:
    """Return the oid and size of a Git LFS pointer file, or None if the file is not one.

    Args:
        path: Local file to inspect. Only its first bytes are read.

    Returns:
        ``{"oid": <sha256 hex>, "size": <bytes>}`` for a pointer file, otherwise None.

    Raises:
        ValueError: If the file looks like an LFS pointer but has no valid oid or size.
    """
    with open(path, "rb") as f:
        head = f.read(512)
    if not head.startswith(_LFS_SIGNATURE):
        return None
    fields = dict(
        line.split(" ", 1) for line in head.decode("utf-8", "replace").splitlines() if " " in line
    )
    oid = fields.get("oid", "").removeprefix("sha256:").strip()
    size = fields.get("size", "").strip()
    if len(oid) != 64 or not size.isdigit():
        raise ValueError(f"Malformed Git LFS pointer in {path}: {head[:200]!r}")
    return {"oid": oid, "size": int(size)}


def _github_media_url(url: str) -> str:
    """Map a ``raw.githubusercontent.com`` URL to the ``media.githubusercontent.com`` URL of the same file.

    Raw URLs return only the pointer for files stored in Git LFS; the media host serves the real content.

    Raises:
        ValueError: If the URL is not on ``raw.githubusercontent.com``.
    """
    parts = urlsplit(url)
    if parts.netloc != _GITHUB_RAW_HOST:
        raise ValueError(
            f"{url} returned a Git LFS pointer instead of the data, and a pointer can only be "
            f"resolved for {_GITHUB_RAW_HOST} URLs (not {parts.netloc!r})."
        )
    return urlunsplit((parts.scheme, _GITHUB_MEDIA_HOST, "/media" + parts.path, parts.query, ""))


def _sha256_file(path: str) -> str:
    """Return the hex sha256 digest of a local file."""
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


@task
def download_file(url: str, local_path: str) -> None:
    """Download a file from the given URL and save it to local_path as-is.

    If the server returns a Git LFS pointer instead of the data (as ``raw.githubusercontent.com``
    does for LFS files), the real file is fetched from ``media.githubusercontent.com`` and its
    size and sha256 are checked against the pointer. A pointer is never left in ``local_path``.

    Raises:
        ValueError: If a pointer cannot be resolved (other host, malformed pointer) or the
            resolved file does not match the pointer's size or sha256.
    """
    logger = get_logger(__name__)
    logger.info("Downloading %s to %s", url, local_path)
    urllib.request.urlretrieve(url, local_path)

    try:
        pointer = _read_lfs_pointer(local_path)
        if pointer is not None:
            media_url = _github_media_url(url)
            logger.info("%s is a Git LFS pointer; downloading the file from %s", url, media_url)
            urllib.request.urlretrieve(media_url, local_path)
            size = os.path.getsize(local_path)
            if size != pointer["size"]:
                raise ValueError(f"Git LFS file from {media_url} has {size} bytes, the pointer says {pointer['size']}.")
            if _sha256_file(local_path) != pointer["oid"]:
                raise ValueError(f"Git LFS file from {media_url} does not match the sha256 in the pointer.")
            logger.info("Git LFS file verified (%d bytes, sha256 %s)", size, pointer["oid"])
    except ValueError:
        # Never leave a pointer or an unverified file behind for a later step to pick up.
        if os.path.exists(local_path):
            os.remove(local_path)
        raise

    logger.info("Download complete")
