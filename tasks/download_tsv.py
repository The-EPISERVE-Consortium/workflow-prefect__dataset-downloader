"""Prefect task for downloading a dataset file to local storage."""

import urllib.request

from prefect import task

from tasks._logging import get_logger


@task
def download_file(url: str, local_path: str) -> None:
    """Download a file from the given URL and save it to local_path as-is."""
    logger = get_logger(__name__)
    logger.info("Downloading %s to %s", url, local_path)
    urllib.request.urlretrieve(url, local_path)
    logger.info("Download complete")
