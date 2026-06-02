"""Unit tests for tasks/download_tsv.py."""

from pathlib import Path
from unittest.mock import patch

from tasks.download_tsv import download_file


def test_download_file_saves_raw_bytes(tmp_path: Path):
    """download_file should call urlretrieve with the given URL and local path."""
    dest = str(tmp_path / "data.csv")

    with patch("tasks.download_tsv.urllib.request.urlretrieve") as mock_retrieve:
        download_file.fn("https://example.com/data.csv", dest)

    mock_retrieve.assert_called_once_with("https://example.com/data.csv", dest)


def test_download_file_preserves_original_format(tmp_path: Path):
    """download_file should write the raw response bytes without conversion."""
    dest = tmp_path / "data.csv"
    raw = b"col1,col2\n1,2\n3,4\n"

    def fake_urlretrieve(url, path):
        Path(path).write_bytes(raw)

    with patch("tasks.download_tsv.urllib.request.urlretrieve", side_effect=fake_urlretrieve):
        download_file.fn("https://example.com/data.csv", str(dest))

    assert dest.read_bytes() == raw
