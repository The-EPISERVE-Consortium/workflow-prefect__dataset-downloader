"""Unit tests for tasks/download_tsv.py."""

import hashlib
from pathlib import Path
from unittest.mock import patch

import pytest

from tasks.download_tsv import _github_media_url, download_file

RAW_URL = "https://raw.githubusercontent.com/org/repo/refs/heads/main/data/file.tsv"
MEDIA_URL = "https://media.githubusercontent.com/media/org/repo/refs/heads/main/data/file.tsv"
CONTENT = b"a\tb\n1\t2\n3\t4\n"


def _pointer(content: bytes = CONTENT, oid: str | None = None, size: int | None = None) -> bytes:
    return (
        "version https://git-lfs.github.com/spec/v1\n"
        f"oid sha256:{oid or hashlib.sha256(content).hexdigest()}\n"
        f"size {len(content) if size is None else size}\n"
    ).encode()


def _fake(responses: dict[str, bytes], calls: list[str] | None = None):
    """Return a urlretrieve replacement that writes the bytes registered for the URL."""

    def fake_urlretrieve(url, path):
        if calls is not None:
            calls.append(url)
        Path(path).write_bytes(responses[url])

    return fake_urlretrieve


def test_download_file_saves_raw_bytes(tmp_path: Path):
    """download_file should call urlretrieve with the given URL and local path."""
    dest = str(tmp_path / "data.csv")

    with patch("tasks.download_tsv.urllib.request.urlretrieve", side_effect=_fake({"https://example.com/data.csv": b"x"})) as mock_retrieve:
        download_file.fn("https://example.com/data.csv", dest)

    mock_retrieve.assert_called_once_with("https://example.com/data.csv", dest)


def test_download_file_preserves_original_format(tmp_path: Path):
    """download_file should write the raw response bytes without conversion."""
    dest = tmp_path / "data.csv"
    raw = b"col1,col2\n1,2\n3,4\n"

    with patch("tasks.download_tsv.urllib.request.urlretrieve", side_effect=_fake({"https://example.com/data.csv": raw})):
        download_file.fn("https://example.com/data.csv", str(dest))

    assert dest.read_bytes() == raw


def test_lfs_pointer_on_raw_url_is_replaced_by_the_real_file(tmp_path: Path):
    dest = tmp_path / "file.tsv"
    calls: list[str] = []

    with patch("tasks.download_tsv.urllib.request.urlretrieve", side_effect=_fake({RAW_URL: _pointer(), MEDIA_URL: CONTENT}, calls)):
        download_file.fn(RAW_URL, str(dest))

    assert calls == [RAW_URL, MEDIA_URL]
    assert dest.read_bytes() == CONTENT


def test_media_url_given_directly_is_downloaded_once(tmp_path: Path):
    dest = tmp_path / "file.tsv"
    calls: list[str] = []

    with patch("tasks.download_tsv.urllib.request.urlretrieve", side_effect=_fake({MEDIA_URL: CONTENT}, calls)):
        download_file.fn(MEDIA_URL, str(dest))

    assert calls == [MEDIA_URL]
    assert dest.read_bytes() == CONTENT


def test_lfs_file_with_wrong_sha256_is_rejected(tmp_path: Path):
    other = b"a\tb\n9\t9\n3\t4\n"  # same length, different content
    assert len(other) == len(CONTENT)

    with patch("tasks.download_tsv.urllib.request.urlretrieve", side_effect=_fake({RAW_URL: _pointer(), MEDIA_URL: other})):
        with pytest.raises(ValueError, match="sha256"):
            download_file.fn(RAW_URL, str(tmp_path / "file.tsv"))

    assert not (tmp_path / "file.tsv").exists()


def test_lfs_file_with_wrong_size_is_rejected(tmp_path: Path):
    with patch("tasks.download_tsv.urllib.request.urlretrieve", side_effect=_fake({RAW_URL: _pointer(size=999), MEDIA_URL: CONTENT})):
        with pytest.raises(ValueError, match="999"):
            download_file.fn(RAW_URL, str(tmp_path / "file.tsv"))

    assert not (tmp_path / "file.tsv").exists()


def test_lfs_pointer_from_another_host_is_rejected_and_not_kept(tmp_path: Path):
    url = "https://example.com/data/file.tsv"
    dest = tmp_path / "file.tsv"

    with patch("tasks.download_tsv.urllib.request.urlretrieve", side_effect=_fake({url: _pointer()})):
        with pytest.raises(ValueError, match="example.com"):
            download_file.fn(url, str(dest))

    assert not dest.exists()


def test_malformed_lfs_pointer_is_rejected(tmp_path: Path):
    broken = b"version https://git-lfs.github.com/spec/v1\nsize 12\n"  # no oid

    with patch("tasks.download_tsv.urllib.request.urlretrieve", side_effect=_fake({RAW_URL: broken})):
        with pytest.raises(ValueError, match="Malformed"):
            download_file.fn(RAW_URL, str(tmp_path / "file.tsv"))

    assert not (tmp_path / "file.tsv").exists()


def test_ordinary_file_that_mentions_lfs_later_is_not_treated_as_a_pointer(tmp_path: Path):
    dest = tmp_path / "file.tsv"
    text = b"title\nversion https://git-lfs.github.com/spec/v1\n"

    with patch("tasks.download_tsv.urllib.request.urlretrieve", side_effect=_fake({RAW_URL: text})):
        download_file.fn(RAW_URL, str(dest))

    assert dest.read_bytes() == text


def test_github_media_url_maps_raw_path_and_keeps_query():
    assert _github_media_url(RAW_URL) == MEDIA_URL
    assert _github_media_url(RAW_URL + "?token=abc").endswith("/file.tsv?token=abc")
