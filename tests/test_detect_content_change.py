"""Unit tests for tasks/detect_content_change.py."""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

from tasks.detect_content_change import _sha256_file, resolve_source_changed_at


def _mock_object(data: dict | None):
    obj = MagicMock()
    if data is None:
        obj.reader.side_effect = Exception("not found")
    else:
        reader = MagicMock()
        reader.__enter__ = lambda s: s
        reader.__exit__ = MagicMock(return_value=False)
        reader.read.return_value = json.dumps(data).encode()
        obj.reader.return_value = reader
    return obj


def test_sha256_file_is_stable(tmp_path: Path):
    f = tmp_path / "data.tsv"
    f.write_text("col1\tcol2\n1\t2\n", encoding="utf-8")
    assert _sha256_file(str(f)) == _sha256_file(str(f))


def test_sha256_file_differs_on_content_change(tmp_path: Path):
    f1 = tmp_path / "a.tsv"
    f1.write_text("col1\tcol2\n1\t2\n", encoding="utf-8")
    f2 = tmp_path / "b.tsv"
    f2.write_text("col1\tcol2\n1\t3\n", encoding="utf-8")
    assert _sha256_file(str(f1)) != _sha256_file(str(f2))


def test_first_run_stamps_now(tmp_path: Path):
    """No previous FDO in lakeFS -> source_changed_at defaults to now."""
    f = tmp_path / "data.tsv"
    f.write_text("col1\tcol2\n1\t2\n", encoding="utf-8")
    obj = _mock_object(None)

    with patch("tasks.detect_content_change._get_lakefs_repository") as mock_repo:
        mock_repo.return_value.branch.return_value.object.return_value = obj
        source_content_hash, source_changed_at = resolve_source_changed_at.fn(
            str(f), "sandbox", "main", "RAW/RKI/grippeweb.tsv"
        )

    assert source_content_hash == _sha256_file(str(f))
    assert source_changed_at  # non-empty, stamped to "now"


def test_unchanged_content_carries_forward_previous_timestamp(tmp_path: Path):
    f = tmp_path / "data.tsv"
    f.write_text("col1\tcol2\n1\t2\n", encoding="utf-8")
    source_content_hash = _sha256_file(str(f))
    previous_fdo = {
        "provenance": {
            "source_content_hash": source_content_hash,
            "source_changed_at": "2026-05-01T00:00:00Z",
        }
    }
    obj = _mock_object(previous_fdo)

    with patch("tasks.detect_content_change._get_lakefs_repository") as mock_repo:
        mock_repo.return_value.branch.return_value.object.return_value = obj
        result_hash, source_changed_at = resolve_source_changed_at.fn(
            str(f), "sandbox", "main", "RAW/RKI/grippeweb.tsv"
        )

    assert result_hash == source_content_hash
    assert source_changed_at == "2026-05-01T00:00:00Z"


def test_changed_content_stamps_new_timestamp(tmp_path: Path):
    f = tmp_path / "data.tsv"
    f.write_text("col1\tcol2\n1\t2\n", encoding="utf-8")
    previous_fdo = {
        "provenance": {
            "source_content_hash": "some-other-hash",
            "source_changed_at": "2026-05-01T00:00:00Z",
        }
    }
    obj = _mock_object(previous_fdo)

    with patch("tasks.detect_content_change._get_lakefs_repository") as mock_repo:
        mock_repo.return_value.branch.return_value.object.return_value = obj
        _, source_changed_at = resolve_source_changed_at.fn(
            str(f), "sandbox", "main", "RAW/RKI/grippeweb.tsv"
        )

    assert source_changed_at != "2026-05-01T00:00:00Z"


def test_reads_fdo_sidecar_path(tmp_path: Path):
    f = tmp_path / "data.tsv"
    f.write_text("x", encoding="utf-8")
    obj = _mock_object(None)

    with patch("tasks.detect_content_change._get_lakefs_repository") as mock_repo:
        branch_mock = mock_repo.return_value.branch.return_value
        branch_mock.object.return_value = obj
        resolve_source_changed_at.fn(str(f), "sandbox", "main", "incidence/RKI__grippeweb.tsv")

    branch_mock.object.assert_called_once_with("incidence/RKI__grippeweb.fdo.json")
