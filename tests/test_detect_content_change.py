"""Unit tests for tasks/detect_content_change.py."""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

from lakefs.exceptions import ObjectNotFoundException
from tasks.detect_content_change import _md5_file, resolve_source_changed_at


def _mock_raw_object(checksum: str | None):
    obj = MagicMock()
    if checksum is None:
        obj.stat.side_effect = ObjectNotFoundException()
    else:
        obj.stat.return_value = MagicMock(checksum=checksum)
    return obj


def _mock_fdo_object(data: dict | None):
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


def test_md5_file_is_stable(tmp_path: Path):
    f = tmp_path / "data.tsv"
    f.write_text("col1\tcol2\n1\t2\n", encoding="utf-8")
    assert _md5_file(str(f)) == _md5_file(str(f))


def test_md5_file_differs_on_content_change(tmp_path: Path):
    f1 = tmp_path / "a.tsv"
    f1.write_text("col1\tcol2\n1\t2\n", encoding="utf-8")
    f2 = tmp_path / "b.tsv"
    f2.write_text("col1\tcol2\n1\t3\n", encoding="utf-8")
    assert _md5_file(str(f1)) != _md5_file(str(f2))


def test_first_run_stamps_now(tmp_path: Path):
    """No previously committed raw object in lakeFS -> source_changed_at defaults to now."""
    f = tmp_path / "data.tsv"
    f.write_text("col1\tcol2\n1\t2\n", encoding="utf-8")
    obj = _mock_raw_object(None)

    with patch("tasks.detect_content_change._get_lakefs_repository") as mock_repo:
        mock_repo.return_value.branch.return_value.object.return_value = obj
        source_changed_at = resolve_source_changed_at.fn(
            str(f), "sandbox", "main", "RAW/RKI/grippeweb.tsv"
        )

    assert source_changed_at  # non-empty, stamped to "now"


def test_unchanged_content_carries_forward_previous_timestamp(tmp_path: Path):
    f = tmp_path / "data.tsv"
    f.write_text("col1\tcol2\n1\t2\n", encoding="utf-8")
    checksum = _md5_file(str(f))
    previous_fdo = {"provenance": {"source_changed_at": "2026-05-01T00:00:00Z"}}

    def _object_side_effect(path):
        if path.endswith(".fdo.json"):
            return _mock_fdo_object(previous_fdo)
        return _mock_raw_object(checksum)

    with patch("tasks.detect_content_change._get_lakefs_repository") as mock_repo:
        mock_repo.return_value.branch.return_value.object.side_effect = _object_side_effect
        source_changed_at = resolve_source_changed_at.fn(
            str(f), "sandbox", "main", "RAW/RKI/grippeweb.tsv"
        )

    assert source_changed_at == "2026-05-01T00:00:00Z"


def test_changed_content_stamps_new_timestamp(tmp_path: Path):
    f = tmp_path / "data.tsv"
    f.write_text("col1\tcol2\n1\t2\n", encoding="utf-8")
    previous_fdo = {"provenance": {"source_changed_at": "2026-05-01T00:00:00Z"}}

    def _object_side_effect(path):
        if path.endswith(".fdo.json"):
            return _mock_fdo_object(previous_fdo)
        return _mock_raw_object("some-other-checksum")

    with patch("tasks.detect_content_change._get_lakefs_repository") as mock_repo:
        mock_repo.return_value.branch.return_value.object.side_effect = _object_side_effect
        source_changed_at = resolve_source_changed_at.fn(
            str(f), "sandbox", "main", "RAW/RKI/grippeweb.tsv"
        )

    assert source_changed_at != "2026-05-01T00:00:00Z"


def test_checksum_comparison_is_case_and_quote_insensitive(tmp_path: Path):
    f = tmp_path / "data.tsv"
    f.write_text("col1\tcol2\n1\t2\n", encoding="utf-8")
    checksum = _md5_file(str(f))
    previous_fdo = {"provenance": {"source_changed_at": "2026-05-01T00:00:00Z"}}

    def _object_side_effect(path):
        if path.endswith(".fdo.json"):
            return _mock_fdo_object(previous_fdo)
        return _mock_raw_object(f'"{checksum.upper()}"')

    with patch("tasks.detect_content_change._get_lakefs_repository") as mock_repo:
        mock_repo.return_value.branch.return_value.object.side_effect = _object_side_effect
        source_changed_at = resolve_source_changed_at.fn(
            str(f), "sandbox", "main", "RAW/RKI/grippeweb.tsv"
        )

    assert source_changed_at == "2026-05-01T00:00:00Z"


def test_reads_fdo_sidecar_path_for_previous_timestamp(tmp_path: Path):
    f = tmp_path / "data.tsv"
    f.write_text("x", encoding="utf-8")
    checksum = _md5_file(str(f))
    called_paths = []

    def _object_side_effect(path):
        called_paths.append(path)
        if path.endswith(".fdo.json"):
            return _mock_fdo_object(None)
        return _mock_raw_object(checksum)

    with patch("tasks.detect_content_change._get_lakefs_repository") as mock_repo:
        mock_repo.return_value.branch.return_value.object.side_effect = _object_side_effect
        resolve_source_changed_at.fn(str(f), "sandbox", "main", "incidence/RKI__grippeweb.tsv")

    assert "incidence/RKI__grippeweb.fdo.json" in called_paths
