"""Unit tests for tasks/convert_to_parquet.py."""

import io
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pandas as pd
import pyarrow.parquet as pq
import pytest

from tasks.convert_to_parquet import convert_to_parquet, shard_qid


SAMPLE_DF = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})


# --- shard_qid ---

def test_shard_qid_long_id():
    assert shard_qid("Q1234567890123") == "12/34/56/Q1234567890123"


def test_shard_qid_short_id_is_padded():
    assert shard_qid("Q123") == "00/01/23/Q123"


def test_shard_qid_normalises_to_uppercase():
    assert shard_qid("q1234567890123") == "12/34/56/Q1234567890123"


def test_shard_qid_rejects_missing_q_prefix():
    with pytest.raises(ValueError, match="must start with 'Q'"):
        shard_qid("1234567890123")


def test_shard_qid_rejects_non_digit_suffix():
    with pytest.raises(ValueError, match="must contain digits after 'Q'"):
        shard_qid("Qabc")


# --- convert_to_parquet ---

def _make_mock_branch(object_path: str, has_changes: bool = True):
    mock_repo = MagicMock()
    mock_branch = MagicMock()
    mock_object = MagicMock()
    mock_repo.branch.return_value = mock_branch
    mock_branch.object.return_value = mock_object
    mock_branch.uncommitted.return_value = [SimpleNamespace(path=object_path)] if has_changes else []
    mock_branch.commit.return_value = SimpleNamespace(id="commit-id")
    return mock_repo, mock_branch, mock_object


def test_convert_to_parquet_uploads_valid_parquet():
    qid = "Q1234567890123"
    expected_path = "12/34/56/Q1234567890123.parquet"
    mock_repo, mock_branch, mock_object = _make_mock_branch(expected_path)

    uploaded = {}

    def capture_upload(**kwargs):
        uploaded.update(kwargs)

    mock_object.upload.side_effect = capture_upload

    with patch("tasks.convert_to_parquet._get_lakefs_repository", return_value=mock_repo):
        convert_to_parquet.fn(SAMPLE_DF, qid, "data-processed")

    mock_branch.object.assert_called_once_with(expected_path)
    assert uploaded["content_type"] == "application/vnd.apache.parquet"

    table = pq.read_table(io.BytesIO(uploaded["data"]))
    assert table.column_names == ["col1", "col2"]
    assert table.num_rows == 2


def test_convert_to_parquet_commits_with_qid_message():
    qid = "Q1234567890123"
    mock_repo, mock_branch, _ = _make_mock_branch("12/34/56/Q1234567890123.parquet")

    with patch("tasks.convert_to_parquet._get_lakefs_repository", return_value=mock_repo):
        convert_to_parquet.fn(SAMPLE_DF, qid, "data-processed")

    mock_branch.commit.assert_called_once_with(message="Parquet conversion of Q1234567890123")


def test_convert_to_parquet_skips_commit_when_no_changes():
    qid = "Q1234567890123"
    mock_repo, mock_branch, _ = _make_mock_branch("12/34/56/Q1234567890123.parquet", has_changes=False)

    with patch("tasks.convert_to_parquet._get_lakefs_repository", return_value=mock_repo):
        convert_to_parquet.fn(SAMPLE_DF, qid, "data-processed")

    mock_branch.commit.assert_not_called()
