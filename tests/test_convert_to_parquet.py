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

SAMPLE_FDO = {
    "@id": "Q1234567890123",
    "@type": "DigitalObject",
    "kernel": {
        "@id": "Q1234567890123",
        "fdo:hasComponent": [
            {"@id": "#RKI__grippeweb.tsv", "componentId": "RKI__grippeweb.tsv", "mediaType": "text/tab-separated-values"}
        ],
    },
}


def _make_mock_branch(has_changes: bool = True):
    mock_repo = MagicMock()
    mock_branch = MagicMock()
    mock_repo.branch.return_value = mock_branch
    mock_branch.uncommitted.return_value = [SimpleNamespace(path="some/path")] if has_changes else []
    mock_branch.commit.return_value = SimpleNamespace(id="commit-id")
    uploaded = {}

    def fake_object(path):
        obj = MagicMock()
        obj.upload.side_effect = lambda **kwargs: uploaded.update({path: kwargs})
        return obj

    mock_branch.object.side_effect = fake_object
    return mock_repo, mock_branch, uploaded


SOURCE_URL = "https://example.com/GrippeWeb_Daten_des_Wochenberichts.tsv"


def test_convert_to_parquet_uploads_valid_parquet():
    qid = "Q1234567890123"
    mock_repo, mock_branch, uploaded = _make_mock_branch()

    with patch("tasks.convert_to_parquet._get_lakefs_repository", return_value=mock_repo):
        convert_to_parquet.fn(SAMPLE_DF, qid, SAMPLE_FDO, SOURCE_URL, "data-processed")

    parquet_path = "12/34/56/Q1234567890123/components/GrippeWeb_Daten_des_Wochenberichts.parquet"
    assert parquet_path in uploaded
    assert uploaded[parquet_path]["content_type"] == "application/vnd.apache.parquet"
    table = pq.read_table(io.BytesIO(uploaded[parquet_path]["data"]))
    assert table.column_names == ["col1", "col2"]
    assert table.num_rows == 2


def test_convert_to_parquet_strips_query_string_from_filename():
    qid = "Q1234567890123"
    mock_repo, mock_branch, uploaded = _make_mock_branch()
    url_with_query = "https://api.example.com/forecast?format=csv&timezone=Europe%2FBerlin"

    with patch("tasks.convert_to_parquet._get_lakefs_repository", return_value=mock_repo):
        convert_to_parquet.fn(SAMPLE_DF, qid, SAMPLE_FDO, url_with_query, "data-processed")

    parquet_path = "12/34/56/Q1234567890123/components/forecast.parquet"
    assert parquet_path in uploaded


def test_convert_to_parquet_uploads_fdo_with_parquet_component():
    import json
    qid = "Q1234567890123"
    mock_repo, mock_branch, uploaded = _make_mock_branch()

    with patch("tasks.convert_to_parquet._get_lakefs_repository", return_value=mock_repo):
        convert_to_parquet.fn(SAMPLE_DF, qid, SAMPLE_FDO, SOURCE_URL, "data-processed")

    fdo_path = "12/34/56/Q1234567890123/Q1234567890123.fdo.json"
    assert fdo_path in uploaded
    assert uploaded[fdo_path]["content_type"] == "application/json"

    stored_fdo = json.loads(uploaded[fdo_path]["data"])
    component = stored_fdo["kernel"]["fdo:hasComponent"][0]
    assert component["componentId"] == "GrippeWeb_Daten_des_Wochenberichts.parquet"
    assert component["@id"] == "components/GrippeWeb_Daten_des_Wochenberichts.parquet"
    assert component["mediaType"] == "application/vnd.apache.parquet"


def test_convert_to_parquet_commits_with_qid_message():
    qid = "Q1234567890123"
    mock_repo, mock_branch, _ = _make_mock_branch()

    with patch("tasks.convert_to_parquet._get_lakefs_repository", return_value=mock_repo):
        convert_to_parquet.fn(SAMPLE_DF, qid, SAMPLE_FDO, SOURCE_URL, "data-processed")

    mock_branch.commit.assert_called_once_with(message="Parquet conversion of Q1234567890123")


def test_convert_to_parquet_skips_commit_when_no_changes():
    qid = "Q1234567890123"
    mock_repo, mock_branch, _ = _make_mock_branch(has_changes=False)

    with patch("tasks.convert_to_parquet._get_lakefs_repository", return_value=mock_repo):
        convert_to_parquet.fn(SAMPLE_DF, qid, SAMPLE_FDO, SOURCE_URL, "data-processed")

    mock_branch.commit.assert_not_called()
