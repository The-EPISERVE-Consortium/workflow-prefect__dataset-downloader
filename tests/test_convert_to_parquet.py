"""Unit tests for tasks/convert_to_parquet.py."""

import io
import json
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pandas as pd
import pyarrow.parquet as pq
import pytest

from tasks.convert_to_parquet import convert_to_parquet, shard_qid
from tasks.create_fdo_metadata import mint_qid


SAMPLE_DF = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})

SOURCE_URL = "https://example.com/GrippeWeb_Daten_des_Wochenberichts.tsv"
CANONICAL_QID = mint_qid(SOURCE_URL)
RAW_QID = CANONICAL_QID + "-raw"
SHARD = shard_qid(CANONICAL_QID)


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
    "@id": RAW_QID,
    "@type": "DigitalObject",
    "kernel": {
        "@id": RAW_QID,
        "fdo:hasComponent": [
            {"@id": "#RKI__grippeweb.tsv", "componentId": "RKI__grippeweb.tsv", "mediaType": "text/tab-separated-values"}
        ],
    },
}

SAMPLE_FDO_WITH_PROFILE = {
    **SAMPLE_FDO,
    "profile": {
        "@type": "Dataset",
        "@id": RAW_QID,
        "name": "grippeweb",
        "description": "Dataset grippeweb",
        "url": SOURCE_URL,
        "distribution": [{"@type": "DataDownload", "contentUrl": SOURCE_URL}],
    },
    "provenance": {
        "prov:generatedAtTime": "2024-01-01T00:00:00Z",
        "prov:wasAttributedTo": "EPISERVE Consortium dataset downloader",
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


def test_convert_to_parquet_uploads_valid_parquet():
    mock_repo, mock_branch, uploaded = _make_mock_branch()

    with patch("tasks.convert_to_parquet._get_lakefs_repository", return_value=mock_repo):
        convert_to_parquet.fn(SAMPLE_DF, SAMPLE_FDO, SOURCE_URL, "data-processed")

    parquet_path = f"{SHARD}/components/GrippeWeb_Daten_des_Wochenberichts.parquet"
    assert parquet_path in uploaded
    assert uploaded[parquet_path]["content_type"] == "application/vnd.apache.parquet"
    table = pq.read_table(io.BytesIO(uploaded[parquet_path]["data"]))
    assert table.column_names == ["col1", "col2"]
    assert table.num_rows == 2


def test_convert_to_parquet_strips_query_string_from_filename():
    mock_repo, mock_branch, uploaded = _make_mock_branch()
    url_with_query = "https://api.example.com/forecast?format=csv&timezone=Europe%2FBerlin"
    fdo = {**SAMPLE_FDO, "@id": mint_qid(url_with_query) + "-raw"}
    canonical = mint_qid(url_with_query)
    shard = shard_qid(canonical)

    with patch("tasks.convert_to_parquet._get_lakefs_repository", return_value=mock_repo):
        convert_to_parquet.fn(SAMPLE_DF, fdo, url_with_query, "data-processed")

    parquet_path = f"{shard}/components/forecast.parquet"
    assert parquet_path in uploaded


def test_convert_to_parquet_uploads_fdo_with_parquet_component():
    mock_repo, mock_branch, uploaded = _make_mock_branch()

    with patch("tasks.convert_to_parquet._get_lakefs_repository", return_value=mock_repo):
        convert_to_parquet.fn(SAMPLE_DF, SAMPLE_FDO, SOURCE_URL, "data-processed")

    fdo_path = f"{SHARD}/{CANONICAL_QID}.fdo.json"
    assert fdo_path in uploaded
    assert uploaded[fdo_path]["content_type"] == "application/json"

    stored_fdo = json.loads(uploaded[fdo_path]["data"])
    component = stored_fdo["kernel"]["fdo:hasComponent"][0]
    assert component["componentId"] == "GrippeWeb_Daten_des_Wochenberichts.parquet"
    assert component["@id"] == "components/GrippeWeb_Daten_des_Wochenberichts.parquet"
    assert component["mediaType"] == "application/vnd.apache.parquet"


def test_convert_to_parquet_uses_canonical_qid_as_identity():
    mock_repo, mock_branch, uploaded = _make_mock_branch()

    with patch("tasks.convert_to_parquet._get_lakefs_repository", return_value=mock_repo):
        convert_to_parquet.fn(SAMPLE_DF, SAMPLE_FDO, SOURCE_URL, "data-processed")

    fdo_path = f"{SHARD}/{CANONICAL_QID}.fdo.json"
    stored_fdo = json.loads(uploaded[fdo_path]["data"])
    assert stored_fdo["@id"] == CANONICAL_QID
    assert stored_fdo["kernel"]["@id"] == CANONICAL_QID
    assert stored_fdo["kernel"]["primaryIdentifier"] == CANONICAL_QID


def test_convert_to_parquet_records_derivation_provenance():
    mock_repo, mock_branch, uploaded = _make_mock_branch()

    with patch("tasks.convert_to_parquet._get_lakefs_repository", return_value=mock_repo):
        convert_to_parquet.fn(SAMPLE_DF, SAMPLE_FDO_WITH_PROFILE, SOURCE_URL, "data-processed")

    fdo_path = f"{SHARD}/{CANONICAL_QID}.fdo.json"
    stored_fdo = json.loads(uploaded[fdo_path]["data"])
    derived = stored_fdo["provenance"]["prov:wasDerivedFrom"]
    assert derived["@id"] == RAW_QID
    assert derived["prov:hadPrimarySource"] == SOURCE_URL


def test_convert_to_parquet_sets_lakefs_content_url():
    mock_repo, mock_branch, uploaded = _make_mock_branch()

    with patch("tasks.convert_to_parquet._get_lakefs_repository", return_value=mock_repo):
        convert_to_parquet.fn(SAMPLE_DF, SAMPLE_FDO_WITH_PROFILE, SOURCE_URL, "data-processed")

    fdo_path = f"{SHARD}/{CANONICAL_QID}.fdo.json"
    stored_fdo = json.loads(uploaded[fdo_path]["data"])
    dist = stored_fdo["profile"]["distribution"][0]
    expected_url = f"lakefs://data-processed/main/{SHARD}/components/GrippeWeb_Daten_des_Wochenberichts.parquet"
    assert dist["contentUrl"] == expected_url


def test_convert_to_parquet_sets_content_size_in_distribution():
    mock_repo, mock_branch, uploaded = _make_mock_branch()

    with patch("tasks.convert_to_parquet._get_lakefs_repository", return_value=mock_repo):
        convert_to_parquet.fn(SAMPLE_DF, SAMPLE_FDO_WITH_PROFILE, SOURCE_URL, "data-processed")

    fdo_path = f"{SHARD}/{CANONICAL_QID}.fdo.json"
    stored_fdo = json.loads(uploaded[fdo_path]["data"])
    dist = stored_fdo["profile"]["distribution"][0]
    assert "contentSize" in dist
    assert dist["contentSize"] > 0


def test_convert_to_parquet_commits_with_qid_message():
    mock_repo, mock_branch, _ = _make_mock_branch()

    with patch("tasks.convert_to_parquet._get_lakefs_repository", return_value=mock_repo):
        convert_to_parquet.fn(SAMPLE_DF, SAMPLE_FDO, SOURCE_URL, "data-processed")

    mock_branch.commit.assert_called_once_with(message=f"Parquet conversion of {CANONICAL_QID}")


def test_convert_to_parquet_skips_commit_when_no_changes():
    mock_repo, mock_branch, _ = _make_mock_branch(has_changes=False)

    with patch("tasks.convert_to_parquet._get_lakefs_repository", return_value=mock_repo):
        convert_to_parquet.fn(SAMPLE_DF, SAMPLE_FDO, SOURCE_URL, "data-processed")

    mock_branch.commit.assert_not_called()
