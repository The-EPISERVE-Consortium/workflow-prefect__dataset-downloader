"""Unit tests for the multi-source (collection) output: create_collection_fdo, write_parquet_component, publish_collection."""

import json
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pandas as pd
import pyarrow.parquet as pq
import pytest

from tasks._sharding import shard_qid
from tasks.convert_to_parquet import publish_collection, write_parquet_component
from tasks.create_fdo_metadata import create_collection_fdo, mint_qid

URL_A = "https://example.org/data/KVIS_Impfquoten_Erwachsene.tsv"
URL_B = "https://media.example.org/data/KVIS_Impfquoten_Kinder.tsv?token=abc"
SEED = "kvis_vaccination_rates"
QID = mint_qid(URL_A, SEED)


def _raw(qid: str, url: str, changed: str) -> dict:
    return {
        "@id": qid + "-raw",
        "profile": {"url": url, "additionalType": "vaccination"},
        "provenance": {"source_changed_at": changed},
    }


RAW_FDOS = [
    _raw("Q1", URL_A, "2026-01-09T12:58:00Z"),
    _raw("Q2", URL_B, "2025-10-01T08:00:00Z"),
]
COMPONENTS = [
    {"componentId": "KVIS_Impfquoten_Erwachsene.parquet", "size": 100},
    {"componentId": "KVIS_Impfquoten_Kinder.parquet", "size": 2000},
]


def _fdo(**overrides):
    kwargs = dict(
        dataset_name="kvis_vaccination_rates",
        raw_fdos=RAW_FDOS,
        components=COMPONENTS,
        lakefs_repo="data-processed",
        lakefs_branch="main",
        qid_seed=SEED,
    )
    kwargs.update(overrides)
    return create_collection_fdo.fn(**kwargs)


# --- create_collection_fdo ---

def test_qid_comes_from_the_seed_not_from_a_file():
    fdo = _fdo()

    assert fdo["@id"] == QID == fdo["kernel"]["@id"] == fdo["kernel"]["primaryIdentifier"] == fdo["profile"]["@id"]
    assert not fdo["@id"].endswith("-raw")
    assert QID != mint_qid(URL_A)  # differs from the plain single-file QID of the first source


def test_one_parquet_component_per_source():
    comps = _fdo()["kernel"]["fdo:hasComponent"]

    assert comps == [
        {"@id": "components/KVIS_Impfquoten_Erwachsene.parquet", "componentId": "KVIS_Impfquoten_Erwachsene.parquet", "mediaType": "application/vnd.apache.parquet"},
        {"@id": "components/KVIS_Impfquoten_Kinder.parquet", "componentId": "KVIS_Impfquoten_Kinder.parquet", "mediaType": "application/vnd.apache.parquet"},
    ]


def test_distribution_lists_lakefs_files_with_sizes_then_the_source_urls():
    dist = _fdo()["profile"]["distribution"]
    base = f"lakefs://data-processed/main/{shard_qid(QID)}/components/"

    assert dist[0] == {"@type": "DataDownload", "contentUrl": base + "KVIS_Impfquoten_Erwachsene.parquet", "contentSize": 100, "encodingFormat": "application/vnd.apache.parquet"}
    assert dist[1]["contentUrl"] == base + "KVIS_Impfquoten_Kinder.parquet" and dist[1]["contentSize"] == 2000
    assert [d["contentUrl"] for d in dist[2:]] == [URL_A, URL_B]


def test_was_derived_from_lists_every_raw_fdo():
    derived = _fdo()["provenance"]["prov:wasDerivedFrom"]

    assert derived == [
        {"@id": "Q1-raw", "prov:hadPrimarySource": URL_A},
        {"@id": "Q2-raw", "prov:hadPrimarySource": URL_B},
    ]


def test_source_changed_at_is_the_latest_of_the_sources():
    assert _fdo()["provenance"]["source_changed_at"] == "2026-01-09T12:58:00Z"


def test_profile_fields_and_defaults():
    fdo = _fdo()

    assert fdo["profile"]["name"] == "kvis_vaccination_rates"
    assert fdo["profile"]["display_name"] == "kvis_vaccination_rates"      # default
    assert fdo["profile"]["description"] == "Dataset kvis_vaccination_rates"  # default
    assert fdo["profile"]["url"] == URL_A                                   # default: first source
    assert fdo["profile"]["additionalType"] == "vaccination"
    assert "license" not in fdo["profile"] and "creditText" not in fdo["profile"]


def test_profile_fields_when_given():
    fdo = _fdo(display_name="Routine Vaccination Rates", description="Rates.", dataset_url="https://example.org/repo",
               license_id="cc-by", attribution="RKI, CC BY 4.0")

    assert fdo["profile"]["display_name"] == "Routine Vaccination Rates"
    assert fdo["profile"]["description"] == "Rates."
    assert fdo["profile"]["url"] == "https://example.org/repo"
    assert fdo["profile"]["license"] == "cc-by"
    assert fdo["profile"]["creditText"] == "RKI, CC BY 4.0"


def test_a_collection_needs_a_seed():
    with pytest.raises(ValueError, match="qid_seed"):
        _fdo(qid_seed="")
    with pytest.raises(ValueError, match="qid_seed"):
        _fdo(qid_seed=None)


def test_a_collection_needs_sources_and_components():
    with pytest.raises(ValueError, match="at least one"):
        _fdo(raw_fdos=[])
    with pytest.raises(ValueError, match="at least one"):
        _fdo(components=[])


# --- write_parquet_component ---

def test_write_parquet_component_names_the_file_after_the_source(tmp_path):
    df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})

    comp = write_parquet_component.fn(df, URL_B, str(tmp_path))  # URL has a query string

    assert comp["componentId"] == "KVIS_Impfquoten_Kinder.parquet"
    assert comp["local_path"] == str(tmp_path / "KVIS_Impfquoten_Kinder.parquet")
    assert comp["size"] == (tmp_path / "KVIS_Impfquoten_Kinder.parquet").stat().st_size > 0
    table = pq.read_table(comp["local_path"])
    assert table.column_names == ["a", "b"] and table.num_rows == 2


# --- publish_collection ---

def _make_mock_branch(has_changes: bool = True):
    repo, branch = MagicMock(), MagicMock()
    repo.branch.return_value = branch
    branch.uncommitted.return_value = [SimpleNamespace(path="x")] if has_changes else []
    branch.commit.return_value = SimpleNamespace(id="commit-id")
    uploaded = {}

    def fake_object(path):
        obj = MagicMock()
        obj.upload.side_effect = lambda **kwargs: uploaded.update({path: kwargs})
        return obj

    branch.object.side_effect = fake_object
    return repo, branch, uploaded


def _files(tmp_path):
    comps = []
    for name, content in [("KVIS_Impfquoten_Erwachsene.parquet", b"adult-bytes"), ("KVIS_Impfquoten_Kinder.parquet", b"child-bytes")]:
        path = tmp_path / name
        path.write_bytes(content)
        comps.append({"componentId": name, "local_path": str(path), "size": len(content)})
    return comps


def test_publish_collection_uploads_files_and_fdo_under_the_shard(tmp_path):
    repo, branch, uploaded = _make_mock_branch()
    fdo = _fdo()
    shard = shard_qid(QID)

    with patch("tasks.convert_to_parquet._get_lakefs_repository", return_value=repo):
        publish_collection.fn(fdo, _files(tmp_path), "data-processed", "main")

    assert set(uploaded) == {
        f"{shard}/components/KVIS_Impfquoten_Erwachsene.parquet",
        f"{shard}/components/KVIS_Impfquoten_Kinder.parquet",
        f"{shard}/{QID}.fdo.json",
    }
    assert uploaded[f"{shard}/components/KVIS_Impfquoten_Kinder.parquet"]["data"] == b"child-bytes"
    assert uploaded[f"{shard}/components/KVIS_Impfquoten_Kinder.parquet"]["content_type"] == "application/vnd.apache.parquet"
    assert json.loads(uploaded[f"{shard}/{QID}.fdo.json"]["data"]) == fdo
    assert all(u["pre_sign"] is False for u in uploaded.values())


def test_publish_collection_commits_once_with_the_qid_message(tmp_path):
    repo, branch, _ = _make_mock_branch()

    with patch("tasks.convert_to_parquet._get_lakefs_repository", return_value=repo):
        publish_collection.fn(_fdo(), _files(tmp_path), "data-processed", "main")

    branch.commit.assert_called_once_with(message=f"Parquet conversion of {QID}")


def test_publish_collection_skips_the_commit_when_nothing_changed(tmp_path):
    repo, branch, _ = _make_mock_branch(has_changes=False)

    with patch("tasks.convert_to_parquet._get_lakefs_repository", return_value=repo):
        publish_collection.fn(_fdo(), _files(tmp_path), "data-processed", "main")

    branch.commit.assert_not_called()
