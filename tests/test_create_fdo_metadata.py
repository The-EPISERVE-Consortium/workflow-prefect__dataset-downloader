"""Unit tests for tasks/create_fdo_metadata.py."""

import re

from tasks.create_fdo_metadata import create_fdo_metadata, mint_qid, _media_type


def test_mint_qid_format():
    qid = mint_qid()
    assert re.match(r"^Q\d{13}$", qid), f"Unexpected QID format: {qid}"


def test_create_fdo_metadata_structure():
    result = create_fdo_metadata.fn("grippeweb", "https://example.com/data.tsv", "incidence/RKI__grippeweb.tsv")

    assert result["@type"] == "DigitalObject"
    assert result["kernel"]["digitalObjectType"] == "https://schema.org/Dataset"
    assert result["kernel"]["kernelVersion"] == "v1"
    assert result["kernel"]["immutable"] is False
    assert result["profile"]["@type"] == "Dataset"
    assert "prov:generatedAtTime" in result["provenance"]


def test_create_fdo_metadata_qid_is_consistent():
    result = create_fdo_metadata.fn("grippeweb", "https://example.com/data.tsv", "incidence/RKI__grippeweb.tsv")
    qid = result["@id"]
    assert result["kernel"]["@id"] == qid
    assert result["kernel"]["primaryIdentifier"] == qid
    assert result["profile"]["@id"] == qid


def test_create_fdo_metadata_name_and_description():
    result = create_fdo_metadata.fn("grippeweb", "https://example.com/data.tsv", "incidence/RKI__grippeweb.tsv")
    assert result["profile"]["name"] == "grippeweb"
    assert result["profile"]["description"] == "Dataset grippeweb downloaded from https://example.com/data.tsv"
    assert result["profile"]["url"] == "https://example.com/data.tsv"
    assert result["profile"]["distribution"][0]["contentUrl"] == "https://example.com/data.tsv"


def test_create_fdo_metadata_component_filename():
    result = create_fdo_metadata.fn("grippeweb", "https://example.com/data.tsv", "incidence/RKI__grippeweb.tsv")
    component = result["kernel"]["fdo:hasComponent"][0]
    assert component["componentId"] == "RKI__grippeweb.tsv"
    assert component["@id"] == "#RKI__grippeweb.tsv"
    assert component["mediaType"] == "text/tab-separated-values"


def test_media_type_csv():
    assert _media_type("some/path/data.csv") == "text/csv"


def test_media_type_parquet():
    assert _media_type("some/path/data.parquet") == "application/vnd.apache.parquet"


def test_media_type_unknown():
    assert _media_type("some/path/data.xyz") == "application/octet-stream"
