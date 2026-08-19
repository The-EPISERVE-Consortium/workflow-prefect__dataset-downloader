"""Unit tests for tasks/create_fdo_metadata.py."""

import re

from tasks.create_fdo_metadata import create_fdo_metadata, mint_qid, _media_type

URL = "https://example.com/GrippeWeb_Daten_des_Wochenberichts.tsv"


def test_mint_qid_format():
    qid = mint_qid(URL)
    assert re.match(r"^Q\d{13}$", qid), f"Unexpected QID format: {qid}"


def test_create_fdo_metadata_qid_has_raw_suffix():
    result = create_fdo_metadata.fn("grippeweb", URL, "incidence/RKI__grippeweb.tsv")
    assert result["@id"].endswith("-raw")


def test_mint_qid_is_stable():
    assert mint_qid(URL) == mint_qid(URL)


def test_mint_qid_differs_per_filename():
    other = "https://example.com/OtherDataset.tsv"
    assert mint_qid(URL) != mint_qid(other)


def test_mint_qid_ignores_query_string():
    base = "https://api.example.com/forecast?format=csv&timezone=Europe%2FBerlin"
    same_file = "https://api.example.com/forecast?format=csv&past_days=3"
    assert mint_qid(base) == mint_qid(same_file)


def test_create_fdo_metadata_structure():
    result = create_fdo_metadata.fn("grippeweb", URL, "incidence/RKI__grippeweb.tsv")

    assert result["@type"] == "DigitalObject"
    assert result["kernel"]["digitalObjectType"] == "https://schema.org/Dataset"
    assert result["kernel"]["kernelVersion"] == "v1"
    assert result["kernel"]["immutable"] is False
    assert result["profile"]["@type"] == "Dataset"
    assert "prov:generatedAtTime" in result["provenance"]


def test_create_fdo_metadata_qid_is_consistent():
    result = create_fdo_metadata.fn("grippeweb", URL, "incidence/RKI__grippeweb.tsv")
    qid = result["@id"]
    assert result["kernel"]["@id"] == qid
    assert result["kernel"]["primaryIdentifier"] == qid
    assert result["profile"]["@id"] == qid


def test_create_fdo_metadata_name_and_description():
    """create_fdo_metadata should default the profile description from the dataset name."""
    result = create_fdo_metadata.fn("grippeweb", URL, "incidence/RKI__grippeweb.tsv")
    assert result["profile"]["name"] == "grippeweb"
    assert result["profile"]["display_name"] == "grippeweb"
    assert result["profile"]["description"] == "Dataset grippeweb"
    assert result["profile"]["url"] == URL
    assert result["profile"]["distribution"][0]["contentUrl"] == URL


def test_create_fdo_metadata_uses_custom_display_name():
    """create_fdo_metadata should use an explicit profile display name when provided."""
    result = create_fdo_metadata.fn(
        "grippeweb",
        URL,
        "incidence/RKI__grippeweb.tsv",
        display_name="GrippeWeb Weekly Report Data",
    )
    assert result["profile"]["display_name"] == "GrippeWeb Weekly Report Data"


def test_create_fdo_metadata_uses_custom_description():
    """create_fdo_metadata should use an explicit profile description when provided."""
    result = create_fdo_metadata.fn(
        "grippeweb",
        URL,
        "incidence/RKI__grippeweb.tsv",
        "Weekly GrippeWeb incidence data.",
    )
    assert result["profile"]["description"] == "Weekly GrippeWeb incidence data."


def test_create_fdo_metadata_component_filename():
    result = create_fdo_metadata.fn("grippeweb", URL, "incidence/RKI__grippeweb.tsv")
    component = result["kernel"]["fdo:hasComponent"][0]
    assert component["componentId"] == "RKI__grippeweb.tsv"
    assert component["@id"] == "#RKI__grippeweb.tsv"
    assert component["mediaType"] == "text/tab-separated-values"


def test_create_fdo_metadata_defaults_source_changed_at_to_now():
    """When no source_changed_at is passed, it should default to the same run timestamp as modified."""
    result = create_fdo_metadata.fn("grippeweb", URL, "incidence/RKI__grippeweb.tsv")
    assert result["provenance"]["source_changed_at"] == result["kernel"]["modified"]
    assert result["provenance"]["source_content_hash"] == ""


def test_create_fdo_metadata_uses_explicit_source_change_fields():
    """When source_content_hash/source_changed_at are passed (unchanged content), they should be used as-is."""
    result = create_fdo_metadata.fn(
        "grippeweb",
        URL,
        "incidence/RKI__grippeweb.tsv",
        source_content_hash="abc123",
        source_changed_at="2026-05-01T00:00:00Z",
    )
    assert result["provenance"]["source_content_hash"] == "abc123"
    assert result["provenance"]["source_changed_at"] == "2026-05-01T00:00:00Z"
    assert result["provenance"]["source_changed_at"] != result["kernel"]["modified"]


def test_media_type_csv():
    assert _media_type("some/path/data.csv") == "text/csv"


def test_media_type_parquet():
    assert _media_type("some/path/data.parquet") == "application/vnd.apache.parquet"


def test_media_type_unknown():
    assert _media_type("some/path/data.xyz") == "application/octet-stream"
