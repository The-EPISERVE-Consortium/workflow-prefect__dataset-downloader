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


def test_mint_qid_seed_override_disambiguates_same_filename():
    """Two datasets whose URL filename collides get distinct QIDs via seed_override."""
    daily = "https://api.open-meteo.com/v1/forecast?daily=temperature_2m_max"
    hourly = "https://api.open-meteo.com/v1/forecast?hourly=temperature_2m"
    assert mint_qid(daily) == mint_qid(hourly)  # the bug this guards against
    assert mint_qid(daily, "weather_berlin_daily") != mint_qid(hourly, "weather_berlin_hourly")
    assert re.match(r"^Q\d{13}$", mint_qid(daily, "weather_berlin_daily"))


def test_mint_qid_seed_override_is_stable():
    url = "https://api.open-meteo.com/v1/forecast?daily=x"
    assert mint_qid(url, "weather_berlin_daily") == mint_qid("https://other.example/forecast?y=2", "weather_berlin_daily")


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


def test_create_fdo_metadata_uses_explicit_source_changed_at():
    """When source_changed_at is passed (unchanged content), it should be used as-is."""
    result = create_fdo_metadata.fn(
        "grippeweb",
        URL,
        "incidence/RKI__grippeweb.tsv",
        source_changed_at="2026-05-01T00:00:00Z",
    )
    assert result["provenance"]["source_changed_at"] == "2026-05-01T00:00:00Z"
    assert result["provenance"]["source_changed_at"] != result["kernel"]["modified"]


def test_create_fdo_metadata_omits_licence_fields_by_default():
    """Without license_id / attribution, the profile should carry neither key."""
    result = create_fdo_metadata.fn("grippeweb", URL, "incidence/RKI__grippeweb.tsv")
    assert "license" not in result["profile"]
    assert "creditText" not in result["profile"]


def test_create_fdo_metadata_adds_licence_fields_when_provided():
    """license_id maps to profile.license and attribution to profile.creditText."""
    result = create_fdo_metadata.fn(
        "weather_berlin_daily",
        URL,
        "climate/temperature/data.csv",
        license_id="cc-by",
        attribution="Weather data by Open-Meteo.com (CC BY 4.0).",
    )
    assert result["profile"]["license"] == "cc-by"
    assert result["profile"]["creditText"] == "Weather data by Open-Meteo.com (CC BY 4.0)."


def test_create_fdo_metadata_uses_qid_seed_when_given():
    """A qid_seed changes the minted @id (still `-raw` suffixed) without touching anything else."""
    forecast_url = "https://api.open-meteo.com/v1/forecast?daily=temperature_2m_max"
    default = create_fdo_metadata.fn("weather_berlin_daily", forecast_url, "climate/temperature/x.csv")
    seeded = create_fdo_metadata.fn(
        "weather_berlin_daily", forecast_url, "climate/temperature/x.csv",
        qid_seed="weather_berlin_daily",
    )
    assert seeded["@id"] != default["@id"]
    assert seeded["@id"].endswith("-raw")
    assert seeded["kernel"]["@id"] == seeded["@id"]
    assert seeded["profile"]["@id"] == seeded["@id"]


def test_create_fdo_metadata_ignores_blank_licence_fields():
    """Empty strings are treated the same as missing."""
    result = create_fdo_metadata.fn(
        "grippeweb",
        URL,
        "incidence/RKI__grippeweb.tsv",
        license_id="",
        attribution="",
    )
    assert "license" not in result["profile"]
    assert "creditText" not in result["profile"]


def test_media_type_csv():
    assert _media_type("some/path/data.csv") == "text/csv"


def test_media_type_parquet():
    assert _media_type("some/path/data.parquet") == "application/vnd.apache.parquet"


def test_media_type_unknown():
    assert _media_type("some/path/data.xyz") == "application/octet-stream"
