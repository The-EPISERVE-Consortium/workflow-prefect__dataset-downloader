"""Unit tests for flow/dataset_flow.py."""

import tempfile
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from flow.dataset_flow import run_dataset, _resolve_delimiter


SAMPLE_DF = pd.DataFrame(
    {
        "Kalenderwoche": ["2024-W01", "2024-W02"],
        "Inzidenz": [12.3, 14.7],
    }
)

SAMPLE_FDO = {"@id": "Q123", "@type": "DigitalObject"}

EXPECTED_LOCAL_PATH = str(Path(tempfile.gettempdir()) / "grippeweb.tsv")


def test_run_dataset_runs_steps_in_order():
    """run_dataset should download, commit to lakeFS with FDO metadata, then parse and store in MariaDB."""
    call_order = []

    with (
        patch("flow.dataset_flow.download_file", side_effect=lambda url, path: call_order.append(("download", url, path))),
        patch(
            "flow.dataset_flow.resolve_source_changed_at",
            side_effect=lambda local_path, repo, branch, object_path: call_order.append(
                ("content_change", local_path, repo, branch, object_path)
            ) or "2026-06-01T00:00:00Z",
        ),
        patch("flow.dataset_flow.create_fdo_metadata", return_value=SAMPLE_FDO),
        patch(
            "flow.dataset_flow.commit_to_lakefs",
            side_effect=lambda path, repo, branch, object_path, commit_message, fdo=None: call_order.append(
                ("lakefs", path, repo, branch, object_path, commit_message, fdo)
            ),
        ),
        patch("flow.dataset_flow.parse_dataset", side_effect=lambda path, delim, skiprows=0: call_order.append(("parse", path, delim)) or SAMPLE_DF),
        patch(
            "flow.dataset_flow.store_to_mariadb",
            side_effect=lambda df, table, database, primary_key=None: call_order.append(("mariadb", table, database, primary_key)),
        ),
        patch(
            "flow.dataset_flow.convert_to_parquet",
            side_effect=lambda df, fdo, url, repo, **kw: call_order.append(("parquet", fdo["@id"], fdo, url, repo)),
        ),
    ):
        run_dataset.fn(
            dataset_name="grippeweb",
            source_url="https://example.com/data.tsv",
            lakefs_repo="sandbox",
            lakefs_branch="main",
            lakefs_object_path="RAW/RKI/grippeweb.tsv",
            lakefs_commit_message="new version from RKI",
            mariadb_table="grippeweb",
            mariadb_database="test",
            lakefs_processed_repo="data-processed",
        )

    assert call_order == [
        ("download", "https://example.com/data.tsv", EXPECTED_LOCAL_PATH),
        ("content_change", EXPECTED_LOCAL_PATH, "sandbox", "main", "RAW/RKI/grippeweb.tsv"),
        ("lakefs", EXPECTED_LOCAL_PATH, "sandbox", "main", "RAW/RKI/grippeweb.tsv", "new version from RKI", SAMPLE_FDO),
        ("parse", EXPECTED_LOCAL_PATH, "\t"),
        ("mariadb", "grippeweb", "test", None),
        ("parquet", SAMPLE_FDO["@id"], SAMPLE_FDO, "https://example.com/data.tsv", "data-processed"),
    ]


def test_run_dataset_passes_description_to_fdo_metadata():
    """run_dataset should pass the optional dataset description into FDO metadata creation."""
    with (
        patch("flow.dataset_flow.download_file"),
        patch("flow.dataset_flow.resolve_source_changed_at", return_value="2026-06-01T00:00:00Z"),
        patch("flow.dataset_flow.create_fdo_metadata", return_value=SAMPLE_FDO) as mock_create_fdo,
        patch("flow.dataset_flow.commit_to_lakefs"),
        patch("flow.dataset_flow.parse_dataset", return_value=SAMPLE_DF),
        patch("flow.dataset_flow.store_to_mariadb"),
        patch("flow.dataset_flow.convert_to_parquet"),
    ):
        run_dataset.fn(
            dataset_name="grippeweb",
            source_url="https://example.com/data.tsv",
            lakefs_repo="sandbox",
            lakefs_branch="main",
            lakefs_object_path="RAW/RKI/grippeweb.tsv",
            lakefs_commit_message="new version from RKI",
            mariadb_table="grippeweb",
            mariadb_database="test",
            lakefs_processed_repo="data-processed",
            description="Weekly GrippeWeb incidence data.",
        )

    mock_create_fdo.assert_called_once_with(
        "grippeweb",
        "https://example.com/data.tsv",
        "RAW/RKI/grippeweb.tsv",
        "Weekly GrippeWeb incidence data.",
        None,
        "2026-06-01T00:00:00Z",
        license_id=None,
        attribution=None,
        qid_seed=None,
    )


def test_run_dataset_passes_display_name_to_fdo_metadata():
    """run_dataset should pass the optional dataset display name into FDO metadata creation."""
    with (
        patch("flow.dataset_flow.download_file"),
        patch("flow.dataset_flow.resolve_source_changed_at", return_value="2026-06-01T00:00:00Z"),
        patch("flow.dataset_flow.create_fdo_metadata", return_value=SAMPLE_FDO) as mock_create_fdo,
        patch("flow.dataset_flow.commit_to_lakefs"),
        patch("flow.dataset_flow.parse_dataset", return_value=SAMPLE_DF),
        patch("flow.dataset_flow.store_to_mariadb"),
        patch("flow.dataset_flow.convert_to_parquet"),
    ):
        run_dataset.fn(
            dataset_name="grippeweb",
            source_url="https://example.com/data.tsv",
            lakefs_repo="sandbox",
            lakefs_branch="main",
            lakefs_object_path="RAW/RKI/grippeweb.tsv",
            lakefs_commit_message="new version from RKI",
            mariadb_table="grippeweb",
            mariadb_database="test",
            lakefs_processed_repo="data-processed",
            display_name="GrippeWeb Weekly Report Data",
        )

    mock_create_fdo.assert_called_once_with(
        "grippeweb",
        "https://example.com/data.tsv",
        "RAW/RKI/grippeweb.tsv",
        None,
        "GrippeWeb Weekly Report Data",
        "2026-06-01T00:00:00Z",
        license_id=None,
        attribution=None,
        qid_seed=None,
    )


def test_run_dataset_passes_license_and_attribution_to_fdo_metadata():
    """run_dataset should forward the optional licence id and attribution into FDO metadata creation."""
    with (
        patch("flow.dataset_flow.download_file"),
        patch("flow.dataset_flow.resolve_source_changed_at", return_value="2026-06-01T00:00:00Z"),
        patch("flow.dataset_flow.create_fdo_metadata", return_value=SAMPLE_FDO) as mock_create_fdo,
        patch("flow.dataset_flow.commit_to_lakefs"),
        patch("flow.dataset_flow.parse_dataset", return_value=SAMPLE_DF),
        patch("flow.dataset_flow.store_to_mariadb"),
        patch("flow.dataset_flow.read_full_table", return_value=SAMPLE_DF),
        patch("flow.dataset_flow.convert_to_parquet"),
    ):
        run_dataset.fn(
            dataset_name="weather_berlin_daily",
            source_url="https://example.com/data.csv",
            lakefs_repo="sandbox",
            lakefs_branch="main",
            lakefs_object_path="climate/temperature/data.csv",
            lakefs_commit_message="new version from Open-Meteo",
            mariadb_table="weather_berlin_daily",
            mariadb_database="test",
            lakefs_processed_repo="data-processed",
            license_id="cc-by",
            attribution="Weather data by Open-Meteo.com (CC BY 4.0).",
        )

    _, kwargs = mock_create_fdo.call_args
    assert kwargs["license_id"] == "cc-by"
    assert kwargs["attribution"] == "Weather data by Open-Meteo.com (CC BY 4.0)."


def test_run_dataset_threads_qid_seed_to_fdo_and_parquet():
    """run_dataset should pass qid_seed into both create_fdo_metadata and convert_to_parquet."""
    with (
        patch("flow.dataset_flow.download_file"),
        patch("flow.dataset_flow.resolve_source_changed_at", return_value="2026-06-01T00:00:00Z"),
        patch("flow.dataset_flow.create_fdo_metadata", return_value=SAMPLE_FDO) as mock_create_fdo,
        patch("flow.dataset_flow.commit_to_lakefs"),
        patch("flow.dataset_flow.parse_dataset", return_value=SAMPLE_DF),
        patch("flow.dataset_flow.store_to_mariadb"),
        patch("flow.dataset_flow.read_full_table", return_value=SAMPLE_DF),
        patch("flow.dataset_flow.convert_to_parquet") as mock_parquet,
    ):
        run_dataset.fn(
            dataset_name="weather_berlin_hourly",
            source_url="https://api.open-meteo.com/v1/forecast?hourly=temperature_2m",
            lakefs_repo="sandbox",
            lakefs_branch="main",
            lakefs_object_path="climate/temperature/open-meteo__weather_berlin_hourly.csv",
            lakefs_commit_message="new version from Open-Meteo",
            mariadb_table="weather_berlin_hourly",
            mariadb_database="test",
            lakefs_processed_repo="data-processed",
            qid_seed="weather_berlin_hourly",
        )

    assert mock_create_fdo.call_args.kwargs["qid_seed"] == "weather_berlin_hourly"
    assert mock_parquet.call_args.kwargs["qid_seed"] == "weather_berlin_hourly"


_FULL_TABLE_DF = pd.DataFrame({"time": ["1940-01-01", "1940-01-02", "2026-01-01"], "t": [1, 2, 3]})


def _weather_run(**overrides):
    kwargs = dict(
        dataset_name="weather_berlin_daily",
        source_url="https://api.open-meteo.com/v1/forecast?daily=temperature_2m_max",
        lakefs_repo="sandbox",
        lakefs_branch="main",
        lakefs_object_path="climate/temperature/open-meteo__weather_berlin_daily.csv",
        lakefs_commit_message="new version from Open-Meteo",
        mariadb_table="weather_berlin_daily",
        mariadb_database="episerve-raw-data",
        lakefs_processed_repo="data-processed",
        mariadb_primary_key="time",
        qid_seed="weather_berlin_daily",
    )
    kwargs.update(overrides)
    return kwargs


def test_run_dataset_publishes_full_table_for_weather():
    """A weather dataset publishes the MariaDB read-back, not the downloaded delta."""
    with (
        patch("flow.dataset_flow.download_file"),
        patch("flow.dataset_flow.resolve_source_changed_at", return_value="2026-06-01T00:00:00Z"),
        patch("flow.dataset_flow.create_fdo_metadata", return_value=SAMPLE_FDO),
        patch("flow.dataset_flow.commit_to_lakefs"),
        patch("flow.dataset_flow.parse_dataset", return_value=SAMPLE_DF),
        patch("flow.dataset_flow.store_to_mariadb"),
        patch("flow.dataset_flow.read_full_table", return_value=_FULL_TABLE_DF) as mock_read,
        patch("flow.dataset_flow.convert_to_parquet") as mock_parquet,
    ):
        run_dataset.fn(**_weather_run())

    mock_read.assert_called_once_with(
        "weather_berlin_daily", "episerve-raw-data", "time", schema_df=SAMPLE_DF
    )
    assert mock_parquet.call_args.args[0] is _FULL_TABLE_DF


def test_run_dataset_skips_full_table_for_non_weather():
    """A non-weather dataset never touches read_full_table and publishes the delta."""
    with (
        patch("flow.dataset_flow.download_file"),
        patch("flow.dataset_flow.resolve_source_changed_at", return_value="2026-06-01T00:00:00Z"),
        patch("flow.dataset_flow.create_fdo_metadata", return_value=SAMPLE_FDO),
        patch("flow.dataset_flow.commit_to_lakefs"),
        patch("flow.dataset_flow.parse_dataset", return_value=SAMPLE_DF),
        patch("flow.dataset_flow.store_to_mariadb"),
        patch("flow.dataset_flow.read_full_table") as mock_read,
        patch("flow.dataset_flow.convert_to_parquet") as mock_parquet,
    ):
        run_dataset.fn(**_weather_run(dataset_name="grippeweb", mariadb_table="grippeweb"))

    mock_read.assert_not_called()
    assert mock_parquet.call_args.args[0] is SAMPLE_DF


def test_run_dataset_raises_when_full_table_read_back_is_short():
    """If the read-back has fewer rows than the delta just written, fail loudly."""
    short = pd.DataFrame({"time": ["2026-01-01"], "t": [3]})  # 1 row < 2-row SAMPLE_DF
    with (
        patch("flow.dataset_flow.download_file"),
        patch("flow.dataset_flow.resolve_source_changed_at", return_value="2026-06-01T00:00:00Z"),
        patch("flow.dataset_flow.create_fdo_metadata", return_value=SAMPLE_FDO),
        patch("flow.dataset_flow.commit_to_lakefs"),
        patch("flow.dataset_flow.parse_dataset", return_value=SAMPLE_DF),
        patch("flow.dataset_flow.store_to_mariadb"),
        patch("flow.dataset_flow.read_full_table", return_value=short),
        patch("flow.dataset_flow.convert_to_parquet") as mock_parquet,
    ):
        with pytest.raises(RuntimeError, match="fewer than the 2"):
            run_dataset.fn(**_weather_run())

    mock_parquet.assert_not_called()


def test_run_dataset_rejects_blank_required_parameters():
    """run_dataset should fail early with a clear error when a required parameter is blank."""
    with pytest.raises(ValueError, match="Missing required flow parameter\\(s\\): source_url"):
        run_dataset.fn(
            dataset_name="grippeweb",
            source_url="",
            lakefs_repo="sandbox",
            lakefs_branch="main",
            lakefs_object_path="RAW/RKI/grippeweb.tsv",
            lakefs_commit_message="new version from RKI",
            mariadb_table="grippeweb",
            mariadb_database="test",
            lakefs_processed_repo="data-processed",
        )


def test_resolve_delimiter_infers_tsv():
    assert _resolve_delimiter("path/to/data.tsv", None) == "\t"


def test_resolve_delimiter_infers_csv():
    assert _resolve_delimiter("path/to/data.csv", None) == ","


def test_resolve_delimiter_override_takes_precedence():
    assert _resolve_delimiter("path/to/data.csv", ";") == ";"


def test_resolve_delimiter_raises_on_unknown_extension():
    with pytest.raises(ValueError, match="Cannot infer delimiter"):
        _resolve_delimiter("path/to/data.parquet", None)
