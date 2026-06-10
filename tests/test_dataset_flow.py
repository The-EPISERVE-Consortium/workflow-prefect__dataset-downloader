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
        ("lakefs", EXPECTED_LOCAL_PATH, "sandbox", "main", "RAW/RKI/grippeweb.tsv", "new version from RKI", SAMPLE_FDO),
        ("parse", EXPECTED_LOCAL_PATH, "\t"),
        ("mariadb", "grippeweb", "test", None),
        ("parquet", SAMPLE_FDO["@id"], SAMPLE_FDO, "https://example.com/data.tsv", "data-processed"),
    ]


def test_run_dataset_passes_description_to_fdo_metadata():
    """run_dataset should pass the optional dataset description into FDO metadata creation."""
    with (
        patch("flow.dataset_flow.download_file"),
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
    )


def test_run_dataset_passes_display_name_to_fdo_metadata():
    """run_dataset should pass the optional dataset display name into FDO metadata creation."""
    with (
        patch("flow.dataset_flow.download_file"),
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
    )


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
