"""Unit tests for flow/dataset_flow.py."""

import pytest
from unittest.mock import patch

import pandas as pd

from flow.dataset_flow import run_dataset


SAMPLE_DF = pd.DataFrame(
    {
        "Kalenderwoche": ["2024-W01", "2024-W02"],
        "Inzidenz": [12.3, 14.7],
    }
)

SAMPLE_FDO = {"@id": "Q123", "@type": "DigitalObject"}


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
        patch("flow.dataset_flow.parse_dataset", return_value=SAMPLE_DF, side_effect=lambda path, delim, skiprows=0: call_order.append(("parse", path)) or SAMPLE_DF),
        patch(
            "flow.dataset_flow.store_to_mariadb",
            side_effect=lambda df, table, database, primary_key=None: call_order.append(("mariadb", table, database, primary_key)),
        ),
    ):
        run_dataset(
            dataset_name="grippeweb",
            source_url="https://example.com/data.tsv",
            source_delimiter="\t",
            local_path="/tmp/grippeweb.tsv",
            lakefs_repo="sandbox",
            lakefs_branch="main",
            lakefs_object_path="RAW/RKI/grippeweb.tsv",
            lakefs_commit_message="new version from RKI",
            mariadb_table="grippeweb",
            mariadb_database="test",
        )

    assert call_order == [
        ("download", "https://example.com/data.tsv", "/tmp/grippeweb.tsv"),
        ("lakefs", "/tmp/grippeweb.tsv", "sandbox", "main", "RAW/RKI/grippeweb.tsv", "new version from RKI", SAMPLE_FDO),
        ("parse", "/tmp/grippeweb.tsv"),
        ("mariadb", "grippeweb", "test", None),
    ]


def test_run_dataset_rejects_blank_required_parameters():
    """run_dataset should fail early with a clear error when a required parameter is blank."""
    with pytest.raises(ValueError, match="Missing required flow parameter\\(s\\): source_url"):
        run_dataset(
            dataset_name="grippeweb",
            source_url="",
            source_delimiter="\t",
            local_path="/tmp/grippeweb.tsv",
            lakefs_repo="sandbox",
            lakefs_branch="main",
            lakefs_object_path="RAW/RKI/grippeweb.tsv",
            lakefs_commit_message="new version from RKI",
            mariadb_table="grippeweb",
            mariadb_database="test",
        )
