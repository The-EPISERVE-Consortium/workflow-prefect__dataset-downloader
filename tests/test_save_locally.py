"""Unit tests for tasks/save_locally.py."""

from pathlib import Path

import pandas as pd

from tasks.save_locally import parse_dataset


def test_parse_dataset_reads_tsv(tmp_path: Path):
    """parse_dataset should read a TSV file into a DataFrame."""
    src = tmp_path / "data.tsv"
    src.write_text("Kalenderwoche\tInzidenz\n2024-W01\t12.3\n2024-W02\t14.7\n", encoding="utf-8")

    result = parse_dataset.fn(str(src), "\t")

    assert list(result.columns) == ["Kalenderwoche", "Inzidenz"]
    assert len(result) == 2


def test_parse_dataset_reads_csv(tmp_path: Path):
    """parse_dataset should read a CSV file into a DataFrame without conversion."""
    src = tmp_path / "data.csv"
    src.write_text("col1,col2\n1,2\n3,4\n", encoding="utf-8")

    result = parse_dataset.fn(str(src), ",")

    assert list(result.columns) == ["col1", "col2"]
    assert len(result) == 2


def test_parse_dataset_passes_skiprows(tmp_path: Path):
    """parse_dataset should skip the given number of header rows."""
    src = tmp_path / "data.csv"
    src.write_text("metadata\ncol1,col2\n1,2\n", encoding="utf-8")

    result = parse_dataset.fn(str(src), ",", skiprows=1)

    assert list(result.columns) == ["col1", "col2"]
