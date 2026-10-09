"""Unit tests for flow/collection_flow.py."""

from contextlib import ExitStack
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from flow.collection_flow import PROCESSED_BRANCH, _validate_sources, run_dataset_collection

URL_A = "https://example.org/data/KVIS_Impfquoten_Erwachsene.tsv"
URL_B = "https://media.example.org/data/KVIS_Impfquoten_Kinder.tsv"

SOURCES = [
    {"name": "adults", "source_url": URL_A, "lakefs_object_path": "vaccination/routine/RKI__adults.tsv",
     "mariadb_table": "kvis_adults", "display_name": "Adults", "description": "Adult rates"},
    {"name": "children", "source_url": URL_B, "lakefs_object_path": "vaccination/routine/RKI__children.tsv",
     "mariadb_table": "kvis_children"},
]

DF_A = pd.DataFrame({"a": [1]})
DF_B = pd.DataFrame({"b": [2, 3]})


def _run(sources=SOURCES, **overrides):
    """Run the flow with every task mocked; return (calls, mocks)."""
    calls: list[tuple] = []
    frames = iter([DF_A, DF_B])  # parse_dataset returns the first source's frame, then the second's

    def rec(name, result=None):
        def side_effect(*args, **kwargs):
            calls.append((name, args, kwargs))
            return result(*args, **kwargs) if callable(result) else result
        return side_effect

    patches = {
        "download_file": rec("download"),
        "resolve_source_changed_at": rec("change", "2026-01-01T00:00:00Z"),
        "create_fdo_metadata": rec("raw_fdo", lambda name, *a, **k: {"@id": f"Q-{name}-raw", "profile": {"url": a[0]}}),
        "commit_to_lakefs": rec("commit"),
        "parse_dataset": rec("parse", lambda path, delim, skiprows=0: next(frames)),
        "store_to_mariadb": rec("mariadb"),
        "write_parquet_component": rec("parquet", lambda df, url, directory: {"componentId": Path(url).stem + ".parquet", "local_path": f"{directory}/x", "size": len(df)}),
        "create_collection_fdo": rec("collection_fdo", {"@id": "QCOLL"}),
        "publish_collection": rec("publish"),
    }
    params = dict(
        dataset_name="kvis", sources=sources, lakefs_repo="data-raw", lakefs_branch="main",
        lakefs_commit_message="periodic download", mariadb_database="db", lakefs_processed_repo="data-processed",
        qid_seed="kvis_seed", display_name="KVIS", description="Collection", dataset_url="https://example.org/repo",
        license_id="cc-by", attribution="RKI",
    )
    params.update(overrides)
    with ExitStack() as stack:
        for name, effect in patches.items():
            stack.enter_context(patch(f"flow.collection_flow.{name}", side_effect=effect))
        run_dataset_collection.fn(**params)
    return calls


def _names(calls):
    return [c[0] for c in calls]


def test_each_source_is_processed_in_full_before_the_next_then_published_once():
    calls = _run()

    assert _names(calls) == [
        "download", "change", "raw_fdo", "commit", "parse", "mariadb", "parquet",   # adults
        "download", "change", "raw_fdo", "commit", "parse", "mariadb", "parquet",   # children
        "collection_fdo", "publish",
    ]


def test_mariadb_gets_one_table_per_source_in_replace_mode():
    mariadb = [c for c in _run() if c[0] == "mariadb"]

    assert [(c[1][1], c[1][2], c[1][3]) for c in mariadb] == [("kvis_adults", "db", None), ("kvis_children", "db", None)]
    assert [len(c[1][0]) for c in mariadb] == [len(DF_A), len(DF_B)]  # each table gets its own source's data


def test_raw_fdos_use_the_file_names_not_the_collection_seed():
    raw_calls = [c for c in _run() if c[0] == "raw_fdo"]

    assert [c[1][0] for c in raw_calls] == ["adults", "children"]           # dataset_name per source
    assert [c[1][3] for c in raw_calls] == ["Adult rates", None]            # per-source description (optional)
    assert all("qid_seed" not in c[2] for c in raw_calls)
    assert all(c[2]["license_id"] == "cc-by" and c[2]["attribution"] == "RKI" for c in raw_calls)


def test_raw_files_are_committed_to_the_raw_repo_with_their_own_sidecar():
    commits = [c for c in _run() if c[0] == "commit"]

    assert [c[1][1:5] for c in commits] == [
        ("data-raw", "main", "vaccination/routine/RKI__adults.tsv", "periodic download"),
        ("data-raw", "main", "vaccination/routine/RKI__children.tsv", "periodic download"),
    ]
    assert [c[1][5]["@id"] for c in commits] == ["Q-adults-raw", "Q-children-raw"]


def test_collection_fdo_and_publish_get_all_sources_and_the_processed_repo():
    calls = _run()
    (fdo_call,) = [c for c in calls if c[0] == "collection_fdo"]
    (publish,) = [c for c in calls if c[0] == "publish"]

    dataset_name, raw_fdos, components, repo, branch, seed = fdo_call[1]
    assert (dataset_name, repo, branch, seed) == ("kvis", "data-processed", PROCESSED_BRANCH, "kvis_seed")
    assert [r["@id"] for r in raw_fdos] == ["Q-adults-raw", "Q-children-raw"]
    assert [c["componentId"] for c in components] == ["KVIS_Impfquoten_Erwachsene.parquet", "KVIS_Impfquoten_Kinder.parquet"]
    assert fdo_call[2] == {"display_name": "KVIS", "description": "Collection", "dataset_url": "https://example.org/repo",
                           "license_id": "cc-by", "attribution": "RKI"}
    assert publish[1] == ({"@id": "QCOLL"}, components, "data-processed", PROCESSED_BRANCH)


def test_files_live_in_a_temporary_directory_that_is_removed_afterwards():
    calls = _run()
    local_paths = [c[1][1] for c in calls if c[0] == "download"]

    assert all(Path(p).name in ("RKI__adults.tsv", "RKI__children.tsv") for p in local_paths)
    assert len({Path(p).parent for p in local_paths}) == 1
    assert not Path(local_paths[0]).parent.exists()


def test_delimiter_comes_from_the_extension_unless_overridden():
    sources = [
        {**SOURCES[0], "lakefs_object_path": "p/a.csv"},
        {**SOURCES[1], "lakefs_object_path": "p/b.tsv", "source_delimiter": ";", "source_skiprows": 2},
    ]
    parses = [c for c in _run(sources=sources) if c[0] == "parse"]

    assert [c[1][1] for c in parses] == [",", ";"]
    assert [c[1][2] if len(c[1]) > 2 else 0 for c in parses] == [0, 2]


# --- validation: nothing is downloaded or written when the configuration is wrong ---

@pytest.mark.parametrize(
    "sources, message",
    [
        ([], "at least one source"),
        ([{**SOURCES[0], "mariadb_table": ""}], "mariadb_table"),
        ([{k: v for k, v in SOURCES[0].items() if k != "name"}], "name"),
        ([SOURCES[0], {**SOURCES[1], "mariadb_table": "kvis_adults"}], "mariadb_table"),
        ([SOURCES[0], {**SOURCES[1], "lakefs_object_path": SOURCES[0]["lakefs_object_path"]}], "lakefs_object_path"),
        ([SOURCES[0], {**SOURCES[1], "source_url": "https://other.example/KVIS_Impfquoten_Erwachsene.tsv?x=1"}], "parquet file name"),
    ],
)
def test_invalid_sources_are_rejected_before_any_work(sources, message):
    with pytest.raises(ValueError, match=message):
        _run(sources=sources)


def test_missing_qid_seed_is_rejected_before_any_work():
    with pytest.raises(ValueError, match="qid_seed"):
        _run(qid_seed="")


def test_validate_sources_accepts_the_sample_sources():
    _validate_sources(SOURCES)
