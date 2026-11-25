"""The FDOs this workflow writes follow the `fdo` spec of episerve_specs.

The sync flow, the DOIP server, the lineage view and the catalog read these records; the spec is their
shared definition, so a change here that breaks it fails in this test and not in one of them.
"""

import json
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pandas as pd
from episerve_specs import validate

from tasks._sharding import shard_qid
from tasks.convert_to_parquet import convert_to_parquet, publish_collection, write_parquet_component
from tasks.create_fdo_metadata import create_collection_fdo, create_fdo_metadata, mint_qid

URL = "https://example.com/GrippeWeb_Daten_des_Wochenberichts.tsv"
PATH = "incidence/influenza/RKI__grippeweb.tsv"
LICENCE = {"license_id": "cc-by", "attribution": "Provider, Data, licensed under CC BY 4.0."}


def assert_conforms(document: dict, kind: str):
    result = validate(document, spec="fdo")
    assert result.valid and not result.warnings, [str(i) for i in result.issues]
    assert result.version == "1"
    from episerve_specs.specs.fdo.checks import kind_of

    assert kind_of(document) == kind


def _raw(url=URL, path=PATH, name="grippeweb", **extra):
    return create_fdo_metadata.fn(name, url, path, "What it contains.", "Display Name", **LICENCE, **extra)


def test_the_raw_fdo_conforms():
    assert_conforms(_raw(), "raw")


def test_the_raw_fdo_of_a_dataset_with_a_qid_seed_conforms():
    assert_conforms(_raw(qid_seed="weather_berlin_daily"), "raw")


def _uploaded(has_changes=True):
    branch = MagicMock()
    branch.uncommitted.return_value = [SimpleNamespace(path="p")] if has_changes else []
    branch.commit.return_value = SimpleNamespace(id="commit-id")
    uploaded = {}

    def fake_object(path):
        obj = MagicMock()
        obj.upload.side_effect = lambda **kwargs: uploaded.update({path: kwargs})
        return obj

    branch.object.side_effect = fake_object
    repo = MagicMock()
    repo.branch.return_value = branch
    return repo, uploaded


def test_the_processed_fdo_conforms():
    repo, uploaded = _uploaded()
    raw = _raw()
    with patch("tasks.convert_to_parquet._get_lakefs_repository", return_value=repo):
        convert_to_parquet.fn(pd.DataFrame({"a": [1, 2]}), raw, URL, "data-processed")

    qid = mint_qid(URL)
    document = json.loads(uploaded[f"{shard_qid(qid)}/{qid}.fdo.json"]["data"])
    assert_conforms(document, "processed")
    assert document["provenance"]["prov:wasDerivedFrom"]["@id"] == raw["@id"]


def test_the_fdo_of_a_collection_conforms():
    urls = ["https://example.org/data/A_file.tsv", "https://example.org/data/B_file.tsv"]
    raws = [_raw(u, f"vaccination/routine/RKI__{n}.tsv", n) for u, n in zip(urls, ["a_file", "b_file"])]
    components = [{"componentId": "A_file.parquet", "size": 10}, {"componentId": "B_file.parquet", "size": 20}]
    document = create_collection_fdo.fn(
        dataset_name="kvis", raw_fdos=raws, components=components, lakefs_repo="data-processed",
        lakefs_branch="main", qid_seed="kvis", display_name="KVIS", description="d", **LICENCE,
    )
    assert_conforms(document, "processed")
    assert [e["@id"] for e in document["provenance"]["prov:wasDerivedFrom"]] == [r["@id"] for r in raws]
