"""Unit tests for deploy/deploy_registry.py."""

from unittest.mock import MagicMock, patch

import pytest
from prefect.client.schemas.schedules import CronSchedule

from deploy import deploy_registry
from deploy.deploy_registry import _deploy_dataset, _load_registry, _validate_dataset_config


DEFAULTS = {
    "lakefs_repo": "data-raw",
    "lakefs_processed_repo": "data-processed",
    "lakefs_branch": "main",
    "lakefs_commit_message": "periodic download",
    "mariadb_database": "episerve-raw-data",
}


def test_validate_dataset_config_adds_top_level_description_to_parameters():
    """_validate_dataset_config should pass a top-level dataset description to the flow."""
    _, parameters, _ = _validate_dataset_config(
        "grippeweb",
        {
            "description": "Weekly GrippeWeb participant reports and estimated incidence.",
            "deployment_name": "download__grippeweb",
            "parameters": {
                "source_url": "https://example.com/grippeweb.tsv",
                "lakefs_object_path": "incidence/influenza/RKI__grippeweb.tsv",
                "mariadb_table": "grippeweb",
            },
        },
        DEFAULTS,
    )

    assert parameters["description"] == "Weekly GrippeWeb participant reports and estimated incidence."


def test_validate_dataset_config_adds_top_level_display_name_to_parameters():
    """_validate_dataset_config should pass a top-level dataset display name to the flow."""
    _, parameters, _ = _validate_dataset_config(
        "grippeweb",
        {
            "display_name": "GrippeWeb Weekly Report Data",
            "deployment_name": "download__grippeweb",
            "parameters": {
                "source_url": "https://example.com/grippeweb.tsv",
                "lakefs_object_path": "incidence/influenza/RKI__grippeweb.tsv",
                "mariadb_table": "grippeweb",
            },
        },
        DEFAULTS,
    )

    assert parameters["display_name"] == "GrippeWeb Weekly Report Data"


def test_validate_dataset_config_parameter_description_takes_precedence():
    """_validate_dataset_config should prefer parameters.description over top-level description."""
    _, parameters, _ = _validate_dataset_config(
        "grippeweb",
        {
            "description": "Top-level description.",
            "deployment_name": "download__grippeweb",
            "parameters": {
                "description": "Parameter description.",
                "source_url": "https://example.com/grippeweb.tsv",
                "lakefs_object_path": "incidence/influenza/RKI__grippeweb.tsv",
                "mariadb_table": "grippeweb",
            },
        },
        DEFAULTS,
    )

    assert parameters["description"] == "Parameter description."


def test_validate_dataset_config_parameter_display_name_takes_precedence():
    """_validate_dataset_config should prefer parameters.display_name over top-level display_name."""
    _, parameters, _ = _validate_dataset_config(
        "grippeweb",
        {
            "display_name": "Top-level display name.",
            "deployment_name": "download__grippeweb",
            "parameters": {
                "display_name": "Parameter display name.",
                "source_url": "https://example.com/grippeweb.tsv",
                "lakefs_object_path": "incidence/influenza/RKI__grippeweb.tsv",
                "mariadb_table": "grippeweb",
            },
        },
        DEFAULTS,
    )

    assert parameters["display_name"] == "Parameter display name."


def test_validate_dataset_config_adds_top_level_license_and_attribution_to_parameters():
    """_validate_dataset_config should pass top-level license_id / attribution to the flow."""
    _, parameters, _ = _validate_dataset_config(
        "weather_berlin_daily",
        {
            "license_id": "cc-by",
            "attribution": "Weather data by Open-Meteo.com (CC BY 4.0).",
            "deployment_name": "download__weather_berlin_daily",
            "parameters": {
                "source_url": "https://example.com/weather.csv",
                "lakefs_object_path": "climate/temperature/open-meteo__weather_berlin_daily.csv",
                "mariadb_table": "weather_berlin_daily",
            },
        },
        DEFAULTS,
    )

    assert parameters["license_id"] == "cc-by"
    assert parameters["attribution"] == "Weather data by Open-Meteo.com (CC BY 4.0)."


def test_validate_dataset_config_omits_license_fields_when_absent():
    """A dataset without licensing keys should not gain license_id / attribution parameters."""
    _, parameters, _ = _validate_dataset_config(
        "grippeweb",
        {
            "deployment_name": "download__grippeweb",
            "parameters": {
                "source_url": "https://example.com/grippeweb.tsv",
                "lakefs_object_path": "incidence/influenza/RKI__grippeweb.tsv",
                "mariadb_table": "grippeweb",
            },
        },
        DEFAULTS,
    )

    assert "license_id" not in parameters
    assert "attribution" not in parameters


def test_validate_dataset_config_adds_top_level_qid_seed_to_parameters():
    """_validate_dataset_config should pass a top-level qid_seed to the flow."""
    _, parameters, _ = _validate_dataset_config(
        "weather_berlin_hourly",
        {
            "qid_seed": "weather_berlin_hourly",
            "deployment_name": "download__weather_berlin_hourly",
            "parameters": {
                "source_url": "https://api.open-meteo.com/v1/forecast?hourly=temperature_2m",
                "lakefs_object_path": "climate/temperature/open-meteo__weather_berlin_hourly.csv",
                "mariadb_table": "weather_berlin_hourly",
            },
        },
        DEFAULTS,
    )

    assert parameters["qid_seed"] == "weather_berlin_hourly"


# --- schedule ---

NIGHTLY = "0 1 * * *"


def _entry(**extra):
    return {
        "deployment_name": "download__x",
        "parameters": {
            "source_url": "https://example.com/x.tsv",
            "lakefs_object_path": "some/RKI__x.tsv",
            "mariadb_table": "x",
            **extra.pop("parameters", {}),
        },
        **extra,
    }


def _schedule(config, defaults=DEFAULTS):
    return _validate_dataset_config("x", config, defaults)[2]


def test_schedule_comes_from_the_registry_defaults():
    assert _schedule(_entry(), {**DEFAULTS, "schedule": NIGHTLY}) == NIGHTLY


def test_no_schedule_anywhere_means_no_schedule():
    assert _schedule(_entry()) is None


def test_dataset_level_schedule_overrides_the_default():
    assert _schedule(_entry(schedule="0 3 1 * *"), {**DEFAULTS, "schedule": NIGHTLY}) == "0 3 1 * *"


def test_parameters_schedule_overrides_the_dataset_level_one():
    config = _entry(schedule="0 3 1 * *", parameters={"schedule": "0 4 * * 1"})

    assert _schedule(config, {**DEFAULTS, "schedule": NIGHTLY}) == "0 4 * * 1"


@pytest.mark.parametrize("value", ["none", "None", "NONE", " none ", "", None])
def test_none_like_values_disable_the_schedule(value):
    assert _schedule(_entry(schedule=value), {**DEFAULTS, "schedule": NIGHTLY}) is None


def test_schedule_is_not_passed_to_the_flow():
    _, parameters, _ = _validate_dataset_config("x", _entry(), {**DEFAULTS, "schedule": NIGHTLY})

    assert "schedule" not in parameters


@pytest.mark.parametrize("value", ["not a cron", "61 * * * *", "0 25 * * *"])
def test_invalid_cron_is_rejected_with_the_dataset_key(value):
    with pytest.raises(ValueError, match="Dataset 'x'.*cron"):
        _schedule(_entry(schedule=value))


def test_non_string_schedule_is_rejected():
    with pytest.raises(ValueError, match="cron string or 'none'"):
        _schedule(_entry(schedule=5))


def test_run_daily_is_no_longer_accepted():
    with pytest.raises(ValueError, match="run_daily.*schedule"):
        _validate_dataset_config("x", _entry(), {**DEFAULTS, "run_daily": True})


def test_every_single_file_dataset_keeps_the_nightly_schedule_and_the_vaccination_collection_runs_weekly():
    defaults, datasets = _load_registry()

    schedules = {key: _validate_dataset_config(key, config, defaults)[2] for key, config in datasets.items()}

    assert schedules.pop("kvis_vaccination_rates") == "0 3 * * 1"
    assert len(schedules) >= 15
    assert set(schedules.values()) == {NIGHTLY}


def _deploy(schedule):
    flow = MagicMock()
    with patch.object(deploy_registry, "run_dataset") as run_dataset, patch.dict("os.environ", {}):
        run_dataset.from_source.return_value = flow
        _deploy_dataset("download__x", {"p": 1}, "http://prefect.example/api", schedule)
    return flow.deploy.call_args.kwargs


def test_deploy_attaches_the_cron_schedule_in_berlin_time():
    kwargs = _deploy("0 3 1 * *")

    [schedule] = kwargs["schedules"]
    assert isinstance(schedule, CronSchedule)
    assert str(schedule.cron) == "0 3 1 * *" and str(schedule.timezone) == "Europe/Berlin"


def test_deploy_without_schedule_passes_no_schedules():
    assert "schedules" not in _deploy(None)


# --- collections (datasets of several source files) ---

COLLECTION_DEFAULTS = {**DEFAULTS, "schedule": NIGHTLY}
SOURCES = [
    {"name": "adults", "source_url": "https://example.com/A.tsv", "lakefs_object_path": "p/RKI__a.tsv", "mariadb_table": "t_adults"},
    {"name": "children", "source_url": "https://example.com/B.tsv", "lakefs_object_path": "p/RKI__b.tsv", "mariadb_table": "t_children"},
]


def _collection(**extra):
    parameters = extra.pop("parameters", {"sources": SOURCES})
    return {"deployment_name": "download__c", "qid_seed": "c_seed", "parameters": parameters, **extra}


def test_collection_passes_sources_and_seed_to_the_flow():
    name, parameters, schedule = _validate_dataset_config("c", _collection(dataset_url="https://example.com/repo"), COLLECTION_DEFAULTS)

    assert name == "download__c"
    assert parameters["sources"] == SOURCES
    assert parameters["qid_seed"] == "c_seed"
    assert parameters["dataset_url"] == "https://example.com/repo"
    assert parameters["dataset_name"] == "c"
    assert schedule == NIGHTLY


def test_collection_needs_a_qid_seed():
    config = _collection()
    del config["qid_seed"]

    with pytest.raises(ValueError, match="qid_seed"):
        _validate_dataset_config("c", config, COLLECTION_DEFAULTS)


def test_collection_must_not_also_set_single_file_parameters():
    config = _collection(parameters={"sources": SOURCES, "source_url": "https://example.com/x.tsv", "mariadb_table": "t"})

    with pytest.raises(ValueError, match="Dataset 'c'.*mariadb_table, source_url"):
        _validate_dataset_config("c", config, COLLECTION_DEFAULTS)


def test_collection_sources_are_validated_with_the_dataset_key():
    bad = [{**SOURCES[0], "mariadb_table": ""}]

    with pytest.raises(ValueError, match="Dataset 'c': Source #1 is missing required key.*mariadb_table"):
        _validate_dataset_config("c", _collection(parameters={"sources": bad}), COLLECTION_DEFAULTS)


def test_collection_needs_the_shared_lakefs_and_mariadb_parameters():
    defaults = {k: v for k, v in COLLECTION_DEFAULTS.items() if k != "mariadb_database"}

    with pytest.raises(ValueError, match="mariadb_database"):
        _validate_dataset_config("c", _collection(), defaults)


def test_dataset_url_is_only_for_collections():
    with pytest.raises(ValueError, match="dataset_url.*sources"):
        _validate_dataset_config("x", _entry(dataset_url="https://example.com/repo"), COLLECTION_DEFAULTS)


def _deploy_flow(parameters):
    with patch.object(deploy_registry, "run_dataset") as single, patch.object(deploy_registry, "run_dataset_collection") as collection, \
            patch.dict("os.environ", {}):
        _deploy_dataset("download__x", parameters, "http://prefect.example/api", None)
    return single, collection


def test_deploy_uses_the_collection_flow_when_there_are_sources():
    single, collection = _deploy_flow({"sources": SOURCES})

    collection.from_source.assert_called_once()
    assert collection.from_source.call_args.kwargs["entrypoint"] == "flow/collection_flow.py:run_dataset_collection"
    single.from_source.assert_not_called()


def test_deploy_uses_the_single_file_flow_otherwise():
    single, collection = _deploy_flow({"source_url": "https://example.com/x.tsv"})

    single.from_source.assert_called_once()
    assert single.from_source.call_args.kwargs["entrypoint"] == "flow/dataset_flow.py:run_dataset"
    collection.from_source.assert_not_called()


def test_the_vaccination_entry_in_the_registry_is_a_valid_collection():
    defaults, datasets = _load_registry()

    _, parameters, _ = _validate_dataset_config("kvis_vaccination_rates", datasets["kvis_vaccination_rates"], defaults)

    assert [s["mariadb_table"] for s in parameters["sources"]] == ["kvis_vaccination_rates_adults", "kvis_vaccination_rates_children"]
    assert parameters["qid_seed"] == "kvis_vaccination_rates"  # keeps the QID of the existing processed dataset
    assert parameters["lakefs_repo"] == "data-raw" and parameters["lakefs_processed_repo"] == "data-processed"
