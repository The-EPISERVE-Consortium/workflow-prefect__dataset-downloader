"""Unit tests for deploy/deploy_registry.py."""

from deploy.deploy_registry import _validate_dataset_config


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
