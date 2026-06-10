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
