"""Shared deployment helper for YAML-backed dataset deployments."""

import os
from pathlib import Path
from json import JSONDecodeError

from prefect.client.schemas.schedules import CronSchedule
from prefect.runner.storage import GitRepository
import yaml

from flow.dataset_flow import run_dataset

GITHUB_REPO_URL = "https://github.com/The-EPISERVE-Consortium/workflow-prefect__dataset-downloader"
DOCKER_IMAGE = "ghcr.io/the-episerve-consortium/workflow-prefect__dataset-downloader:latest"
WORK_POOL_NAME = "kubernetes-pool"
REGISTRY_PATH = Path(__file__).with_name("datasets.yaml")
REQUIRED_PARAMETERS = {
    "source_url",
    "lakefs_repo",
    "lakefs_processed_repo",
    "lakefs_branch",
    "lakefs_object_path",
    "lakefs_commit_message",
    "mariadb_table",
    "mariadb_database",
}


def _require_prefect_api_url() -> str:
    prefect_api_url = os.environ.get("PREFECT_API_URL")
    if not prefect_api_url:
        raise EnvironmentError(
            "PREFECT_API_URL environment variable is not set. "
            "Export it before running this script, e.g.:\n"
            "  export PREFECT_API_URL=https://<your-prefect-server>/api"
        )
    return prefect_api_url


def _load_registry() -> tuple[dict[str, str], dict[str, dict]]:
    with REGISTRY_PATH.open(encoding="utf-8") as infile:
        data = yaml.safe_load(infile) or {}

    defaults = data.get("defaults", {})
    datasets = data.get("datasets")
    if not isinstance(defaults, dict):
        raise ValueError("deploy/datasets.yaml 'defaults' must be a mapping when present.")
    if not isinstance(datasets, dict) or not datasets:
        raise ValueError("deploy/datasets.yaml must contain a non-empty top-level 'datasets' mapping.")
    return defaults, datasets


def get_dataset_keys() -> list[str]:
    _, datasets = _load_registry()
    return sorted(datasets.keys())


def _validate_dataset_config(
    dataset_key: str,
    config: dict,
    defaults: dict[str, str],
) -> tuple[str, dict[str, str], bool]:
    if not isinstance(config, dict):
        raise ValueError(f"Dataset '{dataset_key}' must be a mapping in deploy/datasets.yaml.")

    deployment_name = config.get("deployment_name")
    parameters = config.get("parameters")
    if not deployment_name:
        raise ValueError(f"Dataset '{dataset_key}' is missing 'deployment_name'.")
    if not isinstance(parameters, dict):
        raise ValueError(f"Dataset '{dataset_key}' must define a 'parameters' mapping.")

    merged_parameters = {**defaults, **parameters}
    merged_parameters.setdefault("dataset_name", dataset_key)
    run_daily = merged_parameters.pop("run_daily", True)
    missing = sorted(REQUIRED_PARAMETERS.difference(merged_parameters))
    if missing:
        missing_list = ", ".join(missing)
        raise ValueError(f"Dataset '{dataset_key}' is missing required parameter(s): {missing_list}.")

    return deployment_name, merged_parameters, bool(run_daily)


def _deploy_dataset(deployment_name: str, parameters: dict[str, str], prefect_api_url: str, run_daily: bool) -> None:
    """Deploy one dataset configuration to the shared work pool."""
    os.environ["PREFECT_API_URL"] = prefect_api_url

    schedule_kwargs = {"schedules": [CronSchedule(cron="0 1 * * *", timezone="Europe/Berlin")]} if run_daily else {}

    try:
        run_dataset.from_source(
            source=GitRepository(url=GITHUB_REPO_URL, branch="main"),
            entrypoint="flow/dataset_flow.py:run_dataset",
        ).deploy(
            name=deployment_name,
            work_pool_name=WORK_POOL_NAME,
            parameters=parameters,
            job_variables={
                "image": DOCKER_IMAGE,
                "image_pull_policy": "Always",
            },
            **schedule_kwargs,
        )
    except JSONDecodeError as exc:
        raise RuntimeError(
            "PREFECT_API_URL does not appear to point to a Prefect API endpoint. "
            f"Got a non-JSON response from {prefect_api_url!r}. "
            "Use the Prefect API URL, for example: "
            "'PREFECT_API_URL=https://prefect.medicalbioinformatics.de/api python -m deploy grippeweb'."
        ) from exc


def deploy_from_registry(dataset_key: str | None = None) -> None:
    """Deploy one named dataset or all enabled datasets from deploy/datasets.yaml."""
    defaults, datasets = _load_registry()
    prefect_api_url = _require_prefect_api_url()

    if dataset_key:
        config = datasets.get(dataset_key)
        if config is None:
            available = ", ".join(sorted(datasets))
            raise ValueError(f"Unknown dataset '{dataset_key}'. Available datasets: {available}")
        deployment_name, parameters, run_daily = _validate_dataset_config(dataset_key, config, defaults)
        _deploy_dataset(deployment_name, parameters, prefect_api_url, run_daily)
        return

    for key in sorted(datasets):
        config = datasets[key]
        if not config.get("enabled", True):
            continue
        deployment_name, parameters, run_daily = _validate_dataset_config(key, config, defaults)
        _deploy_dataset(deployment_name, parameters, prefect_api_url, run_daily)
