"""Shared deployment helper for YAML-backed dataset deployments."""

import inspect
import os
import re
from pathlib import Path
from json import JSONDecodeError
from urllib.parse import urlsplit

from prefect.client.schemas.schedules import CronSchedule
from prefect.runner.storage import GitRepository
import yaml

from flow.collection_flow import _validate_sources, run_dataset_collection
from flow.dataset_flow import run_dataset
from tasks.create_fdo_metadata import mint_qid

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
# A collection (a dataset of several source files, see flow/collection_flow.py) has a ``sources`` list
# instead of the single-file parameters; its files each name their own raw path and MariaDB table.
COLLECTION_REQUIRED_PARAMETERS = {
    "sources",
    "lakefs_repo",
    "lakefs_processed_repo",
    "lakefs_branch",
    "lakefs_commit_message",
    "mariadb_database",
    "qid_seed",
}
SINGLE_FILE_PARAMETERS = {"source_url", "lakefs_object_path", "mariadb_table"}


# --- Conventions of the registry, checked by check_registry() (run by the tests, so by CI on every pull request) ---

# What the catalog shows about a dataset; the flows treat these as optional, the registry does not.
REQUIRED_METADATA = ("display_name", "description", "license_id", "attribution")
# Keys an entry may have next to ``parameters``.
ENTRY_KEYS = {
    "enabled", "deployment_name", "parameters", "qid_seed", "dataset_url", "schedule", *REQUIRED_METADATA,
}
SOURCE_KEYS = {
    "name", "source_url", "lakefs_object_path", "mariadb_table", "display_name", "description",
    "source_delimiter", "source_skiprows",
}
# Categories of the catalog: the first part of ``lakefs_object_path``, and the second part where there is one.
# A new category is added here, in the pull request that needs it.
CATEGORIES = {
    "incidence": {"influenza", "covid", "rsv", "respiratory"},
    "surveillance": {"hospital", "emergency_dept", "respiratory"},
    "wastewater": set(),
    "vaccination": {"routine"},
    "climate": {"temperature"},
}
_KEY = re.compile(r"[a-z][a-z0-9_]*")
_OBJECT_FILE = re.compile(r"[A-Za-z0-9-]+__[a-z0-9_]+\.(csv|tsv)")  # PROVIDER__name.csv or .tsv


def _object_path_problem(path) -> str | None:
    """Why ``path`` is not a valid ``lakefs_object_path``: ``<category>[/<subcategory>]/<PROVIDER>__<name>.<csv|tsv>``."""
    if not isinstance(path, str):
        return f"lakefs_object_path must be a string, got {path!r}"
    parts = path.split("/")
    category = parts[0]
    if category not in CATEGORIES:
        return (f"lakefs_object_path {path!r}: unknown category {category!r} (known: {', '.join(sorted(CATEGORIES))}; "
                "add a new one to CATEGORIES in deploy/deploy_registry.py)")
    subcategories = CATEGORIES[category]
    expected_parts = 3 if subcategories else 2
    if len(parts) != expected_parts or (subcategories and parts[1] not in subcategories):
        shape = f"{category}/<{'|'.join(sorted(subcategories))}>/<file>" if subcategories else f"{category}/<file>"
        return f"lakefs_object_path {path!r} must look like {shape}"
    if not _OBJECT_FILE.fullmatch(parts[-1]):
        return f"lakefs_object_path {path!r}: the file name must look like PROVIDER__name.csv or PROVIDER__name.tsv"
    return None


def check_entry(dataset_key: str, config: dict, parameters: dict) -> list[str]:
    """The problems of one entry against the conventions of the registry (empty when there are none).

    Args:
        dataset_key: The entry's key in ``datasets:``.
        config: The entry as written.
        parameters: The entry's flow parameters after ``_validate_dataset_config`` merged the defaults.
    """
    problems = []
    if not _KEY.fullmatch(dataset_key):
        problems.append("the key must be lowercase letters, digits and underscores, starting with a letter")
    if config.get("deployment_name") != f"download__{dataset_key}":
        problems.append(f"deployment_name must be 'download__{dataset_key}', got {config.get('deployment_name')!r}")
    unknown = sorted(set(config) - ENTRY_KEYS)
    if unknown:
        problems.append(f"unknown key(s) next to 'parameters': {', '.join(unknown)} (known: {', '.join(sorted(ENTRY_KEYS))})")

    is_collection = "sources" in parameters
    flow = run_dataset_collection if is_collection else run_dataset
    unknown = sorted(set(parameters) - set(inspect.signature(flow.fn).parameters))
    if unknown:
        problems.append(f"unknown parameter(s): {', '.join(unknown)}")

    for field in REQUIRED_METADATA:
        value = parameters.get(field)
        if not isinstance(value, str) or not value.strip():
            problems.append(f"{field} is required: the catalog shows it")

    if is_collection or "qid_seed" in parameters or "qid_seed" in config:
        seed = parameters.get("qid_seed")
        if not isinstance(seed, str) or not seed.strip():
            problems.append("qid_seed must be a non-blank string")

    if is_collection:
        for index, source in enumerate(parameters["sources"], start=1):
            label = f"source #{index}"
            unknown = sorted(set(source) - SOURCE_KEYS)
            if unknown:
                problems.append(f"{label}: unknown key(s): {', '.join(unknown)}")
            problems += [f"{label}: {m}" for m in _source_problems(dataset_key, source, collection=True)]
    else:
        problems += _source_problems(dataset_key, parameters, collection=False)
    return problems


def _is_https_url(value: object) -> bool:
    if not isinstance(value, str) or any(char.isspace() for char in value):
        return False
    try:
        url = urlsplit(value)
        url.port  # Raises for malformed or out-of-range ports.
        return url.scheme == "https" and bool(url.hostname)
    except ValueError:
        return False


def _source_problems(dataset_key: str, source: dict, *, collection: bool) -> list[str]:
    problems = []
    url = source.get("source_url")
    if not _is_https_url(url):
        problems.append(f"source_url must be an https:// URL with a hostname, got {url!r}")
    problem = _object_path_problem(source.get("lakefs_object_path"))
    if problem:
        problems.append(problem)
    table = source.get("mariadb_table")
    if collection:
        if not (table == dataset_key or str(table).startswith(f"{dataset_key}_")):
            problems.append(f"mariadb_table must be '{dataset_key}' or start with '{dataset_key}_', got {table!r}")
    elif table != dataset_key:
        problems.append(f"mariadb_table must be the dataset key '{dataset_key}', got {table!r}")
    return problems


def _qids(parameters: dict) -> list[str]:
    """Every QID a dataset's files get: the dataset, and its raw file(s) (with the ``-raw`` suffix)."""
    seed = parameters.get("qid_seed")
    if seed is not None and (not isinstance(seed, str) or not seed.strip()):
        return []
    if "sources" in parameters:
        if seed is None:
            return []
        raw = [mint_qid(source["source_url"]) + "-raw" for source in parameters["sources"]]
        return [mint_qid(parameters["sources"][0]["source_url"], seed), *raw]
    source_url = parameters["source_url"]
    if not isinstance(source_url, str):
        return []
    qid = mint_qid(source_url, seed)
    return [qid, qid + "-raw"]


def check_registry(defaults: dict | None = None, datasets: dict | None = None) -> None:
    """Check every entry of ``deploy/datasets.yaml`` against the conventions and against each other.

    Runs ``_validate_dataset_config`` on each entry, then ``check_entry``, then looks for entries whose files
    would get the same QID (the file name of ``source_url`` is hashed; ``qid_seed`` overrides it).

    Args:
        defaults: The registry's defaults; the file's, when omitted.
        datasets: The registry's entries; the file's, when omitted.

    Raises:
        ValueError: With one line per problem, each naming the entry.
    """
    if datasets is None:
        defaults, datasets = _load_registry()
    _validated_registry(defaults or {}, datasets)


def _validated_registry(defaults: dict, datasets: dict) -> dict[str, tuple[str, dict, str | None]]:
    """Prepare deployment arguments after validating the entire registry."""
    problems = []
    owners: dict[str, str] = {}
    deployments = {}
    keys = []
    for key in datasets:
        if isinstance(key, str):
            keys.append(key)
        else:
            problems.append(f"Dataset {key!r}: the key must be a string")
    for key in sorted(keys):
        config = datasets[key]
        try:
            deployment = _validate_dataset_config(key, config, defaults)
            _, parameters, _ = deployment
            found = check_entry(key, config, parameters)
        except ValueError as exc:
            problems.append(str(exc))
            continue
        except (AttributeError, TypeError, KeyError, IndexError) as exc:  # a value of the wrong type
            problems.append(f"Dataset '{key}': malformed entry ({type(exc).__name__}: {exc})")
            continue
        problems += [f"Dataset '{key}': {m}" for m in found]
        deployments[key] = deployment
        try:
            qids = _qids(parameters)
        except UnicodeError:
            problems.append(f"Dataset '{key}': source_url or qid_seed contains invalid Unicode")
            continue
        for qid in qids:
            other = owners.setdefault(qid, key)
            if other != key:
                problems.append(
                    f"Datasets '{other}' and '{key}' would get the same QID {qid}: "
                    "set a different qid_seed on one of them"
                )
    if problems:
        raise ValueError("\n".join(problems))
    return deployments


def _require_prefect_api_url() -> str:
    """Return the configured Prefect API URL.

    Returns:
        Prefect API URL from the environment.

    Raises:
        EnvironmentError: If PREFECT_API_URL is not set.
    """
    prefect_api_url = os.environ.get("PREFECT_API_URL")
    if not prefect_api_url:
        raise EnvironmentError(
            "PREFECT_API_URL environment variable is not set. "
            "Export it before running this script, e.g.:\n"
            "  export PREFECT_API_URL=https://<your-prefect-server>/api"
        )
    return prefect_api_url


def _load_registry() -> tuple[dict[str, str], dict[str, dict]]:
    """Load defaults and dataset entries from the YAML registry.

    Returns:
        Tuple containing registry defaults and dataset configurations.

    Raises:
        ValueError: If the registry shape is invalid.
    """
    with REGISTRY_PATH.open(encoding="utf-8") as infile:
        data = yaml.safe_load(infile) or {}

    if not isinstance(data, dict):
        raise ValueError("deploy/datasets.yaml must contain a top-level mapping.")
    defaults = data.get("defaults", {})
    datasets = data.get("datasets")
    if not isinstance(defaults, dict):
        raise ValueError("deploy/datasets.yaml 'defaults' must be a mapping when present.")
    if not isinstance(datasets, dict) or not datasets:
        raise ValueError("deploy/datasets.yaml must contain a non-empty top-level 'datasets' mapping.")
    return defaults, datasets


def get_dataset_keys() -> list[str]:
    """Return all dataset keys defined in the registry.

    Returns:
        Sorted dataset keys from the YAML registry.
    """
    _, datasets = _load_registry()
    return sorted(datasets.keys())


SCHEDULE_TIMEZONE = "Europe/Berlin"


def _normalize_schedule(dataset_key: str, value) -> str | None:
    """Turn a registry ``schedule`` value into a validated cron string, or None for "no schedule".

    Args:
        dataset_key: Dataset key, for error messages.
        value: Value from the registry: a cron string, ``none`` (any case), an empty value or None.

    Returns:
        The cron string, or None when the dataset should not be scheduled.

    Raises:
        ValueError: If the value is not a string, or not a valid cron expression.
    """
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"Dataset '{dataset_key}': 'schedule' must be a cron string or 'none', got {value!r}.")
    value = value.strip()
    if value == "" or value.lower() == "none":
        return None
    try:
        CronSchedule(cron=value, timezone=SCHEDULE_TIMEZONE)
    except Exception as exc:  # prefect/pydantic raise their own validation errors for bad cron strings
        raise ValueError(f"Dataset '{dataset_key}': invalid cron expression in 'schedule': {value!r}.") from exc
    return value


def _validate_dataset_config(
    dataset_key: str,
    config: dict,
    defaults: dict[str, str],
) -> tuple[str, dict[str, str], str | None]:
    """Validate and normalize one dataset registry entry.

    Args:
        dataset_key: Dataset key from the YAML registry.
        config: Dataset-specific registry entry.
        defaults: Shared default parameters from the YAML registry.

    Returns:
        Deployment name, merged flow parameters, and the cron schedule string (None for no schedule).

    Raises:
        ValueError: If the dataset configuration is invalid.
    """
    if not isinstance(config, dict):
        raise ValueError(f"Dataset '{dataset_key}' must be a mapping in deploy/datasets.yaml.")

    deployment_name = config.get("deployment_name")
    parameters = config.get("parameters")
    if not deployment_name:
        raise ValueError(f"Dataset '{dataset_key}' is missing 'deployment_name'.")
    if not isinstance(parameters, dict):
        raise ValueError(f"Dataset '{dataset_key}' must define a 'parameters' mapping.")

    merged_parameters = {**defaults, **parameters}
    if "display_name" not in merged_parameters and config.get("display_name") is not None:
        merged_parameters["display_name"] = config["display_name"]
    if "description" not in merged_parameters and config.get("description") is not None:
        merged_parameters["description"] = config["description"]
    if "license_id" not in merged_parameters and config.get("license_id") is not None:
        merged_parameters["license_id"] = config["license_id"]
    if "attribution" not in merged_parameters and config.get("attribution") is not None:
        merged_parameters["attribution"] = config["attribution"]
    if "qid_seed" not in merged_parameters and config.get("qid_seed") is not None:
        merged_parameters["qid_seed"] = config["qid_seed"]
    if "dataset_url" not in merged_parameters and config.get("dataset_url") is not None:
        merged_parameters["dataset_url"] = config["dataset_url"]
    merged_parameters.setdefault("dataset_name", dataset_key)
    if "run_daily" in merged_parameters:
        raise ValueError(
            f"Dataset '{dataset_key}': 'run_daily' was replaced by 'schedule' "
            "(a cron string such as \"0 1 * * *\", or 'none' for no schedule)."
        )
    # Precedence: parameters > dataset-level key > registry defaults. No value at all means no schedule.
    merged_parameters.pop("schedule", None)
    if "schedule" in parameters:
        raw_schedule = parameters["schedule"]
    elif "schedule" in config:
        raw_schedule = config["schedule"]
    else:
        raw_schedule = defaults.get("schedule")
    schedule = _normalize_schedule(dataset_key, raw_schedule)
    is_collection = "sources" in merged_parameters
    if is_collection:
        clash = sorted(SINGLE_FILE_PARAMETERS.intersection(merged_parameters))
        if clash:
            raise ValueError(
                f"Dataset '{dataset_key}' has 'sources', so it must not also set {', '.join(clash)} "
                "(each source names its own)."
            )
        try:
            _validate_sources(merged_parameters["sources"])
        except ValueError as exc:
            raise ValueError(f"Dataset '{dataset_key}': {exc}") from exc
    elif "dataset_url" in merged_parameters:
        raise ValueError(f"Dataset '{dataset_key}': 'dataset_url' is only supported for datasets with 'sources'.")
    required = COLLECTION_REQUIRED_PARAMETERS if is_collection else REQUIRED_PARAMETERS
    missing = sorted(required.difference(merged_parameters))
    if missing:
        missing_list = ", ".join(missing)
        raise ValueError(f"Dataset '{dataset_key}' is missing required parameter(s): {missing_list}.")

    return deployment_name, merged_parameters, schedule


def _deploy_dataset(deployment_name: str, parameters: dict[str, str], prefect_api_url: str, schedule: str | None) -> None:
    """Deploy one dataset configuration to the shared work pool.

    Args:
        deployment_name: Prefect deployment name.
        parameters: Flow parameters for the dataset deployment.
        prefect_api_url: Prefect API URL used by the deployment client.
        schedule: Cron expression (Europe/Berlin) to schedule the deployment with, or None for no schedule.
            Note: None does not remove a schedule an already deployed dataset has.

    Raises:
        RuntimeError: If the configured Prefect API URL returns a non-JSON response.
    """
    os.environ["PREFECT_API_URL"] = prefect_api_url

    schedule_kwargs = {"schedules": [CronSchedule(cron=schedule, timezone=SCHEDULE_TIMEZONE)]} if schedule else {}

    try:
        if "sources" in parameters:
            flow, entrypoint = run_dataset_collection, "flow/collection_flow.py:run_dataset_collection"
        else:
            flow, entrypoint = run_dataset, "flow/dataset_flow.py:run_dataset"
        flow.from_source(
            source=GitRepository(url=GITHUB_REPO_URL, branch="main"),
            entrypoint=entrypoint,
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
    """Deploy one named dataset or all enabled datasets from deploy/datasets.yaml.

    Args:
        dataset_key: Optional key for a single dataset deployment.

    Raises:
        EnvironmentError: If PREFECT_API_URL is not set.
        ValueError: If the requested dataset is unknown or invalid.
        RuntimeError: If the Prefect API URL is not a valid API endpoint.
    """
    defaults, datasets = _load_registry()
    deployments = _validated_registry(defaults, datasets)
    if dataset_key and dataset_key not in deployments:
        available = ", ".join(deployments)
        raise ValueError(f"Unknown dataset '{dataset_key}'. Available datasets: {available}")
    selected = [deployments[dataset_key]] if dataset_key else [
        deployment for key, deployment in deployments.items() if datasets[key].get("enabled", True)
    ]
    prefect_api_url = _require_prefect_api_url()
    for deployment_name, parameters, schedule in selected:
        _deploy_dataset(deployment_name, parameters, prefect_api_url, schedule)
