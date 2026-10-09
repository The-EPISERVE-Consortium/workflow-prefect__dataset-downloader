# workflow-prefect__dataset-downloader

Prefect 3 workflow that downloads epidemiological datasets from public sources (RKI GitHub repos, the WHO FluMart API, Open-Meteo API), publishes them to lakeFS with FDO metadata sidecars, converts them to Parquet in `data-processed`, and loads them into MariaDB. Most datasets are one source file; a dataset can also consist of several source files (see [Datasets with several source files](#datasets-with-several-source-files)). Dataset-specific parameters live in `deploy/datasets.yaml`.

## Structure

| Path | Purpose |
|---|---|
| `flow/dataset_flow.py` | Generic Prefect flow for a dataset of one source file (`run_dataset`) |
| `flow/collection_flow.py` | Prefect flow for a dataset of several source files (`run_dataset_collection`) |
| `tasks/*.py` | Individual Prefect tasks |
| `deploy/__main__.py` | Package entrypoint for `python -m deploy` |
| `deploy/deployer.py` | Argument parsing for deployment commands |
| `deploy/deploy_registry.py` | YAML-backed Prefect deployment helper |
| `deploy/datasets.yaml` | Dataset-specific deployment parameters |
| `tests/` | Unit tests (pytest) |
| `Dockerfile` | Image based on `prefecthq/prefect:3.7.1-python3.11`, pulled from the `mirror.gcr.io` mirror of Docker Hub (CI runners hit Docker Hub's anonymous pull limit) |

## Run locally

```bash
pip install -r requirements.txt
pytest tests/
```

Run the generic flow directly by supplying all required parameters from Python. The deployment registry in `deploy/datasets.yaml` shows the full parameter set for each dataset.

## Deploy

Deploy one dataset by key:

```bash
PREFECT_API_URL=https://<your-prefect-server>/api python -m deploy grippeweb
```

Deploy all enabled datasets from `deploy/datasets.yaml`:

```bash
PREFECT_API_URL=https://<your-prefect-server>/api python -m deploy --all
```

Add or change deployments by editing `deploy/datasets.yaml`. Dataset entries may include optional
`display_name` and `description` fields; if omitted, generated metadata uses the dataset key as the
display name and `Dataset <dataset_name>` as the description. Optional `license_id` (a CKAN licence
list value, e.g. `cc-by`) and `attribution` (a free-text credit line) are written into the FDO
profile and surface in CKAN as the dataset's licence and an `attribution` extra. Optional `qid_seed`
overrides the string hashed into the dataset's QID — needed only when the source URL filename is not
unique (the two Open-Meteo weather endpoints both resolve to `forecast`).

The schedule is set with `schedule`, a cron expression in Europe/Berlin time. `defaults:` sets it to
`"0 1 * * *"` (every night at 01:00), so datasets run nightly unless an entry overrides it, either as a
sibling of `deployment_name` or under `parameters:` (e.g. `schedule: "0 3 1 * *"` for the first of the
month). `schedule: none` (or an empty `schedule:`) deploys the dataset without a schedule, for manual runs; leaving the key out falls back to the default. The
former `run_daily` flag was replaced by `schedule`. Note that deploying with `none` does not remove a
schedule that an already deployed dataset has; that has to be done in Prefect.

## Datasets with several source files

A dataset can consist of several source files that are published as **one** dataset (one QID, one
CKAN entry with one resource per file), for example the adult and children files of the RKI
KV-Impfsurveillance. Such an entry has a `sources:` list under `parameters:` instead of the
single-file parameters `source_url`, `lakefs_object_path` and `mariadb_table`; mixing both is rejected.
The registry deploys it with `run_dataset_collection` (`flow/collection_flow.py`).

```yaml
  kvis_vaccination_rates:
    enabled: true
    display_name: Routine Vaccination Rates in Germany (KV-Impfsurveillance)
    description: ...
    license_id: cc-by
    attribution: "..."
    qid_seed: kvis_vaccination_rates                 # required: the QID of the combined dataset
    dataset_url: https://github.com/robert-koch-institut/...   # optional page of the dataset as a whole
    schedule: "0 3 * * 1"                            # weekly (Mondays 03:00)
    deployment_name: download__kvis_vaccination_rates
    parameters:
      sources:
        - name: kvis_vaccination_rates_adults        # required per source: name, source_url,
          source_url: https://raw.githubusercontent.com/.../KVIS_Impfquoten_Erwachsene.tsv
          lakefs_object_path: vaccination/routine/RKI__kvis_vaccination_rates_adults.tsv
          mariadb_table: kvis_vaccination_rates_adults    #   lakefs_object_path, mariadb_table
          display_name: ...                          # optional per source: display_name, description,
          description: ...                           #   source_delimiter, source_skiprows
        - name: kvis_vaccination_rates_children
          ...
```

What a run does, per source one after the other (so only one DataFrame is in memory): download,
commit to `data-raw` with its own raw FDO, load into its own MariaDB table (replace mode), and write
the Parquet file to a temporary directory. After the last source the Parquet files and **one**
processed FDO are uploaded to `data-processed` and committed together. Notes:

- The QID of each raw FDO comes from the source file name (as for single-file datasets), the QID of
  the combined processed FDO from `qid_seed`, which is therefore required. The processed FDO lists the
  raw FDOs in `prov:wasDerivedFrom` and takes the latest `source_changed_at` of the sources.
- There is no early exit when nothing changed: a run always downloads, reloads the MariaDB tables
  and rewrites the Parquet files, like `run_dataset` does. Use a `schedule` that matches how often
  the source changes. The sources are not processed atomically: if the second source fails, the first
  is already committed and loaded.
- Column types come from pandas, as for all datasets; MariaDB columns are `TEXT`.
- Files stored in Git LFS (see below) work with their normal `raw.githubusercontent.com` URL.

## Downloads and MariaDB writes

- `download_file` detects a Git LFS pointer (`raw.githubusercontent.com` returns only that for LFS
  files), fetches the real file from `media.githubusercontent.com`, and verifies size and sha256
  against the pointer. A pointer is never kept; unresolvable pointers raise an error.
- `store_to_mariadb` inserts 50,000 rows per `executemany` call, so memory does not grow with the table
  (a table of up to 50,000 rows is one call, as before). Existing tables keep all columns as `TEXT`.

## Configured datasets

All sources are public RKI GitHub repos, the WHO FluMart API or the Open-Meteo API. Each dataset gets a deployment named `download__<key>`. All run nightly at 01:00 except where the schedule column says otherwise.

| Key | Source | lakeFS path | Schedule |
|---|---|---|---|
| `grippeweb` | RKI GrippeWeb | `incidence/influenza/RKI__grippeweb.tsv` | nightly |
| `influenza_cases_germany` | RKI Influenzafälle | `incidence/influenza/RKI__influenza_cases_germany.tsv` | nightly |
| `corona_incidence_germany` | RKI COVID-19 7-Tage-Inzidenz | `incidence/covid/RKI__covid_germany.csv` | nightly |
| `corona_incidence_states` | RKI COVID-19 7-Tage-Inzidenz Bundesländer | `incidence/covid/RKI__covid_states.csv` | nightly |
| `rsv_cases_germany` | RKI RSV-Fälle | `incidence/rsv/RKI__rsv_cases_germany.tsv` | nightly |
| `are_consultation_incidence` | RKI ARE-Konsultationsinzidenz | `incidence/respiratory/RKI__are_consultation_incidence.tsv` | nightly |
| `wastewater_surveillance_aggregate` | RKI AMELAG aggregiert | `wastewater/RKI__wastewater_surveillance_aggregate.tsv` | nightly |
| `wastewater_surveillance_stations` | RKI AMELAG Einzelstandorte | `wastewater/RKI__wastewater_surveillance_stations.tsv` | nightly |
| `notaufnahme_surveillance` | RKI Notaufnahmesurveillance | `surveillance/emergency_dept/RKI__notaufnahme_surveillance.tsv` | nightly |
| `sari_hospitalization_incidence` | RKI SARI-Hospitalisierungsinzidenz | `surveillance/hospital/RKI__sari_hospitalization_incidence.tsv` | nightly |
| `covid_hospitalizations` | RKI COVID-19-Hospitalisierungen | `surveillance/hospital/RKI__covid_hospitalizations.csv` | nightly |
| `weather_berlin_daily` | Open-Meteo Berlin (daily) | `climate/temperature/open-meteo__weather_berlin_daily.csv` | nightly |
| `weather_berlin_hourly` | Open-Meteo Berlin (hourly) | `climate/temperature/open-meteo__weather_berlin_hourly.csv` | nightly |
| `who_flunet_germany` | WHO FluNet (FluMart API) | `incidence/influenza/WHO__flunet_germany.csv` | nightly |
| `who_fluid_germany` | WHO fluID (FluMart API) | `surveillance/respiratory/WHO__fluid_germany.csv` | nightly |
| `kvis_vaccination_rates` (two files, one dataset) | RKI KV-Impfsurveillance | `vaccination/routine/RKI__kvis_vaccination_rates_adults.tsv` and `..._children.tsv` | weekly, Mon 03:00 |

## CI

Pushing to `main` builds and pushes the image to GHCR, then runs the test suite inside the built image. The workflow can also be started by hand (`workflow_dispatch`: Actions tab, *Run workflow*).
