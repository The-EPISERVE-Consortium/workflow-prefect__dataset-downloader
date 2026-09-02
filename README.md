# workflow-prefect__dataset-downloader

Prefect 3 workflow that downloads epidemiological datasets from public sources (RKI GitHub repos, Open-Meteo API), publishes them to lakeFS with FDO metadata sidecars, converts them to Parquet in `data-processed`, and loads them into MariaDB. Dataset-specific parameters live in `deploy/datasets.yaml`.

## Structure

| Path | Purpose |
|---|---|
| `flow/dataset_flow.py` | Generic Prefect flow orchestration |
| `tasks/*.py` | Individual Prefect tasks |
| `deploy/__main__.py` | Package entrypoint for `python -m deploy` |
| `deploy/deployer.py` | Argument parsing for deployment commands |
| `deploy/deploy_registry.py` | YAML-backed Prefect deployment helper |
| `deploy/datasets.yaml` | Dataset-specific deployment parameters |
| `tests/` | Unit tests (pytest) |
| `Dockerfile` | Image based on `prefecthq/prefect:3.7.1-python3.11` |

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

## Configured datasets

All sources are public RKI GitHub repos or the Open-Meteo API. Each dataset gets a deployment named `download__<key>`.

| Key | Source | lakeFS path |
|---|---|---|
| `grippeweb` | RKI GrippeWeb | `incidence/influenza/RKI__grippeweb.tsv` |
| `influenza_cases_germany` | RKI Influenzafälle | `incidence/influenza/RKI__influenza_cases_germany.tsv` |
| `corona_incidence_germany` | RKI COVID-19 7-Tage-Inzidenz | `incidence/covid/RKI__covid_germany.csv` |
| `corona_incidence_states` | RKI COVID-19 7-Tage-Inzidenz Bundesländer | `incidence/covid/RKI__covid_states.csv` |
| `rsv_cases_germany` | RKI RSV-Fälle | `incidence/rsv/RKI__rsv_cases_germany.tsv` |
| `are_consultation_incidence` | RKI ARE-Konsultationsinzidenz | `incidence/respiratory/RKI__are_consultation_incidence.tsv` |
| `wastewater_surveillance_aggregate` | RKI AMELAG aggregiert | `wastewater/RKI__wastewater_surveillance_aggregate.tsv` |
| `wastewater_surveillance_stations` | RKI AMELAG Einzelstandorte | `wastewater/RKI__wastewater_surveillance_stations.tsv` |
| `notaufnahme_surveillance` | RKI Notaufnahmesurveillance | `surveillance/emergency_dept/RKI__notaufnahme_surveillance.tsv` |
| `sari_hospitalization_incidence` | RKI SARI-Hospitalisierungsinzidenz | `surveillance/hospital/RKI__sari_hospitalization_incidence.tsv` |
| `covid_hospitalizations` | RKI COVID-19-Hospitalisierungen | `surveillance/hospital/RKI__covid_hospitalizations.csv` |
| `weather_berlin_daily` | Open-Meteo Berlin (daily) | `climate/temperature/open-meteo__weather_berlin_daily.csv` |
| `weather_berlin_hourly` | Open-Meteo Berlin (hourly) | `climate/temperature/open-meteo__weather_berlin_hourly.csv` |

## CI

Pushing to `main` builds and pushes the image to GHCR, then runs the test suite inside the built image.
