# 17lands Data Project

> **For AI**: This README provides project context. Reference it with `@README.md` when you need full context, or rely on `.cursor/rules/` for always-on guidance.

## Project Overview

This repository is for analyzing [17lands](https://17lands.com/) drafting data—card performance, pick orders, and format insights for Magic: The Gathering Arena Limited formats.

**Data source**: Use the [17lands public datasets](https://www.17lands.com/public_datasets) only. Do not scrape their API or site.

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt
pip install dbt-duckdb  # for dbt models

# Copy config if needed (datasets.yaml is gitignored)
cp config/datasets.yaml.example config/datasets.yaml

# 1. Ingest raw data (creates raw.* tables in DuckDB)
python -m scripts.ingest.cli --helpers   # helpers + card lists → raw_helpers, raw_card_data
python -m scripts.ingest.cli --set MKM   # draft/game/replay → raw.draft, raw.game, raw.replay

# 2. Build staging models (dbt)
dbt deps   # install dbt-duckdb
DBT_PROFILES_DIR=. dbt run  # or copy profiles.yml to ~/.dbt/

# Run jobs (uses config/jobs.yaml)
python scripts/run_jobs.py ingest_helpers
python scripts/run_jobs.py ingest_set_mkm
```

## Directory Structure

```
17_lands_data/
├── config/         # datasets.yaml, jobs.yaml (see config/*.example)
├── data/           # Raw data + DB (gitignored; see data/.gitkeep)
│   ├── raw/        # Downloaded .gz files (helpers, card_lists, datasets)
│   └── db/         # 17lands.duckdb
├── models/         # dbt models: staging, intermediate, marts
├── macros/         # dbt macros (e.g. empty_to_null)
├── docs/           # Implementation plan, architecture
├── scripts/        # Ingest, jobs, analysis
│   ├── ingest/     # ingest --helpers | --set X | --file URL | --all
│   └── jobs/       # Job scripts for run_jobs.py
├── notebooks/      # Exploratory analysis
└── output/         # Generated reports, charts, tables
```

## Data Layers

| Layer | Schema | Contents |
|-------|--------|----------|
| **raw** | raw, raw_helpers, raw_card_data | Ingested data (unified draft/game/replay tables) |
| **staging** | staging | Cleaned views: empty→null, type casts |
| **intermediate** | intermediate | Joined, deduplicated (add as needed) |
| **marts** | marts | Business-ready aggregates (add as needed) |

See [docs/IMPLEMENTATION_PLAN.md](docs/IMPLEMENTATION_PLAN.md) for ingestion granularity (file/set/all), helper data, and scheduling.

## Data Conventions

- **Source**: [Public datasets](https://www.17lands.com/public_datasets) (CSV/JSON, gzip compressed). No scraping.
- **Types**: Game-level (per game), draft-level (per pick), replay data (per turn). Helper files and card-list mappings available.
- **Format**: UTF-8 CSV, preserve original column names (e.g., `avg_seen_at`, `win_rate`, `ever_drawn_win_rate`)
- **Storage**: Keep raw data immutable; write processed outputs to `output/`

## Docker

```bash
# Build and run interactively
docker compose build
docker compose run --rm app bash

# Or run commands directly
docker compose run --rm app python -m scripts.ingest.cli --helpers
docker compose run --rm app dbt run
docker compose run --rm app python scripts/run_jobs.py ingest_helpers
```

Data (`data/`), config, and output persist via volume mounts. See [docs/DOCKER.md](docs/DOCKER.md) for Docker storage and using a dedicated drive.

## Tech Stack

- **Python** (pandas, DuckDB, matplotlib/seaborn)
- **dbt-duckdb** for staging/intermediate/marts
- **Jupyter** for interactive exploration

## Rules for AI Assistants

1. **Data source**: Use public datasets only—never scrape 17lands
2. **Data integrity**: Never modify raw CSVs; create copies or write to `output/`
3. **Reproducibility**: Scripts should run top-to-bottom without manual edits
4. **Column names**: Use 17lands exact column names (snake_case); avoid renaming
5. **Format context**: When analyzing draft data, consider format, rank, and sample size

---

*Last updated: March 2025*
