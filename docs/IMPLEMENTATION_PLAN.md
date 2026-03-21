# 17lands Data Project — Implementation Plan

## Scale & Storage

| Factor | Estimate | Implication |
|--------|----------|-------------|
| Files per set | 4 (draft, game, replay, + format variant or card list) | ~1.4GB+ per set compressed |
| Sets | 20–50+ (historical) | 30–70GB+ total compressed |
| Uncompressed | ~2–4× compressed | 60–280GB possible at full ingest |
| Database | After load + indexes | Add ~20–50% to uncompressed |

**Storage**: Local SSD recommended. Plan for 100–300GB at full scale. Never commit `data/` to git.

---

## Data Inventory (from 17lands public datasets page)

### Main datasets (per expansion × format)

- **Draft data** — one row per pick
- **Game data** — one row per game  
- **Replay data** — one row per turn (largest)
- **Format variants** — Premier Draft, Quick Draft, Traditional, etc. (may multiply file count)

### Helper data (ingest first)

| Type | Purpose | Ingest order |
|------|---------|--------------|
| **Helper files** | Format/rank mappings, column docs, schema metadata | Before main data |
| **Card lists** | MTGA ID → card name, etc. (per set) | Before replay data |

Helper and card-list data are small; ingest them first so replay data can be joined correctly.

---

## Ingestion Granularity

| Scope | CLI | Use case |
|-------|-----|----------|
| **Single file** | `ingest --file <url_or_path>` | One dataset, debugging |
| **Single set (all formats)** | `ingest --set MKM` | One expansion, all draft/game/replay |
| **Single set + format** | `ingest --set MKM --format premier_draft` | One expansion + format |
| **All files** | `ingest --all` | Full refresh |

**Recommended flow**:
1. `ingest --helpers` — helper files + card lists
2. `ingest --set MKM` — one set (or `--all` for everything)

---

## Directory Structure

```
17_lands_data/
├── config/
│   ├── datasets.yaml      # URLs/manifest: expansions, formats, helper files
│   └── jobs.yaml          # Scheduled job definitions
├── data/
│   ├── raw/               # Downloaded .gz (or decompressed)
│   │   ├── helpers/
│   │   ├── card_lists/
│   │   └── {expansion}/{format}/   # e.g. MKM/premier_draft/
│   ├── db/                # DuckDB/SQLite file(s)
│   └── .gitkeep
├── scripts/
│   ├── ingest/
│   │   ├── __init__.py
│   │   ├── helpers.py     # Ingest helper files
│   │   ├── card_lists.py  # Ingest card lists
│   │   ├── datasets.py    # Ingest draft/game/replay
│   │   └── cli.py         # Main entry: ingest --helpers | --set X | --all
│   ├── jobs/              # Job definitions (runnable modules)
│   │   ├── __init__.py
│   │   ├── ingest_all.py
│   │   ├── ingest_helpers.py
│   │   └── (future analytics jobs)
│   └── run_jobs.py        # Reads config/jobs.yaml, executes jobs
├── notebooks/
├── output/
└── docs/
```

---

## Helper Data Ingestion

### 1. Discover URLs

The 17lands page loads links dynamically. Options:

- **Manual**: Visit https://www.17lands.com/public_datasets, copy helper/card-list URLs into `config/datasets.yaml`
- **Stable paths**: If 17lands uses predictable URLs (e.g. `/data/helpers/...`), document the pattern in config

### 2. Raw layer schemas

| Type | Tables | Contents |
|------|--------|----------|
| **raw_helpers** | helper_dungeon, etc. | Helper CSVs (dungeon abilities, etc.) |
| **raw_card_data** | abilities, cards | MTGA ID → card/ability mappings |
| **raw** | draft, game, replay | Unified tables; filter by expansion, event_type |

### 3. Ingest order

```
1. ingest --helpers
   → Download helper files + card lists
   → Load CSVs into raw_helpers.*, raw_card_data.*

2. ingest --set MKM (or --all)
   → Download draft/game/replay for scope
   → Append to raw.draft, raw.game, raw.replay (delete-then-insert per expansion/format)
   → Replay rows can JOIN raw_card_data.cards on mtga_id

3. dbt run
   → Build staging.* from raw (empty_to_null macro, type casts)
```

---

## Config: datasets.yaml (draft)

```yaml
base_url: "https://www.17lands.com/..."   # or actual base

helpers:
  - name: formats
    url: "..."
    table: raw_helpers.helper_formats
  - name: ranks
    url: "..."
    table: raw_helpers.helper_ranks

card_lists:
  # Per-expansion or single file listing all
  url_pattern: "..."   # or explicit list

expansions:
  - code: MKM
    formats: [premier_draft, quick_draft, traditional_draft]
    draft_url: "..."
    game_url: "..."
    replay_url: "..."
  # ... more expansions
```

URLs must be filled in from the live page. Document in README how to add new sets.

---

## Config: jobs.yaml (scheduling)

```yaml
jobs:
  ingest_helpers:
    script: scripts/jobs/ingest_helpers.py
    description: "Download and load helper files + card lists"
    # schedule: "0 2 * * 0"   # Optional: cron (e.g. weekly Sunday 2am)

  ingest_all:
    script: scripts/jobs/ingest_all.py
    description: "Full ingest of all datasets"
    depends_on: [ingest_helpers]
    # schedule: "0 3 * * 0"

  ingest_set_mkm:
    script: scripts/jobs/ingest_set.py
    args: [--set, MKM]
    description: "Ingest MKM only"
    depends_on: [ingest_helpers]

  # Future
  # analytics_daily:
  #   script: scripts/jobs/analytics_daily.py
```

---

## Scheduling Framework

### Design

- **run_jobs.py** — CLI that reads `config/jobs.yaml` and runs one or more jobs
- **Usage**:
  - `python scripts/run_jobs.py ingest_helpers` — run one job
  - `python scripts/run_jobs.py ingest_helpers ingest_set_mkm` — run multiple (respects depends_on)
  - `python scripts/run_jobs.py --all` — run all (or all scheduled)
- **External scheduler** (e.g. Windows Task Scheduler, cron) invokes `run_jobs.py` with the desired job names
- **Optional**: Add cron parsing (e.g. with `schedule` or `APScheduler`) so `run_jobs.py --daemon` can run jobs on a schedule without external cron

### Adding new jobs later

1. Add a script under `scripts/jobs/`
2. Add an entry to `config/jobs.yaml`
3. Run via `run_jobs.py <job_name>` or schedule it

---

## Processing Strategy (large files)

| Step | Approach |
|------|----------|
| Download | Stream to disk (avoid loading into memory) |
| Decompress | Stream gunzip (Python `gzip` module) or temp file |
| Load | DuckDB `COPY FROM` or pandas `chunksize` → SQLite |
| Memory | Process one file at a time; chunk rows (e.g. 50k–100k) |

DuckDB can `INSERT INTO raw.x SELECT * FROM read_csv_auto('file.csv.gz')` with streaming—preferred for 360MB+ files.

---

## Execution Phases

### Phase 1: Foundation
1. Create `config/datasets.yaml` skeleton; populate with 1–2 known URLs from 17lands
2. Implement `ingest --helpers` (helper files + card lists)
3. Implement `ingest --file <path>` for a single dataset
4. Add `.gitignore` for `data/`

### Phase 2: Set & full ingest
5. Implement `ingest --set <expansion>`
6. Implement `ingest --all`
7. Populate full manifest in `config/datasets.yaml` (manual from 17lands page)

### Phase 3: Scheduling
8. Create `config/jobs.yaml` and `run_jobs.py`
9. Implement `ingest_helpers` and `ingest_all` job scripts
10. Document Task Scheduler / cron setup

### Phase 4: Extensibility
11. Add intermediate and mart layers (via dbt or scripts)
12. Add analytics jobs as new scripts + job entries

---

## Layer Naming (staging → intermediate → mart)

| Layer | Purpose |
|-------|---------|
| **staging** | Raw ingested data, minimal transforms (ingest scripts) |
| **intermediate** | Cleaned, joined, deduplicated (dbt or scripts) |
| **mart** | Business-ready, aggregated for analysis (dbt marts) |

---

## dbt for SQL Models

**Yes, dbt fits well** for building intermediate and mart models from staging. Use [dbt-duckdb](https://github.com/jwills/dbt-duckdb).

**Same repo? Yes.** Typical layout:

```
17_lands_data/
├── scripts/ingest/     # Python: load CSVs → DuckDB staging
├── dbt_project.yml
├── models/
│   ├── staging/        # Optional: thin wrappers over raw tables
│   ├── intermediate/   # Joins, cleans, deduplicates
│   └── marts/          # Win rates, pick orders, archetypes
├── data/db/17lands.duckdb   # Shared DB for ingest + dbt
```

**Flow**: 1) `python -m scripts.ingest.cli --helpers` then `--set MKM` → populates `staging.*`. 2) `dbt run` → builds `intermediate.*` and `marts.*` from staging.

---

## Open Questions

- Exact URL structure on 17lands (static vs dynamic)
- Whether card lists are per-set or one global file
- How often 17lands updates (weekly, per release, etc.)
- Whether to support incremental vs full replace per set

---

*Last updated: March 2025*
