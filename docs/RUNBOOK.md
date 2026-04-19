# Repository runbook

This document describes **what to run**, **when**, and **in what order** for the 17lands data pipeline: helper/card ingest, per-set CSV ingest, and dbt transforms.

Data comes from [17lands public datasets](https://www.17lands.com/public_datasets) only (CSV/JSON, gzip). Do not scrape the API or site.

---

## 1. One-time setup

1. **Python environment**  
   Install dependencies your project uses (e.g. `duckdb`, and any packages listed in `requirements.txt` or your env manager). The ingest CLI and `scripts/db_snapshot.py` need `duckdb`.

2. **Configuration**  
   If you do not already have `config/datasets.yaml`, copy `config/datasets.yaml.example` to `config/datasets.yaml` and edit it. The `.example` file is a **commented template** (defaults like `use_per_set_database: false`); your real file is allowed to differ—same **keys** (`s3_base`, `expansions`, `url_patterns`, etc.), different **values** (e.g. you might use `use_per_set_database: true` and uncomment `ingest_data_types`). They will not look identical, and that is expected.

3. **dbt packages** (if you use dbt packages; this project may have none)  
   From the repo root: `dbt deps --profiles-dir .`

---

## 2. Recommended order of operations

| Step | Command / action | Required? |
|------|------------------|-----------|
| A | `python -m scripts.ingest.cli --helpers` | **Strongly recommended** before analyzing draft/game/replay. Loads small helper files and card lists into DuckDB. |
| B | `python -m scripts.ingest.cli --set MKM` (or `--all`) | **Required** for set-level draft/game/replay CSVs. Replace `MKM` with a code from `config/datasets.yaml`. Optional: `--format premier_draft` to limit one format. |
| C | Point dbt at the same database | Set `DUCKDB_PATH` to match the DB the ingest wrote (see below). |
| D | `dbt run --profiles-dir .` | **Required** to build staging (and optional marts). |

**Per-set databases:** If `use_per_set_database: true` in `datasets.yaml`, helpers targeted at a set use that set’s file, e.g.:

```text
python -m scripts.ingest.cli --helpers --set MKM
python -m scripts.ingest.cli --set MKM
```

Then for dbt:

```powershell
$env:DUCKDB_PATH = "data/db/MKM.duckdb"
dbt run --profiles-dir .
```

**Single shared database:** If `use_per_set_database: false`, the default path is `data/db/17lands.duckdb` (override with `DUCKDB_PATH` if you moved it).

### 2.1 Pipeline helper (`python -m scripts.pipeline`)

One entry point for ingest-only, dbt-only, both, and lightweight checks:

| Command | What it does |
|---------|----------------|
| `python -m scripts.pipeline ingest …` | Delegates to `python -m scripts.ingest.cli` (same flags, including `--config`). |
| `python -m scripts.pipeline dbt --set MKM` | Runs `dbt run` with `DUCKDB_PATH` pointing at that set’s file. |
| `python -m scripts.pipeline dbt --all` | Runs `dbt run` once per configured database (skips missing `.duckdb` files). |
| `python -m scripts.pipeline full --set MKM` | Ingest that set, then `dbt run` on that DB. |
| `python -m scripts.pipeline full --all` | Ingest all expansions from config, then `dbt run` on each DB that exists. |
| `python -m scripts.pipeline verify --set MKM` | Read-only: confirms `raw.draft` / `raw.game` have rows; lists `main_staging` if dbt has been run. |
| `python -m scripts.pipeline verify --all` | Same check for every configured per-set DB (or the single shared DB). |
| `python -m scripts.pipeline parse` | Runs `dbt parse` (fast project validation; needs at least one existing `.duckdb` for `DUCKDB_PATH`). |

`verify` does **not** download data—use it to confirm a DB after ingest or `full`. Add `--require-staging` if staging views must exist (fails until `dbt run` has succeeded). **Avoid** `ingest --all` or `full --all` as a casual test; they pull a lot of data and disk.

---

## 3. What you can run (optional / advanced)

| Action | Command | Notes |
|--------|---------|--------|
| Single remote or local file | `python -m scripts.ingest.cli --file <URL_OR_PATH>` | Uses config for DB path and loading rules. |
| All expansions in config | `python -m scripts.ingest.cli --all` | Can take a long time and use a lot of disk. |
| Skip replay | Set `ingest_data_types: [draft, game]` in `datasets.yaml` | Saves time and space; dbt replay model stays off unless you enable it (see §5). |
| Re-download control | `skip_download_if_unchanged`, `always_refresh_helpers` in `datasets.yaml` | See comments in `datasets.yaml.example`. |

### 3.1 Full re-download (no ETag skip)

With **`skip_download_if_unchanged: true`** (typical), ingest **skips** HTTP GETs when the server reports the file unchanged — fast repeat runs.

To **force** re-downloads for that run (slower, full network traffic):

- **`python -m scripts.ingest.cli --set CODE --fresh`** (or **`--helpers --fresh`**, **`--all --fresh`**).
- **`python -m scripts.pipeline ingest … --fresh`** or **`full … --fresh`** (passes through to ingest).

### 3.2 Drop the DuckDB file and rebuild (no leftover views)

Embedded DuckDB keeps old **views/tables** in the `.duckdb` file until replaced. To **delete the database file** and start clean (close IDE connections to that file first):

- **`python -m scripts.pipeline reset --set CODE`** — removes `data/db/<CODE>.duckdb` when `use_per_set_database: true`, or the single `duckdb_path` / default file when `false`.
- **`python -m scripts.pipeline reset --all`** — every configured per-set DB, or the single shared DB once.
- **`python -m scripts.pipeline full --set CODE --reset-db`** — **delete file → ingest → dbt** in one go.

Then run **`ingest` + `dbt`** (or `full`) as usual; you get a new file with only what the pipeline creates.

---

## 4. dbt: profile, Windows lockfiles, and variables

### 4.1 Profile

`profiles.yml` reads the DuckDB path from:

```text
DUCKDB_PATH   (recommended whenever you use per-set DB files: data/db/<SET>.duckdb)

If unset, dbt falls back to data/db/17lands.duckdb — that path is for **single shared DB** mode only.
With use_per_set_database: true, set DUCKDB_PATH to the set you are building (e.g. data/db/MKM.duckdb).
```

Run dbt from the **project root** so paths resolve.

### 4.2 Windows: “database file in use”

If another process (IDE, DuckDB UI, antivirus) holds `*.duckdb` open, `dbt run` may fail. Workaround: copy the file and point dbt at the copy:

```powershell
Copy-Item data\db\MKM.duckdb data\db\_scratch_dbt.duckdb
$env:DUCKDB_PATH = "data/db/_scratch_dbt.duckdb"
dbt run --profiles-dir .
```

### 4.3 dbt variables (`dbt_project.yml`)

| Variable | Default | Meaning |
|----------|---------|---------|
| `has_replay` | `false` | Set `true` when `raw.replay` exists (after ingesting replay). Enables `stg_replay`. |
| `build_pack_pool_mart` | `false` | When `true`, builds `mart_draft_pack_pool_long` (UNPIVOT of pack/pool columns — can be **very** large). |

Examples:

```text
dbt run --profiles-dir . --vars '{"has_replay": true}'
dbt run --profiles-dir . --vars '{"build_pack_pool_mart": true}'
```

---

## 5. dbt layers and models (this project)

**Sources (ingest):** `raw.*`, `raw_card_data.*`, `raw_helpers.*` — defined in `models/sources.yml` (or equivalent).

**Staging** (`models/staging/`, schema `main_staging` in DuckDB): materialized **tables** over sources, normalized naming and light transforms.

**Marts** (`models/marts/`): optional; currently `mart_draft_pack_pool_long` is **disabled by default** (see `build_pack_pool_mart`).

**Intermediate:** no `models/intermediate/` layer in this repo yet (add when needed).

**Active models** (when defaults apply — no replay, no pack-pool mart):  
`stg_abilities`, `stg_cards`, `stg_draft`, `stg_draft_core`, `stg_dungeon`, `stg_game`.

**Dependency idea:** ingest fills **raw** → dbt builds **staging** (and optional mart). There is no separate “intermediate” layer in the repo today.

---

## 6. Inspecting the database

**Row counts per table:**

```text
python scripts/db_snapshot.py data/db/MKM.duckdb
```

Omit the path to use the default from `config/datasets.yaml` (per-set: **first** `expansions[]` entry — prepend new sets at the top — or `default_expansion` if set); otherwise `data/db/17lands.duckdb`.

**Ad hoc SQL:** Use DuckDB CLI, DBeaver, or `python -c` with `duckdb.connect(...)`.

---

## 7. MKM example: table sizes after ingest + dbt (one discrete set)

The following was produced with a **scratch copy** of an MKM database after a successful `dbt run` (defaults: no `stg_replay`, no `mart_draft_pack_pool_long`). Your counts will match if the same files are loaded.

| Schema.table | Rows | Layer |
|--------------|------|--------|
| `raw_helpers.dungeon` | 21 | ingest (helpers) |
| `raw_card_data.abilities` | 19,741 | ingest (card lists) |
| `raw_card_data.cards` | 24,409 | ingest (card lists) |
| `raw.draft` | 6,180,337 | ingest (set CSVs) |
| `raw.game` | 964,377 | ingest (set CSVs) |
| `main_staging.stg_dungeon` | 21 | dbt staging |
| `main_staging.stg_abilities` | 19,741 | dbt staging |
| `main_staging.stg_cards` | 24,409 | dbt staging |
| `main_staging.stg_draft` | 6,180,337 | dbt staging |
| `main_staging.stg_draft_core` | 6,180,337 | dbt staging |
| `main_staging.stg_game` | 964,377 | dbt staging |

Sample **raw** rows (narrow columns):

- `raw.game`: `(draft_id, game_number, won)` — e.g. `('8e1e9269…', 1, True)`.
- `raw.draft`: `(draft_id, pack_number, pick_number)` — e.g. `(same draft, 0, 0), (0, 1), (0, 2)`.

---

## 8. How long does “ingest everything” take?

There is **no single number**: it depends on how many expansions and formats you enable, whether you include **replay** (large), disk speed, and network. Ingesting **one set** with draft + game only is often on the order of **many minutes to an hour or more** for a popular set with large files. **`--all`** across the full public catalog can take **many hours to days** and require **very large** disk space. Use `ingest_data_types` and a small expansion list while developing.

---

## 9. Docker note

If `docker compose run … dbt` fails with permission errors on a mounted `data/db/*.duckdb` on Windows, prefer **running dbt on the host** with `DUCKDB_PATH`, or adjust volume permissions per your Docker setup.
