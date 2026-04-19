# Plan: Schema evolution, staging, and DS-friendly datasets

## Locked decisions

### 1. Variable card columns (pack / pool / game card matrix): **unpivot**, not nested super-types

For draft (and similarly game/replay where columns are `prefix_<card name>`), we will **not** rely on a single mega-`STRUCT`/`MAP` column as the primary handoff.

- **Preferred**: **long / tidy** facts in dbt (e.g. one row per `(draft_id, pick_number, card_name, role)` with flags or measures), built with **UNPIVOT**-style SQL or an equivalent reshape, then optional marts for matrices.
- **Rationale**: Clear grain for analytics and data science, simpler exports to Parquet/Python, and easier documentation than nested keys.

Super-structs remain optional for experiments only—not the canonical path.

### 2. Cross-set analysis: **out of scope** for v1

Per-set DuckDB files (or one active set at a time) align with **not** unioning incompatible wide schemas. Staging/marts target **one set per database** unless we later add explicit union-by-name for globals only.

---

## Problem (original)

17lands public CSVs are **not guaranteed to share the same columns** across Magic sets or over time. Examples:

- Draft rows include **pack / pool** columns whose **names** are literal **card titles**; they differ for every set (hundreds of columns).
- Game and replay files use the same “card name in the header” pattern for deck / hand / drawn / etc.
- Replay had **type** issues when auto-inference chose `BIGINT` for pipe-separated IDs (`all_varchar` mitigates).

Legacy ingest (`scripts/ingest/datasets.py`) used **one** physical table per kind (`raw.draft`, `raw.game`, `raw.replay`) with `CREATE ... AS SELECT *` then `INSERT ... SELECT *`, which **breaks** when a later file’s columns differ.

---

## Do we abandon SQL?

**No.** Keep **DuckDB + dbt** for curated tables; use **Python** for heavy exploratory reshapes or pipelines that write back to DuckDB/Parquet when needed.

---

## Strategy options (summary)

| Approach | Idea | Pros | Cons |
|----------|------|------|------|
| **A. Wide “union” table** | Superset of columns; `UNION ALL BY NAME` | Single table name | Many NULLs; still painful for DS |
| **B. Per-slice physical tables** | One table per file / set+format | Faithful; no collision | More objects |
| **C. Core + JSON payload** | Stable cols + VARIANT | Flexible | Harder to query |
| **D. All VARCHAR raw** | Like replay today | Fixes bad type inference | Does not fix **different names** |

**Unpivot** addresses the **card-named columns** problem for downstream use; **D** remains useful for ingest safety.

---

## Recommended direction

1. **Raw**: Per-set DB (or per-set load); faithful wide tables acceptable here.
2. **Staging**: `stg_*_core` for **stable** columns (see `macros/draft_stable.sql`); full wide `stg_draft` optional for set-specific work.
3. **Marts / facts**: **Unpivoted** long tables for pack/pool (draft) and analogous patterns for game/replay as needed.
4. **Manifest**: Script or checklist recording **CSV headers per file** when ingesting new sets.
5. **Renames**: Optional `config/column_mappings.yaml` if 17lands renames global columns.

---

## Public datasets: what exists vs what this repo pulls

The [17lands public datasets](https://www.17lands.com/public_datasets) page describes the offering. Two different things get confused here:

1. **Stable URLs you already put in `datasets.yaml`** (e.g. `cards.csv`, `abilities.csv`, draft/game/replay URL patterns) — these do **not** require the webpage to download. S3 serves them at fixed paths; **card data can be refreshed on a schedule** without scraping.

2. **Discovering brand-new files** 17lands might add (an extra helper CSV, a new card-list URL) — there is still **no** public S3 **bucket listing** (listing the prefix returns 403), so a robot cannot enumerate “everything on the bucket.” The site’s tables are also filled **in the browser (JavaScript)**, so a simple HTTP GET of the HTML does not show every row. **That** is why we still recommend an occasional **manual** check of the page when a set drops: add any **new** URLs to `helpers` / `card_lists` once.

### Skipping redundant downloads (no page scrape)

With `skip_download_if_unchanged: true` in `config/datasets.yaml`, **helpers** and **card_lists** use **HTTP HEAD** against the same URLs and compare **ETag** / **Last-Modified** to `data/.ingest_remote_state.json`. If nothing changed, the file is not re-downloaded (DuckDB still reloads from the existing path). This tracks **server** freshness, not the “Last Updated” cell on the website — which is what you want for automation.

Per-set ingest (`--set` / `--all`) uses the same ETag/`Last-Modified` state: unchanged files are not re-downloaded, and if the DuckDB slice already has rows, the heavy reload is skipped. Logs end with a line like `Ingest summary [set MKM]: ...` and `Ingest summary [all sets]: ...` for totals. Helpers/card lists can **always** full-GET via `always_refresh_helpers: true` while leaving set files on the smart-skip path.

### What 17lands documents (static content on the page)

| Category | Description |
|----------|-------------|
| **Draft data** | One row per pick; prior picks, match info, user WR, pack/pool card columns |
| **Game data** | One row per game; deck / opening hand / drawn; card-named columns |
| **Replay data** | One row per turn; draws, casts, combat, etc. |
| **Helper files** | “Useful in starting your analysis” — **table is dynamic** |
| **Card lists** | MTGA id → useful fields for replay — **table is dynamic** |

Formats described in prose align with **PremierDraft**, **TradDraft**, **TradSealed**, **Sealed** (Arena public drops; **Quick Draft** is often absent from public URLs—see `format_map` comments).

### What this repository is configured to ingest (`config/datasets.yaml.example`)

| Area | Coverage |
|------|----------|
| **Draft / game / replay** | Yes — `url_patterns` for `analysis_data/{draft,game,replay}_data/..._{expansion}.{format}.csv.gz` |
| **Formats** | `premier_draft`, `trad_draft`, `trad_sealed`, `sealed` → PascalCase |
| **Helper files** | `replay_dtypes.py`, `dungeon.csv` (extend list as the page adds rows) |
| **Card lists** | `abilities.csv`, `cards.csv` |

### Gaps and maintenance (not a one-time “missing ingest”)

1. **Dynamic tables on the website**  
   **Action:** On each quarterly set release (or when 17lands posts changelog), open the public datasets page and **diff** “Helper Files” and “Card Lists” against `helpers:` and `card_lists.urls` in `config/datasets.yaml`. Add any new URLs.

2. **`replay_dtypes.py`**  
   Downloaded to `data/raw/helpers/` but **not** loaded into DuckDB (non-CSV). It documents replay column types — **optional follow-up**: parse or reference in docs for ingest typing.

3. **JSON-only assets**  
   The site says data may be **CSV or JSON**. Ingest today assumes **gzip CSV**. If 17lands ever publishes **JSON-only** for a dataset, extend `scripts/ingest/datasets.py`.

4. **New Limited formats**  
   If the page adds a new format column (new Arena queue), add a key to `format_map` and ensure `url_patterns` still match the S3 path pattern.

5. **Global vs per-set card lists**  
   Current setup uses **global** `abilities.csv` / `cards.csv` (all MTGA ids). If the page ever lists **per-expansion** card files, add patterns under `card_lists` or `expansions` in config.

6. **Sealed / Trad Sealed**  
   Already in `formats` for expansions where configured; replay may be missing for some sealed products (17lands sometimes has game + replay gaps). Handle skips via existing ingest warnings.

---

## Implementation phases

1. **Inventory** — Headers per file; confirm stable core vs card columns.
2. **Ingest hardening** — Per-set DB path; safe loads (`all_varchar` where needed; no unsafe cross-set `INSERT SELECT *`).
3. **Staging** — Core models + explicit lists; document stable columns.
4. **Marts** — **Unpivot** facts for DS-friendly grain; export Parquet optional.
5. **Tests** — dbt tests on grain and keys.

---

## When to use Python vs SQL

| Task | SQL (DuckDB/dbt) | Python |
|------|------------------|--------|
| Joins, aggregates on known columns | Yes | Optional |
| Unpivot at scale in dbt | Yes | If clearer |
| Header diff / CI checks | Limited | Yes |
| Modeling / notebooks | Consume marts | Yes |

---

## References in this repo

- Ingest: `scripts/ingest/datasets.py`
- Draft stable columns: `macros/draft_stable.sql`, `models/staging/stg_draft_core.sql`
- Data rules: `.cursor/rules/17lands-project.mdc`, `docs/IMPLEMENTATION_PLAN.md`

---

## Readiness: brainstorm more, or implement?

**Enough design is settled** to start implementation:

- Unpivot (not super-structs) for variable card columns  
- Per-set isolation  
- dbt for curated datasets; Python optional downstream  
- Public data coverage matches the **documented** 17lands categories; **ongoing manual audit** for helper/card-list rows the page adds over time  

**Minor decisions to make during implementation** (not blockers):

- Exact **grain** for unpivoted draft facts (e.g. include `pool_` vs `pack_card_` as a `column_role` enum)  
- **Materialize** long tables as views vs tables (size vs refresh cost)  
- **Per-set DB** file naming and wiring `get_paths()` + `profiles.yml` / env var  

No further architecture brainstorm is **required** before coding; iterate on unpivot grain from a first `mart` prototype.

---

*Last updated: April 2026*
