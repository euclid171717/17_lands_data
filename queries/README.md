# SQL queries for exploring the database

These `.sql` files are **small, editor-friendly** introspection queries. They avoid `SELECT *` on `raw.draft` / `raw.game` (millions of columns × rows in the worst case).

## How to run them in this IDE (Cursor / VS Code)

### Option A — DuckDB extension (queries show up in the editor)

1. Install the **DuckDB** extension if you haven’t.
2. Point it at the **same file** you use for ingest/dbt:
   - With `use_per_set_database: true`, use `data/db/<SET>.duckdb` for the set you care about (e.g. `MKM.duckdb`).
   - With a single shared DB, use `data/db/17lands.duckdb`.
3. Settings live in `.vscode/settings.json` under `duckdb.databases[].path`. **Update that path** when you switch sets or DB files.
4. Open a `.sql` file from this folder, select a statement (or put the cursor in it), and run it with the extension’s **Run** command (often **Ctrl+Shift+Enter** or the command palette: “DuckDB: Execute …”).

You do **not** need `ATTACH` in each file if the extension already opened that database as the default.

### Option B — `scripts/run_query.py` (terminal, uses `datasets.yaml` + `DUCKDB_PATH`)

From the repo root:

```powershell
$env:DUCKDB_PATH = "data/db/MKM.duckdb"
python -m scripts.run_query queries/01_table_list.sql
```

If `DUCKDB_PATH` is unset, the script uses `config/datasets.yaml`: per-set DB path for `default_expansion` if set, else the **first** `expansions[]` code (prepend new sets at the top), else single-file `17lands.duckdb`.

### Option C — Row counts only (fast)

```powershell
python scripts/db_snapshot.py data/db/MKM.duckdb
```

## Suggested order

| File | Purpose |
|------|---------|
| `00_connection_check.sql` | Confirm you are connected to the expected DB |
| `01_table_list.sql` | All schemas and table names |
| `02_columns_all_tables.sql` | Column names and types for every table |
| `03_row_counts.sql` | Row counts for **ingest** tables (`raw.*`, helpers, card lists) |
| `04_row_counts_dbt.sql` | Row counts for **`main_staging`** (only after `dbt run`) |
| `05_describe_core_tables.sql` | `DESCRIBE` for each core table (layout without wide `SELECT *`) |
| `06_sample_rows_narrow.sql` | Readable samples (key columns only) |
| `07_sample_staging_narrow.sql` | Staging samples (requires `dbt run`) |

Other:

| File | Purpose |
|------|---------|
| `inspect_db.sql` | Short legacy list + ATTACH comment |
| `top100_all_tables.sql` | Deprecated stub (points you to the files above) |
