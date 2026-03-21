# Sample files for testing

Drop **small** sample CSVs here to validate ingest and dbt logic.

## File size guidance

| Type | Full size | Use in sample |
|------|-----------|---------------|
| cards, abilities, dungeon | &lt;5 MB | Full files OK |
| draft, game, replay | **1–5+ GB each** | Use small extracts only |

**Draft/game/replay** files are several gigabytes each. Do **not** put full files in `sample/`—they bloat the repo and slow tests. Put full files in `data/raw/datasets/{expansion}/{format}/` and use the normal ingest (`--set EOE`). For sample testing, keep only a small extract (e.g. first 1000 rows).

## What to add

| File | Purpose |
|------|---------|
| `cards.csv` | Test `stg_cards` |
| `abilities.csv` | Test `stg_abilities` |
| `dungeon.csv` | Test `stg_dungeon` |
| `draft_data_public.*.csv` (small) | Test raw.draft ingest – use `head -n 1000` extract |
| `game_data_public.*.csv` (small) | Test raw.game ingest |
| `replay_data_public.*.csv` (small) | Test raw.replay ingest |

## Create a small extract from a large file

```powershell
# PowerShell - first 1000 lines (header + 999 rows)
Get-Content "path\to\draft_data_public.EOE.PremierDraft.csv" -TotalCount 1000 | Set-Content "data\sample\draft_data_public.EOE.PremierDraft.csv"
```

```bash
# Bash
head -n 1000 draft_data_public.EOE.PremierDraft.csv > data/sample/draft_data_public.EOE.PremierDraft.csv
```

## Where full files go

Full draft/game/replay files are ingested from `data/raw/datasets/{expansion}/{format}/`. Run `python -m scripts.ingest.cli --set EOE` to download and load them. The sample folder is only for small test extracts.

## How to test with samples

```bash
python -m scripts.ingest.cli --file "data/sample/cards.csv"
dbt run
```
