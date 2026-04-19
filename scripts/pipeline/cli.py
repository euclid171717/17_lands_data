"""
Orchestrate ingest + dbt across one or all per-set databases.

  python -m scripts.pipeline ingest --set MKM
  python -m scripts.pipeline ingest --all
  python -m scripts.pipeline dbt --set MKM
  python -m scripts.pipeline dbt --all
  python -m scripts.pipeline full --set MKM
  python -m scripts.pipeline full --all
  python -m scripts.pipeline verify --set MKM
  python -m scripts.pipeline verify --all
  python -m scripts.pipeline parse
  python -m scripts.pipeline parse --set LCI
  python -m scripts.pipeline reset --set MKM
  python -m scripts.pipeline full --set MKM --reset-db
  python -m scripts.pipeline full --set MKM --fresh

Subcommands accept ``--config path/to/datasets.yaml`` (same as ingest).

``ingest`` delegates to ``scripts.ingest.cli``. ``dbt`` / ``full`` run ``dbt run`` with
``DUCKDB_PATH`` set per database. ``verify`` only reads DuckDB (good smoke test without re-download).
``parse`` runs ``dbt parse`` using the first existing .duckdb it can resolve.
"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path

import duckdb

from scripts.ingest.config import get_paths, get_project_root, load_config


def _load_cfg(ns: argparse.Namespace):
    p = getattr(ns, "config", None)
    if p and isinstance(p, Path) and p.exists():
        return load_config(p)
    return load_config()


def _expansion_codes(cfg: dict) -> list[str]:
    return [e["code"] for e in cfg.get("expansions", []) if isinstance(e, dict) and e.get("code")]


def _db_paths_for_scope(cfg: dict, set_code: str | None, all_sets: bool) -> list[Path]:
    if all_sets:
        if cfg.get("use_per_set_database"):
            return [get_paths(cfg, code)["db"] for code in _expansion_codes(cfg)]
        return [get_paths(cfg)["db"]]
    if not set_code:
        raise SystemExit("Provide --set CODE or --all.")
    return [get_paths(cfg, set_code)["db"]]


def _db_paths_after_ingest(cfg: dict, ns: argparse.Namespace) -> list[Path]:
    """Same DB path resolution as cmd_full uses for dbt (after ingest)."""
    if ns.all:
        return _db_paths_for_scope(cfg, None, True)
    if ns.helpers:
        if ns.expansion:
            return _db_paths_for_scope(cfg, ns.expansion, False)
        return [get_paths(cfg)["db"]]
    if ns.file is not None:
        return [get_paths(cfg)["db"]]
    if ns.expansion:
        return _db_paths_for_scope(cfg, ns.expansion, False)
    return []


def _unlink_db_file(p: Path) -> None:
    try:
        if p.exists():
            p.unlink()
            print(f"Removed database file: {p}")
    except OSError as e:
        raise SystemExit(
            f"Could not remove {p}: {e}\n"
            "Close IDE/database connections (Cursor, SQLTools, DuckDB extension) and retry."
        ) from e


def _run_ingest_from_args(ns: argparse.Namespace) -> None:
    root = get_project_root()
    argv = [sys.executable, "-m", "scripts.ingest.cli"]
    cfg = getattr(ns, "config", None)
    if cfg and isinstance(cfg, Path):
        argv += ["--config", str(cfg)]
    if ns.helpers:
        argv.append("--helpers")
    if ns.file:
        argv += ["--file", ns.file]
    if ns.all:
        argv.append("--all")
    if ns.expansion:
        argv += ["--set", ns.expansion]
    if ns.format:
        argv += ["--format", ns.format]
    if getattr(ns, "fresh", False):
        argv.append("--fresh")
    subprocess.run(argv, cwd=root, check=True)


def _run_dbt(db_path: Path, project_root: Path) -> None:
    dbt = shutil.which("dbt")
    if not dbt:
        raise SystemExit(
            "dbt not found on PATH. Install dbt-duckdb (e.g. pip install dbt-duckdb) or activate your env."
        )
    env = os.environ.copy()
    env["DUCKDB_PATH"] = str(db_path)
    subprocess.run(
        [dbt, "run", "--profiles-dir", str(project_root)],
        cwd=project_root,
        env=env,
        check=True,
    )


def _run_dbt_parse(project_root: Path, duckdb_path: Path) -> None:
    dbt = shutil.which("dbt")
    if not dbt:
        raise SystemExit("dbt not found on PATH.")
    env = os.environ.copy()
    env["DUCKDB_PATH"] = str(duckdb_path)
    subprocess.run(
        [dbt, "parse", "--profiles-dir", str(project_root)],
        cwd=project_root,
        env=env,
        check=True,
    )


def cmd_dbt(ns: argparse.Namespace) -> None:
    cfg = _load_cfg(ns)
    paths = _db_paths_for_scope(cfg, ns.expansion, ns.all_sets)
    root = get_project_root()
    for p in paths:
        if not p.exists():
            print(f"Skip (database missing): {p}", file=sys.stderr)
            continue
        print(f"dbt run -> {p}")
        _run_dbt(p, root)


def cmd_reset(ns: argparse.Namespace) -> None:
    cfg = _load_cfg(ns)
    for p in _db_paths_for_scope(cfg, ns.expansion, ns.all_sets):
        _unlink_db_file(p)


def cmd_full(ns: argparse.Namespace) -> None:
    cfg = _load_cfg(ns)
    if getattr(ns, "reset_db", False):
        for p in _db_paths_after_ingest(cfg, ns):
            _unlink_db_file(p)
    _run_ingest_from_args(ns)
    cfg = _load_cfg(ns)
    root = get_project_root()
    if ns.all:
        paths = _db_paths_for_scope(cfg, None, True)
    elif ns.helpers:
        if ns.expansion:
            paths = _db_paths_for_scope(cfg, ns.expansion, False)
        else:
            paths = [get_paths(cfg)["db"]]
    elif ns.file is not None:
        paths = [get_paths(cfg)["db"]]
    elif ns.expansion:
        paths = _db_paths_for_scope(cfg, ns.expansion, False)
    else:
        paths = []
    for p in paths:
        if not p.exists():
            print(f"Skip dbt (database missing): {p}", file=sys.stderr)
            continue
        print(f"dbt run -> {p}")
        _run_dbt(p, root)


def cmd_verify(ns: argparse.Namespace) -> None:
    cfg = _load_cfg(ns)
    paths = _db_paths_for_scope(cfg, ns.expansion, ns.all_sets)
    require_staging = ns.require_staging
    ok = True
    for p in paths:
        print(f"\n=== {p} ===")
        if not p.exists():
            print("  ERROR: file does not exist")
            ok = False
            continue
        conn = duckdb.connect(str(p), read_only=True)
        try:
            for sch, tbl in (("raw", "draft"), ("raw", "game")):
                try:
                    n = conn.execute(f'SELECT COUNT(*) FROM "{sch}"."{tbl}"').fetchone()[0]
                    label = "ok" if n > 0 else "EMPTY"
                    print(f"  {sch}.{tbl}: {n:,} rows ({label})")
                    if n == 0:
                        ok = False
                except Exception as e:
                    print(f"  {sch}.{tbl}: MISSING - {e}")
                    ok = False
            for st in ("stg_draft", "stg_game"):
                try:
                    n = conn.execute(f'SELECT COUNT(*) FROM main_staging."{st}"').fetchone()[0]
                    print(f"  main_staging.{st}: {n:,} rows")
                    if require_staging and n == 0:
                        ok = False
                except Exception as e:
                    msg = "not built (run dbt)" if "does not exist" in str(e).lower() else str(e)
                    print(f"  main_staging.{st}: - {msg}")
                    if require_staging:
                        ok = False
        finally:
            conn.close()
    raise SystemExit(0 if ok else 1)


def cmd_parse(ns: argparse.Namespace) -> None:
    cfg = _load_cfg(ns)
    root = get_project_root()
    if ns.set_code:
        candidates = [get_paths(cfg, ns.set_code)["db"]]
    elif ns.all_paths:
        candidates = _db_paths_for_scope(cfg, None, True)
    else:
        candidates = [get_paths(cfg)["db"]]

    pick = next((p for p in candidates if p.exists()), None)
    if not pick:
        raise SystemExit(
            "No existing .duckdb file found for dbt parse. Ingest at least one set, or use --set CODE / --all-paths."
        )
    print(f"dbt parse (DUCKDB_PATH={pick})")
    _run_dbt_parse(root, pick)


def main() -> None:
    root = get_project_root()
    default_config = root / "config" / "datasets.yaml"

    common = argparse.ArgumentParser(add_help=False)
    common.add_argument(
        "--config",
        type=Path,
        default=default_config,
        help="Path to datasets.yaml",
    )

    parser = argparse.ArgumentParser(
        description="Ingest + dbt orchestration.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_ing = sub.add_parser(
        "ingest",
        parents=[common],
        help="Run only ingest (delegates to scripts.ingest.cli).",
    )
    p_ing.add_argument("--helpers", action="store_true")
    p_ing.add_argument("--set", dest="expansion", metavar="CODE", default=None)
    p_ing.add_argument("--file", metavar="URL_OR_PATH", default=None)
    p_ing.add_argument("--all", action="store_true")
    p_ing.add_argument(
        "--format",
        choices=["premier_draft", "trad_draft", "trad_sealed", "sealed"],
        default=None,
    )
    p_ing.add_argument(
        "--fresh",
        action="store_true",
        help="Re-download even when unchanged (passed to scripts.ingest.cli).",
    )
    p_ing.set_defaults(_fn=_run_ingest_from_args)

    p_dbt = sub.add_parser(
        "dbt",
        parents=[common],
        help="Run dbt for one set (--set) or every configured set (--all).",
    )
    p_dbt.add_argument("--set", dest="expansion", metavar="CODE", default=None)
    p_dbt.add_argument("--all", action="store_true", dest="all_sets")
    p_dbt.set_defaults(_fn=cmd_dbt)

    p_full = sub.add_parser(
        "full",
        parents=[common],
        help="Ingest then dbt for one set or all sets.",
    )
    p_full.add_argument("--helpers", action="store_true")
    p_full.add_argument("--set", dest="expansion", metavar="CODE", default=None)
    p_full.add_argument("--file", metavar="URL_OR_PATH", default=None)
    p_full.add_argument("--all", action="store_true")
    p_full.add_argument(
        "--format",
        choices=["premier_draft", "trad_draft", "trad_sealed", "sealed"],
        default=None,
    )
    p_full.add_argument(
        "--fresh",
        action="store_true",
        help="Re-download even when unchanged (passed to ingest).",
    )
    p_full.add_argument(
        "--reset-db",
        action="store_true",
        help="Delete target .duckdb file(s) before ingest (close IDE DB connections first).",
    )
    p_full.set_defaults(_fn=cmd_full)

    p_rst = sub.add_parser(
        "reset",
        parents=[common],
        help="Delete target .duckdb file(s) only (no ingest). Close IDE connections first.",
    )
    p_rst.add_argument("--set", dest="expansion", metavar="CODE", default=None)
    p_rst.add_argument("--all", action="store_true", dest="all_sets")
    p_rst.set_defaults(_fn=cmd_reset)

    p_ver = sub.add_parser(
        "verify",
        parents=[common],
        help="Read-only check: raw.draft / raw.game row counts; report staging if present.",
    )
    p_ver.add_argument("--set", dest="expansion", metavar="CODE", default=None)
    p_ver.add_argument("--all", action="store_true", dest="all_sets")
    p_ver.add_argument(
        "--require-staging",
        action="store_true",
        help="Fail if main_staging.stg_draft/stg_game are missing or empty.",
    )
    p_ver.set_defaults(_fn=cmd_verify)

    p_parse = sub.add_parser(
        "parse",
        parents=[common],
        help="Run dbt parse (fast project check; needs one existing .duckdb for profile path).",
    )
    p_parse.add_argument("--set", dest="set_code", metavar="CODE", default=None)
    p_parse.add_argument(
        "--all-paths",
        action="store_true",
        help="Consider every per-set DB from config; use first file that exists.",
    )
    p_parse.set_defaults(_fn=cmd_parse)

    args = parser.parse_args()

    if args.command == "ingest":
        args._fn(args)
    elif args.command == "dbt":
        if not args.all_sets and not args.expansion:
            parser.error("dbt: specify --set CODE or --all")
        args._fn(args)
    elif args.command == "full":
        modes = [
            args.helpers,
            args.file is not None,
            args.expansion is not None and not args.helpers,
            args.all,
        ]
        if sum(bool(x) for x in modes) != 1:
            parser.error(
                "full: specify exactly one of --helpers, --file PATH, --set CODE, or --all "
                "(same rules as scripts.ingest.cli)"
            )
        args._fn(args)
    elif args.command == "verify":
        if not args.all_sets and not args.expansion:
            parser.error("verify: specify --set CODE or --all")
        args._fn(args)
    elif args.command == "parse":
        args._fn(args)
    elif args.command == "reset":
        if not args.all_sets and not args.expansion:
            parser.error("reset: specify --set CODE or --all")
        args._fn(args)


if __name__ == "__main__":
    main()
