#!/usr/bin/env python3
"""
Run jobs from config/jobs.yaml.

Usage:
  python scripts/run_jobs.py ingest_helpers
  python scripts/run_jobs.py ingest_helpers ingest_set_mkm
  python scripts/run_jobs.py --all
"""

import argparse
import subprocess
import sys
from pathlib import Path

import yaml


def get_project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def load_jobs_config() -> dict:
    path = get_project_root() / "config" / "jobs.yaml"
    if not path.exists():
        path = get_project_root() / "config" / "jobs.yaml.example"
    if not path.exists():
        raise FileNotFoundError("config/jobs.yaml or jobs.yaml.example not found")
    with path.open(encoding="utf-8") as f:
        return yaml.safe_load(f)


def resolve_deps(jobs_config: dict, requested: list[str]) -> list[str]:
    """Topological order respecting depends_on."""
    jobs = jobs_config.get("jobs", {})
    seen = set()
    result = []

    def add(name: str) -> None:
        if name in seen:
            return
        seen.add(name)
        deps = jobs.get(name, {}).get("depends_on", [])
        for d in deps:
            add(d)
        result.append(name)

    for name in requested:
        if name in jobs:
            add(name)
        else:
            print(f"WARNING: Unknown job '{name}', skipping")

    return result


def run_job(job_name: str, job_spec: dict, root: Path) -> int:
    """Execute a single job. Returns exit code."""
    script = job_spec.get("script")
    args = job_spec.get("args", [])
    if not script:
        print(f"Job {job_name} has no script")
        return 1
    script_path = root / script
    if not script_path.exists():
        print(f"Script not found: {script_path}")
        return 1
    cmd = [sys.executable, str(script_path)] + args
    print(f"Running: {' '.join(cmd)}")
    return subprocess.run(cmd, cwd=str(root)).returncode


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("jobs", nargs="*", help="Job names to run")
    parser.add_argument("--all", action="store_true", help="Run all jobs")
    args = parser.parse_args()

    root = get_project_root()
    config = load_jobs_config()
    jobs = config.get("jobs", {})

    if args.all:
        to_run = list(jobs.keys())
    elif args.jobs:
        to_run = resolve_deps(config, args.jobs)
    else:
        parser.print_help()
        return 0

    for name in to_run:
        spec = jobs.get(name, {})
        code = run_job(name, spec, root)
        if code != 0:
            print(f"Job {name} failed with exit code {code}")
            return code

    return 0


if __name__ == "__main__":
    sys.exit(main())
