"""Optional skip re-download when remote file unchanged (ETag / Last-Modified).

Does not scrape the public datasets page — uses HTTP HEAD against the same URLs in config.
State file: data/.ingest_remote_state.json (gitignored with data/*).
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import requests

logger = logging.getLogger(__name__)

REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}


def load_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        with path.open(encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}


def save_state(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, sort_keys=True)


def head_metadata(url: str) -> dict[str, str | None] | None:
    """Return ETag and Last-Modified from HEAD, or None if HEAD fails."""
    try:
        r = requests.head(url, timeout=30, headers=REQUEST_HEADERS, allow_redirects=True)
        r.raise_for_status()
        return {
            "etag": r.headers.get("ETag"),
            "last_modified": r.headers.get("Last-Modified"),
        }
    except requests.RequestException as e:
        logger.debug("HEAD failed for %s: %s", url, e)
        return None


def _meta_matches(
    current: dict[str, str | None], stored: dict[str, Any] | None
) -> bool:
    if not stored:
        return False
    # Prefer ETag when both sides have it
    ce, se = current.get("etag"), stored.get("etag")
    if ce and se and ce == se:
        return True
    cl, sl = current.get("last_modified"), stored.get("last_modified")
    if cl and sl and cl == sl:
        return True
    return False


def download_if_newer(
    url: str,
    dest: Path,
    state_path: Path,
    skip_if_unchanged: bool,
) -> str:
    """
    Download url to dest unless skip_if_unchanged and server reports same ETag/Last-Modified.

    Returns: "downloaded" | "skipped"
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    state = load_state(state_path)
    stored = state.get(url)

    if skip_if_unchanged and dest.exists() and stored:
        cur = head_metadata(url)
        if cur and _meta_matches(cur, stored):
            logger.info("Unchanged (ETag/Last-Modified), skip download: %s", url)
            return "skipped"

    logger.info("Downloading %s -> %s", url, dest)
    resp = requests.get(url, stream=True, timeout=120, headers=REQUEST_HEADERS)
    resp.raise_for_status()
    with dest.open("wb") as f:
        for chunk in resp.iter_content(chunk_size=65536):
            f.write(chunk)

    state[url] = {
        "etag": resp.headers.get("ETag"),
        "last_modified": resp.headers.get("Last-Modified"),
    }
    save_state(state_path, state)
    return "downloaded"


def simple_download(url: str, dest: Path) -> None:
    """Always GET (original behavior)."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    logger.info("Downloading %s -> %s", url, dest)
    resp = requests.get(url, stream=True, timeout=120, headers=REQUEST_HEADERS)
    resp.raise_for_status()
    with dest.open("wb") as f:
        for chunk in resp.iter_content(chunk_size=65536):
            f.write(chunk)
