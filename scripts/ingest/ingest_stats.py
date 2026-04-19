"""Lightweight counters for ingest summaries."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class IngestStats:
    downloaded: int = 0
    skipped_unchanged: int = 0
    skipped_http: int = 0  # 403/404 — file not on server for this combo

    def add_download_result(self, result: str) -> None:
        if result == "skipped":
            self.skipped_unchanged += 1
        elif result == "downloaded":
            self.downloaded += 1

    def merge(self, other: IngestStats) -> None:
        self.downloaded += other.downloaded
        self.skipped_unchanged += other.skipped_unchanged
        self.skipped_http += other.skipped_http

    def summary_line(self, label: str) -> str:
        parts = []
        if self.downloaded:
            parts.append(f"downloaded={self.downloaded}")
        if self.skipped_unchanged:
            parts.append(f"unchanged_skipped={self.skipped_unchanged}")
        if self.skipped_http:
            parts.append(f"not_on_server={self.skipped_http}")
        body = ", ".join(parts) if parts else "no files processed"
        return f"{label}: {body}"


@dataclass
class HelpersCardStats:
    """Stats for --helpers (helpers + card lists)."""

    helpers: IngestStats = field(default_factory=IngestStats)
    card_lists: IngestStats = field(default_factory=IngestStats)

    def summary(self) -> str:
        return " | ".join(
            [
                self.helpers.summary_line("helpers"),
                self.card_lists.summary_line("card_lists"),
            ]
        )
