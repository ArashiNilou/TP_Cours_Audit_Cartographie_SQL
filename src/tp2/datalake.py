"""Filesystem helpers for the TP2 Docker shared Data Lake volume."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Iterable

from .event_contracts import json_dumps

DATA_LAKE_ROOT = Path(os.getenv("DATA_LAKE_ROOT", "/data-lake"))


def data_lake_path(*parts: str) -> Path:
    return DATA_LAKE_ROOT.joinpath(*parts)


def ensure_data_lake_layout() -> None:
    for path in (
        data_lake_path("raw", "france_travail"),
        data_lake_path("raw", "communes"),
        data_lake_path("aggregated", "offres"),
        data_lake_path("curated", "offres"),
        data_lake_path("quarantine"),
    ):
        path.mkdir(parents=True, exist_ok=True)


def partition_dir(base: Path, ingestion_date: str) -> Path:
    path = base / f"ingestion_date={ingestion_date}"
    path.mkdir(parents=True, exist_ok=True)
    return path


def write_json_atomic(path: Path, payload: Any, *, overwrite: bool = True) -> bool:
    if not overwrite and path.exists():
        return False
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    with temp_path.open("w", encoding="utf-8", newline="\n") as handle:
        json.dump(payload, handle, ensure_ascii=False, sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    temp_path.replace(path)
    return True


def write_jsonl_atomic(path: Path, records: Iterable[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    with temp_path.open("w", encoding="utf-8", newline="\n") as handle:
        for record in records:
            handle.write(json_dumps(record))
            handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    temp_path.replace(path)


def load_latest_communes() -> dict[str, dict[str, Any]] | None:
    latest = data_lake_path("raw", "communes", "_latest.json")
    if not latest.exists():
        return None
    with latest.open("r", encoding="utf-8") as handle:
        communes = json.load(handle)
    mapping: dict[str, dict[str, Any]] = {}
    for commune in communes:
        code = str(commune.get("code") or "").strip().upper()
        if code:
            mapping[code] = commune
    return mapping
