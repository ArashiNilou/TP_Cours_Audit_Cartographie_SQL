"""Periodic collector for the official French government Geo API communes source."""

from __future__ import annotations

import logging
import os
import time
from typing import Any

import requests
from prometheus_client import Counter, Gauge, start_http_server

from .datalake import data_lake_path, ensure_data_lake_layout, partition_dir, write_json_atomic
from .event_contracts import ingestion_date, isoformat_z

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)

GEO_API_URL = os.getenv(
    "GEO_API_URL",
    "https://geo.api.gouv.fr/communes?fields=nom,code,codesPostaux,centre&format=json",
)
INTERVAL_SECONDS = int(os.getenv("COMMUNES_COLLECTION_INTERVAL_SECONDS", "86400"))
RUN_ONCE = os.getenv("COMMUNES_RUN_ONCE", "false").lower() == "true"
METRICS_PORT = int(os.getenv("METRICS_PORT", "8002"))

COLLECTIONS = Counter("tp2_communes_collections_total", "Successful communes collections")
ERRORS = Counter("tp2_communes_errors_total", "Failed communes collections")
COMMUNES_COUNT = Gauge("tp2_communes_count", "Number of communes in latest source2 file")
LAST_SUCCESS = Gauge("tp2_communes_last_success_timestamp", "Last successful collection epoch")


def fetch_communes() -> list[dict[str, Any]]:
    response = requests.get(GEO_API_URL, timeout=60)
    response.raise_for_status()
    communes = response.json()
    if not isinstance(communes, list):
        raise ValueError("Geo API response is not a JSON array")
    return communes


def collect_once() -> int:
    ensure_data_lake_layout()
    communes = fetch_communes()
    timestamp = isoformat_z()
    day = ingestion_date(timestamp)
    file_timestamp = timestamp.replace(":", "").replace("-", "").replace(".", "_")
    target_dir = partition_dir(data_lake_path("raw", "communes"), day)
    write_json_atomic(target_dir / f"communes_{file_timestamp}.json", communes)
    write_json_atomic(data_lake_path("raw", "communes", "_latest.json"), communes)
    count = len(communes)
    COLLECTIONS.inc()
    COMMUNES_COUNT.set(count)
    LAST_SUCCESS.set(time.time())
    logger.info("Collected %s communes into %s", count, target_dir)
    return count


def main() -> None:
    start_http_server(METRICS_PORT)
    while True:
        try:
            collect_once()
        except Exception:
            ERRORS.inc()
            logger.exception("Communes collection failed")
            if RUN_ONCE:
                raise
        if RUN_ONCE:
            return
        time.sleep(INTERVAL_SECONDS)


if __name__ == "__main__":
    main()

