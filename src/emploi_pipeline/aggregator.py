"""Kafka consumer that persists raw events and enriched JSONL into the Data Lake."""

from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path
from typing import Any

from kafka import KafkaConsumer, TopicPartition
from prometheus_client import Counter, Gauge, start_http_server

from .datalake import (
    data_lake_path,
    ensure_data_lake_layout,
    load_latest_communes,
    partition_dir,
    write_json_atomic,
    write_jsonl_atomic,
)
from .event_contracts import (
    build_aggregated_record,
    extract_offer_commune_code,
    ingestion_date,
    isoformat_z,
    validate_offer_event,
)

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("FT_OFFERS_TOPIC", "france-travail.offres.raw")
GROUP_ID = os.getenv("AGGREGATOR_GROUP_ID", "tp2-raw-aggregator")
COMMUNES_REFRESH_SECONDS = int(os.getenv("COMMUNES_REFRESH_SECONDS", "300"))
STARTUP_COMMUNES_WAIT_SECONDS = int(os.getenv("STARTUP_COMMUNES_WAIT_SECONDS", "60"))
METRICS_PORT = int(os.getenv("METRICS_PORT", "8003"))

RAW_EVENTS = Counter("tp2_aggregator_raw_events_total", "Raw offer events persisted")
AGGREGATED_EVENTS = Counter("tp2_aggregator_events_total", "Aggregated offer records persisted")
QUARANTINED_EVENTS = Counter("tp2_aggregator_quarantined_events_total", "Invalid events quarantined")
ERRORS = Counter("tp2_aggregator_errors_total", "Aggregator processing errors")
DUPLICATES = Counter("tp2_aggregator_duplicate_events_total", "Already persisted offer revisions")
COMMUNES_LOADED = Gauge("tp2_aggregator_communes_loaded", "Communes loaded from source2 latest file")
SOURCE2_AVAILABLE = Gauge("tp2_aggregator_source2_available", "Whether source2 communes latest file is available")


def _consumer() -> KafkaConsumer:
    return KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
        group_id=GROUP_ID,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        key_deserializer=lambda value: value.decode("utf-8") if value else None,
        consumer_timeout_ms=1000,
    )


def _load_communes_with_startup_wait() -> dict[str, dict[str, Any]]:
    deadline = time.time() + STARTUP_COMMUNES_WAIT_SECONDS
    while True:
        communes = load_latest_communes()
        if communes is not None:
            SOURCE2_AVAILABLE.set(1)
            COMMUNES_LOADED.set(len(communes))
            logger.info("Loaded %s communes from source2 latest file", len(communes))
            return communes
        SOURCE2_AVAILABLE.set(0)
        COMMUNES_LOADED.set(0)
        if time.time() >= deadline:
            logger.warning(
                "No source2 communes file found after %ss; records will be tagged missing_source2",
                STARTUP_COMMUNES_WAIT_SECONDS,
            )
            return {}
        time.sleep(2)


def _quarantine(event: Any, reason: str) -> None:
    timestamp = isoformat_z()
    day = ingestion_date(timestamp)
    target_dir = partition_dir(data_lake_path("quarantine", "aggregator"), day)
    write_json_atomic(
        target_dir / f"quarantine_{time.time_ns()}.json",
        {"reason": reason, "quarantined_at": timestamp, "record": event},
    )
    QUARANTINED_EVENTS.inc()


def _write_event(event: dict[str, Any], communes: dict[str, dict[str, Any]]) -> None:
    timestamp = isoformat_z()
    day = ingestion_date(timestamp)
    event_id = event["event_id"]

    raw_dir = partition_dir(data_lake_path("raw", "france_travail"), day)
    if write_json_atomic(raw_dir / f"{event_id}.json", event, overwrite=False):
        RAW_EVENTS.inc()
    else:
        DUPLICATES.inc()

    code_insee = extract_offer_commune_code(event["payload"])
    if not communes:
        commune_reference = None
        commune_status = "missing_source2"
    elif code_insee and code_insee in communes:
        commune_reference = communes[code_insee]
        commune_status = "matched"
    else:
        commune_reference = None
        commune_status = "not_found"

    aggregated = build_aggregated_record(
        event,
        commune_reference=commune_reference,
        commune_reference_status=commune_status,
        aggregated_at=timestamp,
    )
    aggregated_dir = partition_dir(data_lake_path("aggregated", "offres"), day)
    write_jsonl_atomic(aggregated_dir / f"part-{event_id}.jsonl", [aggregated])
    AGGREGATED_EVENTS.inc()


def main() -> None:
    ensure_data_lake_layout()
    start_http_server(METRICS_PORT)
    communes = _load_communes_with_startup_wait()
    last_refresh = time.time()

    consumer = _consumer()
    logger.info("Aggregator consuming topic %s as group %s", TOPIC, GROUP_ID)
    while True:
        if time.time() - last_refresh >= COMMUNES_REFRESH_SECONDS:
            latest = load_latest_communes()
            if latest is not None:
                communes = latest
                SOURCE2_AVAILABLE.set(1)
                COMMUNES_LOADED.set(len(communes))
            else:
                communes = {}
                SOURCE2_AVAILABLE.set(0)
                COMMUNES_LOADED.set(0)
            last_refresh = time.time()

        for message in consumer:
            event = message.value
            valid, reason = validate_offer_event(event) if isinstance(event, dict) else (False, "not_object")
            if not valid:
                _quarantine(event, reason or "invalid")
                consumer.commit()
                continue
            try:
                _write_event(event, communes)
                consumer.commit()
            except Exception:
                ERRORS.inc()
                logger.exception("Failed to persist event %s; Kafka offset not committed", event.get("event_id"))
                consumer.seek(
                    TopicPartition(message.topic, message.partition),
                    message.offset,
                )
                time.sleep(5)
                break


if __name__ == "__main__":
    main()
