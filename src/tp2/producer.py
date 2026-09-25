"""Continuous France Travail polling producer that publishes offer events to Kafka."""

from __future__ import annotations

import logging
import os
import time

from kafka import KafkaProducer
from prometheus_client import Counter, Gauge, start_http_server

from src.api_client import FranceTravailApiError, FranceTravailClient
from src.ingest import DEPARTEMENTS_FRANCE

from .event_contracts import build_offer_event, json_dumps

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("FT_OFFERS_TOPIC", "france-travail.offres.raw")
POLL_INTERVAL_SECONDS = int(os.getenv("FT_PRODUCER_POLL_INTERVAL_SECONDS", "900"))
MAX_RESULTS = int(os.getenv("FT_PRODUCER_MAX_RESULTS", "50"))
ALL_DEPARTEMENTS = os.getenv("FT_PRODUCER_ALL_DEPARTEMENTS", "false").lower() in {
    "1",
    "true",
    "yes",
}
MAX_PER_DEPARTEMENT = int(
    os.getenv("FT_PRODUCER_MAX_PER_DEPARTEMENT", "200")
)
METRICS_PORT = int(os.getenv("METRICS_PORT", "8001"))

PRODUCED = Counter("tp2_ft_events_produced_total", "France Travail events produced")
SCANNED = Counter("tp2_ft_offers_scanned_total", "France Travail offers scanned")
UNCHANGED = Counter(
    "tp2_ft_unchanged_offers_total",
    "Unchanged France Travail offer revisions not republished",
)
DEPARTMENTS_COMPLETED = Counter(
    "tp2_ft_departments_completed_total",
    "France Travail departments collected successfully",
)
ERRORS = Counter("tp2_ft_producer_errors_total", "France Travail producer errors")
LAST_SUCCESS = Gauge("tp2_ft_producer_last_success_timestamp", "Last successful poll epoch")


def _producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
        value_serializer=lambda value: json_dumps(value).encode("utf-8"),
        key_serializer=lambda value: value.encode("utf-8"),
        linger_ms=50,
        retries=5,
        acks="all",
    )


def _publish_offres(
    offres: list[dict],
    producer: KafkaProducer,
    seen_event_ids: set[str],
) -> tuple[int, int]:
    scanned = 0
    count = 0
    for offre in offres:
        scanned += 1
        SCANNED.inc()
        event = build_offer_event(offre)
        if event["event_id"] in seen_event_ids:
            UNCHANGED.inc()
            continue
        future = producer.send(TOPIC, key=event["source_offer_id"], value=event)
        future.get(timeout=30)
        seen_event_ids.add(event["event_id"])
        PRODUCED.inc()
        count += 1
    return scanned, count


def poll_and_publish(
    client: FranceTravailClient,
    producer: KafkaProducer,
    seen_event_ids: set[str] | None = None,
) -> int:
    seen = seen_event_ids if seen_event_ids is not None else set()
    mots_cles = os.getenv("FT_PRODUCER_MOTS_CLES") or None
    code_rome = os.getenv("FT_PRODUCER_CODE_ROME") or None
    total_scanned = 0
    count = 0

    if ALL_DEPARTEMENTS:
        for departement in DEPARTEMENTS_FRANCE:
            try:
                offres = client.search_all_offres(
                    mots_cles=mots_cles,
                    code_rome=code_rome,
                    departement=departement,
                    max_results=MAX_PER_DEPARTEMENT or None,
                )
            except FranceTravailApiError:
                ERRORS.inc()
                logger.exception(
                    "France Travail collection failed for department %s",
                    departement,
                )
                continue
            scanned, published = _publish_offres(offres, producer, seen)
            total_scanned += scanned
            count += published
            DEPARTMENTS_COMPLETED.inc()
            logger.info(
                "Department %s: scanned=%s published=%s",
                departement,
                scanned,
                published,
            )
    else:
        offres = client.search_all_offres(
            mots_cles=mots_cles,
            code_rome=code_rome,
            commune=os.getenv("FT_PRODUCER_COMMUNE") or None,
            departement=os.getenv("FT_PRODUCER_DEPARTEMENT") or None,
            max_results=MAX_RESULTS,
        )
        total_scanned, count = _publish_offres(offres, producer, seen)

    producer.flush(timeout=30)
    LAST_SUCCESS.set(time.time())
    logger.info(
        "France Travail poll completed: scanned=%s published=%s topic=%s",
        total_scanned,
        count,
        TOPIC,
    )
    return count


def main() -> None:
    start_http_server(METRICS_PORT)
    producer = _producer()
    client: FranceTravailClient | None = None
    seen_event_ids: set[str] = set()
    while True:
        try:
            if client is None:
                client = FranceTravailClient()
            poll_and_publish(client, producer, seen_event_ids)
        except Exception:
            client = None
            ERRORS.inc()
            logger.exception("France Travail producer poll failed")
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
