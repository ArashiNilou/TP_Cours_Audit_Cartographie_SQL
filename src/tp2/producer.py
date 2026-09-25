"""Continuous France Travail polling producer that publishes offer events to Kafka."""

from __future__ import annotations

import logging
import os
import time

from kafka import KafkaProducer
from prometheus_client import Counter, Gauge, start_http_server

from src.api_client import FranceTravailClient

from .event_contracts import build_offer_event, json_dumps

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("FT_OFFERS_TOPIC", "france-travail.offres.raw")
POLL_INTERVAL_SECONDS = int(os.getenv("FT_PRODUCER_POLL_INTERVAL_SECONDS", "900"))
MAX_RESULTS = int(os.getenv("FT_PRODUCER_MAX_RESULTS", "50"))
METRICS_PORT = int(os.getenv("METRICS_PORT", "8001"))

PRODUCED = Counter("tp2_ft_events_produced_total", "France Travail events produced")
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


def poll_and_publish(client: FranceTravailClient, producer: KafkaProducer) -> int:
    offres = client.search_all_offres(
        mots_cles=os.getenv("FT_PRODUCER_MOTS_CLES") or None,
        code_rome=os.getenv("FT_PRODUCER_CODE_ROME") or None,
        commune=os.getenv("FT_PRODUCER_COMMUNE") or None,
        departement=os.getenv("FT_PRODUCER_DEPARTEMENT") or None,
        max_results=MAX_RESULTS,
    )
    count = 0
    for offre in offres:
        event = build_offer_event(offre)
        future = producer.send(TOPIC, key=event["source_offer_id"], value=event)
        future.get(timeout=30)
        PRODUCED.inc()
        count += 1
    producer.flush(timeout=30)
    LAST_SUCCESS.set(time.time())
    logger.info("Published %s France Travail offer event(s) to %s", count, TOPIC)
    return count


def main() -> None:
    start_http_server(METRICS_PORT)
    producer = _producer()
    client: FranceTravailClient | None = None
    while True:
        try:
            if client is None:
                client = FranceTravailClient()
            poll_and_publish(client, producer)
        except Exception:
            client = None
            ERRORS.inc()
            logger.exception("France Travail producer poll failed")
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
