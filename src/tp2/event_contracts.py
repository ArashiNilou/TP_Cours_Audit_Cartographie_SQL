"""Pure event contracts shared by TP2 Kafka and Data Lake components."""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any

OFFER_EVENT_SCHEMA_VERSION = "tp2.offer.v1"
OFFER_EVENT_SOURCE = "france_travail"


def utc_now() -> datetime:
    return datetime.now(tz=timezone.utc)


def isoformat_z(value: datetime | None = None) -> str:
    current = value or utc_now()
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    return current.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_iso_datetime(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def ingestion_date(value: str | datetime | None = None) -> str:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).date().isoformat()
    if isinstance(value, str):
        return parse_iso_datetime(value).date().isoformat()
    return utc_now().date().isoformat()


def normalize_code_insee(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip().upper()
    return text[:5] if text else None


def extract_offer_commune_code(offer: dict[str, Any]) -> str | None:
    lieu = offer.get("lieuTravail") or {}
    if not isinstance(lieu, dict):
        return None
    return normalize_code_insee(lieu.get("commune"))


def build_offer_event(
    offer: dict[str, Any],
    *,
    collected_at: str | None = None,
    event_id: str | None = None,
) -> dict[str, Any]:
    source_offer_id = str(offer.get("id") or "").strip()
    if not source_offer_id:
        raise ValueError("France Travail offer payload must contain a non-empty id")
    timestamp = collected_at or isoformat_z()
    revision = str(offer.get("dateActualisation") or offer.get("dateCreation") or "")
    deterministic_id = str(
        uuid.uuid5(uuid.NAMESPACE_URL, f"france-travail:{source_offer_id}:{revision}")
    )
    return {
        "schema_version": OFFER_EVENT_SCHEMA_VERSION,
        "event_id": event_id or deterministic_id,
        "source": OFFER_EVENT_SOURCE,
        "source_offer_id": source_offer_id,
        "collected_at": timestamp,
        "payload": offer,
    }


def enrich_offer_with_commune_reference(
    offer: dict[str, Any],
    commune_reference: dict[str, Any] | None,
) -> dict[str, Any]:
    """Return an offer enriched with the official Geo API commune values."""
    enriched = dict(offer)
    lieu = dict(offer.get("lieuTravail") or {})
    if not commune_reference:
        enriched["lieuTravail"] = lieu
        return enriched

    code = normalize_code_insee(commune_reference.get("code"))
    postcodes = commune_reference.get("codesPostaux") or []
    centre = commune_reference.get("centre") or {}
    coordinates = centre.get("coordinates") or []

    if code:
        lieu["commune"] = code
    if commune_reference.get("nom"):
        lieu["libelle"] = commune_reference["nom"]
    if postcodes:
        lieu["codePostal"] = str(postcodes[0])[:5]
    if len(coordinates) >= 2:
        lieu["longitude"] = coordinates[0]
        lieu["latitude"] = coordinates[1]
    enriched["lieuTravail"] = lieu
    return enriched


def validate_offer_event(event: dict[str, Any]) -> tuple[bool, str | None]:
    if event.get("schema_version") != OFFER_EVENT_SCHEMA_VERSION:
        return False, "unsupported_schema_version"
    if event.get("source") != OFFER_EVENT_SOURCE:
        return False, "unsupported_source"
    for field in ("event_id", "source_offer_id", "collected_at"):
        if not str(event.get(field) or "").strip():
            return False, f"missing_{field}"
    try:
        uuid.UUID(str(event["event_id"]))
    except (ValueError, AttributeError):
        return False, "invalid_event_id"
    if not isinstance(event.get("payload"), dict):
        return False, "payload_not_object"
    lieu = event["payload"].get("lieuTravail")
    if lieu is not None and not isinstance(lieu, dict):
        return False, "invalid_lieu_travail"
    try:
        parse_iso_datetime(str(event["collected_at"]))
    except ValueError:
        return False, "invalid_collected_at"
    return True, None


def build_aggregated_record(
    event: dict[str, Any],
    *,
    commune_reference: dict[str, Any] | None,
    commune_reference_status: str,
    aggregated_at: str | None = None,
) -> dict[str, Any]:
    valid, reason = validate_offer_event(event)
    if not valid:
        raise ValueError(f"Invalid offer event: {reason}")

    timestamp = aggregated_at or isoformat_z()
    payload = event["payload"]
    code_insee = extract_offer_commune_code(payload)
    enriched_payload = enrich_offer_with_commune_reference(payload, commune_reference)
    return {
        "schema_version": "tp2.aggregated_offer.v1",
        "event_id": event["event_id"],
        "source": event["source"],
        "source_offer_id": event["source_offer_id"],
        "collected_at": event["collected_at"],
        "aggregated_at": timestamp,
        "ingestion_date": ingestion_date(timestamp),
        "code_insee": code_insee,
        "commune_reference_status": commune_reference_status,
        "commune_reference": commune_reference,
        "payload": payload,
        "enriched_payload": enriched_payload,
    }


def json_dumps(record: dict[str, Any]) -> str:
    return json.dumps(record, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
