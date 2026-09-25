import unittest

from src.tp2.event_contracts import (
    build_aggregated_record,
    build_offer_event,
    enrich_offer_with_commune_reference,
    extract_offer_commune_code,
    validate_offer_event,
)


class EventContractsTest(unittest.TestCase):
    def test_build_and_validate_offer_event(self) -> None:
        event = build_offer_event(
            {"id": "123ABC", "lieuTravail": {"commune": "44172"}},
            collected_at="2026-09-25T08:00:00Z",
            event_id="00000000-0000-0000-0000-000000000001",
        )

        valid, reason = validate_offer_event(event)

        self.assertTrue(valid)
        self.assertIsNone(reason)
        self.assertEqual(event["schema_version"], "tp2.offer.v1")
        self.assertEqual(event["source_offer_id"], "123ABC")

    def test_invalid_event_reports_contract_reason(self) -> None:
        valid, reason = validate_offer_event({"schema_version": "wrong"})

        self.assertFalse(valid)
        self.assertEqual(reason, "unsupported_schema_version")

    def test_aggregated_record_joins_commune_reference(self) -> None:
        event = build_offer_event(
            {"id": "123ABC", "lieuTravail": {"commune": "44172"}},
            collected_at="2026-09-25T08:00:00Z",
            event_id="00000000-0000-0000-0000-000000000001",
        )
        commune = {"code": "44172", "nom": "Sainte-Luce-sur-Loire"}

        record = build_aggregated_record(
            event,
            commune_reference=commune,
            commune_reference_status="matched",
            aggregated_at="2026-09-25T08:01:00Z",
        )

        self.assertEqual(record["code_insee"], "44172")
        self.assertEqual(record["commune_reference"], commune)
        self.assertEqual(record["enriched_payload"]["lieuTravail"]["libelle"], "Sainte-Luce-sur-Loire")
        self.assertEqual(record["ingestion_date"], "2026-09-25")
        self.assertEqual(extract_offer_commune_code(record["payload"]), "44172")

    def test_offer_event_id_is_stable_for_same_source_revision(self) -> None:
        offer = {
            "id": "123ABC",
            "dateActualisation": "2026-09-25T08:00:00Z",
        }
        self.assertEqual(build_offer_event(offer)["event_id"], build_offer_event(offer)["event_id"])

    def test_commune_reference_enriches_geography(self) -> None:
        offer = {"id": "123ABC", "lieuTravail": {"commune": "44172"}}
        commune = {
            "code": "44172",
            "nom": "Sainte-Luce-sur-Loire",
            "codesPostaux": ["44980"],
            "centre": {"coordinates": [-1.4862, 47.2494]},
        }

        enriched = enrich_offer_with_commune_reference(offer, commune)

        self.assertEqual(enriched["lieuTravail"]["codePostal"], "44980")
        self.assertEqual(enriched["lieuTravail"]["longitude"], -1.4862)
        self.assertEqual(enriched["lieuTravail"]["latitude"], 47.2494)
        self.assertNotIn("codePostal", offer["lieuTravail"])

    def test_invalid_nested_payload_is_rejected(self) -> None:
        event = build_offer_event(
            {"id": "123ABC", "lieuTravail": "invalid"},
            collected_at="2026-09-25T08:00:00Z",
        )

        valid, reason = validate_offer_event(event)

        self.assertFalse(valid)
        self.assertEqual(reason, "invalid_lieu_travail")


if __name__ == "__main__":
    unittest.main()
