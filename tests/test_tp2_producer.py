import unittest
from unittest.mock import patch

from src.tp2 import producer as producer_module


class _Future:
    def get(self, timeout):
        return timeout


class _Producer:
    def __init__(self):
        self.sent = []
        self.flushed = False

    def send(self, topic, key, value):
        self.sent.append((topic, key, value))
        return _Future()

    def flush(self, timeout):
        self.flushed = timeout == 30


class _Client:
    def __init__(self):
        self.departements = []

    def search_all_offres(self, **kwargs):
        departement = kwargs["departement"]
        self.departements.append((departement, kwargs["max_results"]))
        return [
            {
                "id": f"offre-{departement}",
                "dateActualisation": "2026-09-25T12:00:00Z",
            }
        ]


class ProducerTest(unittest.TestCase):
    def test_national_poll_paginates_departments_and_skips_unchanged(self):
        client = _Client()
        producer = _Producer()
        seen = set()

        with (
            patch.object(producer_module, "ALL_DEPARTEMENTS", True),
            patch.object(producer_module, "DEPARTEMENTS_FRANCE", ["01", "02"]),
            patch.object(producer_module, "MAX_PER_DEPARTEMENT", 200),
        ):
            first = producer_module.poll_and_publish(client, producer, seen)
            second = producer_module.poll_and_publish(client, producer, seen)

        self.assertEqual(first, 2)
        self.assertEqual(second, 0)
        self.assertEqual(
            client.departements,
            [("01", 200), ("02", 200), ("01", 200), ("02", 200)],
        )
        self.assertEqual(len(producer.sent), 2)
        self.assertTrue(producer.flushed)


if __name__ == "__main__":
    unittest.main()
