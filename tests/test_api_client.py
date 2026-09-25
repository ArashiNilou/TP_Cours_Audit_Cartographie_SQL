import time
import unittest
from unittest.mock import patch

from src.api_client import AccessToken, FranceTravailClient


class _Response:
    status_code = 200
    content = b'{"resultats":[]}'
    headers = {"Content-Range": "offres 0-0/1"}
    text = "{}"

    def json(self):
        return {"resultats": []}


class ApiClientTest(unittest.TestCase):
    def test_search_uses_bearer_token(self) -> None:
        client = FranceTravailClient(client_id="id", client_secret="secret")
        client._token = AccessToken("token-123", time.time() + 3600)

        with patch("src.api_client.requests.get", return_value=_Response()) as mocked:
            client.search_offres(mots_cles="data", range_="0-0")

        headers = mocked.call_args.kwargs["headers"]
        self.assertEqual(headers["Authorization"], "Bearer token-123")


if __name__ == "__main__":
    unittest.main()
