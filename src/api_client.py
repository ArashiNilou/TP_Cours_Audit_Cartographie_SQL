"""Client OAuth2 pour l'API « Offres d'emploi v2 » de France Travail.

Authentification : flux "client_credentials" auprès de l'endpoint
FT_TOKEN_URL, avec FT_CLIENT_ID / FT_CLIENT_SECRET / FT_SCOPE lus depuis les
variables d'environnement (voir .env.example). Aucun secret n'est codé en
dur dans ce module.
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Any

import requests

DEFAULT_TOKEN_URL = (
    "https://entreprise.francetravail.fr/connexion/oauth2/access_token"
    "?realm=/partenaire"
)
DEFAULT_API_BASE_URL = "https://api.francetravail.io/partenaire/offresdemploi/v2"
DEFAULT_SCOPE = "api_offresdemploiv2 o2dsoffre"


class FranceTravailAuthError(RuntimeError):
    """Levée lorsque l'obtention du jeton d'accès échoue."""


class FranceTravailApiError(RuntimeError):
    """Levée lorsqu'un appel à l'API Offres d'emploi échoue."""


@dataclass
class AccessToken:
    """Jeton d'accès OAuth2 et sa date d'expiration (timestamp epoch)."""

    value: str
    expires_at: float

    def is_valid(self, margin_seconds: int = 30) -> bool:
        """Indique si le jeton est encore valide, avec une marge de sécurité."""
        return time.time() < (self.expires_at - margin_seconds)


class FranceTravailClient:
    """Client HTTP pour s'authentifier et interroger l'API Offres d'emploi v2."""

    def __init__(
        self,
        client_id: str | None = None,
        client_secret: str | None = None,
        token_url: str | None = None,
        scope: str | None = None,
        api_base_url: str | None = None,
    ) -> None:
        self.client_id = client_id or os.getenv("FT_CLIENT_ID")
        self.client_secret = client_secret or os.getenv("FT_CLIENT_SECRET")
        self.token_url = token_url or os.getenv("FT_TOKEN_URL", DEFAULT_TOKEN_URL)
        self.scope = scope or os.getenv("FT_SCOPE", DEFAULT_SCOPE)
        self.api_base_url = api_base_url or os.getenv(
            "FT_API_BASE_URL", DEFAULT_API_BASE_URL
        )
        self._token: AccessToken | None = None

        if not self.client_id or not self.client_secret:
            raise FranceTravailAuthError(
                "FT_CLIENT_ID et FT_CLIENT_SECRET doivent être définis "
                "(variables d'environnement ou fichier .env)."
            )

    def _fetch_token(self) -> AccessToken:
        """Demande un nouveau jeton d'accès via le flux client_credentials."""
        response = requests.post(
            self.token_url,
            data={
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "scope": self.scope,
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=15,
        )
        if response.status_code != 200:
            raise FranceTravailAuthError(
                f"Échec de l'authentification OAuth2 "
                f"(HTTP {response.status_code}) : {response.text[:300]}"
            )
        payload = response.json()
        access_token = payload["access_token"]
        expires_in = payload.get("expires_in", 1499)
        return AccessToken(value=access_token, expires_at=time.time() + expires_in)

    def _get_token(self) -> str:
        """Retourne un jeton d'accès valide, en le renouvelant si nécessaire."""
        if self._token is None or not self._token.is_valid():
            self._token = self._fetch_token()
        return self._token.value

    def search_offres(
        self,
        mots_cles: str | None = None,
        code_rome: str | None = None,
        commune: str | None = None,
        range_: str = "0-49",
    ) -> dict[str, Any]:
        """Recherche des offres d'emploi via l'endpoint /offres/search.

        Retourne le JSON décodé de la réponse (clé "resultats" attendue).
        """
        params: dict[str, str] = {"range": range_}
        if mots_cles:
            params["motsCles"] = mots_cles
        if code_rome:
            params["codeROME"] = code_rome
        if commune:
            params["commune"] = commune

        response = requests.get(
            f"{self.api_base_url}/offres/search",
            params=params,
            headers={"Authorization": f"Bearer {self._get_token()}"},
            timeout=30,
        )
        if response.status_code not in (200, 206):
            raise FranceTravailApiError(
                f"Échec de la recherche d'offres "
                f"(HTTP {response.status_code}) : {response.text[:300]}"
            )
        if response.status_code == 204 or not response.content:
            return {"resultats": []}
        return response.json()
