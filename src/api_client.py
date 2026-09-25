"""Client OAuth2 pour l'API Offres d'emploi v2 de France Travail."""

from __future__ import annotations

import logging
import os
import re
import time
from dataclasses import dataclass
from typing import Any

import requests
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

DEFAULT_TOKEN_URL = (
    "https://entreprise.francetravail.fr/connexion/oauth2/access_token"
    "?realm=/partenaire"
)
DEFAULT_API_BASE_URL = "https://api.francetravail.io/partenaire/offresdemploi/v2"
DEFAULT_SCOPE = "api_offresdemploiv2 o2dsoffre"


class FranceTravailAuthError(RuntimeError):
    """Levee lorsque l'obtention du jeton d'acces echoue."""


class FranceTravailApiError(RuntimeError):
    """Levee lorsqu'un appel a l'API Offres d'emploi echoue."""


@dataclass
class AccessToken:
    """Jeton d'acces OAuth2 et sa date d'expiration."""

    value: str
    expires_at: float

    def is_valid(self, margin_seconds: int = 30) -> bool:
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
                "FT_CLIENT_ID et FT_CLIENT_SECRET doivent etre definis "
                "(variables d'environnement ou fichier .env)."
            )

    def _fetch_token(self) -> AccessToken:
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
                f"Echec de l'authentification OAuth2 "
                f"(HTTP {response.status_code}) : {response.text[:300]}"
            )
        payload = response.json()
        return AccessToken(
            value=payload["access_token"],
            expires_at=time.time() + payload.get("expires_in", 1499),
        )

    def _get_token(self) -> str:
        if self._token is None or not self._token.is_valid():
            self._token = self._fetch_token()
        return self._token.value

    def _authorization_header(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self._get_token()}"}

    def search_offres(
        self,
        mots_cles: str | None = None,
        code_rome: str | None = None,
        commune: str | None = None,
        departement: str | None = None,
        range_: str = "0-49",
    ) -> dict[str, Any]:
        params: dict[str, str] = {"range": range_}
        if mots_cles:
            params["motsCles"] = mots_cles
        if code_rome:
            params["codeROME"] = code_rome
        if commune:
            params["commune"] = commune
        if departement:
            params["departement"] = departement

        response = requests.get(
            f"{self.api_base_url}/offres/search",
            params=params,
            headers=self._authorization_header(),
            timeout=30,
        )
        if response.status_code in (204, 416) or not response.content:
            return {"resultats": [], "total": 0}

        if response.status_code not in (200, 206):
            raise FranceTravailApiError(
                f"Echec de la recherche d'offres "
                f"(HTTP {response.status_code}) : {response.text[:300]}"
            )

        payload = response.json()
        content_range = response.headers.get("Content-Range", "")
        match = re.search(r"/(\d+)$", content_range)
        if match:
            payload["total"] = int(match.group(1))
        return payload

    def search_all_offres(
        self,
        mots_cles: str | None = None,
        code_rome: str | None = None,
        commune: str | None = None,
        departement: str | None = None,
        max_results: int | None = None,
        delay_seconds: float = 0.25,
    ) -> list[dict[str, Any]]:
        offres: list[dict[str, Any]] = []
        start = 0
        batch_step = 149

        while start <= 1000:
            if max_results and len(offres) >= max_results:
                break

            remaining = (max_results - len(offres) - 1) if max_results else batch_step
            step = min(batch_step, max(0, remaining))
            end = min(1148, start + step)
            payload = self.search_offres(
                mots_cles=mots_cles,
                code_rome=code_rome,
                commune=commune,
                departement=departement,
                range_=f"{start}-{end}",
            )
            lot = payload.get("resultats", [])
            if not lot:
                break

            offres.extend(lot)
            if len(lot) < (end - start + 1):
                break

            start = end + 1
            if delay_seconds > 0:
                time.sleep(delay_seconds)

        return offres

    def get_offre(self, offre_id: str) -> dict[str, Any]:
        response = requests.get(
            f"{self.api_base_url}/offres/{offre_id}",
            headers=self._authorization_header(),
            timeout=30,
        )
        if response.status_code != 200:
            raise FranceTravailApiError(
                f"Echec de la recuperation de l'offre {offre_id} "
                f"(HTTP {response.status_code}) : {response.text[:300]}"
            )
        return response.json()
