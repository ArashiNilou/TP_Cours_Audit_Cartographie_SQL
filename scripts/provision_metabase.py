"""Provision Metabase with an admin user and PostgreSQL database connection."""

from __future__ import annotations

import os
import sys
import time
from typing import Any

import requests


METABASE_URL = os.getenv("METABASE_URL", "http://localhost:3000").rstrip("/")


def _request(method: str, path: str, **kwargs: Any) -> requests.Response:
    response = requests.request(method, f"{METABASE_URL}{path}", timeout=30, **kwargs)
    response.raise_for_status()
    return response


def wait_for_metabase() -> None:
    for _ in range(60):
        try:
            _request("GET", "/api/health")
            return
        except requests.RequestException:
            time.sleep(2)
    raise RuntimeError("Metabase did not become healthy")


def session_token() -> str | None:
    email = os.getenv("MB_ADMIN_EMAIL")
    password = os.getenv("MB_ADMIN_PASSWORD")
    if not email or not password:
        return None
    try:
        response = _request(
            "POST",
            "/api/session",
            json={"username": email, "password": password},
        )
        return response.json()["id"]
    except requests.RequestException:
        return None


def setup_if_needed() -> str:
    token = session_token()
    if token:
        return token

    password = os.getenv("MB_ADMIN_PASSWORD")
    if not password:
        raise RuntimeError("MB_ADMIN_PASSWORD must be set to provision Metabase")

    properties = _request("GET", "/api/session/properties").json()
    setup_token = properties.get("setup-token")
    if not setup_token:
        raise RuntimeError("Metabase is already configured but admin login failed")

    payload = {
        "token": setup_token,
        "user": {
            "email": os.getenv("MB_ADMIN_EMAIL", "admin@example.local"),
            "first_name": os.getenv("MB_ADMIN_FIRST_NAME", "TP2"),
            "last_name": os.getenv("MB_ADMIN_LAST_NAME", "Admin"),
            "password": password,
        },
        "prefs": {"site_name": "TP2 Data Platform", "allow_tracking": False},
        "database": _postgres_database_payload(),
    }
    response = _request("POST", "/api/setup", json=payload)
    return response.json()["id"]


def _postgres_database_payload() -> dict[str, Any]:
    return {
        "engine": "postgres",
        "name": "TP2 PostgreSQL",
        "details": {
            "host": os.getenv("PGHOST", "postgres"),
            "port": int(os.getenv("PGPORT", "5432")),
            "dbname": os.getenv("PGDATABASE", "emploi"),
            "user": os.getenv("PGUSER", "postgres"),
            "password": os.getenv("PGPASSWORD", ""),
            "ssl": False,
        },
        "is_full_sync": True,
        "is_on_demand": False,
        "schedules": {},
    }


def ensure_database(token: str) -> int:
    headers = {"X-Metabase-Session": token}
    databases = _request("GET", "/api/database", headers=headers).json()
    existing = databases.get("data", databases if isinstance(databases, list) else [])
    for database in existing:
        if database.get("name") == "TP2 PostgreSQL":
            return int(database["id"])
    response = _request(
        "POST",
        "/api/database",
        headers=headers,
        json=_postgres_database_payload(),
    )
    return int(response.json()["id"])


def _ensure_card(
    token: str,
    database_id: int,
    *,
    name: str,
    query: str,
    display: str,
) -> int:
    headers = {"X-Metabase-Session": token}
    cards_response = _request("GET", "/api/card", headers=headers).json()
    cards = cards_response.get("data", cards_response)
    for card in cards:
        if card.get("name") == name and not card.get("archived"):
            return int(card["id"])

    response = _request(
        "POST",
        "/api/card",
        headers=headers,
        json={
            "name": name,
            "display": display,
            "dataset_query": {
                "database": database_id,
                "type": "native",
                "native": {"query": query, "template-tags": {}},
            },
            "visualization_settings": {},
        },
    )
    return int(response.json()["id"])


def ensure_business_dashboard(token: str, database_id: int) -> None:
    """Create the required business dashboard and its SQL questions."""
    headers = {"X-Metabase-Session": token}
    cards = [
        _ensure_card(
            token,
            database_id,
            name="TP2 - Offres par type de contrat",
            query=(
                "SELECT type_contrat, COUNT(*) AS nombre_offres "
                "FROM tp2_dashboard_offres GROUP BY type_contrat "
                "ORDER BY nombre_offres DESC"
            ),
            display="bar",
        ),
        _ensure_card(
            token,
            database_id,
            name="TP2 - Top compétences recherchées",
            query=(
                "SELECT c.libelle_competence, COUNT(*) AS occurrences "
                "FROM exigence_offre eo JOIN competence c USING (competence_id) "
                "GROUP BY c.libelle_competence ORDER BY occurrences DESC LIMIT 10"
            ),
            display="bar",
        ),
        _ensure_card(
            token,
            database_id,
            name="TP2 - Offres par commune",
            query=(
                "SELECT COALESCE(geo_nom_commune, nom_commune) AS commune, "
                "COUNT(*) AS nombre_offres FROM tp2_dashboard_offres "
                "GROUP BY 1 ORDER BY nombre_offres DESC LIMIT 15"
            ),
            display="row",
        ),
        _ensure_card(
            token,
            database_id,
            name="TP2 - Qualité Raw vs Clean",
            query="SELECT raw_total, clean_total, rejected_total FROM tp2_pipeline_latest_counts",
            display="table",
        ),
    ]

    dashboards_response = _request("GET", "/api/dashboard", headers=headers).json()
    dashboards = dashboards_response.get("data", dashboards_response)
    dashboard = next(
        (item for item in dashboards if item.get("name") == "TP2 - Marché de l'emploi"),
        None,
    )
    if dashboard is None:
        dashboard = _request(
            "POST",
            "/api/dashboard",
            headers=headers,
            json={
                "name": "TP2 - Marché de l'emploi",
                "description": "Contrats, compétences, territoires et qualité du pipeline.",
            },
        ).json()

    dashboard_id = int(dashboard["id"])
    details = _request("GET", f"/api/dashboard/{dashboard_id}", headers=headers).json()
    attached_card_ids = {
        item.get("card_id") for item in details.get("dashcards", [])
    }
    for card_id in cards:
        if card_id not in attached_card_ids:
            _request(
                "POST",
                f"/api/dashboard/{dashboard_id}/cards",
                headers=headers,
                json={"cardId": card_id},
            )


def main() -> None:
    wait_for_metabase()
    token = setup_if_needed()
    database_id = ensure_database(token)
    ensure_business_dashboard(token, database_id)
    print("Metabase provisioned with PostgreSQL and the TP2 business dashboard")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        sys.exit(f"Metabase provisioning failed: {exc}")
