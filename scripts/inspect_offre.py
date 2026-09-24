"""Utilitaire de développement : récupère une offre France Travail brute
et l'enregistre en JSON, pour inspecter manuellement sa structure et ses
champs disponibles (utile avant d'étendre src/ingest.py).

Ce script réutilise src/api_client.py : il n'implémente aucune logique
d'authentification propre.

Usage :
    python scripts/inspect_offre.py               # première offre trouvée (motsCles="data")
    python scripts/inspect_offre.py <ID_OFFRE>     # offre précise par identifiant
"""

import json
import sys
from pathlib import Path

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.api_client import (  # noqa: E402
    FranceTravailApiError,
    FranceTravailAuthError,
    FranceTravailClient,
)

load_dotenv()

# Les JSON récupérés sont des artefacts de développement, jamais versionnés
# (voir la règle scripts/output/ dans .gitignore).
OUTPUT_DIR = Path(__file__).resolve().parent / "output"


def fetch_offre(client: FranceTravailClient, offre_id: str | None) -> dict:
    """Récupère une offre par identifiant, ou la première résultant d'une recherche."""
    if offre_id:
        print(f"Récupération de l'offre {offre_id}...")
        return client.get_offre(offre_id)

    print("Recherche d'une offre récente (motsCles='data')...")
    resultats = client.search_offres(mots_cles="data", range_="0-0").get("resultats", [])
    if not resultats:
        sys.exit("Aucune offre trouvée.")
    return resultats[0]


def main() -> None:
    offre_id = sys.argv[1].strip() if len(sys.argv) > 1 else None

    try:
        client = FranceTravailClient()
        offre = fetch_offre(client, offre_id)
    except (FranceTravailAuthError, FranceTravailApiError) as exc:
        sys.exit(f"Échec de la récupération : {exc}")

    OUTPUT_DIR.mkdir(exist_ok=True)
    output_file = OUTPUT_DIR / f"{offre.get('id', 'offre')}.json"
    output_file.write_text(
        json.dumps(offre, indent=2, ensure_ascii=False), encoding="utf-8"
    )

    print(f"\n--- Offre {offre.get('id', 'inconnue')} ---")
    print(json.dumps(offre, indent=2, ensure_ascii=False))
    print(f"\nJSON sauvegardé dans {output_file}")


if __name__ == "__main__":
    main()
