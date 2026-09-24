"""Affiche et sauvegarde le contenu JSON complet d'une offre France Travail.

Usage :
    python inspect_offre.py               # Récupère la première offre trouvée
    python inspect_offre.py <ID_OFFRE>    # Récupère une offre précise par son ID
"""

import json
import os
import sys

import requests
from dotenv import load_dotenv

load_dotenv(override=True)

TOKEN_URL = "https://entreprise.francetravail.fr/connexion/oauth2/access_token"
BASE_URL = "https://api.francetravail.io/partenaire/offresdemploi/v2/offres"

client_id = os.environ.get("FT_CLIENT_ID", "").strip().strip("'\"")
client_secret = os.environ.get("FT_CLIENT_SECRET", "").strip().strip("'\"")

if not client_id or not client_secret:
    sys.exit("Erreur : FT_CLIENT_ID ou FT_CLIENT_SECRET introuvables dans le fichier .env.")

# 1. Obtention du token OAuth2
print("Authentification en cours...")
r_token = requests.post(
    TOKEN_URL,
    params={"realm": "/partenaire"},
    data={
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": f"application_{client_id} api_offresdemploiv2 o2dsoffre",
    },
    timeout=30,
)

if not r_token.ok:
    sys.exit(f"Erreur token ({r_token.status_code}) : {r_token.text}")

token = r_token.json()["access_token"]
headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

# 2. Récupération de l'offre
if len(sys.argv) > 1:
    # Si un identifiant est fourni en argument
    offre_id = sys.argv[1].strip()
    print(f"Récupération de l'offre ID : {offre_id}...")
    r = requests.get(f"{BASE_URL}/{offre_id}", headers=headers, timeout=30)
    if not r.ok:
        sys.exit(f"Erreur offre ({r.status_code}) : {r.text}")
    offre = r.json()
else:
    # Sinon, on prend la 1ère offre issue d'une recherche
    print("Recherche d'une offre récente (motsCles='data')...")
    r = requests.get(
        f"{BASE_URL}/search",
        headers=headers,
        params={"motsCles": "data", "range": "0-0"},
        timeout=30,
    )
    if not r.ok:
        sys.exit(f"Erreur recherche ({r.status_code}) : {r.text}")
    resultats = r.json().get("resultats", [])
    if not resultats:
        sys.exit("Aucune offre trouvée.")
    offre = resultats[0]

# 3. Sauvegarde dans un fichier pour inspection facile dans l'éditeur
output_file = "offre.json"
with open(output_file, "w", encoding="utf-8") as f:
    json.dump(offre, f, indent=2, ensure_ascii=False)

# 4. Affichage formaté dans le terminal
print(f"\n--- Contenu JSON (ID: {offre.get('id', 'Inconnu')}) ---")
print(json.dumps(offre, indent=2, ensure_ascii=False))
print(f"\nJSON complet sauvegardé dans [offre.json] à la racine.")
