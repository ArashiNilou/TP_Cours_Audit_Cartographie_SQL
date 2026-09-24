"""Exemple minimal : API Offres d'emploi v2 de France Travail.

Usage :
    pip install requests
    export FT_CLIENT_ID="ton_identifiant"     # Windows PowerShell : $env:FT_CLIENT_ID="..."
    export FT_CLIENT_SECRET="ta_cle_secrete"
    python exemple_offres.py
"""
import csv
import os
import sys

import requests

TOKEN_URL = "https://entreprise.francetravail.fr/connexion/oauth2/access_token"
SEARCH_URL = "https://api.francetravail.io/partenaire/offresdemploi/v2/offres/search"

client_id = os.environ.get("FT_CLIENT_ID")
client_secret = os.environ.get("FT_CLIENT_SECRET")
if not client_id or not client_secret:
    sys.exit("Définis FT_CLIENT_ID et FT_CLIENT_SECRET dans tes variables d'environnement.")

# 1. Token OAuth2 (client credentials)
r = requests.post(
    TOKEN_URL,
    params={"realm": "/partenaire"},
    data={
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": "api_offresdemploiv2 o2dsoffre",
    },
    timeout=30,
)
r.raise_for_status()
token = r.json()["access_token"]

# 2. Recherche (modifie les paramètres à ta guise)
params = {"motsCles": "data analyst", "departement": "75", "range": "0-149"}
r = requests.get(
    SEARCH_URL,
    headers={"Authorization": f"Bearer {token}"},
    params=params,
    timeout=30,
)

if r.status_code == 204:
    sys.exit("Aucune offre trouvée pour ces critères.")
r.raise_for_status()  # 200 et 206 (résultats partiels/paginés) sont normaux

offres = r.json().get("resultats", [])
print(f"{len(offres)} offres récupérées\n")

for o in offres[:5]:
    lieu = (o.get("lieuTravail") or {}).get("libelle", "")
    print(f"- {o.get('intitule')} | {o.get('typeContrat')} | {lieu}")

# 3. Export CSV
with open("offres.csv", "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["id", "intitule", "typeContrat", "lieu", "dateCreation", "url"])
    for o in offres:
        w.writerow([
            o.get("id"),
            o.get("intitule"),
            o.get("typeContrat"),
            (o.get("lieuTravail") or {}).get("libelle"),
            o.get("dateCreation"),
            (o.get("origineOffre") or {}).get("urlOrigine"),
        ])
print("\nExport écrit dans offres.csv")