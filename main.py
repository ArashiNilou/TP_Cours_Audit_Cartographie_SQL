"""API Offres d'emploi v2 de France Travail : token + recherche + export CSV.

Prérequis (dans ton venv) :
    pip install requests python-dotenv      # ou : uv add requests python-dotenv

Fichier .env à côté du script (sans guillemets, sans espaces autour du "=") :
    FT_CLIENT_ID=ton_identifiant
    FT_CLIENT_SECRET=ta_cle_secrete
"""
import csv
import os
import sys

import requests
from dotenv import load_dotenv

load_dotenv(override=True)

TOKEN_URL = "https://entreprise.francetravail.fr/connexion/oauth2/access_token"
SEARCH_URL = "https://api.francetravail.io/partenaire/offresdemploi/v2/offres/search"

# .strip() enlève espaces, retours à la ligne et guillemets parasites
client_id = os.environ.get("FT_CLIENT_ID", "").strip().strip("'\"")
client_secret = os.environ.get("FT_CLIENT_SECRET", "").strip().strip("'\"")
if not client_id or not client_secret:
    sys.exit("FT_CLIENT_ID / FT_CLIENT_SECRET introuvables : vérifie ton fichier .env.")

# Vérification sans afficher la clé
print(f"client_id : {client_id[:12]}... ({len(client_id)} caractères)")
print(f"client_secret : {len(client_secret)} caractères\n")

# 1. Token OAuth2 (client credentials)
r = requests.post(
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
if not r.ok:
    print("Erreur token :", r.status_code, r.text)
    sys.exit(1)
token = r.json()["access_token"]
print("Token obtenu.\n")

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
if r.status_code not in (200, 206):  # 206 = résultats partiels/paginés, normal
    print("Erreur recherche :", r.status_code, r.text)
    sys.exit(1)

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