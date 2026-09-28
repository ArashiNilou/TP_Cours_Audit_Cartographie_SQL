"""Extraction, nettoyage et chargement des offres France Travail en base.

Ce module transforme les objets JSON semi-structurés renvoyés par l'API
« Offres d'emploi v2 » en lignes normalisées conformes au schéma 3NF défini
dans sql/01_schema.sql (commune, entreprise, metier_rome, competence, offre,
exigence_offre). Les insertions sont idempotentes (ON CONFLICT) afin de
pouvoir rejouer une synchronisation sans dupliquer les données.
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any, Iterable

import psycopg

from .api_client import FranceTravailClient
from postgres.connection import get_connection

logger = logging.getLogger(__name__)

# Liste des 101 départements français pour le partitionnement de la collecte globale
DEPARTEMENTS_FRANCE: list[str] = [
    f"{i:02d}" for i in range(1, 96) if i != 20
] + ["2A", "2B", "971", "972", "973", "974", "976"]

# Capture les nombres décimaux (à virgule ou point) présents dans un texte
# libre de salaire, par exemple "Annuel de 38000.0 Euros à 45000.0 Euros".
_SALAIRE_NOMBRE_RE = re.compile(r"(\d+(?:[.,]\d+)?)")

# Types de contrat acceptés par la contrainte CHECK ck_offre_type_contrat_valide.
TYPES_CONTRAT_VALIDES = {"CDI", "CDD", "MIS", "SAI", "CCE"}


# Grands domaines professionnels de la nomenclature ROME 4.0 (France Travail),
# indexés par la première lettre du code ROME. Utilisé en dernier recours pour
# alimenter metier_rome.domaine_professionnel lorsque l'API ne fournit pas
# directement ce libellé : le champ "secteurActiviteLibelle" de l'API décrit
# le secteur d'activité de l'entreprise recruteuse (NAF), une notion distincte
# du domaine professionnel du métier, et ne doit donc pas être utilisé ici.
ROME_GRANDS_DOMAINES: dict[str, str] = {
    "A": "Arts et façonnage d'ouvrages d'art",
    "B": "Arts et façonnage d'ouvrages d'art",
    "C": "Banque, assurance, immobilier",
    "D": "Commerce, vente et grande distribution",
    "E": "Communication, média et multimédia",
    "F": "Construction, bâtiment et travaux publics",
    "G": "Hôtellerie-restauration, tourisme, loisirs et animation",
    "H": "Industrie",
    "I": "Installation et maintenance",
    "J": "Santé",
    "K": "Services à la personne et à la collectivité",
    "L": "Spectacle",
    "M": "Support à l'entreprise",
    "N": "Transport et logistique",
}


def resolve_domaine_professionnel(rome_code: str | None) -> str:
    """Déduit le grand domaine professionnel ROME à partir du code métier.

    Le domaine est déterminé par la première lettre du code ROME (ex. "M1805"
    -> domaine M "Support à l'entreprise"), conformément à la nomenclature
    officielle des 14 grands domaines professionnels France Travail.
    """
    if not rome_code:
        return "Non renseigné"
    return ROME_GRANDS_DOMAINES.get(rome_code[0].upper(), "Non renseigné")


def parse_salaire_annuel(libelle: str | None) -> float | None:
    """Extrait un salaire brut annuel estimé à partir d'un texte libre.

    Règle métier : lorsque deux bornes sont présentes, on retient leur
    moyenne. Lorsque la périodicité est mensuelle, la valeur est ramenée à
    un équivalent annuel (x12). En l'absence de nombre exploitable, la
    fonction retourne None plutôt qu'une valeur arbitraire.
    """
    if not libelle:
        return None

    nombres = [
        float(valeur.replace(",", "."))
        for valeur in _SALAIRE_NOMBRE_RE.findall(libelle)
    ]
    if not nombres:
        return None

    moyenne = sum(nombres) / len(nombres)
    if "mensuel" in libelle.lower():
        moyenne *= 12
    return round(moyenne, 2)


def _extract_commune(offre_json: dict[str, Any]) -> dict[str, Any] | None:
    lieu = offre_json.get("lieuTravail") or {}
    code_insee = lieu.get("commune")
    if not code_insee:
        return None
    nom = (lieu.get("libelle") or code_insee)[:100]
    return {
        "code_insee": code_insee[:5],
        "code_postal": (lieu.get("codePostal") or code_insee[:2] + "000")[:5],
        "nom_commune": nom,
        "latitude": lieu.get("latitude"),
        "longitude": lieu.get("longitude"),
    }


def _extract_entreprise(offre_json: dict[str, Any]) -> dict[str, Any]:
    import json
    entreprise = offre_json.get("entreprise")
    if isinstance(entreprise, str):
        try:
            entreprise = json.loads(entreprise)
        except Exception:
            entreprise = {"nom": entreprise}
    entreprise = entreprise or {}
    nom = (entreprise.get("nom") or "").strip()[:250]
    return {
        "raison_sociale": nom or None,
        "entreprise_anonyme": not bool(nom),
    }


def _extract_competences(offre_json: dict[str, Any]) -> list[dict[str, str]]:
    competences = []
    for item in offre_json.get("competences") or []:
        libelle = (item.get("libelle") or "").strip()[:300]
        exigence = item.get("exigence")
        if not libelle:
            continue
        # L'API renvoie parfois "E"/"S" et parfois "Exigée"/"Souhaitée".
        statut = "E" if str(exigence).upper().startswith("E") else "S"
        competences.append({"libelle": libelle, "statut_exigence": statut})
    return competences


def transform_offre(offre_json: dict[str, Any]) -> dict[str, Any]:
    """Convertit une offre JSON brute en dictionnaire prêt pour le chargement."""
    duree = (
        offre_json.get("dureeTravailLibelleConverti")
        or offre_json.get("dureeTravailLibelle")
    )
    rome_lib = (
        offre_json.get("romeLibelle")
        or offre_json.get("romeCode")
        or ""
    )[:250]

    return {
        "source_offre_id": str(offre_json["id"])[:20],
        "libelle_poste": offre_json.get("intitule", "")[:200],
        "description": offre_json.get("description"),
        "date_publication": (offre_json.get("dateCreation") or "")[:10] or None,
        "type_contrat": (offre_json.get("typeContrat") or "")[:5],
        "duree_travail": duree[:100] if duree else None,
        "salaire_brut_annuel_estime": parse_salaire_annuel(
            (json.loads(offre_json.get("salaire")) if isinstance(offre_json.get("salaire"), str) else (offre_json.get("salaire") or {})).get("libelle")
        ) if offre_json.get("salaire") else None,
        "rome_code": offre_json.get("romeCode"),
        "rome_libelle": rome_lib,
        "domaine_professionnel": resolve_domaine_professionnel(
            offre_json.get("romeCode")
        )[:150],
        "commune": _extract_commune(offre_json),
        "entreprise": _extract_entreprise(offre_json),
        "competences": _extract_competences(offre_json),
    }



def _upsert_commune(cur: psycopg.Cursor, commune: dict[str, Any]) -> None:
    cur.execute(
        """
        INSERT INTO commune (code_insee, code_postal, nom_commune, latitude, longitude)
        VALUES (%(code_insee)s, %(code_postal)s, %(nom_commune)s, %(latitude)s, %(longitude)s)
        ON CONFLICT (code_insee) DO UPDATE SET
            code_postal = EXCLUDED.code_postal,
            nom_commune = EXCLUDED.nom_commune,
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude;
        """,
        commune,
    )


def _upsert_entreprise(cur: psycopg.Cursor, entreprise: dict[str, Any]) -> int:
    if entreprise["entreprise_anonyme"]:
        cur.execute(
            "INSERT INTO entreprise (raison_sociale, entreprise_anonyme) "
            "VALUES (NULL, TRUE) RETURNING entreprise_id;"
        )
        return cur.fetchone()[0]

    cur.execute(
        "SELECT entreprise_id FROM entreprise WHERE raison_sociale = %(raison_sociale)s;",
        entreprise,
    )
    row = cur.fetchone()
    if row:
        return row[0]

    cur.execute(
        "INSERT INTO entreprise (raison_sociale, entreprise_anonyme) "
        "VALUES (%(raison_sociale)s, FALSE) RETURNING entreprise_id;",
        entreprise,
    )
    return cur.fetchone()[0]


def _upsert_metier_rome(cur: psycopg.Cursor, offre: dict[str, Any]) -> None:
    cur.execute(
        """
        INSERT INTO metier_rome (rome_code, libelle_fiche_metier, domaine_professionnel)
        VALUES (%(rome_code)s, %(rome_libelle)s, %(domaine_professionnel)s)
        ON CONFLICT (rome_code) DO UPDATE SET
            libelle_fiche_metier = EXCLUDED.libelle_fiche_metier,
            domaine_professionnel = EXCLUDED.domaine_professionnel;
        """,
        offre,
    )


def _upsert_competence(cur: psycopg.Cursor, libelle: str) -> int:
    cur.execute(
        """
        INSERT INTO competence (libelle_competence, type_competence)
        VALUES (%(libelle)s, 'Savoir-faire')
        ON CONFLICT (libelle_competence) DO UPDATE SET libelle_competence = EXCLUDED.libelle_competence
        RETURNING competence_id;
        """,
        {"libelle": libelle},
    )
    return cur.fetchone()[0]



from pymongo import MongoClient

def load_offres(offres_json: Iterable[dict[str, Any]]) -> int:
    """Charge une collection d'offres JSON brute dans MongoDB (Data Lake)."""
    if not offres_json:
        return 0
        
    import os
    client = MongoClient(os.getenv("MONGO_URI", "mongodb://localhost:27017/"))
    db = client["emploi_datalake"]
    collection = db["raw_jobs"]
    
    docs_to_insert = []
    for offre in offres_json:
        # On injecte la metadata "source"
        offre["source"] = "france_travail"
        
        # On s'assure qu'on ne duplique pas bêtement (utiliser l'id de France Travail)
        # Mais pour le datalake pur, insert_one ou insert_many basique suffit.
        docs_to_insert.append(offre)
        
    if docs_to_insert:
        collection.insert_many(docs_to_insert)
        
    client.close()
    return len(docs_to_insert)


def sync_from_api(
    mots_cles: str | None = None,
    code_rome: str | None = None,
    commune: str | None = None,
    departement: str | None = None,
    range_: str | None = None,
    paginate: bool = False,
    max_results: int | None = None,
) -> int:
    """Interroge l'API France Travail et charge les résultats en base.

    - Si paginate=True : parcourt automatiquement toutes les pages (par tranches
      de 150) jusqu'à épuisement ou max_results (plafond API à 1149).
    - Sinon : effectue une seule requête sur la plage range_ (par défaut '0-49').

    Retourne le nombre d'offres chargées.
    """
    client = FranceTravailClient()

    if paginate:
        offres = client.search_all_offres(
            mots_cles=mots_cles,
            code_rome=code_rome,
            commune=commune,
            departement=departement,
            max_results=max_results,
        )
    else:
        effective_range = range_ or "0-49"
        payload = client.search_offres(
            mots_cles=mots_cles,
            code_rome=code_rome,
            commune=commune,
            departement=departement,
            range_=effective_range,
        )
        offres = payload.get("resultats", [])

    return load_offres(offres)


def sync_all_departements(
    departements: list[str] | None = None,
    mots_cles: str | None = None,
    code_rome: str | None = None,
    max_per_departement: int | None = None,
) -> int:
    """Parcourt les départements pour récupérer et charger toutes les offres.

    Contourne le plafond des 1149 offres de France Travail en partitionnant la collecte
    sur les 101 départements français (ou la sous-liste fournie).

    Retourne le nombre total d'offres insérées/mises à jour.
    """
    client = FranceTravailClient()
    deps = departements or DEPARTEMENTS_FRANCE
    total_charges = 0

    print(f"Démarrage de la synchronisation sur {len(deps)} département(s)...")
    for index, dep in enumerate(deps, start=1):
        try:
            offres = client.search_all_offres(
                mots_cles=mots_cles,
                code_rome=code_rome,
                departement=dep,
                max_results=max_per_departement,
            )
            nb = load_offres(offres) if offres else 0
            total_charges += nb
            print(
                f"[{index:03d}/{len(deps):03d}] Dépt {dep:3s} : "
                f"{len(offres):4d} trouvée(s) -> {nb:4d} chargée(s) (Cumul: {total_charges})"
            )
        except Exception as err:
            logger.warning("Erreur département %s : %s", dep, err)
            print(f"[{index:03d}/{len(deps):03d}] Dépt {dep:3s} : Erreur ({err})")

    return total_charges

