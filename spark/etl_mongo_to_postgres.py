from typing import Any
import psycopg
from pymongo import MongoClient
from postgres.connection import get_connection
from api.ingest import transform_offre

TYPES_CONTRAT_VALIDES = {"CDI", "CDD", "MIS", "SAI", "CCE"}

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
    if entreprise.get("entreprise_anonyme"):
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


def transform_hellowork(job: dict) -> dict:
    import re
    # Pour HelloWork, on tente d'extraire le code postal s'il y en a un
    lieu = str(job.get("lieu", "Lieu Inconnu"))
    cp = "00000"
    if "-" in lieu:
        parts = lieu.split("-")
        if parts[-1].strip().isdigit():
            cp = parts[-1].strip().ljust(5, '0')
            
    # Traitement du type de contrat
    raw_contrat = job.get("type_contrat", "").lower()
    if "cdd" in raw_contrat:
        type_contrat = "CDD"
    elif "intérim" in raw_contrat or "freelance" in raw_contrat:
        type_contrat = "MIS"
    else:
        type_contrat = "CDI" # Fallback pour satisfaire Postgres
        
    # Traitement du salaire (Extraction numérique)
    raw_salaire = job.get("salaire", "Non spécifié")
    salaire_estime = None
    if raw_salaire != "Non spécifié":
        # On supprime tous les espaces (y compris les espaces insécables \u202f)
        clean_salaire = re.sub(r'\s+', '', raw_salaire)
        match = re.search(r'(\d{4,6})', clean_salaire)
        if match:
            salaire_estime = float(match.group(1))
            # Si c'est un salaire mensuel (ex: 2500), on le passe en annuel
            if salaire_estime < 10000:
                salaire_estime *= 12
    
    desc = f"Lien vers l'offre : {job.get('lien', '')}"
    if raw_salaire != "Non spécifié":
        desc += f"\nSalaire indiqué : {raw_salaire}"
    if job.get("type_contrat") not in [None, "Non spécifié"]:
        desc += f"\nContrat indiqué : {job.get('type_contrat')}"
        
    return {
        "source_offre_id": "HW-" + str(job.get("_id", "0"))[-15:],
        "libelle_poste": str(job.get("titre", "Titre inconnu"))[:150],
        "description": desc,
        "date_publication": str(job.get("date_extraction", "2024-01-01"))[:10],
        "type_contrat": type_contrat,
        "duree_travail": "Temps plein",
        "salaire_brut_annuel_estime": salaire_estime,
        "rome_code": "Z9999",
        "rome_libelle": "Métier non catégorisé",
        "domaine_professionnel": "Secteur Indéterminé",
        "entreprise": {
            "raison_sociale": str(job.get("entreprise") or "Entreprise Inconnue").strip() or "Entreprise Inconnue",
            "entreprise_anonyme": False
        },
        "commune": {
            "code_insee": "99999",
            "code_postal": cp,
            "nom_commune": lieu[:100],
            "latitude": None,
            "longitude": None
        },
        "competences": []
    }


def load_transformed_offres(offres: list[dict[str, Any]]) -> int:
    nombre_traitees = 0
    with get_connection() as connection:
        with connection.cursor() as cur:
            for offre in offres:
                if (
                    not offre["rome_code"]
                    or not offre["commune"]
                    or not offre["date_publication"]
                    or offre["type_contrat"] not in TYPES_CONTRAT_VALIDES
                ):
                    continue

                _upsert_commune(cur, offre["commune"])
                _upsert_metier_rome(cur, offre)
                entreprise_id = _upsert_entreprise(cur, offre["entreprise"])

                cur.execute(
                    """
                    INSERT INTO offre (
                        source_offre_id, libelle_poste, description, date_publication,
                        type_contrat, duree_travail, salaire_brut_annuel_estime,
                        rome_code, entreprise_id, code_insee
                    ) VALUES (
                        %(source_offre_id)s, %(libelle_poste)s, %(description)s,
                        %(date_publication)s, %(type_contrat)s, %(duree_travail)s,
                        %(salaire_brut_annuel_estime)s, %(rome_code)s,
                        %(entreprise_id)s, %(code_insee)s
                    )
                    ON CONFLICT (source_offre_id) DO UPDATE SET
                        libelle_poste = EXCLUDED.libelle_poste,
                        description = EXCLUDED.description,
                        date_publication = EXCLUDED.date_publication,
                        type_contrat = EXCLUDED.type_contrat,
                        duree_travail = EXCLUDED.duree_travail,
                        salaire_brut_annuel_estime = EXCLUDED.salaire_brut_annuel_estime,
                        rome_code = EXCLUDED.rome_code,
                        entreprise_id = EXCLUDED.entreprise_id,
                        code_insee = EXCLUDED.code_insee
                    RETURNING offre_id;
                    """,
                    {
                        "source_offre_id": offre["source_offre_id"],
                        "libelle_poste": offre["libelle_poste"],
                        "description": offre["description"],
                        "date_publication": offre["date_publication"],
                        "type_contrat": offre["type_contrat"],
                        "duree_travail": offre["duree_travail"],
                        "salaire_brut_annuel_estime": offre["salaire_brut_annuel_estime"],
                        "rome_code": offre["rome_code"],
                        "entreprise_id": entreprise_id,
                        "code_insee": offre["commune"]["code_insee"],
                    },
                )
                offre_id = cur.fetchone()[0]

                cur.execute("DELETE FROM exigence_offre WHERE offre_id = %s;", (offre_id,))
                for competence in offre["competences"]:
                    competence_id = _upsert_competence(cur, competence["libelle"])
                    cur.execute(
                        """
                        INSERT INTO exigence_offre (offre_id, competence_id, statut_exigence)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (offre_id, competence_id) DO UPDATE SET
                            statut_exigence = EXCLUDED.statut_exigence;
                        """,
                        (offre_id, competence_id, competence["statut_exigence"]),
                    )

                nombre_traitees += 1

        connection.commit()
    return nombre_traitees


def run_etl():
    print("🚀 Démarrage de l'ETL : MongoDB -> PostgreSQL")
    
    mongo_client = MongoClient("mongodb://localhost:27017/")
    db = mongo_client["emploi_datalake"]
    collection = db["raw_jobs"]
    
    total_docs = collection.count_documents({})
    print(f"📦 {total_docs} offres trouvées dans le Data Lake MongoDB.")
    
    transformed_jobs = []
    
    for job in collection.find():
        source = job.get("source", "inconnu")
        
        if source == "france_travail":
            try:
                transformed = transform_offre(job)
                transformed_jobs.append(transformed)
            except Exception as e:
                print(f"Erreur transformation FT: {e}")
        elif source == "hellowork":
            transformed = transform_hellowork(job)
            transformed_jobs.append(transformed)
            
    print(f"🔄 Transformation terminée. {len(transformed_jobs)} offres HelloWork prêtes pour PostgreSQL.")
    
    if transformed_jobs:
        inserted = load_transformed_offres(transformed_jobs)
        print(f"✅ {inserted} offres insérées avec succès dans PostgreSQL !")

if __name__ == "__main__":
    run_etl()
