"""Extraction, nettoyage et chargement des offres France Travail en base.

Ce module transforme les objets JSON semi-structurés renvoyés par l'API
« Offres d'emploi v2 » en lignes normalisées conformes au schéma 3NF défini
dans sql/01_schema.sql (commune, entreprise, metier_rome, competence, offre,
exigence_offre). Les insertions sont idempotentes (ON CONFLICT) afin de
pouvoir rejouer une synchronisation sans dupliquer les données.
"""

from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING, Any, Iterable

from .api_client import FranceTravailClient

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    import psycopg

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
        "code_postal": (lieu.get("codePostal") or "00000")[:5],
        "nom_commune": nom,
        "latitude": lieu.get("latitude"),
        "longitude": lieu.get("longitude"),
    }


def _extract_entreprise(offre_json: dict[str, Any]) -> dict[str, Any]:
    entreprise = offre_json.get("entreprise") or {}
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
            (offre_json.get("salaire") or {}).get("libelle")
        ),
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
            "SELECT entreprise_id FROM entreprise "
            "WHERE entreprise_anonyme = TRUE AND raison_sociale IS NULL "
            "ORDER BY entreprise_id LIMIT 1;"
        )
        row = cur.fetchone()
        if row:
            return row[0]
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


def load_offres(
    offres_json: Iterable[dict[str, Any]],
    *,
    rejected_ids: list[str] | None = None,
) -> int:
    """Charge une collection d'offres JSON dans le schéma 3NF PostgreSQL.

    Retourne le nombre d'offres effectivement insérées ou mises à jour.
    """
    from .connection import get_connection

    nombre_traitees = 0
    with get_connection() as connection:
        with connection.cursor() as cur:
            for offre_json in offres_json:
                cur.execute("SAVEPOINT offre_savepoint;")
                try:
                    offre = transform_offre(offre_json)

                    if (
                        not offre["libelle_poste"].strip()
                        or not offre["rome_code"]
                        or not offre["commune"]
                        or not offre["date_publication"]
                        or offre["type_contrat"] not in TYPES_CONTRAT_VALIDES
                    ):
                        if rejected_ids is not None:
                            rejected_ids.append(str(offre_json.get("id", "inconnue")))
                        cur.execute("RELEASE SAVEPOINT offre_savepoint;")
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

                    cur.execute(
                        "DELETE FROM exigence_offre WHERE offre_id = %s;", (offre_id,)
                    )
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

                    cur.execute("RELEASE SAVEPOINT offre_savepoint;")
                    nombre_traitees += 1
                except Exception:
                    cur.execute("ROLLBACK TO SAVEPOINT offre_savepoint;")
                    cur.execute("RELEASE SAVEPOINT offre_savepoint;")
                    logger.exception(
                        "Offre %s rejetée pendant le chargement PostgreSQL",
                        offre_json.get("id", "inconnue"),
                    )
                    if rejected_ids is not None:
                        rejected_ids.append(str(offre_json.get("id", "inconnue")))

        connection.commit()
    return nombre_traitees


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
