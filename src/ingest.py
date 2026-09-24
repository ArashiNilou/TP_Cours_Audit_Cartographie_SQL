"""Extraction, nettoyage et chargement des offres France Travail en base.

Ce module transforme les objets JSON semi-structurés renvoyés par l'API
« Offres d'emploi v2 » en lignes normalisées conformes au schéma 3NF défini
dans sql/01_schema.sql (commune, entreprise, metier_rome, competence, offre,
exigence_offre). Les insertions sont idempotentes (ON CONFLICT) afin de
pouvoir rejouer une synchronisation sans dupliquer les données.
"""

from __future__ import annotations

import re
from typing import Any, Iterable

import psycopg

from .api_client import FranceTravailClient
from .connection import get_connection

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
    """Construit la ligne `commune` à partir du bloc `lieuTravail` de l'offre.

    Retourne None si aucun code INSEE n'est fourni : l'offre sera alors
    rejetée par `load_offres` (le rattachement géographique est obligatoire).
    """
    lieu = offre_json.get("lieuTravail") or {}
    code_insee = lieu.get("commune")
    if not code_insee:
        return None
    # Repli sur un code postal par défaut lorsque l'API ne le fournit pas.
    # Les codes INSEE corses (2A/2B) ne sont pas numériques : dans ce cas,
    # on utilise un code postal neutre plutôt qu'une valeur qui violerait
    # la contrainte CHECK ck_commune_code_postal_format (chiffres uniquement).
    code_postal_repli = (
        code_insee[:2] + "000" if code_insee[:2].isdigit() else "00000"
    )
    return {
        "code_insee": code_insee,
        "code_postal": lieu.get("codePostal") or code_postal_repli,
        "nom_commune": lieu.get("libelle") or code_insee,
        "latitude": lieu.get("latitude"),
        "longitude": lieu.get("longitude"),
    }


def _extract_entreprise(offre_json: dict[str, Any]) -> dict[str, Any]:
    """Construit la ligne `entreprise`, en marquant l'anonymat si le nom est absent."""
    entreprise = offre_json.get("entreprise") or {}
    nom = entreprise.get("nom")
    return {
        "raison_sociale": nom,
        "entreprise_anonyme": not bool(nom),
    }


def _extract_competences(offre_json: dict[str, Any]) -> list[dict[str, str]]:
    """Normalise le tableau `competences[]` de l'offre en statuts E/S exploitables."""
    competences = []
    for item in offre_json.get("competences") or []:
        libelle = (item.get("libelle") or "").strip()
        exigence = item.get("exigence")
        if not libelle:
            continue
        # L'API renvoie parfois "E"/"S" et parfois "Exigée"/"Souhaitée".
        statut = "E" if str(exigence).upper().startswith("E") else "S"
        competences.append({"libelle": libelle, "statut_exigence": statut})
    return competences


def transform_offre(offre_json: dict[str, Any]) -> dict[str, Any]:
    """Convertit une offre JSON brute en dictionnaire prêt pour le chargement."""
    return {
        "source_offre_id": offre_json["id"],
        "libelle_poste": offre_json.get("intitule", "")[:200],
        "description": offre_json.get("description"),
        "date_publication": (offre_json.get("dateCreation") or "")[:10] or None,
        "type_contrat": (offre_json.get("typeContrat") or "")[:5],
        "duree_travail": offre_json.get("dureeTravailLibelleConverti")
        or offre_json.get("dureeTravailLibelle"),
        "salaire_brut_annuel_estime": parse_salaire_annuel(
            (offre_json.get("salaire") or {}).get("libelle")
        ),
        "rome_code": offre_json.get("romeCode"),
        "rome_libelle": offre_json.get("romeLibelle") or offre_json.get("romeCode"),
        "domaine_professionnel": resolve_domaine_professionnel(
            offre_json.get("romeCode")
        ),
        "commune": _extract_commune(offre_json),
        "entreprise": _extract_entreprise(offre_json),
        "competences": _extract_competences(offre_json),
    }


def _upsert_commune(cur: psycopg.Cursor, commune: dict[str, Any]) -> None:
    """Insère ou met à jour une commune (référentiel INSEE/BAN) par code_insee."""
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
    """Résout l'entreprise_id, en créant l'entreprise si elle est inconnue.

    Chaque entreprise anonyme donne lieu à une nouvelle ligne (aucune notion
    d'identité à dédupliquer). Pour une entreprise nommée, la déduplication
    se fait par égalité stricte de `raison_sociale` : deux libellés différant
    par la casse ou des espaces créeront deux entreprises distinctes (limite
    acceptée en l'absence de référentiel SIREN dans le JSON de l'API).
    """
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
    """Insère ou met à jour la fiche métier ROME associée à l'offre."""
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
    """Résout le competence_id, en créant la compétence si son libellé est inédit.

    Le type est fixé à 'Savoir-faire' par défaut : l'API ne distingue pas
    explicitement savoir-faire et savoir-être dans le tableau `competences[]`.
    """
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


def _load_single_offre(cur: psycopg.Cursor, offre: dict[str, Any]) -> None:
    """Insère ou met à jour une offre déjà transformée et ses dépendances.

    Suppose que `offre` a déjà passé les contrôles de complétude de
    `load_offres` (rome_code, commune, date_publication, type_contrat).
    """
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


def load_offres(offres_json: Iterable[dict[str, Any]]) -> int:
    """Charge une collection d'offres JSON dans le schéma 3NF PostgreSQL.

    Chaque offre est isolée dans un SAVEPOINT : une erreur inattendue (par
    exemple une violation de contrainte non détectée par le filtre de
    complétude ci-dessous) ne fait rollback que de cette offre et n'annule
    pas les offres déjà traitées dans le même lot.

    Retourne le nombre d'offres effectivement insérées ou mises à jour.
    """
    nombre_traitees = 0
    with get_connection() as connection:
        with connection.cursor() as cur:
            for offre_json in offres_json:
                offre = transform_offre(offre_json)

                if (
                    not offre["rome_code"]
                    or not offre["commune"]
                    or not offre["date_publication"]
                    or offre["type_contrat"] not in TYPES_CONTRAT_VALIDES
                ):
                    # Offre incomplète ou non conforme aux contraintes CHECK
                    # du schéma : rejetée par la règle d'audit qualité
                    # (code ROME, commune, date ou type de contrat invalides).
                    continue

                cur.execute("SAVEPOINT offre_courante;")
                try:
                    _load_single_offre(cur, offre)
                except psycopg.Error as exc:
                    # Une offre non conforme à une contrainte imprévue ne doit
                    # pas faire échouer tout le lot : on l'ignore et on log.
                    cur.execute("ROLLBACK TO SAVEPOINT offre_courante;")
                    print(
                        f"Offre {offre['source_offre_id']} ignorée "
                        f"(erreur base de données) : {exc}"
                    )
                    continue
                else:
                    cur.execute("RELEASE SAVEPOINT offre_courante;")
                    nombre_traitees += 1

        connection.commit()
    return nombre_traitees


def sync_from_api(
    mots_cles: str | None = None,
    code_rome: str | None = None,
    commune: str | None = None,
    range_: str = "0-49",
) -> int:
    """Interroge l'API France Travail et charge les résultats en base.

    Retourne le nombre d'offres chargées.
    """
    client = FranceTravailClient()
    payload = client.search_offres(
        mots_cles=mots_cles, code_rome=code_rome, commune=commune, range_=range_
    )
    offres = payload.get("resultats", [])
    return load_offres(offres)
