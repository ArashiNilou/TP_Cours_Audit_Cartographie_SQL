"""Point d'entrée du TP : vérifie la connexion PostgreSQL, initialise la base ou synchronise les offres.

Usage :
    python main.py                          # vérifie simplement la connexion
    python main.py --init                   # recrée le schéma et charge le jeu de données
    python main.py --sync-api               # récupère une tranche d'offres (défaut 0-49)
    python main.py --sync-api --paginate    # récupère TOUTES les offres de la recherche (jusqu'à 1149)
    python main.py --sync-api --departement 75 --paginate
    python main.py --sync-all               # récupère toutes les offres de France (parcourt les 101 départements)
    python main.py --sync-all --mots-cles "data" --max-per-dep 200
"""

import argparse

from dotenv import load_dotenv

from src.connection import get_connection
from src.ingest import sync_all_departements, sync_from_api
from src.schema import initialize_database

load_dotenv()


def check_connection() -> None:
    """Ouvre une connexion et affiche l'état et les données réelles stockées en base."""
    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT current_database(), current_user;")
            database, user = cursor.fetchone()
            print(f"Connexion PostgreSQL réussie : base={database}, utilisateur={user}\n")

            # Volumétrie des tables principales
            tables = ["offre", "entreprise", "commune", "metier_rome", "competence", "exigence_offre"]
            print("--- Volumétrie des données en base (volume) ---")
            for t in tables:
                cursor.execute(f"SELECT count(*) FROM {t};")
                cnt = cursor.fetchone()[0]
                print(f"  * {t:15s} : {cnt:7,d} ligne(s)".replace(",", " "))

            # Aperçu des 3 dernières offres réelles
            cursor.execute("""
                SELECT o.source_offre_id, o.libelle_poste, o.type_contrat, c.nom_commune,
                       o.salaire_brut_annuel_estime, e.raison_sociale
                FROM offre o
                LEFT JOIN commune c ON c.code_insee = o.code_insee
                LEFT JOIN entreprise e ON e.entreprise_id = o.entreprise_id
                ORDER BY o.offre_id DESC
                LIMIT 3;
            """)
            dernieres = cursor.fetchall()
            if dernieres:
                print("\n--- Dernières offres enregistrées ---")
                for src_id, poste, contrat, ville, salaire, ent in dernieres:
                    sal_str = f"{salaire:,.0f} €/an".replace(",", " ") if salaire else "Non renseigné"
                    ent_str = ent or "Entreprise confidentielle"
                    print(f"  [{src_id}] {poste}")
                    print(f"    -> {contrat} | {ville} | {sal_str} | {ent_str}")
            print()



def main() -> None:
    parser = argparse.ArgumentParser(
        description="Outils de gestion de la base PostgreSQL du domaine Emploi."
    )
    parser.add_argument(
        "--init",
        action="store_true",
        help="Recrée le schéma (sql/01_schema.sql) et charge le jeu de données "
        "de test (sql/02_seed.sql).",
    )
    parser.add_argument(
        "--sync-api",
        action="store_true",
        help="Interroge l'API France Travail (Offres d'emploi v2) et charge "
        "les offres récupérées dans le schéma 3NF.",
    )
    parser.add_argument(
        "--sync-all",
        action="store_true",
        help="Lance une collecte globale sur tous les départements français.",
    )
    parser.add_argument(
        "--paginate",
        action="store_true",
        help="Active la pagination automatique par tranches de 150 pour récupérer "
        "toutes les offres disponibles pour la recherche (jusqu'à 1149 max).",
    )
    parser.add_argument(
        "--mots-cles",
        help="Filtre de recherche par mots-clés (ex. 'data engineer').",
    )
    parser.add_argument(
        "--code-rome",
        help="Filtre de recherche par code ROME (ex. 'M1805').",
    )
    parser.add_argument(
        "--commune",
        help="Filtre de recherche par code INSEE de commune (ex. '44172').",
    )
    parser.add_argument(
        "--departement",
        help="Filtre de recherche par code département (ex. '75', '44').",
    )
    parser.add_argument(
        "--range",
        dest="range_",
        default=None,
        help="Plage fixe de résultats à récupérer, format 'debut-fin' (ex. '0-49', '0-149').",
    )
    parser.add_argument(
        "--max-results",
        type=int,
        default=None,
        help="Nombre maximum d'offres à récupérer au total pour la recherche.",
    )
    parser.add_argument(
        "--max-per-dep",
        type=int,
        default=None,
        help="Nombre maximum d'offres par département avec --sync-all.",
    )
    args = parser.parse_args()

    if args.init:
        initialize_database()
        print("Base initialisée : schéma recréé et données de test chargées.")
    elif args.sync_all:
        nombre = sync_all_departements(
            mots_cles=args.mots_cles,
            code_rome=args.code_rome,
            max_per_departement=args.max_per_dep,
        )
        print(f"\nCollecte nationale terminée : {nombre} offre(s) totale(s) chargée(s) en base.")
    elif args.sync_api:
        # Si --paginate ou si aucun range spécifié avec un filtre, on active la pagination
        should_paginate = args.paginate or (args.range_ is None and args.max_results is not None)
        nombre = sync_from_api(
            mots_cles=args.mots_cles,
            code_rome=args.code_rome,
            commune=args.commune,
            departement=args.departement,
            range_=args.range_,
            paginate=should_paginate,
            max_results=args.max_results,
        )
        print(f"Synchronisation terminée : {nombre} offre(s) chargée(s) en base.")
    else:
        check_connection()


if __name__ == "__main__":
    main()

