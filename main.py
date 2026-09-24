"""Point d'entrée du TP : vérifie la connexion PostgreSQL ou initialise la base.

Usage :
    python main.py                          # vérifie simplement la connexion
    python main.py --init                   # recrée le schéma et charge le jeu de données
    python main.py --sync-api                # récupère des offres réelles et les charge
    python main.py --sync-api --mots-cles python --commune 44172
"""

import argparse

from dotenv import load_dotenv

from src.connection import get_connection
from src.ingest import sync_from_api
from src.schema import initialize_database

load_dotenv()


def check_connection() -> None:
    """Ouvre une connexion de test et affiche la base et l'utilisateur connectés."""
    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT current_database(), current_user;")
            database, user = cursor.fetchone()
    print(f"Connexion PostgreSQL réussie : base={database}, utilisateur={user}")


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
        "--range",
        dest="range_",
        default="0-49",
        help="Plage de résultats à récupérer, format 'debut-fin' (défaut 0-49).",
    )
    args = parser.parse_args()

    if args.init:
        initialize_database()
        print("Base initialisée : schéma recréé et données de test chargées.")
    elif args.sync_api:
        nombre = sync_from_api(
            mots_cles=args.mots_cles,
            code_rome=args.code_rome,
            commune=args.commune,
            range_=args.range_,
        )
        print(f"Synchronisation terminée : {nombre} offre(s) chargée(s) en base.")
    else:
        check_connection()


if __name__ == "__main__":
    main()
