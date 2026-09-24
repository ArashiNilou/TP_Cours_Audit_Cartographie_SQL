"""Point d'entrée du TP : vérifie la connexion PostgreSQL ou initialise la base.

Usage :
    python main.py            # vérifie simplement la connexion
    python main.py --init     # recrée le schéma et charge le jeu de données
"""

import argparse

from src.connection import get_connection
from src.schema import initialize_database


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
    args = parser.parse_args()

    if args.init:
        initialize_database()
        print("Base initialisée : schéma recréé et données de test chargées.")
    else:
        check_connection()


if __name__ == "__main__":
    main()
