"""Test de connexion a la base PostgreSQL emploie."""

import os
from contextlib import closing

import psycopg


def get_connection() -> psycopg.Connection:
    """Ouvre une connexion avec les variables d'environnement PostgreSQL."""
    return psycopg.connect(
        host=os.getenv("PGHOST", "localhost"),
        port=os.getenv("PGPORT", "5432"),
        dbname=os.getenv("PGDATABASE", "emploie"),
        user=os.getenv("PGUSER", "postgres"),
        password=os.getenv("PGPASSWORD"),
        connect_timeout=5,
    )


def main() -> None:
    with closing(get_connection()) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT current_database(), current_user;")
            database, user = cursor.fetchone()
        print(f"Connexion PostgreSQL reussie : base={database}, utilisateur={user}")


if __name__ == "__main__":
    main()
