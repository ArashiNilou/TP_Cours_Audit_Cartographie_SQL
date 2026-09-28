"""Connexion PostgreSQL parametree par variables d'environnement.

Variables lues : PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD.
Aucune valeur sensible n'est codee en dur : voir .env.example a la racine
du depot pour la liste des variables attendues.
"""

import os

import psycopg


def get_connection() -> psycopg.Connection:
    """Ouvre une connexion vers la base PostgreSQL cible."""
    connection_parameters = {
        "host": os.getenv("PGHOST", "localhost"),
        "port": os.getenv("PGPORT", "5432"),
        "dbname": os.getenv("PGDATABASE", "emploi"),
        "user": os.getenv("PGUSER", "postgres"),
        "connect_timeout": 5,
    }
    password = os.getenv("PGPASSWORD")
    if password:
        connection_parameters["password"] = password
    return psycopg.connect(**connection_parameters)
