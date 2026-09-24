"""Exécution des scripts SQL de schéma et de jeu de données de test.

Ce module permet d'initialiser la base de données emploi directement depuis
Python, sans dépendre du client `psql`. Les scripts SQL restent la source de
vérité : ce module se contente de les lire et de les exécuter dans le bon
ordre, en transaction.
"""

from pathlib import Path

import psycopg

from .connection import get_connection

SQL_DIR = Path(__file__).resolve().parent.parent / "sql"


def _run_script(connection: psycopg.Connection, filename: str) -> None:
    """Lit et exécute un fichier SQL du dossier sql/ dans une transaction."""
    script_path = SQL_DIR / filename
    sql_text = script_path.read_text(encoding="utf-8")
    with connection.cursor() as cursor:
        cursor.execute(sql_text)
    connection.commit()


def apply_schema() -> None:
    """Recrée le schéma relationnel (DROP puis CREATE) dans la base cible."""
    with get_connection() as connection:
        _run_script(connection, "01_schema.sql")


def load_seed_data() -> None:
    """Charge le jeu de données de test dans le schéma existant."""
    with get_connection() as connection:
        _run_script(connection, "02_seed.sql")


def initialize_database() -> None:
    """Recrée le schéma puis charge le jeu de données de test."""
    apply_schema()
    load_seed_data()
