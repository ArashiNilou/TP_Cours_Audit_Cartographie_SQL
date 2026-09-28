import time
import os
import psycopg
from pymongo import MongoClient
from prometheus_client import start_http_server, Gauge

# Définition des métriques
RAW_COUNT = Gauge('data_raw_count', 'Nombre de documents bruts dans le Data Lake (MongoDB)')
CLEAN_COUNT = Gauge('data_clean_count', 'Nombre d\'offres nettoyées dans le Data Warehouse (PostgreSQL)')

def fetch_metrics():
    # Connexion MongoDB
    try:
        mongo_uri = os.getenv("MONGO_URI", "mongodb://mongodb:27017/")
        mongo_client = MongoClient(mongo_uri)
        raw_count = mongo_client["emploi_datalake"]["raw_jobs"].count_documents({})
        RAW_COUNT.set(raw_count)
        mongo_client.close()
    except Exception as e:
        print("Erreur Mongo:", e)

    # Connexion PostgreSQL
    try:
        pg_uri = os.getenv("PG_URI", "postgresql://postgres:postgres@postgres:5432/emploi")
        conn = psycopg.connect(pg_uri)
        cur = conn.cursor()
        cur.execute("SELECT count(*) FROM offre;")
        clean_count = cur.fetchone()[0]
        CLEAN_COUNT.set(clean_count)
        cur.close()
        conn.close()
    except Exception as e:
        print("Erreur Postgres:", e)

if __name__ == '__main__':
    # Démarrage du serveur web Prometheus sur le port 8000
    start_http_server(8000)
    print("Exportateur Raw vs Clean démarré sur le port 8000...")
    while True:
        fetch_metrics()
        time.sleep(10) # Mise à jour toutes les 10 secondes
