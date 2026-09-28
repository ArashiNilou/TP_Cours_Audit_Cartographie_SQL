import time
import logging
import yaml
from api.ingest import sync_from_api

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")

def load_config():
    with open("config.yaml", "r") as f:
        return yaml.safe_load(f)

def job():
    config = load_config()
    queries = config['scraping']['france_travail']['queries']
    max_results = config['scraping']['france_travail']['max_results']
    
    logging.info(f"Démarrage de l'ingestion France Travail pour {len(queries)} requêtes (Max {max_results} résultats/requête)...")
    for query in queries:
        try:
            logging.info(f"Recherche France Travail pour: '{query}'")
            sync_from_api(mots_cles=query, paginate=True, max_results=max_results)
        except Exception as e:
            logging.error(f"Erreur lors de l'ingestion FT pour '{query}': {e}")
            
    logging.info("Ingestion France Travail terminée avec succès.")

while True:
    config = load_config()
    interval_seconds = config['scraping']['interval_hours'] * 3600
    
    job()
    
    logging.info(f"Attente de {config['scraping']['interval_hours']} heure(s) avant la prochaine exécution France Travail...")
    time.sleep(interval_seconds)
