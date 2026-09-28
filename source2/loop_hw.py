import time
import asyncio
import logging
import yaml
from source2.scrape_hellowork import scrape_hellowork

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")

def load_config():
    with open("config.yaml", "r") as f:
        return yaml.safe_load(f)

async def run_jobs():
    config = load_config()
    queries = config['scraping']['hellowork']['queries']
    max_pages = config['scraping']['hellowork']['max_pages']

    for query in queries:
        logging.info(f"Démarrage du scraping HelloWork pour '{query}' sur {max_pages} page(s)...")
        try:
            await scrape_hellowork(query=query, location="France", max_pages=max_pages)
        except Exception as e:
            logging.error(f"Erreur lors du scraping HW pour '{query}': {e}")

while True:
    config = load_config()
    interval_seconds = config['scraping']['interval_hours'] * 3600
    
    asyncio.run(run_jobs())
    
    logging.info(f"Attente de {config['scraping']['interval_hours']} heure(s) avant la prochaine exécution HelloWork...")
    time.sleep(interval_seconds)
