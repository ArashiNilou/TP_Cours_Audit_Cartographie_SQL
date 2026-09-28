import asyncio
import json
import argparse
import random
from playwright.async_api import async_playwright
from datetime import datetime
import urllib.parse
from confluent_kafka import Producer

def delivery_report(err, msg):
    """ Callback appelé à chaque message envoyé à Kafka pour confirmer le statut. """
    if err is not None:
        print(f" Échec de l'envoi du message à Kafka : {err}")
    else:
        print(f" Offre envoyée sur le topic {msg.topic()} [Partition: {msg.partition()}]")

async def scrape_hellowork(query: str, location: str, max_pages: int = 1):
    """
    Scrape le site HelloWork et envoie chaque offre en streaming vers Kafka (Producer).
    """
    # Configuration du Producer Kafka
    producer_conf = {'bootstrap.servers': 'localhost:9092'}
    kafka_producer = Producer(producer_conf)
    topic_name = "hellowork_jobs"
    
    total_sent = 0

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1440, "height": 900},
            locale="fr-FR",
        )
        page = await context.new_page()

        for current_page in range(1, max_pages + 1):
            query_encoded = urllib.parse.quote_plus(query)
            loc_encoded = urllib.parse.quote_plus(location)
            url = f"https://www.hellowork.com/fr-fr/emploi/recherche.html?k={query_encoded}&l={loc_encoded}&p={current_page}"
            
            print(f"\nNavigation vers la page {current_page}: {url}")
            await page.goto(url)
            await asyncio.sleep(random.uniform(2.5, 4.5))

            if current_page == 1:
                try:
                    cookie_btn = await page.query_selector("button#hw-cc-notice-accept-btn")
                    if cookie_btn:
                        await cookie_btn.hover()
                        await asyncio.sleep(random.uniform(0.5, 1.5))
                        await cookie_btn.click()
                        await asyncio.sleep(random.uniform(1.0, 2.0))
                except Exception:
                    pass

            print("Simulation de lecture humaine (défilement de la page)...")
            for _ in range(4):
                scroll_y = int(random.uniform(300, 800))
                await page.mouse.wheel(0, scroll_y)
                await asyncio.sleep(random.uniform(0.8, 2.2))
                
            await page.mouse.wheel(0, -300)
            await asyncio.sleep(random.uniform(1.0, 2.0))

            page_jobs = await page.evaluate('''() => {
                const results = [];
                const h3s = document.querySelectorAll('h3');
                h3s.forEach(h3 => {
                    const li = h3.closest('li');
                    if (li) {
                        const a = li.querySelector('a');
                        const texts = li.innerText.split('\\n').filter(t => t.trim() !== '');
                        
                        let startIndex = 0;
                        if (texts[0].includes('candidature') || texts[0].includes('Sponsorisé')) {
                            startIndex = 1;
                        }
                        
                        if (texts.length > startIndex + 2) {
                            let type_contrat = "Non spécifié";
                            let salaire = "Non spécifié";
                            
                            const motsContrats = ["CDI", "CDD", "Alternance", "Freelance", "Intérim", "Stage", "Apprentissage"];
                            
                            for (let i = startIndex + 3; i < texts.length; i++) {
                                const txt = texts[i].trim();
                                if (type_contrat === "Non spécifié" && motsContrats.some(k => txt.toLowerCase().includes(k.toLowerCase()))) {
                                    type_contrat = txt;
                                } else if (txt.includes("€") || txt.toLowerCase().includes("eur ")) {
                                    salaire = txt;
                                }
                            }

                            results.push({
                                titre: texts[startIndex],
                                entreprise: texts[startIndex + 1],
                                lieu: texts[startIndex + 2],
                                type_contrat: type_contrat,
                                salaire: salaire,
                                lien: a ? a.href : ""
                            });
                        }
                    }
                });
                return results;
            }''')
            
            if not page_jobs:
                print("Aucune offre trouvée sur cette page.")
                break
                
            # ENVOI EN STREAMING VERS KAFKA
            for job in page_jobs:
                job["date_extraction"] = datetime.now().isoformat()
                job["source"] = "hellowork" # Métadonnée importante pour notre Data Lake
                
                # Convertir l'objet Python en chaîne JSON
                message_json = json.dumps(job, ensure_ascii=False)
                
                # Produire le message dans Kafka (asynchrone)
                kafka_producer.produce(topic_name, value=message_json.encode('utf-8'), callback=delivery_report)
                
                total_sent += 1
            
            # Déclencher l'envoi physique des messages en attente pour cette page
            kafka_producer.poll(0)
            
            print(f"{len(page_jobs)} offres poussées dans Kafka pour la page {current_page}.")
            
            if current_page < max_pages:
                pause = random.uniform(4.0, 7.0)
                await asyncio.sleep(pause)

        await browser.close()
        
    # Flush garantit que tous les derniers messages sont bien partis avant de quitter le script
    print("\nFlushing Kafka producer (attente de confirmation des derniers messages)...")
    kafka_producer.flush()
    print(f"Scraping terminé. {total_sent} offres streamées avec succès vers Kafka !")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scraper HelloWork -> Kafka Producer")
    parser.add_argument("--query", type=str, default="data engineer", help="Mots-clés de recherche")
    parser.add_argument("--location", type=str, default="France", help="Lieu de recherche")
    parser.add_argument("--pages", type=int, default=1, help="Nombre de pages à scraper")
    
    args = parser.parse_args()
    
    print(f"Lancement du streaming Kafka pour '{args.query}' à '{args.location}' ({args.pages} page(s))...")
    asyncio.run(scrape_hellowork(args.query, args.location, args.pages))
