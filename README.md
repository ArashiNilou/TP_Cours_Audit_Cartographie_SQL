# TP2 - Pipeline Data Engineer : Cartographie de l'Emploi en France

##  Description du Projet
Ce projet est un pipeline complet de Data Engineering visant à agréger et analyser les offres d'emploi en France provenant de deux sources distinctes (France Travail et HelloWork), dans le respect des contraintes d'une architecture Big Data moderne.

###  Architecture
L'architecture est entièrement conteneurisée via Docker et s'articule autour des composants suivants :
1. **Sources (Ingestion)** :
   - `api/` : Scripts de consommation de l'API France Travail.
   - `source2/` : Web Scraper HelloWork.
2. **Streaming (Kafka)** :
   - Le scraper pousse les données brutes sur un topic Kafka.
   - Le script `kafka/consume_to_mongo.py` dépile les messages pour les sauvegarder.
3. **Data Lake (MongoDB)** :
   - Stockage orienté document des données brutes (Raw Data).
4. **Processing (PySpark)** :
   - Transformation, nettoyage et normalisation des données (`spark/spark_etl.py`).
5. **Data Warehouse (PostgreSQL)** :
   - Stockage relationnel des données nettoyées (Clean Data) en étoile (Star Schema).
6. **Observabilité (Monitoring)** :
   - `cAdvisor` : Métriques Docker (CPU, RAM).
   - `Postgres-Exporter` : Métriques PostgreSQL.
   - `Custom Exporter` : Script Python exposant le volume (Raw vs Clean).
   - `Prometheus` : Scraping et stockage des métriques.
   - `Grafana` : Tableaux de bord d'exploitation technique.

##  Déploiement et Lancement

### 1. Démarrer l'infrastructure
Lancer l'ensemble des conteneurs en arrière-plan :
```bash
docker compose up -d
```
Les bases de données s'initialisent automatiquement avec le schéma situé dans `postgres/sql/`.

### 2. Lancer l'ingestion de données
**A. API France Travail** (Alimente MongoDB directement) :
```bash
PYTHONPATH=. .venv/bin/python main.py --sync-all
```

**B. Scraper HelloWork + Kafka** (Streaming) :
Dans un premier terminal, lancer le consommateur :
```bash
PYTHONPATH=. .venv/bin/python kafka/consume_to_mongo.py
```
Dans un second terminal, lancer le scraper :
```bash
PYTHONPATH=. .venv/bin/python source2/scrape_hellowork.py --query "data engineer" --pages 1
```

### 3. Traitement PySpark (ETL)
Transférer les données du Data Lake vers le Data Warehouse :
```bash
./run_spark.sh
```

### 4. Vérification et Observabilité
* **Résumé métier** : `PYTHONPATH=. .venv/bin/python main.py`
* **Grafana (Monitoring Infra)** : `http://localhost:3000` (admin/admin)
* **Kafka UI** : `http://localhost:8080`

##  Critères de Réussite Valides
- [x] Deux sources complémentaires intégrées
- [x] Le scraper web alimente Kafka en temps réel
- [x] PySpark produit les données propres
- [x] PostgreSQL alimenté automatiquement
- [x] Indicateur Raw vs Clean (Via Custom Exporter sur le port 8000)
- [x] Orchestration complète via docker-compose
- [x] Structure de dépôt conforme (api, kafka, datalake, postgres, etc.)
