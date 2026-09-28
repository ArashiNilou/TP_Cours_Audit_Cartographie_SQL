# Projet de Data Engineering : Cartographie de l'Emploi en France (TP1, TP2, TP3)

##  Présentation du Projet
Ce dépôt centralise l'ensemble des travaux réalisés pour construire un pipeline complet d'Ingestion, de Traitement et de Restitution de la donnée. Il répond aux exigences d'une architecture Big Data moderne de type **ELT / Medallion Architecture**, en couvrant les aspects de collecte, d'orchestration, d'audit qualité et d'observabilité.

### Couverture des TPs
- **TP1** : Modélisation des données (PostgreSQL), Scripts de création (DDL), et architecture globale.
- **TP2** : Implémentation du pipeline distribué (Kafka, MongoDB, PySpark).
- **TP3** : Démarche de Data Stewardship, profilage, audit qualité et nettoyage SQL post-ingestion.

---

##  Architecture Technique

L'architecture est entièrement conteneurisée via **Docker Compose** et est scindée en grandes phases métier :

1. **Extraction & Chargement (EL) - *Phase Continue (Automatisée)*** :
   - Les scrapers (API France Travail et Playwright HelloWork) tournent en boucle via Docker.
   - Les données HelloWork transitent par **Kafka** avant d'être consommées.
   - Toutes les offres brutes atterrissent dans le **Data Lake (MongoDB)**.
2. **Transformation (T) - *Phase Batch (Manuelle)*** :
   - Un script **PySpark** applique le schéma cible (Schema Enforcement) et migre les données du Data Lake vers le **Data Warehouse (PostgreSQL)** en modèle Étoile.
3. **Audit et Qualité (Data Stewardship) - *Post-Ingestion*** :
   - Des scripts SQL d'audit et de correction (imputation, dédoublonnage) garantissent la pureté de la donnée dans le Warehouse (cf. `docs/TP3_Audit_Qualite/`).
4. **Restitution & Observabilité** :
   - **Metabase** (Business Intelligence) : Tableaux de bord métier (Salaires, Localisations, Contrats).
   - **Grafana & Prometheus** (SRE / Infra) : Tableaux de bord techniques (Santé des conteneurs, Métriques de volumétrie via des sondes Python personnalisées).

---

## ️ Configuration (Fichier `.env`)

L'intégralité du comportement du projet est pilotable dynamiquement. Avant de lancer le projet, assurez-vous de configurer votre fichier `.env` (à la racine) :

```env
# Clés API France Travail
FT_CLIENT_ID=votre_cle
FT_CLIENT_SECRET=votre_secret

# Configuration des recherches HelloWork
HELLOWORK_QUERIES="data engineer, developpeur python, devops, data scientist, administrateur systeme"
HELLOWORK_MAX_PAGES=2

# Configuration des recherches France Travail
FRANCETRAVAIL_QUERIES="developpeur, data engineer"
FRANCETRAVAIL_MAX_RESULTS=150

# Configuration PostgreSQL
PGHOST=localhost
...
```

---

## 🛠️ Prérequis et Installation locale

Bien que l'ingestion soit sous Docker, la transformation PySpark se lance localement pour ce projet. Avant la première utilisation :

1. **Cloner le projet** et initialiser l'environnement :
   ```bash
   cp .env.example .env
   # Remplissez FT_CLIENT_ID et FT_CLIENT_SECRET dans le fichier .env
   ```
2. **Installer Python et les dépendances** :
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```
3. **Java 17** : Assurez-vous d'avoir Java 11 ou 17 installé (`JAVA_HOME` configuré), prérequis strict pour exécuter PySpark localement.

---

## 🚀 Lancement et Utilisation

### 1. Démarrer l'infrastructure et l'Ingestion
Déployez les 13 conteneurs d'un seul coup :
```bash
docker compose up -d --build
```
> **Magie de l'automatisation :** Les bases s'initialisent, et les 3 conteneurs de collecte (`ft-scraper`, `hw-scraper`, `kafka-consumer`) commencent immédiatement à aspirer le web en arrière-plan. Votre Data Lake (MongoDB) se remplit tout seul !

### 2. Lancer la Transformation PySpark (Batch ETL)
Quand vous jugez avoir assez de données brutes, lancez le traitement lourd manuellement pour alimenter PostgreSQL :
```bash
./run_spark.sh
```

### 3. Exécuter l'Audit Qualité (TP3)
Pour auditer, nettoyer, imputer et standardiser les données fraîchement arrivées, exécutez ces commandes directement depuis votre terminal (elles injectent les scripts SQL dans le conteneur PostgreSQL) :

```bash
# 1. Voir l'état des données brutes
docker exec -i postgres_emploi psql -U postgres -d emploi < docs/TP3_Audit_Qualite/sql/01_audit_avant.sql

# 2. Lancer le nettoyage (Filtrage des outliers, dédoublonnage, etc.)
docker exec -i postgres_emploi psql -U postgres -d emploi < docs/TP3_Audit_Qualite/sql/02_nettoyage.sql

# 3. Vérifier que les anomalies ont bien disparu
docker exec -i postgres_emploi psql -U postgres -d emploi < docs/TP3_Audit_Qualite/sql/03_audit_apres.sql
```
*(Toute la démarche et les justifications statistiques sont documentées dans `docs/TP3_Audit_Qualite/README.md`)*

---

##  Interfaces et Tableaux de bord

Une fois l'infrastructure lancée, vous avez accès à tous les portails web locaux :

| Outil | URL | Identifiants | Usage |
| :--- | :--- | :--- | :--- |
| **Metabase** | [http://localhost:3001](http://localhost:3001) | *À définir au 1er lancement* | Création des Dashboards Métiers (Data Viz). *Connectez-le au host "postgres", port 5432, user "postgres".* |
| **Grafana** | [http://localhost:3000](http://localhost:3000) | `admin` / `admin` | Monitoring Technique (Santé Docker, Volumétrie des bases de données). |
| **Kafka UI** | [http://localhost:8080](http://localhost:8080) | *Aucun* | Visualisation en direct du streaming des messages HelloWork. |
| **Spark UI** | [http://localhost:4040](http://localhost:4040) | *Aucun* | Uniquement disponible pendant les ~10 secondes où `run_spark.sh` s'exécute. |
