# TP3 — Audit qualité et nettoyage des données

Ce dossier prolonge les TP1 et TP2. Il audite les données réellement chargées
dans PostgreSQL, corrige uniquement les anomalies justifiables et conserve les
données sources du Data Lake intactes.

## Livrables

| Fichier | Contenu |
|---|---|
| [`cartographie-tp3.mmd`](cartographie-tp3.mmd) | Cheminement mis à jour, de la source au contrôle qualité |
| [`matrice-controles.md`](matrice-controles.md) | 17 contrôles couvrant les cinq dimensions de qualité |
| [`sql/00_objets_audit.sql`](sql/00_objets_audit.sql) | Tables, vues et fonction d'audit rejouables |
| [`sql/01_audit_avant.sql`](sql/01_audit_avant.sql) | Mesure et historisation de l'état initial |
| [`sql/02_nettoyage.sql`](sql/02_nettoyage.sql) | Corrections transactionnelles et journalisées |
| [`sql/03_controle_apres.sql`](sql/03_controle_apres.sql) | Effet immédiat du nettoyage SQL |
| [`sql/04_controle_pipeline.sql`](sql/04_controle_pipeline.sql) | Effet durable après rejeu de PySpark |
| [`resultats-avant-apres.md`](resultats-avant-apres.md) | Résultats mesurés sur la base réelle |
| [`technique.md`](technique.md) | Choix, règles, limites et procédure d'exécution |
| [`synthese-orale.md`](synthese-orale.md) | Trame courte pour la restitution au professeur |
| [`resultats/avant_apres.csv`](resultats/avant_apres.csv) | Export exploitable de la comparaison |

## Exécution

La base doit être démarrée. Le producteur et Spark sont mis en pause afin de
comparer exactement la même population :

```bash
docker compose up -d postgres
docker compose stop ft-producer spark-batch
```

Copier le dossier dans le conteneur, puis exécuter les scripts dans l'ordre :

```bash
docker cp tp3/. postgres_emploi:/tmp/tp3/

docker exec -w /tmp/tp3/sql postgres_emploi \
  psql -v ON_ERROR_STOP=1 -U postgres -d emploie -f 01_audit_avant.sql

docker exec -w /tmp/tp3/sql postgres_emploi \
  psql -v ON_ERROR_STOP=1 -U postgres -d emploie -f 02_nettoyage.sql

docker exec -w /tmp/tp3/sql postgres_emploi \
  psql -v ON_ERROR_STOP=1 -U postgres -d emploie -f 03_controle_apres.sql
```

Pour vérifier que la correction reste vraie après un rejeu du pipeline :

```bash
docker compose build spark-batch
docker compose run --rm -e SPARK_BATCH_RUN_ONCE=true spark-batch

docker exec -w /tmp/tp3/sql postgres_emploi \
  psql -v ON_ERROR_STOP=1 -U postgres -d emploie -f 04_controle_pipeline.sql

docker compose up -d ft-producer spark-batch
```

Le nettoyage est rejouable : après le premier passage, les tables de
correspondance ne trouvent plus de doublons et aucune correction n'est répétée.
Les résultats restent historisés dans `tp3_quality_run` et
`tp3_quality_result`.

## Principe de sécurité

- aucune suppression du Data Lake ;
- aucun `DROP` des tables TP1/TP2 ;
- toutes les corrections SQL sont dans une transaction ;
- chaque valeur corrigée est tracée dans `tp3_cleaning_log` ;
- aucune donnée manquante n'est inventée ;
- le parseur Python est corrigé pour éviter la réapparition des anomalies.
