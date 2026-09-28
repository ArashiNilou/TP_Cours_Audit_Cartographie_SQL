# Audit qualité et nettoyage des données

Ce dossier documente la couche qualité du projet unifié. Elle audite les
données chargées dans PostgreSQL, corrige uniquement les anomalies
justifiables et conserve les données sources du Data Lake intactes.

## Livrables

| Fichier | Contenu |
|---|---|
| [`../architecture/data-quality.mmd`](../architecture/data-quality.mmd) | Cheminement mis à jour, de la source au contrôle qualité |
| [`control-matrix.md`](control-matrix.md) | 17 contrôles couvrant les cinq dimensions de qualité |
| [`../../database/quality/00_audit_objects.sql`](../../database/quality/00_audit_objects.sql) | Tables, vues et fonction d'audit rejouables |
| [`../../database/quality/01_audit_before.sql`](../../database/quality/01_audit_before.sql) | Mesure et historisation de l'état initial |
| [`../../database/quality/02_clean_data.sql`](../../database/quality/02_clean_data.sql) | Corrections transactionnelles et journalisées |
| [`../../database/quality/03_audit_after.sql`](../../database/quality/03_audit_after.sql) | Effet immédiat du nettoyage SQL |
| [`../../database/quality/04_audit_pipeline.sql`](../../database/quality/04_audit_pipeline.sql) | Effet durable après rejeu de PySpark |
| [`before-after-results.md`](before-after-results.md) | Résultats mesurés sur la base réelle |
| [`technical-guide.md`](technical-guide.md) | Choix, règles, limites et procédure d'exécution |
| [`oral-summary.md`](oral-summary.md) | Trame courte pour la restitution au professeur |
| [`results/before-after.csv`](results/before-after.csv) | Export exploitable de la comparaison |

## Exécution

La base doit être démarrée. Le producteur et Spark sont mis en pause afin de
comparer exactement la même population :

```bash
docker compose up -d postgres
docker compose stop ft-producer spark-batch
```

Copier les scripts qualité dans le conteneur, puis les exécuter dans l'ordre :

```bash
docker cp database/quality/. postgres_emploi:/tmp/quality/

docker exec -w /tmp/quality postgres_emploi \
  psql -v ON_ERROR_STOP=1 -U postgres -d emploie -f 01_audit_before.sql

docker exec -w /tmp/quality postgres_emploi \
  psql -v ON_ERROR_STOP=1 -U postgres -d emploie -f 02_clean_data.sql

docker exec -w /tmp/quality postgres_emploi \
  psql -v ON_ERROR_STOP=1 -U postgres -d emploie -f 03_audit_after.sql
```

Pour vérifier que la correction reste vraie après un rejeu du pipeline :

```bash
docker compose build spark-batch
docker compose run --rm -e SPARK_BATCH_RUN_ONCE=true spark-batch

docker exec -w /tmp/quality postgres_emploi \
  psql -v ON_ERROR_STOP=1 -U postgres -d emploie -f 04_audit_pipeline.sql

docker compose up -d ft-producer spark-batch
```

Le nettoyage est rejouable : après le premier passage, les tables de
correspondance ne trouvent plus de doublons et aucune correction n'est répétée.
Les résultats restent historisés dans `tp3_quality_run` et
`tp3_quality_result`.

## Principe de sécurité

- aucune suppression du Data Lake ;
- aucun `DROP` des tables métier ou du pipeline ;
- toutes les corrections SQL sont dans une transaction ;
- chaque valeur corrigée est tracée dans `tp3_cleaning_log` ;
- aucune donnée manquante n'est inventée ;
- le parseur Python est corrigé pour éviter la réapparition des anomalies.
