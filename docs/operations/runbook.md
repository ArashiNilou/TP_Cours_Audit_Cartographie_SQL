# Guide d'exploitation

Ce guide explique comment vérifier que la plateforme fonctionne, réagir à une
panne, sauvegarder les données et entretenir le Data Lake. Les commandes sont
à lancer à la racine du projet.

## 1. Vérification quotidienne (2 minutes)

| Vérification | Où | Résultat attendu |
|---|---|---|
| Services démarrés | `docker compose ps` | Tous `running` / `healthy` (sauf `kafka-init` et `metabase-setup`, tâches ponctuelles) |
| Cibles surveillées | <http://localhost:9090/targets> | Toutes **UP** |
| Flux de données | Grafana, tableau *Employment Data Platform Overview* | La courbe Raw augmente à chaque collecte, Clean suit après le passage Spark |
| Dernier traitement Spark | requête ci-dessous | `status = success`, moins de 30 minutes |
| Qualité | `database/quality/04_audit_pipeline.sql` | Aucun contrôle `FAIL` |

```bash
docker exec postgres_emploi psql -U postgres -d emploie -c \
  "SELECT run_id, status, started_at, raw_count, clean_count, rejected_count, error_message
   FROM tp2_pipeline_run ORDER BY started_at DESC LIMIT 5;"
```

## 2. Incidents fréquents

| Symptôme | Cause probable | Diagnostic | Action |
|---|---|---|---|
| Aucune nouvelle offre dans Kafka | Identifiants France Travail invalides ou API indisponible | `docker compose logs --tail 50 ft-producer` ; métrique `tp2_ft_producer_errors_total` | Vérifier `FT_CLIENT_ID` / `FT_CLIENT_SECRET` dans `.env`, puis `docker compose up -d ft-producer`. Le producteur réessaie seul à la collecte suivante. |
| Offres `missing_source2` | Référentiel communes absent | Métrique `tp2_aggregator_source2_available = 0` | `docker compose restart communes-collector`, puis attendre le rafraîchissement de l'agrégateur (5 min) |
| Messages Kafka qui s'accumulent (lag) | Agrégateur arrêté ou disque plein | `docker compose logs raw-aggregator` ; Kafka UI → *Consumers* | Libérer de la place (section 5), `docker compose restart raw-aggregator`. Aucun message n'est perdu : l'offset n'est validé qu'après écriture. |
| Traitement Spark `failed` | Erreur de lecture, base indisponible | Colonne `error_message` de `tp2_pipeline_run` ; `docker compose logs spark-batch` | Corriger la cause, puis relancer : `docker compose run --rm -e SPARK_BATCH_RUN_ONCE=true spark-batch` |
| Beaucoup de rejets | Changement de format de la source | `quarantine/spark/run_id=<id>/` : champ `rejection_reason` | Adapter les règles dans `spark_batch.py`, reconstruire (`docker compose build spark-batch`) et relancer |
| Dash affiche une erreur ou reste vide | Base non joignable ou redémarrage en cours | <http://localhost:8050/health> ; `docker compose logs dataviz` | `docker compose restart dataviz`. Premier chargement de l'index des fiches : environ 30 s |
| Fiche d'offre sans détail | Fichier brut pas encore indexé | Journaux `dataviz` | Attendre le rescan (automatique) ou redémarrer `dataviz` |
| PostgreSQL ne démarre pas | Volume corrompu ou port 5432 occupé | `docker compose logs postgres` | Libérer le port ou changer `PGPORT` ; en dernier recours, restauration (section 3) |

## 3. Sauvegarde et restauration

Sauvegarde de la base (format compressé) :

```bash
docker exec postgres_emploi pg_dump -U postgres -d emploie -Fc -f /tmp/emploie.dump
docker cp postgres_emploi:/tmp/emploie.dump ./backups/emploie_$(date +%F).dump
```

Restauration :

```bash
docker cp ./backups/emploie_AAAA-MM-JJ.dump postgres_emploi:/tmp/emploie.dump
docker exec postgres_emploi pg_restore -U postgres -d emploie --clean --if-exists /tmp/emploie.dump
```

La base peut aussi être entièrement reconstruite à partir du Data Lake : il
suffit de relancer un traitement Spark, qui recharge toutes les offres de la
zone `aggregated`, puis de rejouer `02_clean_data.sql`.

Sauvegarde du Data Lake (volume Docker) :

```bash
docker run --rm -v tp_cours_audit_cartographie_sql_data_lake:/data-lake:ro -v "$PWD/backups:/backup" \
  alpine tar czf /backup/data-lake_$(date +%F).tar.gz -C /data-lake raw
```

Seule la zone `raw` est indispensable : toutes les autres zones peuvent être
régénérées à partir d'elle.

## 4. Rejouer le pipeline après une correction

1. Corriger le code (`src/emploi_pipeline/`) et lancer les tests :
   `python -m unittest discover -s tests -t . -v`.
2. Reconstruire l'image : `docker compose build spark-batch`.
3. Relancer un traitement : `docker compose run --rm -e SPARK_BATCH_RUN_ONCE=true spark-batch`.
4. Contrôler : `04_audit_pipeline.sql` (voir [docs/quality](../quality/README.md)).
5. Redémarrer le service planifié : `docker compose up -d spark-batch`.

## 5. Entretien du Data Lake

La zone `curated` reçoit un nouveau dossier Parquet à chaque traitement
(toutes les 30 minutes) et n'est pas purgée automatiquement. Pour ne garder
que les 5 derniers traitements :

```bash
docker exec raw_aggregator_emploi python -c "
import pathlib, shutil
runs = sorted(pathlib.Path('/data-lake/curated/offres').glob('run_id=*'), key=lambda p: p.stat().st_mtime)
for run in runs[:-5]:
    shutil.rmtree(run)
print(f'{max(len(runs) - 5, 0)} dossier(s) supprimé(s)')
"
```

La même commande s'applique à `quarantine/spark` si nécessaire. Ne jamais
supprimer `raw/` : c'est la seule copie de la donnée d'origine.

## 6. Arrêt et redémarrage

| Besoin | Commande |
|---|---|
| Arrêter sans perdre de données | `docker compose down` |
| Redémarrer | `docker compose up -d` |
| Mettre à jour après un changement de code | `docker compose up -d --build` |
| Suspendre la collecte (pour un audit) | `docker compose stop ft-producer spark-batch` |
| Tout effacer | `docker compose down -v` (supprime les 5 volumes, irréversible) |
