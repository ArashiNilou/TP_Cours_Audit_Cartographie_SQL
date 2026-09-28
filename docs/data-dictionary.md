# Dictionnaire de données

Ce dictionnaire décrit les champs du modèle relationnel 3NF alimenté par le
pipeline. Les tailles correspondent au schéma PostgreSQL de
[`database/schema/01_schema.sql`](../database/schema/01_schema.sql).

| Champ | Description fonctionnelle | Type SQL cible | Exemple | Contraintes principales |
|---|---|---|---|---|
| `offre_id` | Identifiant technique interne de l'offre | `bigint` | `1` | PK, identité |
| `source_offre_id` | Identifiant France Travail | `varchar(20)` | `178XYZW` | NOT NULL, UNIQUE |
| `libelle_poste` | Intitulé du poste | `varchar(200)` | `Data Engineer` | NOT NULL, non vide |
| `description` | Description de l'offre | `text` | `Conception de pipelines...` | Nullable |
| `date_publication` | Date de publication | `date` | `2026-09-18` | NOT NULL |
| `type_contrat` | Type de contrat | `varchar(5)` | `CDI` | NOT NULL, liste contrôlée |
| `duree_travail` | Régime horaire | `varchar(100)` | `35H Horaires normaux` | Nullable |
| `salaire_brut_annuel_estime` | Salaire annuel estimé | `numeric(10,2)` | `41500.00` | Nullable, positif |
| `entreprise_id` | Entreprise recruteuse | `bigint` | `1` | PK de `entreprise`, FK dans `offre` |
| `raison_sociale` | Nom de l'entreprise | `varchar(250)` | `TECH INNOVATION` | Nullable si anonyme |
| `entreprise_anonyme` | Indicateur d'anonymat | `boolean` | `false` | NOT NULL |
| `rome_code` | Code métier ROME | `varchar(5)` | `M1805` | PK de `metier_rome`, FK dans `offre` |
| `libelle_fiche_metier` | Libellé métier ROME | `varchar(250)` | `Études et développement informatique` | NOT NULL |
| `domaine_professionnel` | Domaine métier | `varchar(150)` | `Support à l'entreprise` | NOT NULL |
| `competence_id` | Identifiant de compétence | `bigint` | `1` | PK, identité |
| `libelle_competence` | Libellé de compétence | `varchar(300)` | `Langage SQL` | NOT NULL, UNIQUE |
| `type_competence` | Savoir-faire ou savoir-être | `varchar(20)` | `Savoir-faire` | NOT NULL, liste contrôlée |
| `statut_exigence` | Compétence exigée ou souhaitée | `varchar(1)` | `E` | NOT NULL, `E` ou `S` |
| `code_insee` | Identifiant officiel de commune | `char(5)` | `44172` | PK de `commune`, FK dans `offre` |
| `code_postal` | Code postal principal | `varchar(5)` | `44980` | NOT NULL |
| `nom_commune` | Nom officiel de commune | `varchar(100)` | `Sainte-Luce-sur-Loire` | NOT NULL |
| `latitude` | Latitude du centre communal | `numeric(9,6)` | `47.249400` | Nullable, -90 à 90 |
| `longitude` | Longitude du centre communal | `numeric(9,6)` | `-1.486200` | Nullable, -180 à 180 |

## Champs d'audit et d'enrichissement du pipeline

### `tp2_pipeline_run` — une ligne par exécution PySpark

| Champ | Description fonctionnelle | Type SQL cible | Contraintes principales |
|---|---|---|---|
| `run_id` | Identifiant d'un traitement PySpark | `varchar(64)` | PK |
| `started_at` | Début du traitement | `timestamptz` | NOT NULL, défaut `now()`, indexé |
| `completed_at` | Fin du traitement | `timestamptz` | Nullable tant que `running` |
| `status` | État du traitement | `varchar(20)` | `running`, `success`, `failed`, `no_input` |
| `raw_count` | Nombre de lignes lues | `integer` | Positif ou nul |
| `clean_count` | Nombre de lignes propres | `integer` | Positif ou nul |
| `rejected_count` | Nombre de lignes rejetées | `integer` | Positif ou nul |
| `input_path` | Zone lue dans le Data Lake | `text` | Ex. `/data-lake/aggregated/offres` |
| `curated_path` | Dossier Parquet produit | `text` | `curated/offres/run_id=<id>` |
| `quarantine_path` | Dossier des rejets | `text` | `quarantine/spark/run_id=<id>` |
| `error_message` | Cause d'un échec | `text` | Rempli si `failed`, 1 000 caractères max |

### `tp2_offre_enrichment` — rapprochement avec l'API Géo

| Champ | Description fonctionnelle | Type SQL cible | Contraintes principales |
|---|---|---|---|
| `source_offre_id` | Offre enrichie | `varchar(20)` | PK, FK vers `offre.source_offre_id` (CASCADE) |
| `commune_reference_status` | Résultat du rapprochement Geo API | `varchar(20)` | `matched`, `not_found`, `missing_source2` |
| `geo_nom_commune` | Nom officiel obtenu via la Geo API | `varchar(100)` | Nullable |
| `geo_code_postal` | Code postal obtenu via la Geo API | `varchar(5)` | Nullable |
| `geo_longitude` | Longitude officielle | `numeric(9,6)` | Nullable |
| `geo_latitude` | Latitude officielle | `numeric(9,6)` | Nullable |
| `aggregated_at` | Horodatage de l'enrichissement | `timestamptz` | Nullable |

## Champs d'audit qualité

| Champ | Description fonctionnelle | Type SQL cible | Contraintes principales |
|---|---|---|---|
| `run_id` | Identifiant d'un audit qualité | `bigint` | PK de `tp3_quality_run`, FK dans les résultats |
| `phase` | Moment de la mesure | `varchar(30)` | `before_cleaning`, `after_cleaning`, `after_pipeline_reload`, `monitoring` |
| `executed_at` | Horodatage de l'audit ou de la correction | `timestamptz` | NOT NULL |
| `total_offres` | Nombre d'offres au moment de l'audit | `bigint` | NOT NULL, défaut 0 |
| `notes` | Commentaire libre sur l'audit | `text` | Nullable |
| `baseline_run_id` | Audit de référence pour la comparaison | `bigint` | FK vers `tp3_quality_run` (SET NULL) |
| `control_code` | Identifiant stable d'un contrôle | `varchar(10)` | Composé avec `run_id` dans la PK |
| `dimension` | Dimension de qualité | `varchar(20)` | `completude`, `unicite`, `validite`, `coherence`, `integrite` |
| `control_label` | Libellé lisible du contrôle | `text` | NOT NULL |
| `severity` | Gravité de l'anomalie | `varchar(10)` | `INFO`, `LOW`, `MEDIUM`, `HIGH` |
| `anomaly_count` | Nombre d'anomalies observées | `bigint` | Positif ou nul |
| `population_count` | Nombre de lignes contrôlées | `bigint` | Positif ou nul |
| `anomaly_rate_pct` | Taux d'anomalie en pourcentage | `numeric(8,4)` | Calculé par le script d'audit |
| `result_status` | Résultat du contrôle | `varchar(10)` | `PASS`, `WARN`, `FAIL` |
| `decision` | Décision de traitement associée | `text` | NOT NULL |
| `cleaning_id` | Identifiant d'une correction journalisée | `bigint` | PK de `tp3_cleaning_log` |
| `rule_code` | Règle de nettoyage appliquée | `varchar(20)` | `R01_COMPETENCE`, `R02_ENTREPRISE`, `R03_SALAIRE`, `R04_DOMAINE_ROME` |
| `table_name` | Table corrigée | `varchar(100)` | NOT NULL |
| `record_key` | Clé métier ou technique corrigée | `text` | NOT NULL |
| `column_name` | Colonne corrigée | `varchar(100)` | NOT NULL |
| `old_value` | Valeur avant correction | `text` | Nullable |
| `new_value` | Valeur après correction | `text` | Nullable |
| `justification` | Décision métier expliquant la correction | `text` | NOT NULL |

## Vues

| Vue | Définie dans | Rôle | Utilisée par |
|---|---|---|---|
| `tp2_pipeline_latest_counts` | `03_pipeline_schema.sql` | Compteurs du dernier traitement Spark réussi | Grafana (via postgres-exporter), Metabase |
| `tp2_dashboard_offres` | `03_pipeline_schema.sql` | Offre dénormalisée (commune, métier, entreprise, enrichissement) pour la BI | Metabase |
| `tp3_quality_current` | `00_audit_objects.sql` | Calcul en direct des 17 contrôles qualité | Fonction `tp3_execute_quality_audit` |
| `tp3_quality_latest_comparison` | `00_audit_objects.sql` | Dernier audit comparé à son audit de référence | `04_audit_pipeline.sql` |
| `tp3_quality_cleaning_comparison` | `00_audit_objects.sql` | Effet immédiat du nettoyage (avant / après) | `03_audit_after.sql` |

## Index secondaires

| Index | Table | Colonnes | Usage |
|---|---|---|---|
| `idx_offre_rome_code` | `offre` | `rome_code` | Jointure métier, filtre domaine |
| `idx_offre_type_contrat` | `offre` | `type_contrat` | Filtre contrat |
| `idx_offre_code_insee` | `offre` | `code_insee` | Jointure commune, carte |
| `idx_offre_date_publication` | `offre` | `date_publication` | Filtres temporels |
| `idx_offre_commune_contrat` | `offre` | `code_insee, type_contrat` | Filtre combiné lieu + contrat |
| `idx_exigence_offre_competence` | `exigence_offre` | `competence_id` | Classement des compétences |
| `idx_commune_code_postal` | `commune` | `code_postal` | Recherche par code postal |
| `idx_entreprise_raison_sociale` | `entreprise` | `raison_sociale` | Recherche d'entreprise |
| `ux_competence_normalized_label` | `competence` | `lower(libellé normalisé)` | UNIQUE, empêche les doublons (R01) |
| `ux_entreprise_normalized_name` | `entreprise` | `lower(nom normalisé)` | UNIQUE partiel, empêche les doublons (R02) |
| `idx_tp2_pipeline_run_started_at` | `tp2_pipeline_run` | `started_at DESC` | Dernier traitement |
| `idx_tp3_quality_result_dimension` | `tp3_quality_result` | `run_id, dimension` | Synthèse par dimension |
| `idx_tp3_cleaning_log_rule` | `tp3_cleaning_log` | `rule_code, executed_at DESC` | Historique des corrections |

La correspondance entre ces champs et les champs JSON des sources est décrite
dans la [cartographie source → cible](cartography/source-to-target-mapping.md).
