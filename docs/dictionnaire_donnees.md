# Dictionnaire de données

Ce dictionnaire décrit les champs du modèle relationnel 3NF alimenté par le
pipeline. Les tailles correspondent au schéma PostgreSQL de
[`sql/01_schema.sql`](../sql/01_schema.sql).

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

## Champs d'audit et d'enrichissement TP2

| Champ | Description fonctionnelle | Type SQL cible | Contraintes principales |
|---|---|---|---|
| `run_id` | Identifiant d'un traitement PySpark | `varchar(64)` | PK |
| `status` | État du traitement | `varchar(20)` | `running`, `success`, `failed`, `no_input` |
| `raw_count` | Nombre de lignes lues | `integer` | Positif ou nul |
| `clean_count` | Nombre de lignes propres | `integer` | Positif ou nul |
| `rejected_count` | Nombre de lignes rejetées | `integer` | Positif ou nul |
| `commune_reference_status` | Résultat du rapprochement Geo API | `varchar(20)` | `matched`, `not_found`, `missing_source2` |
| `geo_nom_commune` | Nom officiel obtenu via la Geo API | `varchar(100)` | Nullable |
| `geo_code_postal` | Code postal obtenu via la Geo API | `varchar(5)` | Nullable |

