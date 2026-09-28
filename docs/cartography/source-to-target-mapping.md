# Cartographie source → cible (lignage des champs)

Ce document décrit, champ par champ, d'où vient chaque donnée stockée dans
PostgreSQL, quelles transformations elle subit et où elle est rangée.

- **Source 1** : API France Travail « Offres d'emploi v2 » (JSON).
- **Source 2** : API Géo (`geo.api.gouv.fr/communes`) (JSON).
- **Code de transformation** : `src/emploi_pipeline/ingest.py` (`transform_offre`),
  `src/emploi_pipeline/event_contracts.py` (enrichissement),
  `src/emploi_pipeline/spark_batch.py` (validation et dédoublonnage).

## 1. Chemin suivi par une offre

| # | Étape | Composant | Entrée | Sortie | Format |
|---|---|---|---|---|---|
| 1 | Collecte | `ft-producer` | API France Travail | événement `tp2.offer.v1` | JSON |
| 2 | Transport | Kafka, topic `france-travail.offres.raw` | événement | message clé = `source_offer_id` | JSON UTF-8 |
| 3 | Archivage brut | `raw-aggregator` | message Kafka | `raw/france_travail/ingestion_date=AAAA-MM-JJ/<event_id>.json` | JSON |
| 4 | Enrichissement | `raw-aggregator` + `raw/communes/_latest.json` | événement + référentiel communes | `aggregated/offres/.../part-<event_id>.jsonl` (`tp2.aggregated_offer.v1`) | JSONL |
| 5 | Validation, dédoublonnage | `spark-batch` | zone `aggregated` | `curated/offres/run_id=<id>/` et `quarantine/spark/run_id=<id>/` | Parquet / JSON |
| 6 | Normalisation 3NF | `spark-batch` → `ingest.load_offres` | `enriched_payload` | tables `commune`, `entreprise`, `metier_rome`, `competence`, `offre`, `exigence_offre` | SQL |
| 7 | Traçabilité | `spark-batch` → `db.py` | exécution et enrichissement | `tp2_pipeline_run`, `tp2_offre_enrichment` | SQL |
| 8 | Contrôle qualité | scripts `database/quality/` | tables 3NF | `tp3_quality_run`, `tp3_quality_result`, `tp3_cleaning_log` | SQL |
| 9 | Restitution | Dash, Metabase | tables et vues + zone `raw` (fiche détaillée) | tableaux de bord | HTML |

## 2. Correspondance des champs métier

Chemin JSON relatif à `enriched_payload` (l'offre France Travail enrichie par
l'API Géo). « Source 2 » signifie que la valeur officielle de l'API Géo
remplace la valeur France Travail quand la commune est retrouvée
(`commune_reference_status = matched`).

### Table `offre`

| Colonne cible | Champ source | Transformation | Règle de rejet / qualité |
|---|---|---|---|
| `offre_id` | — | Identité générée par PostgreSQL | PK |
| `source_offre_id` | `id` | `str`, tronqué à 20 caractères | Obligatoire, UNIQUE ; clé de dédoublonnage Spark (dernière version selon `aggregated_at`) |
| `libelle_poste` | `intitule` | Tronqué à 200 | Rejet si vide (`missing_job_title`) |
| `description` | `description` | Aucune | NULL accepté (contrôle C01) |
| `date_publication` | `dateCreation` | 10 premiers caractères → `date` | Rejet si absente ou invalide (`invalid_date_publication`) |
| `type_contrat` | `typeContrat` | Majuscules, tronqué à 5 | Rejet si hors `CDI, CDD, MIS, SAI, CCE` |
| `duree_travail` | `dureeTravailLibelleConverti`, sinon `dureeTravailLibelle` | Tronqué à 100 | NULL accepté |
| `salaire_brut_annuel_estime` | `salaire.libelle` | Nombres suivis de « Euros », moyenne de la fourchette, annualisation (mensuel ×12, horaire ×35×52, hebdomadaire ×52, journalier ×218) | NULL hors 1 000–250 000 € (règle R03) |
| `rome_code` | `romeCode` | Majuscules | Rejet si format ≠ `^[A-Z][0-9]{4}$` |
| `entreprise_id` | `entreprise.nom` | Recherche de l'entreprise existante par nom normalisé (casse, espaces) | FK ; une seule entreprise « anonyme » partagée si nom absent |
| `code_insee` | `lieuTravail.commune` | Majuscules, 5 caractères | Rejet si format ≠ `^[0-9][0-9AB][0-9]{3}$` |

### Table `commune`

| Colonne cible | Champ source | Transformation | Remarque |
|---|---|---|---|
| `code_insee` | `lieuTravail.commune` (source 2 : `code`) | 5 caractères | PK |
| `nom_commune` | `lieuTravail.libelle` (source 2 : `nom`) | Tronqué à 100 | Nom officiel si commune retrouvée |
| `code_postal` | `lieuTravail.codePostal` (source 2 : `codesPostaux[0]`) | 5 caractères, `00000` si absent | Premier code postal officiel |
| `latitude` | `lieuTravail.latitude` (source 2 : `centre.coordinates[1]`) | Aucune | -90 à 90 |
| `longitude` | `lieuTravail.longitude` (source 2 : `centre.coordinates[0]`) | Aucune | -180 à 180 |

### Table `entreprise`

| Colonne cible | Champ source | Transformation | Remarque |
|---|---|---|---|
| `entreprise_id` | — | Identité | PK |
| `raison_sociale` | `entreprise.nom` | Espaces multiples réduits, tronqué à 250 | UNIQUE sur nom normalisé (règle R02) |
| `entreprise_anonyme` | `entreprise.nom` | `true` si nom absent | |

### Table `metier_rome`

| Colonne cible | Champ source | Transformation | Remarque |
|---|---|---|---|
| `rome_code` | `romeCode` | Majuscules | PK |
| `libelle_fiche_metier` | `romeLibelle`, sinon `romeCode` | Tronqué à 250 | |
| `domaine_professionnel` | Première lettre de `romeCode` | Table `ROME_GRANDS_DOMAINES` (14 grands domaines ROME 4.0) | `secteurActiviteLibelle` n'est pas utilisé : c'est le secteur de l'entreprise, pas le domaine du métier (règle R04) |

### Tables `competence` et `exigence_offre`

| Colonne cible | Champ source | Transformation | Remarque |
|---|---|---|---|
| `competence.libelle_competence` | `competences[].libelle` | Espaces réduits, tronqué à 300 | UNIQUE sur libellé normalisé (règle R01) |
| `competence.type_competence` | — | Valeur fixe `Savoir-faire` | Les savoir-être (`qualitesProfessionnelles`) ne sont pas chargés, voir [limites connues](../known-limitations.md) |
| `exigence_offre.statut_exigence` | `competences[].exigence` | `E…` → `E`, sinon `S` | L'API renvoie `E`/`S` ou `Exigée`/`Souhaitée` |

### Table `tp2_offre_enrichment`

| Colonne cible | Champ source | Transformation |
|---|---|---|
| `source_offre_id` | `source_offer_id` de l'enregistrement agrégé | FK vers `offre` |
| `commune_reference_status` | calculé par l'agrégateur | `matched`, `not_found` ou `missing_source2` |
| `geo_nom_commune`, `geo_code_postal` | `commune_reference.nom`, `codesPostaux[0]` | Source 2 brute |
| `geo_longitude`, `geo_latitude` | `commune_reference.centre.coordinates` | Source 2 brute |
| `aggregated_at` | `aggregated_at` | Horodatage de l'enrichissement |

## 3. Champs source non chargés dans le modèle 3NF

Ces champs restent disponibles dans la zone `raw` du Data Lake. La fiche
détaillée de l'application Dash les lit à la demande.

| Champ France Travail | Usage |
|---|---|
| `origineOffre.urlOrigine` | Lien « Voir l'offre sur France Travail » |
| `contact.urlPostulation` | Bouton « Postuler » (≈ 27 % des offres) |
| `experienceLibelle`, `qualificationLibelle`, `formations`, `langues`, `permis` | Profil recherché dans la fiche |
| `qualitesProfessionnelles` | Savoir-être affichés dans la fiche |
| `secteurActiviteLibelle`, `trancheEffectifEtab` | Informations entreprise dans la fiche |
| `nombrePostes`, `alternance`, `accessibleTH`, `deplacementLibelle` | Informations complémentaires |
| `contact.nom`, `contact.courriel`, `contact.telephone` | Jamais chargés ni affichés (données personnelles, voir [licences et RGPD](../governance/licences-rgpd.md)) |

## 4. Motifs de rejet

| Motif | Étape | Destination |
|---|---|---|
| `unsupported_schema_version`, `missing_event_id`, `payload_not_object`… | Agrégateur (`validate_offer_event`) | `quarantine/aggregator/` |
| `invalid_code_insee`, `invalid_rome_code`, `invalid_date_publication`, `missing_job_title`, `invalid_type_contrat` | Spark | `quarantine/spark/run_id=<id>/` |
| `postgresql_load_rejected` | Chargement PostgreSQL (contrainte violée) | `quarantine/spark/run_id=<id>/postgresql_rejections.json` |
