# Architecture de la plateforme

Ce document regroupe l'organisation du Data Lake, les formats de données
échangés entre composants et les choix d'architecture. Les schémas associés
sont [`pipeline.mmd`](pipeline.mmd) (pipeline complet) et
[`data-quality.mmd`](data-quality.mmd) (place de l'audit qualité).

## Sommaire

1. [Data Lake](#1-data-lake)
2. [Contrats d'événements](#2-contrats-dévénements)
3. [Choix d'architecture](#3-choix-darchitecture)

## 1. Data Lake

Le Data Lake est le volume Docker `data_lake`, monté sur `/data-lake` dans les
conteneurs. Il conserve toutes les versions reçues des offres, ce qui permet de
rejouer le pipeline et de revenir à la donnée d'origine à tout moment.

### Organisation des zones

| Zone | Chemin | Contenu | Format | Partition | Écrit par | Lu par |
|---|---|---|---|---|---|---|
| Brute offres | `raw/france_travail/` | Un événement Kafka par fichier, tel que reçu | JSON (clés triées) | `ingestion_date=AAAA-MM-JJ/<event_id>.json` | `raw-aggregator` | Dash (fiche détaillée, lecture seule) |
| Brute communes | `raw/communes/` | Référentiel complet de l'API Géo, un instantané par collecte | JSON | `ingestion_date=AAAA-MM-JJ/communes_<horodatage>.json` + `_latest.json` | `communes-collector` | `raw-aggregator` |
| Agrégée | `aggregated/offres/` | Offre + référence commune + statut de rapprochement | JSONL (`tp2.aggregated_offer.v1`) | `ingestion_date=AAAA-MM-JJ/part-<event_id>.jsonl` | `raw-aggregator` | `spark-batch` |
| Curée | `curated/offres/` | Offres valides, une seule version par offre | Parquet | `run_id=<id>/` | `spark-batch` | analyses ad hoc |
| Quarantaine | `quarantine/aggregator/`, `quarantine/spark/` | Enregistrements rejetés avec leur motif | JSON | `ingestion_date=…/` ou `run_id=<id>/` | agrégateur, Spark | audit, diagnostic |

### Volumétrie observée (28/09/2026)

| Zone | Fichiers | Taille |
|---|---:|---:|
| `raw/france_travail` | 43 988 | 193 Mo |
| `raw/communes` | 8 | 37 Mo |
| `aggregated/offres` | 43 988 | 379 Mo |
| `curated/offres` | 572 | 726 Mo |
| `quarantine` | 12 662 | 119 Mo |

La base PostgreSQL `emploie` occupe 116 Mo pour 21 916 offres.

### Principes d'écriture

- **Écriture atomique** : chaque fichier est d'abord écrit en `.tmp`, vidé sur
  disque (`fsync`) puis renommé. Un fichier à moitié écrit n'est jamais visible.
- **Idempotence** : l'identifiant `event_id` est déterministe (UUID v5 de
  `id` + date d'actualisation). Une même version d'offre reçue deux fois ne crée
  qu'un seul fichier brut (compteur `tp2_aggregator_duplicate_events_total`).
- **Aucune perte** : l'offset Kafka n'est validé qu'après l'écriture durable du
  fichier. En cas d'erreur, le message est relu.
- **Immutabilité de la zone brute** : ni le nettoyage SQL (TP3) ni Spark ne
  modifient `raw/`. Les corrections sont faites en aval.

### Rétention

Aucune purge automatique n'est en place. La zone `curated` grossit d'un
dossier par exécution Spark (toutes les 30 minutes), ce qui explique qu'elle
soit la plus volumineuse. Voir [limites connues](../known-limitations.md) et la
procédure de purge dans le [guide d'exploitation](../operations/runbook.md).

## 2. Contrats d'événements

Les formats échangés entre composants sont versionnés par le champ
`schema_version`. Ils sont définis dans
`src/emploi_pipeline/event_contracts.py` et couverts par
`tests/test_event_contracts.py`.

### Topic Kafka

| Propriété | Valeur |
|---|---|
| Nom | `france-travail.offres.raw` (variable `FT_OFFERS_TOPIC`) |
| Partitions | 3 (variable `FT_OFFERS_TOPIC_PARTITIONS`) |
| Réplication | 1 (poste de développement, un seul broker KRaft) |
| Clé du message | `source_offer_id` : toutes les versions d'une offre vont dans la même partition, donc restent ordonnées |
| Valeur | JSON UTF-8, clés triées |
| Producteur | `ft-producer`, `acks=all`, 5 tentatives |
| Consommateur | groupe `tp2-raw-aggregator`, validation manuelle des offsets après écriture |

### `tp2.offer.v1` — événement publié dans Kafka

| Champ | Type | Obligatoire | Description |
|---|---|---|---|
| `schema_version` | string | oui | Toujours `tp2.offer.v1` |
| `event_id` | UUID | oui | UUID v5 de `france-travail:<id>:<dateActualisation ou dateCreation>` : identique pour une même version d'offre |
| `source` | string | oui | Toujours `france_travail` |
| `source_offer_id` | string | oui | Identifiant France Travail (`payload.id`) |
| `collected_at` | datetime ISO 8601 UTC | oui | Heure de collecte |
| `payload` | objet | oui | Offre brute renvoyée par l'API, non modifiée |

Exemple (valeurs illustratives, `payload` abrégé) :

```json
{
  "collected_at": "2026-09-28T09:15:02.418Z",
  "event_id": "3f1c2c8e-7d0a-5b8e-9a51-2f1e4d6c7b90",
  "payload": { "id": "199KQZL", "intitule": "Chauffeur de car scolaire", "...": "..." },
  "schema_version": "tp2.offer.v1",
  "source": "france_travail",
  "source_offer_id": "199KQZL"
}
```

Règles de validation (`validate_offer_event`), en cas d'échec l'événement part
en `quarantine/aggregator/` avec le motif :

| Motif | Condition |
|---|---|
| `unsupported_schema_version` | `schema_version` différent de `tp2.offer.v1` |
| `unsupported_source` | `source` différent de `france_travail` |
| `missing_event_id`, `missing_source_offer_id`, `missing_collected_at` | champ vide |
| `invalid_event_id` | pas un UUID |
| `payload_not_object` | `payload` n'est pas un objet JSON |
| `invalid_lieu_travail` | `payload.lieuTravail` présent mais pas un objet |
| `invalid_collected_at` | date non ISO 8601 |

### `tp2.aggregated_offer.v1` — enregistrement agrégé (zone `aggregated`)

| Champ | Type | Description |
|---|---|---|
| `schema_version` | string | `tp2.aggregated_offer.v1` |
| `event_id`, `source`, `source_offer_id`, `collected_at` | — | Recopiés de l'événement |
| `aggregated_at` | datetime | Heure d'enrichissement, sert au dédoublonnage Spark (version la plus récente gardée) |
| `ingestion_date` | date | Partition |
| `code_insee` | string / null | Code commune de l'offre, normalisé |
| `commune_reference_status` | string | `matched`, `not_found`, `missing_source2` |
| `commune_reference` | objet / null | Commune officielle de l'API Géo (`nom`, `code`, `codesPostaux`, `centre`) |
| `payload` | objet | Offre brute |
| `enriched_payload` | objet | Offre dont `lieuTravail` est remplacé par les valeurs officielles quand la commune est retrouvée |

### Évolution d'un contrat

Toute modification incompatible (champ supprimé ou renommé) impose une nouvelle
version (`tp2.offer.v2`). Les consommateurs rejettent en quarantaine les
versions qu'ils ne connaissent pas : rien n'est perdu, les messages peuvent être
retraités après mise à jour.

## 3. Choix d'architecture

Chaque décision est présentée au format court « contexte, décision,
alternatives écartées, conséquences ».

### D01 — Modèle relationnel en 3ᵉ forme normale

- **Contexte** : une offre France Travail est un document JSON qui répète
  l'entreprise, la commune, le métier et la liste des compétences.
- **Décision** : six tables 3NF (`offre`, `entreprise`, `commune`,
  `metier_rome`, `competence`, `exigence_offre`).
- **Alternatives écartées** : table plate unique (redondance, anomalies de mise
  à jour) ; stockage JSONB (requêtes et contraintes plus difficiles).
- **Conséquences** : chaque information n'est écrite qu'une fois, l'intégrité
  est garantie par les clés et `CHECK`. Une vue dénormalisée
  (`tp2_dashboard_offres`) est fournie pour la BI.

### D02 — Kafka entre la collecte et le stockage

- **Contexte** : la collecte tourne en continu et ne doit pas dépendre de la
  disponibilité du stockage.
- **Décision** : le producteur publie dans un topic Kafka ; l'agrégateur
  consomme et ne valide l'offset qu'après écriture durable.
- **Alternatives écartées** : écriture directe en base (perte en cas de panne,
  couplage fort) ; file RabbitMQ (pas de relecture de l'historique).
- **Conséquences** : aucune perte si un composant aval tombe ; relecture
  possible. Un broker unique suffit en local, mais n'offre pas de tolérance aux
  pannes matérielles.

### D03 — Data Lake en zones (raw, aggregated, curated, quarantine)

- **Contexte** : il faut pouvoir revenir à la donnée d'origine et rejouer les
  traitements après une correction.
- **Décision** : zone brute immuable, zone enrichie, zone propre en Parquet et
  zone de quarantaine avec motif de rejet. Voir [Data Lake](#1-data-lake).
- **Alternatives écartées** : garder uniquement la base (impossible de rejouer
  après un bug de transformation).
- **Conséquences** : la correction des salaires (TP3) a pu être rejouée sur
  tout l'historique. Le volume disque augmente et demande une rétention.

### D04 — PySpark en traitement par lots toutes les 30 minutes

- **Contexte** : validation, dédoublonnage par offre et chargement de dizaines
  de milliers d'enregistrements.
- **Décision** : batch PySpark rejouable, lancé toutes les 30 minutes.
- **Alternatives écartées** : Spark Structured Streaming (complexité non
  justifiée par la fréquence de publication des offres) ; pandas (tout en
  mémoire sur une seule machine, ne passe pas à l'échelle).
- **Conséquences** : une nouvelle offre apparaît au plus 30 minutes après sa
  collecte. Le dédoublonnage garde la version la plus récente.

### D05 — Enrichissement par l'API Géo

- **Contexte** : les noms et positions de communes de France Travail sont
  parfois absents ou non officiels.
- **Décision** : référentiel officiel des communes collecté une fois par jour et
  rapproché par code INSEE ; statut tracé dans `tp2_offre_enrichment`.
- **Conséquences** : 98,5 % des offres rattachées à une commune officielle ;
  les autres sont conservées et signalées (`not_found`).

### D06 — PostgreSQL comme stockage de référence

- **Décision** : PostgreSQL 16, contraintes déclaratives, upserts idempotents
  (`ON CONFLICT`).
- **Conséquences** : un rejeu ne crée jamais de doublon. Metabase, Dash et
  postgres-exporter lisent la même source.

### D07 — Deux outils de restitution : Metabase et Dash

- **Décision** : Metabase pour l'exploration libre, Dash pour une application
  sur mesure (filtres, carte, fiche détaillée d'offre, affichage adaptatif).
- **Conséquences** : Dash utilise un cache mémoire rafraîchi toutes les 60 s et
  tourne avec **un seul processus gunicorn (8 threads)** : le cache et l'index
  des fichiers bruts vivent dans ce processus. Plusieurs processus
  dupliqueraient le cache et ont provoqué des erreurs de chargement.

### D08 — Fiche détaillée lue dans la zone brute

- **Contexte** : afficher toutes les informations d'une offre (URL, profil,
  savoir-être) sans alourdir le modèle 3NF.
- **Décision** : le volume du Data Lake est monté **en lecture seule** dans Dash ;
  un index `source_offer_id → fichier` est construit en arrière-plan.
- **Conséquences** : le modèle reste minimal ; la fiche s'ouvre en 0,3 s environ.

### D09 — Qualité : corriger à la source, ne rien inventer

- **Décision** : chaque correction est faite dans une transaction, journalisée
  dans `tp3_cleaning_log` et reportée dans le code du pipeline pour ne pas
  réapparaître. Les valeurs manquantes ne sont pas imputées.
- **Conséquences** : les erreurs graves sont à zéro ; les absences de la source
  restent visibles et mesurées.

### D10 — Supervision Prometheus + Grafana

- **Décision** : chaque service Python expose `/metrics` ; exporters PostgreSQL,
  Kafka et cAdvisor ; tableau de bord Grafana provisionné.
- **Conséquences** : une panne ou un retard est visible immédiatement. Aucune
  alerte automatique n'est encore configurée.

### D11 — Docker Compose

- **Décision** : toute la plateforme (16 services, dont 2 tâches ponctuelles
  d'initialisation) démarre avec `docker compose up -d`.
- **Conséquences** : installation reproductible sur n'importe quel poste.
  Pour une production, il faudrait des secrets gérés, plusieurs brokers Kafka
  et des sauvegardes planifiées.
