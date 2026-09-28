# Audit qualité et nettoyage des données

Ce dossier documente la couche qualité du projet unifié. Elle audite les
données chargées dans PostgreSQL, corrige uniquement les anomalies
justifiables et conserve les données sources du Data Lake intactes.

## Sommaire

1. [Livrables et exécution](#1-livrables-et-exécution)
2. [Matrice des contrôles](#2-matrice-des-contrôles)
3. [Documentation technique](#3-documentation-technique)
4. [Résultats de l'audit](#4-résultats-de-laudit)
5. [Synthèse pour l'oral](#5-synthèse-pour-loral)


---

## 1. Livrables et exécution

### Livrables

| Fichier | Contenu |
|---|---|
| [`../architecture/data-quality.mmd`](../architecture/data-quality.mmd) | Cheminement mis à jour, de la source au contrôle qualité |
| [Section 2](#2-matrice-des-contrôles) | 17 contrôles couvrant les cinq dimensions de qualité |
| [`../../database/quality/00_audit_objects.sql`](../../database/quality/00_audit_objects.sql) | Tables, vues et fonction d'audit rejouables |
| [`../../database/quality/01_audit_before.sql`](../../database/quality/01_audit_before.sql) | Mesure et historisation de l'état initial |
| [`../../database/quality/02_clean_data.sql`](../../database/quality/02_clean_data.sql) | Corrections transactionnelles et journalisées |
| [`../../database/quality/03_audit_after.sql`](../../database/quality/03_audit_after.sql) | Effet immédiat du nettoyage SQL |
| [`../../database/quality/04_audit_pipeline.sql`](../../database/quality/04_audit_pipeline.sql) | Effet durable après rejeu de PySpark |
| [Section 4](#4-résultats-de-laudit) | Résultats mesurés sur la base réelle |
| [Section 3](#3-documentation-technique) | Choix, règles, limites et procédure d'exécution |
| [Section 5](#5-synthèse-pour-loral) | Trame courte pour la restitution au professeur |
| [`results/before-after.csv`](results/before-after.csv) | Export exploitable de la comparaison |

### Exécution

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

### Principe de sécurité

- aucune suppression du Data Lake ;
- aucun `DROP` des tables métier ou du pipeline ;
- toutes les corrections SQL sont dans une transaction ;
- chaque valeur corrigée est tracée dans `tp3_cleaning_log` ;
- aucune donnée manquante n'est inventée ;
- le parseur Python est corrigé pour éviter la réapparition des anomalies.

---

## 2. Matrice des contrôles

Les contrôles sont exécutés par la vue `tp3_quality_current`. Un résultat
`PASS` signifie zéro anomalie. Une anomalie `HIGH` produit `FAIL`; les autres
niveaux produisent `WARN`.

| Code | Dimension | Contrôle | Population | Règle / seuil | Gravité | Décision |
|---|---|---|---|---|---|---|
| C01 | Complétude | Description absente | Offres | `NULL` ou vide | LOW | Conserver `NULL`, ne pas inventer de texte |
| C02 | Complétude | Durée de travail absente | Offres | `NULL` ou vide | LOW | Conserver et mesurer |
| C03 | Complétude | Salaire absent | Offres | `NULL` | INFO | Pas d'imputation statistique |
| C04 | Complétude | Offre sans compétence | Offres | aucune ligne dans `exigence_offre` | MEDIUM | Conserver l'offre, signaler la lacune |
| C05 | Complétude | Coordonnées communales incomplètes | Communes | latitude ou longitude `NULL` | LOW | Compléter seulement depuis l'API Géo |
| U01 | Unicité | Identifiant source dupliqué | Offres | doublon de `source_offre_id` | HIGH | Rejet ; contrainte `UNIQUE` |
| U02 | Unicité | Ligne de compétence redondante | Compétences | même valeur après `lower(trim(espaces))` | MEDIUM | Fusionner vers la plus ancienne clé |
| U03 | Unicité | Ligne d'entreprise redondante | Entreprises | même valeur après `lower(trim(espaces))` | MEDIUM | Fusionner vers la plus ancienne clé |
| V01 | Validité | Date future | Offres | `date_publication > current_date` | HIGH | Rejeter |
| V02 | Validité | Salaire nul ou négatif | Offres salariées | `<= 0` | HIGH | Mettre à `NULL`, corriger le parseur |
| V03 | Validité | Salaire annuel hors échelle | Offres | `< 1 000` ou `> 250 000` € | MEDIUM | Mettre à `NULL`, conserver le JSON brut |
| V04 | Validité | Format INSEE, postal ou ROME | Référentiels | non conforme aux expressions régulières | HIGH | Rejeter ; contraintes `CHECK` |
| H01 | Cohérence | Nom d'entreprise / anonymat | Entreprises | nom incompatible avec le booléen | HIGH | Corriger selon la valeur source |
| H02 | Cohérence | Commune non appariée | Enrichissements | statut différent de `matched` | MEDIUM | Conserver le statut, investiguer le code |
| H03 | Cohérence | Offre sans enrichissement du pipeline | Offres | absence dans `tp2_offre_enrichment` | INFO | Toléré pour les 3 offres de démonstration |
| I01 | Intégrité | Référence orpheline dans `offre` | Offres | commune, entreprise ou ROME absent | HIGH | Doit être nul grâce aux FK |
| I02 | Intégrité | Association orpheline | Exigences | offre ou compétence absente | HIGH | Doit être nul grâce aux FK |

### Règles de correction retenues

1. **Fusionner les doublons strictement normalisés** : aucune fusion
   approximative ou sémantique n'est effectuée.
2. **Préserver le statut d'exigence le plus fort** : si deux variantes de la
   même compétence existent pour une offre, `E` (exigée) prévaut sur `S`.
3. **Neutraliser un salaire non fiable** : la valeur dérivée devient `NULL`,
   mais le texte original reste dans le Data Lake.
4. **Ne pas imputer les champs métier manquants** : description, durée,
   compétence ou coordonnées restent manquantes si aucune source officielle
   ne permet de les compléter.
5. **Garantir la règle dans PostgreSQL** : deux index fonctionnels `UNIQUE`
   empêchent désormais le retour des doublons normalisés.

---

## 3. Documentation technique

### 3.1 Architecture contrôlée

La couche qualité contrôle la sortie PostgreSQL du pipeline :

```text
France Travail + API Géo
          ↓
Kafka → Data Lake raw → aggregated → PySpark → curated
                                                ↓
                                       PostgreSQL 3NF
                                                ↓
                          Audit avant → nettoyage → audit après
```

Le Data Lake reste la source de vérité. Les corrections SQL ne modifient
jamais les fichiers `raw/`.

### 3.2 Objets PostgreSQL ajoutés

| Objet | Rôle |
|---|---|
| `tp3_quality_run` | Identifie chaque audit et son périmètre |
| `tp3_quality_result` | Conserve les 17 résultats d'un audit |
| `tp3_cleaning_log` | Journalise chaque correction |
| `tp3_quality_current` | Calcule les contrôles sur l'état présent |
| `tp3_quality_latest_comparison` | Compare les derniers audits avant/après |
| `tp3_quality_cleaning_comparison` | Compare le run initial à l'effet immédiat SQL |
| `tp3_execute_quality_audit()` | Prend un instantané historisé |

### 3.3 Corrections

#### Doublons de compétences

La clé canonique est le plus petit `competence_id` d'un groupe identique après
normalisation de la casse et des espaces. Les lignes de `exigence_offre` sont
reliées à cette clé. En cas de conflit, le statut exigé (`E`) prévaut.

#### Doublons d'entreprises

La même règle choisit l'entreprise canonique. Les offres sont mises à jour
avant la suppression de la variante, ce qui préserve toutes les clés
étrangères.

Deux index fonctionnels uniques (`ux_competence_normalized_label` et
`ux_entreprise_normalized_name`) rendent cette règle concurrente et durable,
tout en accélérant les recherches normalisées de l'ingestion.

#### Salaires

L'ancien parseur mélangeait parfois le taux horaire avec des heures présentes
dans les commentaires et ne convertissait pas les périodicités horaires.

Le parseur corrigé :

- ne lit que les nombres immédiatement suivis de `Euros` ;
- calcule la moyenne d'une fourchette ;
- convertit mensuel × 12, horaire × 35 × 52, hebdomadaire × 52 et
  journalier × 218 ;
- renvoie `NULL` hors de la plage 1 000–250 000 € annuels.

La valeur brute n'est jamais perdue : elle reste dans le JSON du Data Lake.

#### Domaine professionnel ROME « A » (R04)

La table de correspondance du parseur associait la lettre `A` au libellé
« Arts et façonnage d'ouvrages d'art », qui correspond en réalité à la lettre
`B`. Dans la nomenclature ROME 4.0, `A` est le grand domaine « Agriculture et
pêche, espaces naturels et espaces verts, soins aux animaux ».

- 52 métiers (611 offres) étaient concernés ;
- la règle `R04_DOMAINE_ROME` corrige `metier_rome.domaine_professionnel`
  et trace l'ancienne valeur dans `tp3_cleaning_log` ;
- `ROME_GRANDS_DOMAINES` est corrigé dans `ingest.py` et couvert par un test
  unitaire, ce qui empêche le pipeline de réintroduire l'erreur.

### 3.4 Rejouabilité et intégrité

- `CREATE ... IF NOT EXISTS` évite les collisions ;
- les vues et la fonction sont remplacées proprement ;
- le nettoyage s'exécute dans une transaction ;
- les clés étrangères restent actives ;
- les fusions ne trouvent plus aucune ligne au deuxième passage ;
- le chargement Python utilise désormais la même normalisation, donc les
  doublons ne réapparaissent pas au prochain batch.

Chaque audit après correction contient `baseline_run_id`. La comparaison ne
dépend donc pas d'un ordre implicite : le run SQL immédiat et le run après
rejeu Spark sont tous les deux reliés au même audit initial.

### 3.5 Limites assumées

- une offre sans compétence reste exploitable pour les analyses hors
  compétences ;
- un salaire absent n'est pas remplacé par une moyenne, qui créerait une
  fausse information ;
- quatre communes sans coordonnées restent inchangées, car l'API Géo ne
  fournit pas de coordonnées utilisables pour ces codes ;
- les trois offres de démonstration sans enrichissement sont conservées et identifiées ;
- 317 rapprochements géographiques non appariés restent auditables.

---

## 4. Résultats de l'audit

### Périmètre

- audit initial : **20 813 offres** ;
- contrôle final : **20 942 offres** ;
- l'écart de 129 offres provient des derniers messages déjà présents dans
  Kafka/Data Lake avant la mise en pause du pipeline ;
- les taux, plutôt que les seuls effectifs, permettent donc la comparaison.

Volumes finaux :

| Table | Lignes |
|---|---:|
| `offre` | 20 942 |
| `entreprise` | 9 836 |
| `commune` | 5 948 |
| `metier_rome` | 839 |
| `competence` | 6 668 |
| `exigence_offre` | 67 391 |
| `tp2_offre_enrichment` | 20 939 |

### Effet immédiat du nettoyage SQL

L'audit initial (run 1) et l'audit juste après la transaction (run 2)
portent tous les deux sur **20 813 offres**.

| Code | Contrôle | Avant | Après SQL | Taux avant | Taux après | État |
|---|---|---:|---:|---:|---:|---|
| U01 | Identifiant offre dupliqué | 0 | 0 | 0 % | 0 % | PASS |
| U02 | Lignes de compétence redondantes | 114 (98 groupes) | 0 | 1,6849 % | 0 % | PASS |
| U03 | Lignes d'entreprise redondantes | 20 | 0 | 0,2039 % | 0 % | PASS |
| V01 | Date future | 0 | 0 | 0 % | 0 % | PASS |
| V02 | Salaire nul ou négatif | 0 | 0 | 0 % | 0 % | PASS |
| V03 | Salaire annuel hors échelle | 7 614 | 0 | 36,5829 % | 0 % | PASS |
| V04 | Format INSEE/postal/ROME invalide | 0 | 0 | 0 % | 0 % | PASS |
| H01 | Anonymat entreprise incohérent | 0 | 0 | 0 % | 0 % | PASS |
| I01 | Référence orpheline dans offre | 0 | 0 | 0 % | 0 % | PASS |
| I02 | Association offre-compétence orpheline | 0 | 0 | 0 % | 0 % | PASS |
| C01 | Description absente | 1 | 1 | 0,0048 % | 0,0048 % | WARN |
| C02 | Durée de travail absente | 46 | 46 | 0,2210 % | 0,2210 % | WARN |
| C03 | Salaire absent | 5 502 | 13 116 | 26,4354 % | 63,0183 % | WARN |
| C04 | Offre sans compétence | 2 487 | 2 487 | 11,9493 % | 11,9493 % | WARN |
| C05 | Commune sans coordonnées complètes | 4 | 4 | 0,0675 % | 0,0675 % | WARN |
| H02 | Enrichissement communal non apparié | 315 | 315 | 1,5137 % | 1,5137 % | WARN |
| H03 | Offre sans enrichissement du pipeline | 3 | 3 | 0,0144 % | 0,0144 % | WARN |

La hausse temporaire des salaires absents est attendue : R03 neutralise 7 614
valeurs non fiables, donc `5 502 + 7 614 = 13 116`. Le parseur corrigé relit
ensuite le JSON brut pour recalculer les taux horaires correctement.

### Effet durable après rejeu PySpark

Le contrôle `after_pipeline_reload` est explicitement lié au run initial par
`baseline_run_id`. Après rejeu :

| Indicateur | Avant | Après rejeu |
|---|---:|---:|
| Lignes de compétence redondantes | 114 | 0 |
| Lignes d'entreprise redondantes | 20 | 0 |
| Salaires hors échelle | 7 614 | 0 |
| Salaires absents | 5 502 | 5 608 |
| Références orphelines | 0 | 0 |

La population finale comporte 129 offres supplémentaires déjà en attente dans
Kafka/Data Lake. Pour cette comparaison, les taux sont donc plus pertinents
que les variations brutes des contrôles non corrigés.

### Corrections journalisées

| Règle | Enregistrements tracés | Effet |
|---|---:|---|
| `R01_COMPETENCE` | 114 compétences fusionnées | 98 groupes de doublons supprimés |
| `R02_ENTREPRISE` | 20 entreprises fusionnées | 293 offres rattachées à la clé canonique |
| `R03_SALAIRE` | 7 614 valeurs neutralisées | 0 salaire hors échelle après correction durable |
| `R04_DOMAINE_ROME` | 52 métiers corrigés | 611 offres rattachées au bon domaine « Agriculture » |

### Interprétation

Toutes les anomalies d'intégrité et de validité bloquantes sont à zéro. Les
alertes restantes correspondent à de vraies absences de la source : les
remplir artificiellement diminuerait le nombre d'anomalies mais dégraderait la
fiabilité. Le choix est donc de les conserver, de les mesurer et de les
exposer comme limites du jeu de données.

---

## 5. Synthèse pour l'oral

### Plan conseillé — 5 à 7 minutes

#### 1. Objectif — 30 secondes

> La couche qualité reprend les données produites automatiquement par le
> pipeline. Mon objectif est de mesurer leur qualité, corriger les anomalies
> fiables, puis prouver le résultat avec un contrôle avant/après.

#### 2. Cartographie — 45 secondes

Montrer [`docs/architecture/data-quality.mmd`](../architecture/data-quality.mmd) :

> Les sources restent France Travail et l'API Géo. Les données passent par
> Kafka, le Data Lake et PySpark avant PostgreSQL. La couche qualité se place
> après le chargement : matrice de contrôles, audit SQL, nettoyage transactionnel,
> journal des corrections et audit final.

#### 3. Matrice qualité — 1 minute

Présenter les cinq dimensions :

- complétude ;
- unicité ;
- validité ;
- cohérence ;
- intégrité.

> J'ai automatisé 17 contrôles, avec une gravité et une décision définies à
> l'avance. Une anomalie n'est pas automatiquement corrigée : il faut une
> source ou une règle métier fiable.

#### 4. Anomalies détectées — 1 minute

Chiffres principaux :

- 114 compétences redondantes réparties dans 98 groupes ;
- 20 groupes d'entreprises en double ;
- 7 614 salaires mal interprétés ;
- aucune clé étrangère orpheline ;
- aucun identifiant d'offre dupliqué ;
- aucune date future.

#### 5. Nettoyage — 1 minute

> J'ai fusionné uniquement les textes strictement identiques après
> normalisation casse/espaces. J'ai conservé la clé la plus ancienne et toutes
> les relations. Pour les salaires, j'ai neutralisé les valeurs non fiables et
> corrigé le parseur à la source : taux horaire annualisé, commentaires
> ignorés et plafond de plausibilité. Toutes les corrections sont journalisées.

#### 6. Résultat — 1 minute

| Indicateur | Avant | Après |
|---|---:|---:|
| Lignes de compétences redondantes | 114 | 0 |
| Doublons entreprises | 20 | 0 |
| Salaires hors échelle | 7 614 | 0 |
| Orphelins relationnels | 0 | 0 |

> Les champs encore manquants ne sont pas inventés. Ils restent en avertissement
> et sont documentés comme limites de la source.

Préciser les deux étapes : le SQL neutralise d'abord les 7 614 salaires
suspects (`NULL`), puis le rejeu du parseur corrigé récupère les montants
horaires fiables depuis le Data Lake.

#### 7. Conclusion — 30 secondes

> Le résultat est un nettoyage traçable, rejouable et durable. Les données
> brutes sont préservées, PostgreSQL reste intègre et les règles ont été
> corrigées dans le pipeline pour éviter la réapparition des anomalies.
