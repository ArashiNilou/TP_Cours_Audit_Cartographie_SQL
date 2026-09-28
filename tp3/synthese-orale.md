# Synthèse pour la restitution orale

## Plan conseillé — 5 à 7 minutes

### 1. Objectif — 30 secondes

> Le TP3 reprend les données produites automatiquement dans le TP2. Mon
> objectif est de mesurer leur qualité, corriger les anomalies fiables, puis
> prouver le résultat avec un contrôle avant/après.

### 2. Cartographie — 45 secondes

Montrer `cartographie-tp3.mmd` :

> Les sources restent France Travail et l'API Géo. Les données passent par
> Kafka, le Data Lake et PySpark avant PostgreSQL. Le TP3 se place après le
> chargement : matrice de contrôles, audit SQL, nettoyage transactionnel,
> journal des corrections et audit final.

### 3. Matrice qualité — 1 minute

Présenter les cinq dimensions :

- complétude ;
- unicité ;
- validité ;
- cohérence ;
- intégrité.

> J'ai automatisé 17 contrôles, avec une gravité et une décision définies à
> l'avance. Une anomalie n'est pas automatiquement corrigée : il faut une
> source ou une règle métier fiable.

### 4. Anomalies détectées — 1 minute

Chiffres principaux :

- 114 compétences redondantes réparties dans 98 groupes ;
- 20 groupes d'entreprises en double ;
- 7 614 salaires mal interprétés ;
- aucune clé étrangère orpheline ;
- aucun identifiant d'offre dupliqué ;
- aucune date future.

### 5. Nettoyage — 1 minute

> J'ai fusionné uniquement les textes strictement identiques après
> normalisation casse/espaces. J'ai conservé la clé la plus ancienne et toutes
> les relations. Pour les salaires, j'ai neutralisé les valeurs non fiables et
> corrigé le parseur à la source : taux horaire annualisé, commentaires
> ignorés et plafond de plausibilité. Toutes les corrections sont journalisées.

### 6. Résultat — 1 minute

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

### 7. Conclusion — 30 secondes

> Le résultat est un nettoyage traçable, rejouable et durable. Les données
> brutes sont préservées, PostgreSQL reste intègre et les règles ont été
> corrigées dans le pipeline pour éviter la réapparition des anomalies.
