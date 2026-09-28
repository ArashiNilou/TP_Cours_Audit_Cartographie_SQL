# Documentation technique

## 1. Architecture contrôlée

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

## 2. Objets PostgreSQL ajoutés

| Objet | Rôle |
|---|---|
| `tp3_quality_run` | Identifie chaque audit et son périmètre |
| `tp3_quality_result` | Conserve les 17 résultats d'un audit |
| `tp3_cleaning_log` | Journalise chaque correction |
| `tp3_quality_current` | Calcule les contrôles sur l'état présent |
| `tp3_quality_latest_comparison` | Compare les derniers audits avant/après |
| `tp3_quality_cleaning_comparison` | Compare le run initial à l'effet immédiat SQL |
| `tp3_execute_quality_audit()` | Prend un instantané historisé |

## 3. Corrections

### Doublons de compétences

La clé canonique est le plus petit `competence_id` d'un groupe identique après
normalisation de la casse et des espaces. Les lignes de `exigence_offre` sont
reliées à cette clé. En cas de conflit, le statut exigé (`E`) prévaut.

### Doublons d'entreprises

La même règle choisit l'entreprise canonique. Les offres sont mises à jour
avant la suppression de la variante, ce qui préserve toutes les clés
étrangères.

Deux index fonctionnels uniques (`ux_competence_normalized_label` et
`ux_entreprise_normalized_name`) rendent cette règle concurrente et durable,
tout en accélérant les recherches normalisées de l'ingestion.

### Salaires

L'ancien parseur mélangeait parfois le taux horaire avec des heures présentes
dans les commentaires et ne convertissait pas les périodicités horaires.

Le parseur corrigé :

- ne lit que les nombres immédiatement suivis de `Euros` ;
- calcule la moyenne d'une fourchette ;
- convertit mensuel × 12, horaire × 35 × 52, hebdomadaire × 52 et
  journalier × 218 ;
- renvoie `NULL` hors de la plage 1 000–250 000 € annuels.

La valeur brute n'est jamais perdue : elle reste dans le JSON du Data Lake.

## 4. Rejouabilité et intégrité

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

## 5. Limites assumées

- une offre sans compétence reste exploitable pour les analyses hors
  compétences ;
- un salaire absent n'est pas remplacé par une moyenne, qui créerait une
  fausse information ;
- quatre communes sans coordonnées restent inchangées, car l'API Géo ne
  fournit pas de coordonnées utilisables pour ces codes ;
- les trois offres de démonstration sans enrichissement sont conservées et identifiées ;
- 317 rapprochements géographiques non appariés restent auditables.
