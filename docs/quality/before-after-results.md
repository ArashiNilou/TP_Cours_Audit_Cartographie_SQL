# Résultats de l'audit qualité

## Périmètre

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

## Effet immédiat du nettoyage SQL

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

## Effet durable après rejeu PySpark

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

## Corrections journalisées

| Règle | Enregistrements tracés | Effet |
|---|---:|---|
| `R01_COMPETENCE` | 114 compétences fusionnées | 98 groupes de doublons supprimés |
| `R02_ENTREPRISE` | 20 entreprises fusionnées | 293 offres rattachées à la clé canonique |
| `R03_SALAIRE` | 7 614 valeurs neutralisées | 0 salaire hors échelle après correction durable |

## Interprétation

Toutes les anomalies d'intégrité et de validité bloquantes sont à zéro. Les
alertes restantes correspondent à de vraies absences de la source : les
remplir artificiellement diminuerait le nombre d'anomalies mais dégraderait la
fiabilité. Le choix est donc de les conserver, de les mesurer et de les
exposer comme limites du jeu de données.
