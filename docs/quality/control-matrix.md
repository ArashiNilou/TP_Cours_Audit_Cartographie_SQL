# Matrice des contrôles qualité

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

## Règles de correction retenues

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
