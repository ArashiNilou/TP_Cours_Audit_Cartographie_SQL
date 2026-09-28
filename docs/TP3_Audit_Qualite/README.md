# TP3 : Audit Qualité et Nettoyage des données

Ce dossier contient les livrables du TP3. Il démontre la démarche "Data Stewardship" appliquée au Data Warehouse (PostgreSQL) une fois les données ingérées par les pipelines ELT (PySpark & API).

## Démarche globale

L'organisation des fichiers reflète le cycle de vie d'un audit qualité en entreprise :

1. **Cartographie et Règles** : 
   - Fichier : [`1_matrice_controles.md`](1_matrice_controles.md)
   - *Objectif :* Traduire les besoins métiers en règles de base de données (Complétude, Unicité, Validité, Cohérence, Intégrité).

2. **Phase 1 : Détection (Audit Avant)** :
   - Fichier : [`sql/01_audit_avant.sql`](sql/01_audit_avant.sql)
   - *Objectif :* Écrire des requêtes SQL de profilage (Data Profiling) pour mesurer l'état de la donnée brute.

3. **Phase 2 : Nettoyage et Normalisation** :
   - Fichier : [`sql/02_nettoyage.sql`](sql/02_nettoyage.sql)
   - *Objectif :* Exécuter les `UPDATE` et `DELETE` justifiés par la matrice (ex: filtrage des valeurs aberrantes, dédoublonnage, normalisation textuelle).

4. **Phase 3 : Vérification (Audit Après)** :
   - Fichier : [`sql/03_audit_apres.sql`](sql/03_audit_apres.sql)
   - *Objectif :* Relancer l'audit pour prouver l'efficacité du script de nettoyage (Data Quality Checks).

## Justification des actions de nettoyage

- **Suppression (Spam)** : Les offres postées plusieurs fois le même jour par la même entreprise avec le même titre sont considérées comme des erreurs de "re-post" des recruteurs (spam). On garde l'ID le plus récent et on supprime les autres.
- **Validité des Salaires (Refus d'imputation)** : Beaucoup d'offres n'ont pas de salaire défini. **Nous avons fait le choix statistique fort de ne PAS imputer ces valeurs par des moyennes**. Remplacer un salaire non renseigné par une moyenne sectorielle crée un biais énorme et fausse la réalité du "marché caché" de l'emploi. Le script `02_nettoyage.sql` se contente donc de repérer les valeurs totalement irréalistes (Outliers : < 15k€ ou > 300k€) pour les passer à `NULL`, mais préserve les `NULL` existants comme étant une information authentique.
- **Correction** : Les noms de villes et d'entreprises sont standardisés (`TRIM` et `UPPER`) pour éviter que le tableau de bord compte "Paris" et "PARIS " comme deux lieux différents.

## Comment l'exécuter

Pour simuler ce nettoyage directement dans la base de données PostgreSQL, connectez-vous au conteneur Docker et exécutez les scripts dans l'ordre :

```bash
docker exec -it postgres_emploi psql -U postgres -d emploi
```
