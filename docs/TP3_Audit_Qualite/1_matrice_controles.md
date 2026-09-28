# Matrice des Contrôles Qualité (TP3)

Cette matrice définit les règles de gestion et de qualité appliquées aux données du Data Warehouse (PostgreSQL) concernant les offres d'emploi.

| Catégorie | Entité | Champ | Règle de contrôle | Action corrective (Nettoyage) | Justification |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Validité (Outliers)** | `offre` | `salaire_brut_annuel_estime` | Le salaire doit être réaliste (compris entre 15k€ et 300k€). | Les valeurs aberrantes sont passées à `NULL`. **Aucune imputation par la moyenne n'est faite** pour les salaires non renseignés. | Imputer un salaire fausse la réalité statistique. Un salaire "non renseigné" est une vraie information métier à conserver telle quelle. |
| **Unicité** | `entreprise` | `raison_sociale` | Pas de doublons sur le nom de l'entreprise (insensible à la casse). | Fusion des doublons (Dédoublonnage) et réaffectation des clés étrangères. | Éviter d'avoir "Capgemini" et "CAPGEMINI" comme deux entités distinctes. |
| **Validité** | `offre` | `type_contrat` | Doit appartenir à une liste fermée (CDI, CDD, MIS, etc.). | Harmonisation (ex: "Contrat à durée indéterminée" -> "CDI") ou suppression si aberrant. | Standardiser les axes d'analyse. |
| **Cohérence** | `commune` | `nom_commune` | Le format doit être propre (Majuscules, pas d'espaces superflus). | Application de `TRIM()` et `UPPER()`. | Rendre l'affichage propre dans les tableaux de bord. |
| **Intégrité** | `offre` | `entreprise_id`, `rome_code` | Toute offre doit être rattachée à une entreprise et un métier valide. | Suppression des offres orphelines (ON DELETE CASCADE). | Maintenir l'intégrité référentielle (3NF). |
| **Unicité** | `offre` | `libelle_poste`, `entreprise_id` | Éviter qu'une même entreprise poste 10 fois la même offre le même jour. | Suppression des doublons stricts (partition by entreprise, libelle, date). | Ne pas fausser les volumes de recrutement réels. |
