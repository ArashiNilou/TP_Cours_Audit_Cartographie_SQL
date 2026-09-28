-- ==============================================================================
-- TP3 : AUDIT DES DONNÉES APRÈS NETTOYAGE
-- Objectif : Vérifier l'efficacité des corrections
-- ==============================================================================

-- 1. Complétude : Pourcentage d'offres sans salaire estimé (devrait avoir drastiquement baissé)
SELECT 
    COUNT(*) AS total_offres,
    SUM(CASE WHEN salaire_brut_annuel_estime IS NULL THEN 1 ELSE 0 END) AS offres_sans_salaire,
    ROUND((SUM(CASE WHEN salaire_brut_annuel_estime IS NULL THEN 1 ELSE 0 END)::numeric / COUNT(*)) * 100, 2) AS pourcentage_manquant
FROM offre;

-- 2. Unicité : Offres exactement identiques (devrait retourner 0 ligne)
SELECT 
    entreprise_id, 
    libelle_poste, 
    date_publication, 
    COUNT(*) AS nb_publications_identiques
FROM offre
GROUP BY entreprise_id, libelle_poste, date_publication
HAVING COUNT(*) > 1;

-- 3. Cohérence : Noms de communes mal formatés (devrait retourner 0 ligne)
SELECT code_insee, nom_commune 
FROM commune 
WHERE nom_commune != UPPER(TRIM(nom_commune));
