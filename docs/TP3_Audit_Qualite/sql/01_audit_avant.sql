-- ==============================================================================
-- TP3 : AUDIT DES DONNÉES AVANT NETTOYAGE
-- Objectif : Identifier les anomalies selon la matrice de contrôle
-- ==============================================================================

-- 1. Complétude : Pourcentage d'offres sans salaire estimé
SELECT 
    COUNT(*) AS total_offres,
    SUM(CASE WHEN salaire_brut_annuel_estime IS NULL THEN 1 ELSE 0 END) AS offres_sans_salaire,
    ROUND((SUM(CASE WHEN salaire_brut_annuel_estime IS NULL THEN 1 ELSE 0 END)::numeric / COUNT(*)) * 100, 2) AS pourcentage_manquant
FROM offre;

-- 2. Unicité : Doublons potentiels d'entreprises (casse différente ou espaces)
SELECT 
    UPPER(TRIM(raison_sociale)) AS raison_sociale_normalisee, 
    COUNT(*) AS nb_doublons
FROM entreprise
WHERE raison_sociale IS NOT NULL
GROUP BY UPPER(TRIM(raison_sociale))
HAVING COUNT(*) > 1
ORDER BY nb_doublons DESC;

-- 3. Unicité : Offres exactement identiques postées le même jour par la même entreprise
SELECT 
    entreprise_id, 
    libelle_poste, 
    date_publication, 
    COUNT(*) AS nb_publications_identiques
FROM offre
GROUP BY entreprise_id, libelle_poste, date_publication
HAVING COUNT(*) > 1;

-- 4. Cohérence : Noms de communes mal formatés (espaces superflus ou minuscules)
SELECT code_insee, nom_commune 
FROM commune 
WHERE nom_commune != UPPER(TRIM(nom_commune));

-- 5. Validité : Vérification des types de contrats hors standards
SELECT type_contrat, COUNT(*) AS volume
FROM offre
GROUP BY type_contrat
ORDER BY volume DESC;
