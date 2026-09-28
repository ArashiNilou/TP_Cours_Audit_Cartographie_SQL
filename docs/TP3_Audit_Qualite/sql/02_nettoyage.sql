-- ==============================================================================
-- TP3 : NETTOYAGE ET CORRECTION DES DONNÉES
-- Objectif : Appliquer les règles correctives de la matrice
-- ==============================================================================

-- 1. COHÉRENCE : Nettoyage des chaînes de caractères (Communes)
UPDATE commune
SET nom_commune = UPPER(TRIM(nom_commune))
WHERE nom_commune != UPPER(TRIM(nom_commune));

-- 2. VALIDITÉ (Outliers) : Passer à NULL les salaires totalement irréalistes
-- Remarque métier : Nous faisons le choix conscient de NE PAS imputer les salaires 
-- manquants par une moyenne, car cela fausserait l'analyse statistique du marché.
UPDATE offre
SET salaire_brut_annuel_estime = NULL
WHERE salaire_brut_annuel_estime < 15000 
   OR salaire_brut_annuel_estime > 300000;

-- 3. UNICITÉ (Dédoublonnage) : Supprimer les offres strictement identiques (même entreprise, même poste, même jour)
-- On garde uniquement l'offre avec le source_offre_id le plus grand (arbitraire) pour éliminer les copies.
DELETE FROM offre
WHERE source_offre_id IN (
    SELECT source_offre_id
    FROM (
        SELECT 
            source_offre_id,
            ROW_NUMBER() OVER (
                PARTITION BY entreprise_id, libelle_poste, date_publication 
                ORDER BY source_offre_id DESC
            ) AS rn
        FROM offre
    ) doublons
    WHERE rn > 1
);

-- 4. COHÉRENCE : Harmonisation des entreprises
UPDATE entreprise
SET raison_sociale = UPPER(TRIM(raison_sociale))
WHERE raison_sociale IS NOT NULL 
  AND raison_sociale != UPPER(TRIM(raison_sociale));
