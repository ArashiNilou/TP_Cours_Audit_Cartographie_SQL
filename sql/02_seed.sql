-- =============================================================================
-- TP Audit / Cartographie / Modélisation
-- Domaine : Emploi, marché du travail et recrutement
-- Fichier : 02_seed.sql
-- Objet   : jeu de données de test cohérent (à exécuter après 01_schema.sql).
-- SGBD cible : PostgreSQL 14+
-- =============================================================================

BEGIN;

-- 2 communes (bassins d'emploi)
INSERT INTO commune (code_insee, code_postal, nom_commune, latitude, longitude) VALUES
    ('44172', '44980', 'Sainte-Luce-sur-Loire', 47.249400, -1.486200),
    ('75111', '75011', 'Paris 11e Arrondissement', 48.857700, 2.379900);

-- 2 entreprises, dont une anonyme
INSERT INTO entreprise (raison_sociale, entreprise_anonyme) VALUES
    ('TECH INNOVATION', FALSE),
    (NULL, TRUE);

-- 2 fiches métier ROME
INSERT INTO metier_rome (rome_code, libelle_fiche_metier, domaine_professionnel) VALUES
    ('M1805', 'Études et développement informatique', 'Systèmes d''information et télécommunications'),
    ('M1806', 'Conseil et maîtrise d''ouvrage en systèmes d''information', 'Systèmes d''information et télécommunications');

-- 4 compétences (savoir-faire et savoir-être)
INSERT INTO competence (libelle_competence, type_competence) VALUES
    ('Langage SQL', 'Savoir-faire'),
    ('Concevoir une base de données', 'Savoir-faire'),
    ('Python', 'Savoir-faire'),
    ('Travail en équipe', 'Savoir-être');

-- 3 offres d'emploi
INSERT INTO offre (
    source_offre_id, libelle_poste, description, date_publication,
    type_contrat, duree_travail, salaire_brut_annuel_estime,
    rome_code, entreprise_id, code_insee
) VALUES
    ('178XYZW',
     'Data Engineer / DBA PostgreSQL (F/H)',
     'Au sein du pôle Data, vous concevez et maintenez les pipelines de données.',
     DATE '2026-09-18', 'CDI', '35H Horaires normaux', 41500.00,
     'M1805',
     (SELECT entreprise_id FROM entreprise WHERE raison_sociale = 'TECH INNOVATION'),
     '44172'),
    ('178ABCD',
     'Consultant SI Data (F/H)',
     'Vous accompagnez nos clients dans la modernisation de leur système d''information.',
     DATE '2026-09-10', 'CDD', '39H Horaires normaux', 46000.00,
     'M1806',
     (SELECT entreprise_id FROM entreprise WHERE entreprise_anonyme = TRUE),
     '75111'),
    ('178EFGH',
     'Administrateur de bases de données (F/H)',
     'Vous administrez et sécurisez les bases PostgreSQL de production.',
     DATE '2026-09-05', 'CDI', '35H Horaires normaux', 39000.00,
     'M1805',
     (SELECT entreprise_id FROM entreprise WHERE raison_sociale = 'TECH INNOVATION'),
     '75111');

-- Liaisons offre/compétence avec statut d'exigence
INSERT INTO exigence_offre (offre_id, competence_id, statut_exigence)
SELECT o.offre_id, c.competence_id, v.statut_exigence
FROM (VALUES
        ('178XYZW', 'Langage SQL', 'E'),
        ('178XYZW', 'Concevoir une base de données', 'E'),
        ('178XYZW', 'Python', 'S'),
        ('178ABCD', 'Langage SQL', 'E'),
        ('178ABCD', 'Travail en équipe', 'E'),
        ('178EFGH', 'Langage SQL', 'E'),
        ('178EFGH', 'Concevoir une base de données', 'E'),
        ('178EFGH', 'Travail en équipe', 'S')
     ) AS v(source_offre_id, libelle_competence, statut_exigence)
JOIN offre o       ON o.source_offre_id = v.source_offre_id
JOIN competence c  ON c.libelle_competence = v.libelle_competence;

COMMIT;
