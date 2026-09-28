\ir 00_audit_objects.sql

CREATE TEMP TABLE tp3_cleaning_context AS
SELECT clock_timestamp() AS started_at;

BEGIN;

-- R01 - Fusion des compétences identiques après normalisation casse/espaces.
CREATE TEMP TABLE tp3_competence_merge ON COMMIT DROP AS
WITH ranked AS (
    SELECT
        competence_id,
        min(competence_id) OVER (PARTITION BY normalized_label) AS canonical_id
    FROM (
        SELECT
            competence_id,
            lower(
                regexp_replace(btrim(libelle_competence), '\s+', ' ', 'g')
            ) AS normalized_label
        FROM competence
    ) normalized
)
SELECT competence_id AS duplicate_id, canonical_id
FROM ranked
WHERE competence_id <> canonical_id;

INSERT INTO tp3_cleaning_log (
    rule_code, table_name, record_key, column_name,
    old_value, new_value, justification
)
SELECT
    'R01_COMPETENCE',
    'competence',
    duplicate.competence_id::text,
    'competence_id',
    duplicate.competence_id::text || ':' || duplicate.libelle_competence,
    canonical.competence_id::text || ':' || canonical.libelle_competence,
    'Fusion exacte après lower(trim(espaces)); aucune proximité sémantique.'
FROM tp3_competence_merge mapping
JOIN competence duplicate
    ON duplicate.competence_id = mapping.duplicate_id
JOIN competence canonical
    ON canonical.competence_id = mapping.canonical_id;

INSERT INTO exigence_offre (offre_id, competence_id, statut_exigence)
SELECT
    eo.offre_id,
    mapping.canonical_id,
    CASE
        WHEN bool_or(eo.statut_exigence = 'E') THEN 'E'
        ELSE 'S'
    END
FROM exigence_offre eo
JOIN tp3_competence_merge mapping
    ON mapping.duplicate_id = eo.competence_id
GROUP BY eo.offre_id, mapping.canonical_id
ON CONFLICT (offre_id, competence_id) DO UPDATE
SET statut_exigence = CASE
    WHEN exigence_offre.statut_exigence = 'E'
      OR EXCLUDED.statut_exigence = 'E'
    THEN 'E'
    ELSE 'S'
END;

DELETE FROM exigence_offre eo
USING tp3_competence_merge mapping
WHERE eo.competence_id = mapping.duplicate_id;

DELETE FROM competence c
USING tp3_competence_merge mapping
WHERE c.competence_id = mapping.duplicate_id;

-- R02 - Fusion des entreprises identiques après normalisation casse/espaces.
CREATE TEMP TABLE tp3_entreprise_merge ON COMMIT DROP AS
WITH ranked AS (
    SELECT
        entreprise_id,
        min(entreprise_id) OVER (PARTITION BY normalized_name) AS canonical_id
    FROM (
        SELECT
            entreprise_id,
            lower(
                regexp_replace(btrim(raison_sociale), '\s+', ' ', 'g')
            ) AS normalized_name
        FROM entreprise
        WHERE raison_sociale IS NOT NULL
    ) normalized
)
SELECT entreprise_id AS duplicate_id, canonical_id
FROM ranked
WHERE entreprise_id <> canonical_id;

INSERT INTO tp3_cleaning_log (
    rule_code, table_name, record_key, column_name,
    old_value, new_value, justification
)
SELECT
    'R02_ENTREPRISE',
    'entreprise',
    duplicate.entreprise_id::text,
    'entreprise_id',
    duplicate.entreprise_id::text || ':' || duplicate.raison_sociale,
    canonical.entreprise_id::text || ':' || canonical.raison_sociale,
    'Fusion exacte après lower(trim(espaces)); la clé la plus ancienne est conservée.'
FROM tp3_entreprise_merge mapping
JOIN entreprise duplicate
    ON duplicate.entreprise_id = mapping.duplicate_id
JOIN entreprise canonical
    ON canonical.entreprise_id = mapping.canonical_id;

UPDATE offre o
SET entreprise_id = mapping.canonical_id
FROM tp3_entreprise_merge mapping
WHERE o.entreprise_id = mapping.duplicate_id;

DELETE FROM entreprise e
USING tp3_entreprise_merge mapping
WHERE e.entreprise_id = mapping.duplicate_id;

-- R03 - Suppression de valeurs qui ne peuvent pas être des salaires annuels.
-- Le texte original reste disponible dans la zone raw du Data Lake.
INSERT INTO tp3_cleaning_log (
    rule_code, table_name, record_key, column_name,
    old_value, new_value, justification
)
SELECT
    'R03_SALAIRE',
    'offre',
    source_offre_id,
    'salaire_brut_annuel_estime',
    salaire_brut_annuel_estime::text,
    NULL,
    'Valeur < 1 000 ou > 250 000 EUR : périodicité source ambiguë, aucune imputation.'
FROM offre
WHERE salaire_brut_annuel_estime < 1000
   OR salaire_brut_annuel_estime > 250000;

UPDATE offre
SET salaire_brut_annuel_estime = NULL
WHERE salaire_brut_annuel_estime <= 0
   OR salaire_brut_annuel_estime < 1000
   OR salaire_brut_annuel_estime > 250000;

-- R04 - Domaine professionnel ROME "A" mal libellé par l'ancien parseur.
-- La nomenclature ROME 4.0 associe la lettre A à l'agriculture, pas aux arts.
INSERT INTO tp3_cleaning_log (
    rule_code, table_name, record_key, column_name,
    old_value, new_value, justification
)
SELECT
    'R04_DOMAINE_ROME',
    'metier_rome',
    rome_code,
    'domaine_professionnel',
    domaine_professionnel,
    'Agriculture et pêche, espaces naturels et espaces verts, soins aux animaux',
    'Code ROME en A : grand domaine officiel Agriculture (nomenclature ROME 4.0).'
FROM metier_rome
WHERE rome_code LIKE 'A%'
  AND domaine_professionnel <> 'Agriculture et pêche, espaces naturels et espaces verts, soins aux animaux';

UPDATE metier_rome
SET domaine_professionnel = 'Agriculture et pêche, espaces naturels et espaces verts, soins aux animaux'
WHERE rome_code LIKE 'A%'
  AND domaine_professionnel <> 'Agriculture et pêche, espaces naturels et espaces verts, soins aux animaux';

CREATE UNIQUE INDEX IF NOT EXISTS ux_competence_normalized_label
    ON competence (
        (lower(regexp_replace(btrim(libelle_competence), '\s+', ' ', 'g')))
    );

CREATE UNIQUE INDEX IF NOT EXISTS ux_entreprise_normalized_name
    ON entreprise (
        (lower(regexp_replace(btrim(raison_sociale), '\s+', ' ', 'g')))
    )
    WHERE raison_sociale IS NOT NULL;

COMMIT;

SELECT
    rule_code,
    count(*) AS corrected_rows
FROM tp3_cleaning_log
WHERE executed_at >= (
    SELECT started_at FROM tp3_cleaning_context
)
GROUP BY rule_code
ORDER BY rule_code;
