-- =============================================================================
-- TP2 Data Platform - additive schema only
-- Object: pipeline audit tables and BI/monitoring views.
-- Safe to rerun; does not drop or recreate TP1 3NF tables.
-- =============================================================================

CREATE TABLE IF NOT EXISTS tp2_pipeline_run (
    run_id              VARCHAR(64) PRIMARY KEY,
    started_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at        TIMESTAMPTZ,
    status              VARCHAR(20) NOT NULL,
    raw_count           INTEGER NOT NULL DEFAULT 0,
    clean_count         INTEGER NOT NULL DEFAULT 0,
    rejected_count      INTEGER NOT NULL DEFAULT 0,
    input_path          TEXT,
    curated_path        TEXT,
    quarantine_path     TEXT,
    error_message       TEXT,
    CONSTRAINT ck_tp2_pipeline_run_status
        CHECK (status IN ('running', 'success', 'failed', 'no_input'))
);

CREATE INDEX IF NOT EXISTS idx_tp2_pipeline_run_started_at
    ON tp2_pipeline_run USING btree (started_at DESC);

CREATE TABLE IF NOT EXISTS tp2_offre_enrichment (
    source_offre_id             VARCHAR(20) PRIMARY KEY,
    commune_reference_status   VARCHAR(20) NOT NULL,
    geo_nom_commune             VARCHAR(100),
    geo_code_postal             VARCHAR(5),
    geo_longitude               NUMERIC(9,6),
    geo_latitude                NUMERIC(9,6),
    aggregated_at               TIMESTAMPTZ,
    CONSTRAINT fk_tp2_enrichment_offre
        FOREIGN KEY (source_offre_id) REFERENCES offre (source_offre_id)
        ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT ck_tp2_enrichment_status
        CHECK (commune_reference_status IN ('matched', 'not_found', 'missing_source2'))
);

CREATE OR REPLACE VIEW tp2_pipeline_latest_counts AS
SELECT
    raw_count::bigint      AS raw_total,
    clean_count::bigint    AS clean_total,
    rejected_count::bigint AS rejected_total,
    completed_at           AS last_completed_at
FROM tp2_pipeline_run
WHERE status IN ('success', 'no_input')
ORDER BY started_at DESC
LIMIT 1;

CREATE OR REPLACE VIEW tp2_dashboard_offres AS
SELECT
    o.source_offre_id,
    o.libelle_poste,
    o.date_publication,
    o.type_contrat,
    o.salaire_brut_annuel_estime,
    c.code_insee,
    c.nom_commune,
    c.code_postal,
    mr.rome_code,
    mr.libelle_fiche_metier,
    mr.domaine_professionnel,
    e.raison_sociale,
    e.entreprise_anonyme,
    enr.commune_reference_status,
    enr.geo_nom_commune,
    enr.geo_code_postal
FROM offre o
JOIN commune c ON c.code_insee = o.code_insee
JOIN metier_rome mr ON mr.rome_code = o.rome_code
JOIN entreprise e ON e.entreprise_id = o.entreprise_id
LEFT JOIN tp2_offre_enrichment enr
    ON enr.source_offre_id = o.source_offre_id;
