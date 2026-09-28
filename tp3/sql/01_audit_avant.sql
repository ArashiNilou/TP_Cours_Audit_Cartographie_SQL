\ir 00_objets_audit.sql

SELECT tp3_execute_quality_audit(
    'before_cleaning',
    'Mesure TP3 avant exécution des règles de nettoyage.'
) AS audit_run_id;

SELECT
    control_code,
    dimension,
    severity,
    anomaly_count,
    population_count,
    anomaly_rate_pct,
    result_status,
    control_label
FROM tp3_quality_result
WHERE run_id = (SELECT max(run_id) FROM tp3_quality_run)
ORDER BY
    CASE severity
        WHEN 'HIGH' THEN 1
        WHEN 'MEDIUM' THEN 2
        WHEN 'LOW' THEN 3
        ELSE 4
    END,
    control_code;

