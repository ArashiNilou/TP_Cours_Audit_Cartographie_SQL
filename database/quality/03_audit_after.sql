\ir 00_audit_objects.sql

SELECT tp3_execute_quality_audit(
    'after_cleaning',
    'Mesure immédiate après le nettoyage SQL, avant rejeu du pipeline.',
    (SELECT max(run_id) FROM tp3_quality_run WHERE phase = 'before_cleaning')
) AS audit_run_id;

SELECT *
FROM tp3_quality_cleaning_comparison
ORDER BY
    CASE severity
        WHEN 'HIGH' THEN 1
        WHEN 'MEDIUM' THEN 2
        WHEN 'LOW' THEN 3
        ELSE 4
    END,
    control_code;
