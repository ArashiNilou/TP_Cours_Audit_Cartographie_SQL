"""PostgreSQL helpers for TP2 additive schema and pipeline audit."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable

from src.connection import get_connection

SQL_DIR = Path(__file__).resolve().parents[2] / "sql"


def apply_tp2_schema() -> None:
    sql_text = (SQL_DIR / "04_tp2_additive.sql").read_text(encoding="utf-8")
    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(sql_text)
        connection.commit()


def start_pipeline_run(run_id: str, input_path: str) -> None:
    apply_tp2_schema()
    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO tp2_pipeline_run (run_id, status, input_path)
                VALUES (%s, 'running', %s)
                ON CONFLICT (run_id) DO UPDATE SET
                    started_at = now(),
                    completed_at = NULL,
                    status = 'running',
                    raw_count = 0,
                    clean_count = 0,
                    rejected_count = 0,
                    input_path = EXCLUDED.input_path,
                    curated_path = NULL,
                    quarantine_path = NULL,
                    error_message = NULL;
                """,
                (run_id, input_path),
            )
        connection.commit()


def finish_pipeline_run(
    run_id: str,
    *,
    status: str,
    raw_count: int,
    clean_count: int,
    rejected_count: int,
    curated_path: str | None,
    quarantine_path: str | None,
    error_message: str | None = None,
) -> None:
    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE tp2_pipeline_run
                SET completed_at = now(),
                    status = %s,
                    raw_count = %s,
                    clean_count = %s,
                    rejected_count = %s,
                    curated_path = %s,
                    quarantine_path = %s,
                    error_message = %s
                WHERE run_id = %s;
                """,
                (
                    status,
                    raw_count,
                    clean_count,
                    rejected_count,
                    curated_path,
                    quarantine_path,
                    error_message,
                    run_id,
                ),
            )
        connection.commit()


def upsert_enrichment_records(records: Iterable[dict[str, Any]]) -> None:
    """Persist source-2 matching details so enrichment remains auditable."""
    with get_connection() as connection:
        with connection.cursor() as cursor:
            for record in records:
                commune = record.get("commune_reference") or {}
                centre = commune.get("centre") or {}
                coordinates = centre.get("coordinates") or []
                cursor.execute(
                    """
                    INSERT INTO tp2_offre_enrichment (
                        source_offre_id,
                        commune_reference_status,
                        geo_nom_commune,
                        geo_code_postal,
                        geo_longitude,
                        geo_latitude,
                        aggregated_at
                    )
                    SELECT %s, %s, %s, %s, %s, %s, %s
                    WHERE EXISTS (
                        SELECT 1 FROM offre WHERE source_offre_id = %s
                    )
                    ON CONFLICT (source_offre_id) DO UPDATE SET
                        commune_reference_status = EXCLUDED.commune_reference_status,
                        geo_nom_commune = EXCLUDED.geo_nom_commune,
                        geo_code_postal = EXCLUDED.geo_code_postal,
                        geo_longitude = EXCLUDED.geo_longitude,
                        geo_latitude = EXCLUDED.geo_latitude,
                        aggregated_at = EXCLUDED.aggregated_at;
                    """,
                    (
                        record["source_offer_id"],
                        record["commune_reference_status"],
                        commune.get("nom"),
                        (commune.get("codesPostaux") or [None])[0],
                        coordinates[0] if len(coordinates) >= 2 else None,
                        coordinates[1] if len(coordinates) >= 2 else None,
                        record.get("aggregated_at"),
                        record["source_offer_id"],
                    ),
                )
        connection.commit()
