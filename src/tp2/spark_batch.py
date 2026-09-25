"""Repeatable PySpark batch: aggregated JSONL -> curated Parquet/quarantine -> PostgreSQL."""

from __future__ import annotations

import glob
import json
import logging
import os
import time
import uuid
from pathlib import Path

from src.ingest import load_offres

from .datalake import data_lake_path, ensure_data_lake_layout
from .datalake import write_json_atomic
from .db import finish_pipeline_run, start_pipeline_run, upsert_enrichment_records

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)

INTERVAL_SECONDS = int(os.getenv("SPARK_BATCH_INTERVAL_SECONDS", "1800"))
RUN_ONCE = os.getenv("SPARK_BATCH_RUN_ONCE", "false").lower() == "true"


def _input_files() -> list[str]:
    pattern = str(data_lake_path("aggregated", "offres", "ingestion_date=*", "*.jsonl"))
    return sorted(glob.glob(pattern))


def _build_spark():
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.appName("tp2-offres-curation")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def run_once() -> dict[str, int | str]:
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    ensure_data_lake_layout()
    files = _input_files()
    input_path = str(data_lake_path("aggregated", "offres"))
    run_id = uuid.uuid4().hex
    start_pipeline_run(run_id, input_path)

    curated_path = str(data_lake_path("curated", "offres", f"run_id={run_id}"))
    quarantine_path = str(data_lake_path("quarantine", "spark", f"run_id={run_id}"))

    if not files:
        finish_pipeline_run(
            run_id,
            status="no_input",
            raw_count=0,
            clean_count=0,
            rejected_count=0,
            curated_path=curated_path,
            quarantine_path=quarantine_path,
        )
        logger.info("No aggregated input files found")
        return {"run_id": run_id, "raw_count": 0, "clean_count": 0, "rejected_count": 0}

    spark = _build_spark()
    try:
        df = spark.read.json(files)
        raw_count = df.count()

        base = (
            df.withColumn("source_offer_id", F.col("source_offer_id").cast("string"))
            .withColumn("code_insee_norm", F.upper(F.substring(F.col("payload.lieuTravail.commune"), 1, 5)))
            .withColumn("rome_code_norm", F.upper(F.col("payload.romeCode")))
            .withColumn("date_publication", F.to_date(F.substring(F.col("payload.dateCreation"), 1, 10)))
            .withColumn("type_contrat_norm", F.upper(F.col("payload.typeContrat")))
            .withColumn(
                "rejection_reason",
                F.when(F.col("source_offer_id").isNull() | (F.length("source_offer_id") == 0), F.lit("missing_source_offer_id"))
                .when(F.col("code_insee_norm").isNull() | ~F.col("code_insee_norm").rlike(r"^[0-9][0-9AB][0-9]{3}$"), F.lit("invalid_code_insee"))
                .when(F.col("rome_code_norm").isNull() | ~F.col("rome_code_norm").rlike(r"^[A-Z][0-9]{4}$"), F.lit("invalid_rome_code"))
                .when(F.col("date_publication").isNull(), F.lit("invalid_date_publication"))
                .when(F.col("payload.intitule").isNull() | (F.length(F.trim(F.col("payload.intitule"))) == 0), F.lit("missing_job_title"))
                .when(F.col("type_contrat_norm").isNull() | ~F.col("type_contrat_norm").isin("CDI", "CDD", "MIS", "SAI", "CCE"), F.lit("invalid_type_contrat"))
                .otherwise(F.lit(None)),
            )
        )

        rejected = base.filter(F.col("rejection_reason").isNotNull())
        rejected_count = rejected.count()
        if rejected_count:
            rejected.write.mode("overwrite").json(quarantine_path)

        valid = base.filter(F.col("rejection_reason").isNull())
        window = Window.partitionBy("source_offer_id").orderBy(F.col("aggregated_at").desc_nulls_last())
        clean = valid.withColumn("row_number", F.row_number().over(window)).filter(F.col("row_number") == 1)

        curated = clean.select(
            "source_offer_id",
            "code_insee_norm",
            "rome_code_norm",
            "date_publication",
            "type_contrat_norm",
            "commune_reference_status",
            "commune_reference",
            "payload",
            "enriched_payload",
            "collected_at",
            "aggregated_at",
            "ingestion_date",
        )
        curated.write.mode("overwrite").parquet(curated_path)

        load_rows = clean.select(
            "source_offer_id",
            "commune_reference_status",
            "commune_reference",
            "aggregated_at",
            F.to_json(F.col("enriched_payload")).alias("payload_json"),
        ).collect()
        payloads = [json.loads(row.payload_json) for row in load_rows]
        database_rejections: list[str] = []
        loaded_count = load_offres(payloads, rejected_ids=database_rejections) if payloads else 0
        if database_rejections:
            write_json_atomic(
                Path(quarantine_path) / "postgresql_rejections.json",
                {
                    "reason": "postgresql_load_rejected",
                    "source_offer_ids": database_rejections,
                },
            )
        upsert_enrichment_records([row.asDict(recursive=True) for row in load_rows])
        rejected_count += len(database_rejections)
        clean_count = loaded_count

        finish_pipeline_run(
            run_id,
            status="success",
            raw_count=raw_count,
            clean_count=clean_count,
            rejected_count=rejected_count,
            curated_path=curated_path,
            quarantine_path=quarantine_path,
        )
        logger.info(
            "Spark batch %s completed: raw=%s clean=%s rejected=%s",
            run_id,
            raw_count,
            clean_count,
            rejected_count,
        )
        return {
            "run_id": run_id,
            "raw_count": raw_count,
            "clean_count": clean_count,
            "rejected_count": rejected_count,
        }
    except Exception as exc:
        finish_pipeline_run(
            run_id,
            status="failed",
            raw_count=0,
            clean_count=0,
            rejected_count=0,
            curated_path=curated_path,
            quarantine_path=quarantine_path,
            error_message=str(exc)[:1000],
        )
        raise
    finally:
        spark.stop()


def main() -> None:
    while True:
        try:
            run_once()
        except Exception:
            logger.exception("Spark batch failed; the service will retry after the interval")
            if RUN_ONCE:
                raise
        if RUN_ONCE:
            return
        time.sleep(INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
