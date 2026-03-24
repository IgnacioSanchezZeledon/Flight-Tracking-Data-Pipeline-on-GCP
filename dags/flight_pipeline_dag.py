from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task

from extract.extract_flights import extract_and_save_raw
from transform.normalize_flights import transform_raw_to_processed
from load.load_to_bigquery import load_raw_file_to_bigquery, load_processed_to_bigquery
from checks.bq_checks import run_staging_checks

logger = logging.getLogger(__name__)


DEFAULT_ARGS = {
    "owner": "ignacio",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}


@dag(
    dag_id="flight_pipeline_local",
    schedule=None,
    start_date=datetime(2026, 3, 1),
    catchup=False,
    tags=["local", "pipeline", "opensky", "bigquery"],
    doc_md="""
    ### Flight Pipeline Local

    Pipeline steps:
    1. Extract raw flight data from OpenSky API
    2. Save raw JSON locally
    3. Load raw snapshot into BigQuery raw table
    4. Transform raw JSON into normalized NDJSON
    5. Load normalized data into BigQuery staging table
    6. Run data quality checks on staging
    """,
)

def flight_pipeline_local():
    @task(
        task_id="extract_raw_file",
        execution_timeout=timedelta(minutes=2),
    )
    def extract_raw_file() -> dict:
        result = extract_and_save_raw()

        logger.info(
            "Extract task completed",
            extra={
                "raw_file_path": result["raw_file_path"],
                "ingestion_timestamp": result["ingestion_timestamp"],
            },
        )

        return result

    @task(
        task_id="load_raw_table",
        execution_timeout=timedelta(minutes=3),
    )
    def load_raw_table(extract_result: dict) -> dict:
        raw_file_path = extract_result["raw_file_path"]
        result = load_raw_file_to_bigquery(raw_file_path)

        logger.info(
            "Raw table load task completed",
            extra={
                "raw_file_path": raw_file_path,
                "rows_loaded": result["rows_loaded"],
                "table_id": result.get("table_id"),
            },
        )

        return result

    @task(
        task_id="transform_processed_file",
        execution_timeout=timedelta(minutes=3),
    )
    def transform_processed_file(extract_result: dict) -> dict:
        raw_file_path = extract_result["raw_file_path"]
        result = transform_raw_to_processed(raw_file_path)

        logger.info(
            "Transform task completed",
            extra={
                "raw_file_path": raw_file_path,
                "processed_file_path": result["processed_file_path"],
                "row_count": result["row_count"],
                "ingestion_timestamp": result["ingestion_timestamp"],
            },
        )

        return result

    @task(
        task_id="load_staging_table",
        execution_timeout=timedelta(minutes=5),
    )
    def load_staging_table(transform_result: dict) -> dict:
        processed_file_path = transform_result["processed_file_path"]
        result = load_processed_to_bigquery(processed_file_path)

        logger.info(
            "Staging table load task completed",
            extra={
                "processed_file_path": processed_file_path,
                "rows_loaded": result["rows_loaded"],
                "table_id": result.get("table_id"),
            },
        )

        return result

    @task(
        task_id="run_staging_quality_checks",
        execution_timeout=timedelta(minutes=3),
    )
    def run_staging_quality_checks(load_result: dict) -> None:
        rows_loaded = load_result["rows_loaded"]

        if rows_loaded == 0:
            raise ValueError("Staging load completed with zero rows")

        run_staging_checks()

        logger.info(
            "Staging quality checks completed successfully",
            extra={"rows_loaded": rows_loaded},
        )

    extract_result = extract_raw_file()
    raw_load_result = load_raw_table(extract_result)
    transform_result = transform_processed_file(extract_result)
    staging_load_result = load_staging_table(transform_result)
    quality_check_result = run_staging_quality_checks(staging_load_result)

    raw_load_result
    quality_check_result


flight_pipeline_local()