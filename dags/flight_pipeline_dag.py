from __future__ import annotations

from datetime import datetime

from airflow.sdk import dag, task

from extract.extract_flights import extract_and_save_raw
from transform.normalize_flights import transform_raw_to_processed
from load.load_to_bigquery import (
    load_raw_file_to_bigquery,
    load_processed_to_bigquery,
)


@dag(
    dag_id="flight_pipeline_local",
    schedule=None,
    start_date=datetime(2026, 3, 1),
    catchup=False,
    tags=["local", "pipeline"],
)
def flight_pipeline_local():

    @task
    def extract_raw_file():
        result = extract_and_save_raw()
        return result["raw_file_path"]

    @task
    def load_raw_table(raw_file_path: str):
        result = load_raw_file_to_bigquery(raw_file_path)
        return result["rows_loaded"]

    @task
    def transform_processed_file(raw_file_path: str):
        result = transform_raw_to_processed(raw_file_path)
        return result["processed_file_path"]

    @task
    def load_staging_table(processed_file_path: str):
        result = load_processed_to_bigquery(processed_file_path)
        return result["rows_loaded"]

    raw_file = extract_raw_file()
    load_raw_table(raw_file)
    processed_file = transform_processed_file(raw_file)
    load_staging_table(processed_file)


flight_pipeline_local()