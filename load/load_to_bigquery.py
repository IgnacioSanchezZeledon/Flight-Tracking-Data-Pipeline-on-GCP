import json
from datetime import datetime, timezone
from google.cloud import bigquery

from config.settings import PROJECT_ID, DATASET, RAW_TABLE, STAGING_TABLE, BATCH_SIZE


def get_bigquery_client():
    return bigquery.Client(project=PROJECT_ID)


def to_iso_utc(unix_timestamp):
    if unix_timestamp is None:
        return None
    return datetime.fromtimestamp(unix_timestamp, timezone.utc).isoformat()


def chunk_list(data: list[dict], size: int):
    for i in range(0, len(data), size):
        yield data[i:i + size]


def load_json_file(file_path: str) -> dict:
    with open(file_path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_ndjson_file(file_path: str) -> list[dict]:
    rows = []
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def build_raw_row(payload: dict) -> dict:
    source_unix_time = payload.get("data", {}).get("time")
    source_timestamp = to_iso_utc(source_unix_time)
    states = payload.get("data", {}).get("states", [])

    return {
        "ingestion_timestamp": payload["ingestion_timestamp"],
        "source_timestamp": source_timestamp,
        "raw_payload": payload["data"],
        "record_count": len(states)
    }


def load_raw_file_to_bigquery(raw_file_path: str) -> dict:
    client = get_bigquery_client()
    table_id = f"{PROJECT_ID}.{DATASET}.{RAW_TABLE}"

    payload = load_json_file(raw_file_path)
    row = build_raw_row(payload)

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    job = client.load_table_from_json([row], table_id, job_config=job_config)
    job.result()

    if job.errors:
        raise RuntimeError(f"Error loading raw table: {job.errors}")

    print("Raw snapshot loaded to BigQuery")

    return {
        "raw_file_path": raw_file_path,
        "rows_loaded": 1
    }


def load_rows_to_bigquery_in_batches(rows: list[dict]) -> int:
    client = get_bigquery_client()
    table_id = f"{PROJECT_ID}.{DATASET}.{STAGING_TABLE}"

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    total_inserted = 0

    for batch_number, batch in enumerate(chunk_list(rows, BATCH_SIZE), start=1):
        print(f"Cargando batch {batch_number} con {len(batch)} filas...")

        job = client.load_table_from_json(
            batch,
            table_id,
            job_config=job_config
        )
        job.result()

        if job.errors:
            raise RuntimeError(f"Falló el batch {batch_number}: {job.errors}")

        total_inserted += len(batch)
        print(f"Batch {batch_number} completado.")

    print(f"Proceso finalizado. Filas cargadas: {total_inserted}")
    return total_inserted


def load_processed_to_bigquery(processed_file_path: str) -> dict:
    rows = load_ndjson_file(processed_file_path)
    total_inserted = load_rows_to_bigquery_in_batches(rows)

    return {
        "processed_file_path": processed_file_path,
        "rows_loaded": total_inserted
    }