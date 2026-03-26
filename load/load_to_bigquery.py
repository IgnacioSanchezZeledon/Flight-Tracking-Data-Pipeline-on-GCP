import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Iterable, Any

from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError

from config.settings import PROJECT_ID, DATASET, RAW_TABLE, STAGING_TABLE, BATCH_SIZE, FACT_TABLE

logger = logging.getLogger(__name__)


class BigQueryLoadError(RuntimeError):
    """Raised when a BigQuery load job fails."""


def get_bigquery_client() -> bigquery.Client:
    return bigquery.Client(project=PROJECT_ID)


def to_iso_utc(unix_timestamp: int | float | None) -> str | None:
    if unix_timestamp is None:
        return None
    return datetime.fromtimestamp(unix_timestamp, timezone.utc).isoformat()


def chunked(iterable: list[dict], size: int) -> Generator[list[dict], None, None]:
    if size <= 0:
        raise ValueError("Chunk size must be greater than 0")

    for i in range(0, len(iterable), size):
        yield iterable[i:i + size]


def load_json_file(file_path: str | Path) -> dict[str, Any]:
    file_path = Path(file_path)

    if not file_path.exists():
        raise FileNotFoundError(f"JSON file not found: {file_path}")

    with file_path.open("r", encoding="utf-8") as f:
        return json.load(f)


def iter_ndjson_file(file_path: str | Path) -> Generator[dict[str, Any], None, None]:
    file_path = Path(file_path)

    if not file_path.exists():
        raise FileNotFoundError(f"NDJSON file not found: {file_path}")

    with file_path.open("r", encoding="utf-8") as f:
        for line_number, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue

            try:
                yield json.loads(line)
            except json.JSONDecodeError as e:
                raise ValueError(
                    f"Invalid NDJSON at line {line_number} in file {file_path}"
                ) from e


def load_ndjson_file(file_path: str | Path) -> list[dict[str, Any]]:
    return list(iter_ndjson_file(file_path))


def build_raw_row(payload: dict[str, Any]) -> dict[str, Any]:
    data = payload.get("data", {})
    source_unix_time = data.get("time")
    source_timestamp = to_iso_utc(source_unix_time)
    states = data.get("states") or []

    ingestion_timestamp = payload.get("ingestion_timestamp")
    if ingestion_timestamp is None:
        raise ValueError("Missing required field: ingestion_timestamp")

    return {
        "ingestion_timestamp": ingestion_timestamp,
        "source_timestamp": source_timestamp,
        "raw_payload": data,
        "record_count": len(states),
    }


def _run_load_job(
    client: bigquery.Client,
    rows: list[dict[str, Any]],
    table_id: str,
    write_disposition: str,
    job_labels: dict[str, str] | None = None,
) -> None:
    if not rows:
        raise ValueError(f"No rows provided for load into {table_id}")

    job_config = bigquery.LoadJobConfig(
        write_disposition=write_disposition,
    )

    try:
        job = client.load_table_from_json(
            rows,
            table_id,
            job_config=job_config,
        )
        logger.info(
            "Starting BigQuery load job",
            extra={
                "table_id": table_id,
                "row_count": len(rows),
                "job_id": job.job_id,
                "write_disposition": write_disposition,
                "labels": job_labels or {},
            },
        )

        job.result()

        if job.errors:
            raise BigQueryLoadError(
                f"BigQuery load job failed for {table_id}. "
                f"job_id={job.job_id}, errors={job.errors}"
            )

        logger.info(
            "BigQuery load job completed successfully",
            extra={
                "table_id": table_id,
                "row_count": len(rows),
                "job_id": job.job_id,
            },
        )

    except GoogleAPIError as e:
        raise BigQueryLoadError(
            f"Google API error while loading {table_id}: {e}"
        ) from e
    except Exception as e:
        raise BigQueryLoadError(
            f"Unexpected error while loading {table_id}: {e}"
        ) from e


def load_raw_file_to_bigquery(raw_file_path: str | Path) -> dict[str, Any]:
    client = get_bigquery_client()
    table_id = f"{PROJECT_ID}.{DATASET}.{RAW_TABLE}"

    payload = load_json_file(raw_file_path)
    row = build_raw_row(payload)

    _run_load_job(
        client=client,
        rows=[row],
        table_id=table_id,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        job_labels={"layer": "raw", "source": "opensky"},
    )

    logger.info(
        "Raw snapshot loaded to BigQuery",
        extra={
            "table_id": table_id,
            "raw_file_path": str(raw_file_path),
        },
    )

    return {
        "raw_file_path": str(raw_file_path),
        "rows_loaded": 1,
        "table_id": table_id,
    }


def load_rows_to_bigquery_in_batches(
    rows: list[dict[str, Any]],
    client: bigquery.Client,
    write_disposition_first_batch: str = bigquery.WriteDisposition.WRITE_APPEND,
) -> int:
    if not rows:
        raise ValueError("Processed rows are empty")

    table_id = f"{PROJECT_ID}.{DATASET}.{STAGING_TABLE}"
    total_loaded = 0

    for batch_number, batch in enumerate(chunked(rows, BATCH_SIZE), start=1):
        write_disposition = (
            write_disposition_first_batch
            if batch_number == 1
            else bigquery.WriteDisposition.WRITE_APPEND
        )

        logger.info(
            "Loading batch to BigQuery",
            extra={
                "table_id": table_id,
                "batch_number": batch_number,
                "batch_size": len(batch),
                "write_disposition": write_disposition,
            },
        )

        _run_load_job(
            client=client,
            rows=batch,
            table_id=table_id,
            write_disposition=write_disposition,
            job_labels={"layer": "staging", "batch": str(batch_number)},
        )

        total_loaded += len(batch)

    logger.info(
        "Processed load completed",
        extra={
            "table_id": table_id,
            "total_rows_loaded": total_loaded,
            "batch_size": BATCH_SIZE,
        },
    )

    return total_loaded


def load_processed_to_bigquery(processed_file_path: str | Path) -> dict[str, Any]:
    client = get_bigquery_client()
    rows = load_ndjson_file(processed_file_path)

    total_loaded = load_rows_to_bigquery_in_batches(
        rows=rows,
        client=client,
        write_disposition_first_batch=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    table_id = f"{PROJECT_ID}.{DATASET}.{STAGING_TABLE}"

    logger.info(
        "Staging table loaded successfully",
        extra={
            "table_id": table_id,
            "processed_file_path": str(processed_file_path),
            "rows_loaded": total_loaded,
        },
    )

    return {
        "processed_file_path": str(processed_file_path),
        "rows_loaded": total_loaded,
        "table_id": table_id,
    }

def load_rows_to_fact_bigquery_in_batches(
    rows: list[dict[str, Any]],
    client: bigquery.Client,
) -> int:
    if not rows:
        raise ValueError("Processed rows are empty")

    table_id = f"{PROJECT_ID}.{DATASET}.{FACT_TABLE}"
    total_loaded = 0

    for batch_number, batch in enumerate(chunked(rows, BATCH_SIZE), start=1):
        logger.info(
            "Loading batch to fact table in BigQuery",
            extra={
                "table_id": table_id,
                "batch_number": batch_number,
                "batch_size": len(batch),
                "write_disposition": bigquery.WriteDisposition.WRITE_APPEND,
            },
        )

        _run_load_job(
            client=client,
            rows=batch,
            table_id=table_id,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            job_labels={"layer": "fact", "batch": str(batch_number)},
        )

        total_loaded += len(batch)

    logger.info(
        "Fact table load completed",
        extra={
            "table_id": table_id,
            "total_rows_loaded": total_loaded,
            "batch_size": BATCH_SIZE,
        },
    )

    return total_loaded

def load_processed_to_fact_bigquery(processed_file_path: str | Path) -> dict[str, Any]:
    client = get_bigquery_client()
    rows = load_ndjson_file(processed_file_path)

    total_loaded = load_rows_to_fact_bigquery_in_batches(
        rows=rows,
        client=client,
    )

    table_id = f"{PROJECT_ID}.{DATASET}.{FACT_TABLE}"

    logger.info(
        "Fact table loaded successfully",
        extra={
            "table_id": table_id,
            "processed_file_path": str(processed_file_path),
            "rows_loaded": total_loaded,
        },
    )

    return {
        "processed_file_path": str(processed_file_path),
        "rows_loaded": total_loaded,
        "table_id": table_id,
    }