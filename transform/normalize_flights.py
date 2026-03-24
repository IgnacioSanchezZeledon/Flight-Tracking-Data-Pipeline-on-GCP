import json
from pathlib import Path
from datetime import datetime, timezone

from config.settings import PROCESSED_DATA_PATH


def to_iso_utc(unix_timestamp):
    if unix_timestamp is None:
        return None
    return datetime.fromtimestamp(unix_timestamp, timezone.utc).isoformat()


def load_raw_payload(raw_file_path: str) -> dict:
    with open(raw_file_path, "r", encoding="utf-8") as f:
        return json.load(f)


def normalize_flights(payload: dict) -> list[dict]:
    ingestion_timestamp = payload.get("ingestion_timestamp")
    source_unix_time = payload.get("data", {}).get("time")
    source_timestamp = to_iso_utc(source_unix_time)

    states = payload.get("data", {}).get("states", [])
    normalized_rows = []

    for state in states:
        row = {
            "ingestion_timestamp": ingestion_timestamp,
            "source_timestamp": source_timestamp,
            "icao24": state[0],
            "callsign": state[1].strip() if state[1] else None,
            "origin_country": state[2],
            "longitude": state[5],
            "latitude": state[6],
            "velocity": state[9],
            "true_track": state[10],
            "vertical_rate": state[11],
            "on_ground": state[8],
        }
        normalized_rows.append(row)

    return normalized_rows


def build_processed_file_path(ingestion_timestamp: str) -> Path:
    safe_ts = (
        ingestion_timestamp
        .replace("-", "")
        .replace(":", "")
        .replace("+00:00", "Z")
    )
    return Path(PROCESSED_DATA_PATH) / f"flights_{safe_ts}.ndjson"


def save_processed_rows(rows: list[dict], ingestion_timestamp: str) -> Path:
    output_path = build_processed_file_path(ingestion_timestamp)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")

    print(f"Processed file saved to {output_path}")
    return output_path


def transform_raw_to_processed(raw_file_path: str) -> dict:
    payload = load_raw_payload(raw_file_path)
    rows = normalize_flights(payload)
    ingestion_timestamp = payload["ingestion_timestamp"]

    processed_file_path = save_processed_rows(rows, ingestion_timestamp)

    return {
        "processed_file_path": str(processed_file_path),
        "row_count": len(rows),
        "ingestion_timestamp": ingestion_timestamp
    }