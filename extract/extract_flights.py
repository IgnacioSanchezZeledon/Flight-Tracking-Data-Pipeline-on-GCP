import json
from pathlib import Path
from datetime import datetime, timezone

import requests

from config.settings import API_URL, RAW_DATA_PATH


def extract_flights() -> dict:
    response = requests.get(API_URL, timeout=30)
    response.raise_for_status()
    data = response.json()

    payload = {
        "ingestion_timestamp": datetime.now(timezone.utc).isoformat(),
        "data": data
    }
    return payload


def build_raw_file_path(ingestion_timestamp: str) -> Path:
    safe_ts = (
        ingestion_timestamp
        .replace("-", "")
        .replace(":", "")
        .replace("+00:00", "Z")
    )
    return Path(RAW_DATA_PATH) / f"flights_{safe_ts}.json"


def save_raw_payload(payload: dict) -> Path:
    output_path = build_raw_file_path(payload["ingestion_timestamp"])
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(payload, f)

    print(f"Raw file saved to {output_path}")
    return output_path


def extract_and_save_raw() -> dict:
    payload = extract_flights()
    raw_file_path = save_raw_payload(payload)

    return {
        "raw_file_path": str(raw_file_path),
        "ingestion_timestamp": payload["ingestion_timestamp"]
    }
