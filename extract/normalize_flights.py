import json
from pathlib import Path
from datetime import datetime, timezone
import uuid


def get_latest_raw_file() -> Path:
    raw_files = sorted(Path("data/raw").glob("flights_*.json"))
    if not raw_files:
        raise FileNotFoundError("No raw flight files found in data/raw")
    return raw_files[-1]


def to_iso_utc(unix_timestamp):
    if unix_timestamp is None:
        return None
    return datetime.fromtimestamp(unix_timestamp, timezone.utc).isoformat()


def normalize_flights(payload: dict) -> list[dict]:
    ingestion_timestamp = payload.get("ingestion_timestamp")
    source_unix_time = payload.get("data", {}).get("time")
    source_timestamp = to_iso_utc(source_unix_time)

    states = payload.get("data", {}).get("states", [])
    normalized_rows = []

    for state in states:
        row = {
            "flight_id": str(uuid.uuid4()),
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


def save_processed_data(rows: list[dict]) -> Path:
    output_dir = Path("data/processed")
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    output_file = output_dir / f"normalized_flights_{timestamp}.json"

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)

    return output_file


def main():
    latest_file = get_latest_raw_file()

    with open(latest_file, "r", encoding="utf-8") as f:
        payload = json.load(f)

    rows = normalize_flights(payload)
    output_file = save_processed_data(rows)

    print(f"Normalized {len(rows)} rows")
    print(f"Processed data saved to: {output_file}")


if __name__ == "__main__":
    main()