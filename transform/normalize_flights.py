from datetime import datetime, timezone
import uuid

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