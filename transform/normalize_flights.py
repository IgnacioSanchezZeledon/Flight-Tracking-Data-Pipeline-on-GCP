import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from config.settings import PROCESSED_DATA_PATH

logger = logging.getLogger(__name__)


class TransformError(RuntimeError):
    """Raised when raw-to-processed transformation fails."""


def to_iso_utc(unix_timestamp: int | float | None) -> str | None:
    if unix_timestamp is None:
        return None
    return datetime.fromtimestamp(unix_timestamp, timezone.utc).isoformat()


def load_raw_payload(raw_file_path: str | Path) -> dict[str, Any]:
    raw_file_path = Path(raw_file_path)

    if not raw_file_path.exists():
        raise FileNotFoundError(f"Raw file not found: {raw_file_path}")

    try:
        with raw_file_path.open("r", encoding="utf-8") as f:
            payload = json.load(f)
    except json.JSONDecodeError as e:
        raise TransformError(f"Invalid JSON in raw file: {raw_file_path}") from e

    if not isinstance(payload, dict):
        raise TransformError(f"Raw payload must be a JSON object: {raw_file_path}")

    return payload


def validate_raw_payload(payload: dict[str, Any]) -> None:
    if "ingestion_timestamp" not in payload:
        raise TransformError("Raw payload missing 'ingestion_timestamp'")

    data = payload.get("data")
    if not isinstance(data, dict):
        raise TransformError("Raw payload field 'data' must be an object")

    if "time" not in data:
        raise TransformError("Raw payload missing 'data.time'")

    if "states" not in data:
        raise TransformError("Raw payload missing 'data.states'")

    states = data["states"]
    if states is not None and not isinstance(states, list):
        raise TransformError("Raw payload field 'data.states' must be a list or null")


def get_state_value(state: list[Any], index: int) -> Any:
    return state[index] if len(state) > index else None


def build_flight_id(icao24: str | None, source_timestamp: str | None) -> str:
    if not icao24:
        raise TransformError("Cannot build flight_id: missing icao24")
    if not source_timestamp:
        raise TransformError("Cannot build flight_id: missing source_timestamp")

    return f"{icao24}_{source_timestamp}"


def normalize_state(
    state: list[Any],
    ingestion_timestamp: str,
    source_timestamp: str | None,
) -> dict[str, Any]:
    if not isinstance(state, list):
        raise TransformError(f"Invalid state row type: expected list, got {type(state).__name__}")

    icao24 = get_state_value(state, 0)
    callsign = get_state_value(state, 1)

    flight_id = build_flight_id(icao24, source_timestamp)

    return {
        "flight_id": flight_id,
        "ingestion_timestamp": ingestion_timestamp,
        "source_timestamp": source_timestamp,
        "icao24": icao24,
        "callsign": callsign.strip() if isinstance(callsign, str) and callsign.strip() else None,
        "origin_country": get_state_value(state, 2),
        "longitude": get_state_value(state, 5),
        "latitude": get_state_value(state, 6),
        "on_ground": get_state_value(state, 8),
        "velocity": get_state_value(state, 9),
        "true_track": get_state_value(state, 10),
        "vertical_rate": get_state_value(state, 11),
    }


def normalize_flights(payload: dict[str, Any]) -> list[dict[str, Any]]:
    validate_raw_payload(payload)

    ingestion_timestamp = payload["ingestion_timestamp"]
    source_unix_time = payload["data"]["time"]
    source_timestamp = to_iso_utc(source_unix_time)
    states = payload["data"]["states"] or []

    normalized_rows: list[dict[str, Any]] = []

    for index, state in enumerate(states, start=1):
        try:
            row = normalize_state(
                state=state,
                ingestion_timestamp=ingestion_timestamp,
                source_timestamp=source_timestamp,
            )
            normalized_rows.append(row)
        except Exception as e:
            raise TransformError(f"Failed to normalize state row #{index}: {e}") from e

    logger.info(
        "Flights normalized successfully",
        extra={
            "ingestion_timestamp": ingestion_timestamp,
            "source_timestamp": source_timestamp,
            "input_state_count": len(states),
            "output_row_count": len(normalized_rows),
        },
    )

    return normalized_rows


def build_processed_file_path(ingestion_timestamp: str) -> Path:
    try:
        dt = datetime.fromisoformat(ingestion_timestamp.replace("Z", "+00:00"))
    except ValueError as e:
        raise ValueError(f"Invalid ingestion_timestamp: {ingestion_timestamp}") from e

    safe_ts = dt.strftime("%Y%m%dT%H%M%SZ")
    return Path(PROCESSED_DATA_PATH) / f"flights_{safe_ts}.ndjson"


def save_processed_rows(rows: list[dict[str, Any]], ingestion_timestamp: str) -> Path:
    if not rows:
        logger.warning(
            "Processed rows are empty",
            extra={"ingestion_timestamp": ingestion_timestamp},
        )

    output_path = build_processed_file_path(ingestion_timestamp)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        with output_path.open("w", encoding="utf-8") as f:
            for row in rows:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
    except OSError as e:
        raise TransformError(f"Failed to write processed file: {output_path}") from e

    logger.info(
        "Processed file saved",
        extra={
            "processed_file_path": str(output_path),
            "row_count": len(rows),
            "ingestion_timestamp": ingestion_timestamp,
            "file_size_bytes": output_path.stat().st_size,
        },
    )

    return output_path


def transform_raw_to_processed(raw_file_path: str | Path) -> dict[str, Any]:
    payload = load_raw_payload(raw_file_path)
    rows = normalize_flights(payload)

    ingestion_timestamp = payload["ingestion_timestamp"]
    processed_file_path = save_processed_rows(rows, ingestion_timestamp)

    result = {
        "processed_file_path": str(processed_file_path),
        "row_count": len(rows),
        "ingestion_timestamp": ingestion_timestamp,
    }

    logger.info("Raw-to-processed transformation completed", extra=result)

    return result