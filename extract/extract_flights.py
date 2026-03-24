import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from requests import Response
from requests.exceptions import RequestException

from config.settings import OPENSKY_API_URL, RAW_DATA_PATH

logger = logging.getLogger(__name__)


class ExtractError(RuntimeError):
    pass


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def validate_opensky_response(data: dict[str, Any]) -> None:
    if not isinstance(data, dict):
        raise ExtractError("OpenSky response is not a JSON object")

    if "time" not in data:
        raise ExtractError("OpenSky response missing 'time'")

    if "states" not in data:
        raise ExtractError("OpenSky response missing 'states'")

    if data["states"] is not None and not isinstance(data["states"], list):
        raise ExtractError("OpenSky field 'states' must be a list or null")


def fetch_opensky_data() -> dict[str, Any]:
    try:
        response: Response = requests.get(OPENSKY_API_URL, timeout=30)
        response.raise_for_status()
        data = response.json()
        validate_opensky_response(data)
        return data

    except RequestException as e:
        raise ExtractError(f"HTTP error while calling OpenSky API: {e}") from e
    except ValueError as e:
        raise ExtractError(f"Invalid JSON returned by OpenSky API: {e}") from e
    except Exception as e:
        raise ExtractError(f"Unexpected extraction error: {e}") from e


def build_ingestion_payload(data: dict[str, Any]) -> dict[str, Any]:
    return {
        "ingestion_timestamp": utc_now_iso(),
        "data": data,
    }


def extract_flights() -> dict[str, Any]:
    data = fetch_opensky_data()
    payload = build_ingestion_payload(data)

    logger.info(
        "Flights extracted successfully",
        extra={
            "ingestion_timestamp": payload["ingestion_timestamp"],
            "state_count": len(data.get("states") or []),
            "source_time": data.get("time"),
        },
    )

    return payload


def build_raw_file_path(ingestion_timestamp: str) -> Path:
    dt = datetime.fromisoformat(ingestion_timestamp.replace("Z", "+00:00"))
    safe_ts = dt.strftime("%Y%m%dT%H%M%SZ")
    return Path(RAW_DATA_PATH) / f"flights_{safe_ts}.json"


def save_raw_payload(payload: dict[str, Any]) -> Path:
    ingestion_timestamp = payload.get("ingestion_timestamp")
    data = payload.get("data")

    if not ingestion_timestamp:
        raise ValueError("Missing required field: ingestion_timestamp")
    if data is None:
        raise ValueError("Missing required field: data")

    output_path = build_raw_file_path(ingestion_timestamp)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    logger.info(
        "Raw payload saved",
        extra={
            "raw_file_path": str(output_path),
            "ingestion_timestamp": ingestion_timestamp,
        },
    )

    return output_path


def extract_and_save_raw() -> dict[str, Any]:
    payload = extract_flights()
    raw_file_path = save_raw_payload(payload)

    return {
        "raw_file_path": str(raw_file_path),
        "ingestion_timestamp": payload["ingestion_timestamp"],
    }