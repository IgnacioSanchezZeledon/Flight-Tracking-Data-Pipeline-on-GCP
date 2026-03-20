import requests
import json
from datetime import datetime, timezone
from pathlib import Path

API_URL = "https://opensky-network.org/api/states/all"

def fetch_flights():
    response = requests.get(API_URL, timeout=30)
    response.raise_for_status()
    data = response.json()

    output_dir = Path("data/raw")
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    output_file = output_dir / f"flights_{timestamp}.json"

    payload = {
        "ingestion_timestamp": datetime.now(timezone.utc).isoformat(),
        "data": data
    }

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    print(f"Raw flight data saved to: {output_file}")

if __name__ == "__main__":
    fetch_flights()