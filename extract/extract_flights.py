import requests
from datetime import datetime, timezone
from config.settings import API_URL

def extract_flights() -> dict:
    response = requests.get(API_URL, timeout=30)
    response.raise_for_status()
    data = response.json()

    payload = {
        "ingestion_timestamp": datetime.now(timezone.utc).isoformat(),
        "data": data
    }
    return payload


def main():
    payload = extract_flights()
    print(f"Extracted payload with keys: {list(payload.keys())}")


if __name__ == "__main__":
    main()