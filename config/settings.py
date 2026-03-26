from pathlib import Path
import os

# ========================
# Environment
# ========================
ENV = os.getenv("ENV", "dev")

# ========================
# Paths
# ========================
BASE_DIR = Path(__file__).resolve().parent.parent

DATA_DIR = BASE_DIR / "data"
RAW_DATA_PATH = DATA_DIR / "raw"
PROCESSED_DATA_PATH = DATA_DIR / "processed"

# ========================
# GCP / BigQuery
# ========================
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "my-flight-project-123")
DATASET = os.getenv("BQ_DATASET", "flight_data")

RAW_TABLE = "raw_flights_api"
STAGING_TABLE = "stg_flights"
FACT_TABLE = "fact_flight_observations"

# ========================
# API
# ========================
OPENSKY_API_URL = "https://opensky-network.org/api/states/all"
REQUEST_TIMEOUT = 10
BATCH_SIZE = 3000

# ========================
# Validation
# ========================
def validate_settings():
    assert PROJECT_ID, "PROJECT_ID not defined"
    assert DATASET, "DATASET not defined"