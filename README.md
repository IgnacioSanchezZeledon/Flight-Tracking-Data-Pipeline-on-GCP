# Flight Tracking Data Pipeline

This project implements a data engineering pipeline that ingests real-time flight data from the OpenSky API, stores it across different data layers, and loads it into BigQuery for analysis.

The main goal is to demonstrate best practices in pipeline design, data layering, and orchestration readiness using Airflow.

---

## Project Description

The pipeline follows a structured flow where data moves through multiple stages:

1. **Extraction**
   - Data is retrieved from the OpenSky API.
   - A full snapshot of flight states is generated.

2. **Raw Layer**
   - The original payload is stored without modification.
   - One row per API request is inserted into BigQuery.
   - Enables traceability and reproducibility.

3. **Transformation**
   - Raw data is processed and cleaned.
   - Relevant flight attributes are extracted and normalized.

4. **Staging Layer**
   - Structured, tabular data is stored in BigQuery.
   - Each row represents the state of a flight.

---

## Architecture


- OpenSky API
  - Raw (JSON snapshots)
    - BigQuery - raw_flights_api
  - Transformation
    - Processed (NDJSON)
      - BigQuery - stg_flights

---

## Data Model

### Raw Layer (`raw_flights_api`)

- One row per API request
- Stores full payload as JSON
- Used for auditing and debugging

### Staging Layer (`stg_flights`)

- One row per flight
- Normalized and structured data
- Optimized for querying and analysis

---

## Design Decisions

- Clear separation between **raw** and **staging** layers
- Local file system used to simulate cloud storage (GCS)
- Explicit data flow between pipeline stages (no implicit dependencies)
- Modular pipeline design (extract, transform, load)
- Designed to be easily orchestrated with Airflow

---

## Technologies Used

- Python
- BigQuery
- OpenSky API
- Local storage (GCS simulation)
- Airflow (planned)

---

## Purpose

This project was built as a hands-on exercise to:

- Learn data pipeline design
- Implement layered data architecture (raw vs staging)
- Integrate Python with BigQuery
- Prepare pipelines for orchestration
- Build a strong data engineering portfolio project

---

## Next Steps

- Add Airflow orchestration
- Migrate local storage to GCS
- Implement table partitioning in BigQuery
- Add data quality validations
