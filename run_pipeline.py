from extract.extract_flights import extract_flights
from transform.normalize_flights import normalize_flights
from load.load_to_bigquery import load_rows_to_bigquery_in_batches

def main():
    payload = extract_flights()
    rows = normalize_flights(payload)
    load_rows_to_bigquery_in_batches(rows)

if __name__ == "__main__":
    main()