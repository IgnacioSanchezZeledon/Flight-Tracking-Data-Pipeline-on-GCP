from extract.extract_flights import extract_and_save_raw
from transform.normalize_flights import transform_raw_to_processed
from load.load_to_bigquery import load_processed_to_bigquery


def main():
    extract_result = extract_and_save_raw()
    raw_file_path = extract_result["raw_file_path"]

    transform_result = transform_raw_to_processed(raw_file_path)
    processed_file_path = transform_result["processed_file_path"]

    load_result = load_processed_to_bigquery(processed_file_path)

    print("Pipeline completed successfully")
    print({
        "raw_file_path": raw_file_path,
        "processed_file_path": processed_file_path,
        "rows_loaded": load_result["rows_loaded"]
    })


if __name__ == "__main__":
    main()