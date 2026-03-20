from google.cloud import bigquery
from config.settings import PROJECT_ID, DATASET, TABLE, BATCH_SIZE

def get_bigquery_client():
    return bigquery.Client(project=PROJECT_ID)


def chunk_list(data: list[dict], size: int):
    for i in range(0, len(data), size):
        yield data[i:i + size]


def load_rows_to_bigquery_in_batches(rows: list[dict]) -> None:
    client = get_bigquery_client()
    table_id = f"{PROJECT_ID}.{DATASET}.{TABLE}"

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    total_inserted = 0

    for batch_number, batch in enumerate(chunk_list(rows, BATCH_SIZE), start=1):
        print(f"Cargando batch {batch_number} con {len(batch)} filas...")

        job = client.load_table_from_json(
            batch,
            table_id,
            job_config=job_config
        )

        job.result()

        if job.errors:
            print(f"Errores en batch {batch_number}:")
            for error in job.errors:
                print(error)
            raise RuntimeError(f"Falló el batch {batch_number}")

        total_inserted += len(batch)
        print(f"Batch {batch_number} completado.")

    print(f"Proceso finalizado. Filas cargadas: {total_inserted}")