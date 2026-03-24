from google.cloud import bigquery

from config.settings import PROJECT_ID, DATASET, STAGING_TABLE


def get_bigquery_client():
    return bigquery.Client(project=PROJECT_ID)


def run_scalar_check(client: bigquery.Client, query: str, predicate, error_message: str) -> None:
    result = list(client.query(query).result())
    if not result:
        raise ValueError("Check returned no rows")

    value = list(result[0].values())[0]
    if not predicate(value):
        raise ValueError(f"{error_message}. Returned value: {value}")


def run_staging_checks() -> None:
    client = get_bigquery_client()
    table_id = f"{PROJECT_ID}.{DATASET}.{STAGING_TABLE}"

    run_scalar_check(
        client,
        f"SELECT COUNT(*) FROM `{table_id}`",
        lambda x: x > 0,
        "stg_flights is empty",
    )

    run_scalar_check(
        client,
        f"""
        SELECT COUNT(*)
        FROM `{table_id}`
        WHERE flight_id IS NULL
        """,
        lambda x: x == 0,
        "Null flight_id values found",
    )

    run_scalar_check(
        client,
        f"""
        SELECT COUNT(*)
        FROM (
            SELECT flight_id
            FROM `{table_id}`
            GROUP BY flight_id
            HAVING COUNT(*) > 1
        )
        """,
        lambda x: x == 0,
        "Duplicate flight_id values found",
    )