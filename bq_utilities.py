"""Module containing utilities for accessing BigQuery from Outerbounds scripts"""

from google.cloud import bigquery
import pandas as pd

def pull(project: str, query: str):
    bq_client = bigquery.Client(project=project)
    query_job = bq_client.query(query)

    return query_job.to_dataframe()

def pull_dau_test():
    query = """
    SELECT submission_date AS x,
    IF(country = 'US', country, 'ROW') AS country,
    SUM(dau) AS y,
    FROM `moz-fx-data-shared-prod.telemetry.active_users_aggregates`
    WHERE app_name = "Firefox Desktop" AND submission_date >= "2025-11-01"
    GROUP BY ALL
    ORDER BY 1, 2 ASC
    LIMIT 10
    """
    project = 'moz-fx-data-bq-data-science'
    return pull(project, query)


def write_dau_test(df: pd.DataFrame):
    project = 'moz-fx-data-bq-data-science'
    table = 'moz-fx-data-bq-data-science.brwells.mozaic_daily_trivial_test'

    # Ensure consistent types
    df = df.copy()
    df["x"] = pd.to_datetime(df["x"]).dt.date
    df["country"] = df["country"].astype(str)
    df["y"] = pd.to_numeric(df["y"]).astype('Int64')

    client = bigquery.Client(project=project)

    job_config = bigquery.LoadJobConfig(
        schema=[
        bigquery.SchemaField("x", "DATE", 'REQUIRED'),
        bigquery.SchemaField("country", "STRING", 'REQUIRED'),
        bigquery.SchemaField("y", "INTEGER", 'REQUIRED'),
        ],
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )

    # table_id = f"{project}.{table}" if "." not in table else table
    table_id = table

    load_job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    result = load_job.result()
    print(f"Loaded {result.output_rows} rows into {table_id}.")

if __name__ == '__main__':
    df = pull_dau_test()
    write_dau_test(df)