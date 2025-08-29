"""
For an ETF, find out delta for ALL tickers between 2 dates
"""

from google.cloud import bigquery
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET")
BIGQUERY_TABLE = os.getenv("BIGQUERY_TABLE")


def get_etf_delta_all(table: str, start_date: str, end_date: str, limit: int = 100):
    client = bigquery.Client(project=GCP_PROJECT_ID)
    table_id = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{table}"

    query = f"""
        WITH filtered AS (
            SELECT
                Ticker,
                Date,
                Quantity,
                `Market_Value`
            FROM `{table_id}`
            WHERE Date IN (@start_date, @end_date)
        ),
        ranked AS (
            SELECT
                Ticker,
                Date,
                Quantity,
                `Market_Value`,
                ROW_NUMBER() OVER (PARTITION BY Ticker ORDER BY PARSE_DATE('%d-%m-%Y', Date)) AS rn
            FROM filtered
        ),
        paired AS (
            SELECT
                f1.Ticker,
                f1.`Market_Value` AS start_mv,
                f1.Quantity AS start_qty,
                f2.`Market_Value` AS end_mv,
                f2.Quantity AS end_qty
            FROM ranked f1
            JOIN ranked f2
              ON f1.Ticker = f2.Ticker
             AND f1.rn = 1
             AND f2.rn = 2
        )
        SELECT
            Ticker,
            end_mv AS MarketValue,
            end_qty AS Quantity,
            (end_qty - start_qty) AS Delta
        FROM paired
        ORDER BY MarketValue DESC
        LIMIT @limit
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("start_date", "STRING", start_date),
            bigquery.ScalarQueryParameter("end_date", "STRING", end_date),
            bigquery.ScalarQueryParameter("limit", "INT64", limit),
        ]
    )

    df = client.query(query, job_config=job_config).to_dataframe()

    return df


if __name__ == "__main__":
    print("This function will calculate deltas between 2 dates for ALL tickers")
    date1 = input("Enter date 1:")
    date2 = input("Enter date 2:")
    res = get_etf_delta_all("IVW", date1, date2, limit=510)
    print(res.to_string(index=False))
