"""
Calculate weighted average product for a ticker between 2 dates:
Result = SUM(Product between dates) / (Quantity[end_date] - Quantity[start_date])
"""
from google.cloud import bigquery
import pandas as pd
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET")
BIGQUERY_TABLE = os.getenv("BIGQUERY_TABLE")

def calculate_weighted_product(table: str, ticker: str, start_date: str, end_date: str):
    client = bigquery.Client(project=GCP_PROJECT_ID)
    table_id = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{table}"

    query = f"""
        SELECT Date, Quantity, Product
        FROM `{table_id}`
        WHERE Ticker = @ticker
        AND Date BETWEEN @start_date AND @end_date
        ORDER BY Date
    """


    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("ticker", "STRING", ticker),
            bigquery.ScalarQueryParameter("start_date", "STRING", start_date),
            bigquery.ScalarQueryParameter("end_date", "STRING", end_date),
        ]
    )

    df = client.query(query, job_config=job_config).to_dataframe()

    if df.empty or df.shape[0] < 2:
        raise ValueError(f"Not enough data found for {ticker} between {start_date} and {end_date}")

    start_qty = float(df.iloc[0]["Quantity"])
    end_qty = float(df.iloc[-1]["Quantity"])
    custom_delta = end_qty - start_qty

    if custom_delta == 0:
        raise ValueError("Custom delta is zero, cannot divide by zero")

    sum_product = df["Product"].sum()

    result = sum_product / custom_delta
    return result


if __name__ == "__main__":
    print("Calculate weighted product for a ticker between 2 dates")
    ticker = input("Enter ticker: ").strip().upper()
    date1 = input("Enter start date (DD-MM-YYYY): ").strip()
    date2 = input("Enter end date (DD-MM-YYYY): ").strip()

    try:
        res = calculate_weighted_product("IVW", ticker, date1, date2)
        print(f"Weighted product for {ticker} between {date1} and {date2}: {res}")
    except Exception as e:
        print("Error:", e)
