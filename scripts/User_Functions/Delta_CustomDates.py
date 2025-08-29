'''
For an ETF, find out delta for a given ticker between 2 dates
'''

from google.cloud import bigquery
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET")
BIGQUERY_TABLE = os.getenv("BIGQUERY_TABLE")

def get_etf_quantity_delta(table: str, ticker: str, start_date: str, end_date: str):
    
    client = bigquery.Client(project=GCP_PROJECT_ID)
    table_id = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{table}"

    query = f"""
        SELECT Date, Quantity
        FROM `{table_id}`
        WHERE Ticker = @ticker
        AND Date IN (@start_date, @end_date)
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
        return {"error": f"No data found for {ticker} between {start_date} and {end_date}"}

    df = df.sort_values("Date")
    start_qty = float(df.iloc[0]["Quantity"])
    end_qty = float(df.iloc[-1]["Quantity"])
    delta = end_qty - start_qty

    action = "No Change"
    if delta > 0:
        action = "Bought"
    elif delta < 0:
        action = "Sold"

    return {
        "ticker": ticker,
        "start_date": start_date,
        "end_date": end_date,
        "start_quantity": start_qty,
        "end_quantity": end_qty,
        "delta": delta,
        "action": action,
    }

if __name__ == "__main__":
    print("This function will calculate delta between 2 dates for a given ticker")
    date1 = input("Enter date 1:")
    date2 = input("Enter date 2:")
    ticker = input("Enter ticker:")
    res = get_etf_quantity_delta("IVW",ticker,date1,date2)
    print("res")
    print(res)
