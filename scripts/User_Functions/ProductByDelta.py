"""
Calculate ratio of SUM(Product) / SUM(Delta) for a ticker between 2 dates
"""
from google.cloud import bigquery
import os
from dotenv import load_dotenv

load_dotenv()

GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET")
BIGQUERY_TABLE = os.getenv("BIGQUERY_TABLE")

def calculate_product_over_delta(table: str, ticker: str, start_date: str, end_date: str):
    client = bigquery.Client(project=GCP_PROJECT_ID)
    table_id = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{table}"

    query = f"""
        SELECT
            SUM(Product) AS sum_product,
            SUM(Delta) AS sum_delta
        FROM `{table_id}`
        WHERE Ticker = @ticker
          AND Date BETWEEN @start_date AND @end_date
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("ticker", "STRING", ticker),
            bigquery.ScalarQueryParameter("start_date", "STRING", start_date),
            bigquery.ScalarQueryParameter("end_date", "STRING", end_date),
        ]
    )

    df = client.query(query, job_config=job_config).to_dataframe()

    if df.empty or df.iloc[0]["sum_delta"] == 0:
        raise ValueError("No data found - Sum of Delta is zero, cannot divide")

    # print("sum_product")
    # print(df.iloc[0]["sum_product"])
    # print("sum_delta")
    # print(df.iloc[0]["sum_delta"])

    result = df.iloc[0]["sum_product"] / df.iloc[0]["sum_delta"]
    return result


if __name__ == "__main__":
    print("Calculate SUM(Product)/SUM(Delta) for a ticker between 2 dates")
    ticker = input("Enter ticker: ").strip().upper()
    date1 = input("Enter start date (DD-MM-YYYY): ").strip()
    date2 = input("Enter end date (DD-MM-YYYY): ").strip()

    try:
        res = calculate_product_over_delta("IVW", ticker, date1, date2)
        print(f"Result for {ticker} between {date1} and {date2}: {res}")
    except Exception as e:
        print("Error:", e)
