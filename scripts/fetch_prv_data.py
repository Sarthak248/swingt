import yfinance as yf
import pandas as pd
import yaml
from concurrent.futures import ThreadPoolExecutor
import warnings
from google.cloud import bigquery
from dotenv import load_dotenv
import os

warnings.simplefilter(action='ignore', category=FutureWarning)

load_dotenv()

def fetch_ticker(ticker):
    try:
        df = yf.download(
            ticker,
            start="2025-08-01",
            end="2025-09-01",
            progress=False,
            auto_adjust=True
        )
        if df.empty:
            print(f"No data for {ticker}")
            return None

        df.reset_index(inplace=True)
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [col[0] if col[0] != "" else col[1] for col in df.columns]

        df.columns = [c.strip().capitalize() for c in df.columns]

        df = df.loc[:, ~df.columns.duplicated()]

        df["Ticker"] = ticker
        df.columns.name = None
        return df

    except Exception as e:
        print(f"Failed to fetch {ticker}: {e}")
        return None


def fetch_data_parallel():
    with open("../config/config.yaml") as f:
        config = yaml.safe_load(f)
    tickers = config["tickers"]

    all_rows = []

    with ThreadPoolExecutor(max_workers=10) as executor:
        results = executor.map(fetch_ticker, tickers)

    for df in results:
        if df is not None:
            all_rows.append(df)

    if all_rows:
        required_cols = ["Date", "Open", "High", "Low", "Close", "Volume", "Ticker"]
        all_clean = [df[[c for c in required_cols if c in df.columns]] for df in all_rows]
        final_df = pd.concat(all_clean, ignore_index=True)
        final_df.columns.name = None
        print(final_df.head(40))
        return final_df
    else:
        print("No data fetched.")
        return pd.DataFrame()

def push_to_bigquery(df):
    project_id = os.getenv("GCP_PROJECT_ID")
    dataset_id = os.getenv("BIGQUERY_DATASET")
    table_id = os.getenv("BIGQUERY_TABLE")

    client = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        schema=[
            bigquery.SchemaField("Date", "DATE"),
            bigquery.SchemaField("Open", "FLOAT"),
            bigquery.SchemaField("High", "FLOAT"),
            bigquery.SchemaField("Low", "FLOAT"),
            bigquery.SchemaField("Close", "FLOAT"),
            bigquery.SchemaField("Volume", "INTEGER"),
            bigquery.SchemaField("Ticker", "STRING"),
        ],
    )

    df["Date"] = pd.to_datetime(df["Date"]).dt.date

    load_job = client.load_table_from_dataframe(
        df, table_ref, job_config=job_config
    )
    load_job.result()

    print(f"\Loaded {len(df)} rows into {table_ref}")


if __name__ == "__main__":
    df = fetch_data_parallel()
    # if not df.empty:
    #     push_to_bigquery(df)
