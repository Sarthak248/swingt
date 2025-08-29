import os
import yfinance as yf
import yaml
import pandas as pd
from datetime import datetime
from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv()

GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET")
BIGQUERY_TABLE = os.getenv("BIGQUERY_TABLE")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GOOGLE_APPLICATION_CREDENTIALS

base_dir = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(base_dir, "../config/config.yaml")

with open(config_path) as f:
    config = yaml.safe_load(f)

tickers = config.get("tickers", [])
tickers = [str(t).strip().upper() for t in tickers if t]
# print("Using tickers:", tickers)

year = datetime.now().year
start_date = f"{year}-08-01"
end_date = f"{year}-08-31"

all_data = []

for ticker in tickers:
    try:
        stock = yf.Ticker(ticker)
        df = stock.history(start=start_date, end=end_date)

        if df.empty:
            print(f"No data for {ticker}")
            continue

        df = df.reset_index()
        df["Ticker"] = ticker
        df = df[["Date", "Open", "High", "Low", "Close", "Volume", "Ticker"]]

        all_data.append(df)
    except Exception as e:
        print(f"Error fetching {ticker}: {e}")

if all_data:
    combined_df = pd.concat(all_data, ignore_index=True)
    print("Combined data preview:")
    print(combined_df.head(1000))

    client = bigquery.Client(project=GCP_PROJECT_ID)
    table_id = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}"

    job = client.load_table_from_dataframe(combined_df, table_id)
    job.result()
    print(f"Data successfully loaded to {table_id}")
else:
    print("No data fetched for any ticker.")
