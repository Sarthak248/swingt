import os
import pandas as pd
from google.cloud import bigquery
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET")

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GOOGLE_APPLICATION_CREDENTIALS

ETF_FILE = os.path.join(os.path.dirname(__file__), "../data/holdings/ivw.csv")
ETF_NAME = "IVW"


def load_and_format_etf_csv(file_path):
    columns = [
        "Date", "Ticker", "Name", "Sector", "Asset Class", "Market Value", "Weight",
        "Notional Value", "Quantity", "CUSIP", "ISIN", "SEDOL"
    ]

    df = pd.read_csv(file_path, sep=',', header=None, names=columns, dtype=str)

    numeric_cols = ["Market Value", "Weight", "Notional Value", "Quantity"]
    for col in numeric_cols:
        df[col] = df[col].str.replace(r'[\$,]', '', regex=True)
        df[col] = pd.to_numeric(df[col], errors='coerce')

    df["Accrual_Date"] = pd.to_datetime(df["Date"], errors='coerce').dt.date
    df.columns = [c.strip().replace(" ", "_") for c in df.columns]
    return df

file_path = "./data/holdings/ivw.csv"
df = load_and_format_etf_csv(file_path)

df.columns = [c.strip().replace(" ", "_") for c in df.columns]

print(df.head(1000))

client = bigquery.Client(project=GCP_PROJECT_ID)
table_id = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{ETF_NAME}"

job = client.load_table_from_dataframe(df, table_id, job_config=bigquery.LoadJobConfig(
    write_disposition="WRITE_APPEND"
))
job.result()

print(f"{len(df)} rows successfully loaded to {table_id}")
