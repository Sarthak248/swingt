import yfinance as yf
import pandas as pd
import yaml
from google.cloud import bigquery
from concurrent.futures import ThreadPoolExecutor
import warnings

warnings.simplefilter(action='ignore', category=FutureWarning)

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
        df.columns.name = None
        df["Ticker"] = ticker
        return df
    except Exception as e:
        print(f"❌ Failed to fetch {ticker}: {e}")
        return None

def fetch_august_data_parallel():
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
        final_df = pd.concat(all_rows, ignore_index=True)
        print(final_df.head(400))
        return final_df
    else:
        print("No data fetched.")
        return pd.DataFrame()

if __name__ == "__main__":
    fetch_august_data_parallel()
