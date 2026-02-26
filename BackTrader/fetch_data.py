import yfinance as yf
import pandas as pd

def download_data():
    tickers = ["AAPL", "MSFT"]
    start = "2020-01-01"
    end = "2024-01-01"

    for ticker in tickers:
        print(f"Downloading {ticker}...")
        df = yf.download(ticker, start=start, end=end)
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        df.to_csv(f"{ticker}.csv")
        print(f"Saved {ticker}.csv")

if __name__ == "__main__":
    download_data()
