from datetime import datetime, timezone

import pandas as pd
import yfinance as yf

from pipeline.config import WTI_TICKER


def fetch_wti(period: str = "max") -> pd.DataFrame:
    ticker = yf.Ticker(WTI_TICKER)
    df = ticker.history(period=period, auto_adjust=False)
    df = df.reset_index()
    df = df.rename(columns={
        "Date": "date",
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Adj Close": "adj_close",
        "Volume": "volume",
        "Dividends": "dividends",
        "Stock Splits": "stock_splits",
    })
    df["date"] = df["date"].dt.date
    ingestion_timestamp = datetime.now(timezone.utc)
    df["ingestion_timestamp"] = ingestion_timestamp
    df["ingestion_date"] = ingestion_timestamp.date()
    return df
