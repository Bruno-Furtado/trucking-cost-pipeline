import os
from datetime import datetime, timezone

import pandas as pd
import requests

from pipeline.config import (
    EIA_API_BASE_URL,
    EIA_DIESEL_AREA,
    EIA_DIESEL_PRODUCT,
    EIA_PAGE_SIZE,
)


def fetch_diesel() -> pd.DataFrame:
    api_key = os.environ.get("EIA_API_KEY")
    if not api_key:
        raise RuntimeError("EIA_API_KEY env var not set")

    rows = []
    offset = 0
    total = None

    while True:
        params = {
            "api_key": api_key,
            "frequency": "weekly",
            "data[0]": "value",
            "facets[product][]": EIA_DIESEL_PRODUCT,
            "facets[duoarea][]": EIA_DIESEL_AREA,
            "sort[0][column]": "period",
            "sort[0][direction]": "asc",
            "offset": offset,
            "length": EIA_PAGE_SIZE,
        }
        response = requests.get(EIA_API_BASE_URL, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        if total is None:
            total = int(data["response"]["total"])

        rows.extend(data["response"]["data"])
        offset += EIA_PAGE_SIZE
        if offset >= total:
            break

    df = pd.DataFrame(rows)
    df = df.rename(columns={
        "area-name": "area_name",
        "product-name": "product_name",
        "process-name": "process_name",
    })
    df["period"] = pd.to_datetime(df["period"]).dt.date
    df["value"] = df["value"].astype(float)

    ingestion_timestamp = datetime.now(timezone.utc)
    df["ingestion_timestamp"] = ingestion_timestamp
    df["ingestion_date"] = ingestion_timestamp.date()
    return df
