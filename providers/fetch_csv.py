import datetime
from typing import Dict, Any, List
import os 
import requests
from models.data_models import StandardizedRecord
from fastapi import HTTPException
import pandas as pd
from io import StringIO
# from providers.fetch_csv import fetch_csv

def fetch_csv(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Fetch and standardize data from a CSV file.
    Example payloads:
      {"path": "/data/fisheries.csv"}
      {"url": "https://example.com/fisheries.csv"}
    """
    try:
        if "url" in payload:
            r = requests.get(payload["url"], timeout=30)
            r.raise_for_status()
            df = pd.read_csv(StringIO(r.text))
        elif "path" in payload:
            df = pd.read_csv(payload["path"])
        else:
            raise ValueError("Payload must include either 'url' or 'path' for CSV source.")

        records = []
        for _, row in df.iterrows():
            record = {
                "timestamp": datetime.datetime.now().isoformat(),
                "source": "csv",
                **row.to_dict()
            }
            records.append(record)
        return records
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"CSV ingestion failed: {e}")