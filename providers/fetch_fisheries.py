import datetime
from typing import Dict, Any, List
import os 
import requests
from models.data_models import StandardizedRecord
from fastapi import HTTPException
from models.data_models import FisheriesData

def fetch_fisheries(payload: dict, api_key: str) -> List[Dict[str, Any]]:
    url = "https://api.data.gov.in/resource/a66f8149-d060-43f9-bc94-e9daeb2c0188"
    all_records = []
    offset = 0
    limit = 100

    while True:
        params = {"api-key": api_key, "format": "json", "limit": limit, "offset": offset}
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        if "records" not in data or not data["records"]:
            break

        for item in data["records"]:
            try:
                standardized_data = FisheriesData(
                    year=item.get("financial_year", "N/A"),
                    total_fish_production_lakh_tonnes=float(item.get("total_fish_production_lakh_tonnes", 0)),
                    marine_fish_production_lakh_tonnes=float(item.get("marine_fish_production_lakh_tonnes", 0)),
                    inland_fish_production_lakh_tonnes=float(item.get("inland_fish_production_lakh_tonnes", 0)),
                    total_exports_crores=float(item.get("total_exports_crores", 0)),
                    ingestion_timestamp=datetime.datetime.now(),
                    source="data.gov.in",
                ).model_dump()
                all_records.append(standardized_data)
            except Exception:
                continue

        offset += limit

    return all_records