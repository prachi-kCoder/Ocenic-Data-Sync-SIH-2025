import datetime
from typing import Dict, Any, List
import os 
import requests
from models.data_models import StandardizedRecord
from fastapi import HTTPException

def fetch_obis(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Fetch species occurrence data from OBIS API.
    Tailored for ocean biodiversity monitoring (CMLRE-style).
    
    Example payload:
      {"endpoint": "occurrence", "params": {"scientificname": "Sardinella", "size": 10}}
      {"endpoint": "occurrence", "params": {"taxonid": 12345, "size": 20}}
    """
    endpoint = payload.get("endpoint", "occurrence")
    params = payload.get("params", {"size": 10})
    base = "https://api.obis.org/v3"
    url = f"{base}/{endpoint}"

    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()

    items = data.get("results", data.get("data", []))
    records = []

    for item in items:
        lat = item.get("decimalLatitude")
        lon = item.get("decimalLongitude")

        # Build structured record
        record = {
            "latitude": lat,
            "longitude": lon,
            "species": item.get("scientificName"),
            "taxonRank": item.get("taxonRank"),
            "family": item.get("family"),
            "order": item.get("order"),
            "class": item.get("class"),
            "basisOfRecord": item.get("basisOfRecord"),  # e.g., HumanObservation
            "depth": item.get("depth"),
            "eventDate": item.get("eventDate"),
            "timestamp": datetime.datetime.now().isoformat(),
            "source": f"obis/{endpoint}"
        }

        records.append(record)

    return records 