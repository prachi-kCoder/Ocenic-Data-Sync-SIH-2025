import datetime
from typing import Dict, Any, List
import os 
import requests
from models.data_models import StandardizedRecord
from fastapi import HTTPException

def fetch_open_meteo(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Fetch marine/oceanographic data from Open-Meteo.
    Example payload:
    {
        "latitude": 20.59,
        "longitude": 78.96,
        "hourly": ["wave_height", "sea_surface_temperature"]
    }
    """
    url = "https://marine-api.open-meteo.com/v1/marine"
    params = {
        "latitude": payload.get("latitude"),
        "longitude": payload.get("longitude"),
        "hourly": ",".join(payload.get("hourly", ["wave_height", "sea_surface_temperature" ])),
    }

    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()

    records = []
    lat, lon = payload.get("latitude"), payload.get("longitude")
    timestamps = data.get("hourly", {}).get("time", [])

    for param in params["hourly"].split(","):
        values = data.get("hourly", {}).get(param, [])
        for t, v in zip(timestamps, values):
            if v is None:  # skip missing values
                continue
            records.append(StandardizedRecord(
                latitude=lat,
                longitude=lon,
                parameter=param,
                value=v,
                timestamp=datetime.datetime.fromisoformat(t),
                source="open-meteo"
            ).model_dump())
    return records