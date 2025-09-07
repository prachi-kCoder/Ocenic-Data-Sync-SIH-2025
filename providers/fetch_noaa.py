import datetime
from typing import Dict, Any, List
import os 
import requests
from models.data_models import StandardizedRecord
from fastapi import HTTPException


def fetch_noaa(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Fetch data from NOAA Tides & Currents API with extended support.
    Example payload:
    {
        "station": "8723214",
        "product": "salinity",
        "begin_date": "20250101",
        "end_date": "20250105"
    }
    """
    import requests
    import datetime
    from fastapi import HTTPException

    station = payload.get("station")
    product = payload.get("product", "water_temperature")

    if not station:
        raise HTTPException(status_code=400, detail="Missing 'station' in payload")

    VALID_PRODUCTS = {
        "water_level", "water_temperature", "air_temperature", "wind", "air_pressure",
        "visibility", "humidity", "conductivity", "salinity", "currents", "predictions",
        "hourly_height", "high_low", "monthly_mean", "daily_max_min", "six_minute"
    }

    if product not in VALID_PRODUCTS:
        raise HTTPException(status_code=400, detail=f"Unsupported NOAA product: {product}")

    url = "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter"
    params = {
        "product": product,
        "station": station,
        "units": "metric",
        "time_zone": "gmt",
        "format": "json"
    }

    # Add date parameters dynamically
    for key in ["date", "range", "begin_date", "end_date"]:
        if key in payload:
            params[key] = payload[key]

    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()

    if "data" not in data or not data["data"]:
        raise HTTPException(status_code=404, detail="No data found from NOAA")

    # Optional metadata enrichment
    meta = data.get("metadata", {})
    if not meta:
        try:
            meta_url = f"https://api.tidesandcurrents.noaa.gov/mdapi/prod/webapi/stations/{station}/metadata.json"
            meta_resp = requests.get(meta_url, timeout=10)
            meta_data = meta_resp.json().get("stations", [{}])[0]
            meta["lat"] = meta_data.get("lat")
            meta["lon"] = meta_data.get("lng")
        except Exception:
            pass  # fallback to empty metadata

    records = []
    for item in data["data"]:
        raw_time = item["t"].replace(" ", "T")
        try:
            timestamp = datetime.datetime.fromisoformat(raw_time)
        except ValueError:
            timestamp = datetime.datetime.strptime(raw_time, "%Y-%m-%dT%H:%M:%S")

        records.append(StandardizedRecord(
            station=station,
            latitude=meta.get("lat"),
            longitude=meta.get("lon"),
            parameter=product,
            value=float(item["v"]),
            timestamp=timestamp,
            source="NOAA"
        ).model_dump())

    return records