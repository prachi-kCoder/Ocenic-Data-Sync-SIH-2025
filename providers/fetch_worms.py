import datetime
from typing import Dict, Any, List
import os 
import requests
from models.data_models import StandardizedRecord
from fastapi import HTTPException


def fetch_worms(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    endpoint = payload.get("endpoint", "AphiaRecordsByName")
    params = payload.get("params", {})
    limit = payload.get("limit", 100)  # default cap at 100
    base = "https://www.marinespecies.org/rest"

    # Build endpoint-specific URL
    if "scientificname" in params:
        sci_name = params["scientificname"]
        url = f"{base}/{endpoint}/{sci_name}"
        params = {}
    elif "AphiaID" in params:
        aphia_id = params["AphiaID"]
        url = f"{base}/{endpoint}/{aphia_id}"
        params = {}
    else:
        raise ValueError("WoRMS requires either 'scientificname' or 'AphiaID' in params")

    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()

    # ✅ Case 1: API just returns an integer (e.g., AphiaIDByName)
    if isinstance(data, int):
        return [{
            "aphiaID": data,
            "timestamp": datetime.datetime.now().isoformat(),
            "source": f"worms/{endpoint}"
        }]

    # ✅ Case 2: normalize dict → list
    if isinstance(data, dict):
        data = [data]

    # ✅ Apply limit
    data = data[:limit]

    records = []
    for item in data:
        record = {
            "aphiaID": item.get("AphiaID"),
            "scientificName": item.get("scientificname"),
            "rank": item.get("rank"),
            "status": item.get("status"),
            "valid_name": item.get("valid_name"),
            "valid_AphiaID": item.get("valid_AphiaID"),
            "kingdom": item.get("kingdom"),
            "phylum": item.get("phylum"),
            "class": item.get("class"),
            "order": item.get("order"),
            "family": item.get("family"),
            "genus": item.get("genus"),
            "timestamp": datetime.datetime.now().isoformat(),
            "source": f"worms/{endpoint}"
        }
        records.append(record)

    return records