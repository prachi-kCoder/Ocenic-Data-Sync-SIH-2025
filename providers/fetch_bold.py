
import datetime
from typing import Dict, Any, List
import os 
import requests
from models.data_models import StandardizedRecord
from fastapi import HTTPException

def fetch_bold(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Fetch specimen or sequence data from BOLD Systems API.
    Example payload:
      {"endpoint": "specimen", "params": {"taxon": "Gadus", "format": "json", "limit": 10}}
    """
    endpoint = payload.get("endpoint", "specimen")
    params = payload.get("params", {})
    base = "http://www.boldsystems.org/index.php/API_Public"
    url = f"{base}/{endpoint}"

    # Pop limit if user passed it, default 20
    limit = int(params.pop("limit", 20))

    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()

    records = []
    if isinstance(data, dict):
        for key, val in data.items():
            if isinstance(val, dict):
                val["id"] = key
                records.append(val)
    elif isinstance(data, list):
        records.extend(data)

    # Apply client-side limit
    records = records[:limit]

    return [{
        "processid": item.get("processid"),
        "species_name": item.get("species_name"),
        "lat": item.get("lat"),
        "lon": item.get("lon"),
        "marker": item.get("marker"),
        "genbank_accession": item.get("genbank_accession"),
        "timestamp": datetime.datetime.now().isoformat(),
        "source": f"bold/{endpoint}"
    } for item in records]