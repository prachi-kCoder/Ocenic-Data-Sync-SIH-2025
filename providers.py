# providers.py 
from fastapi import FastAPI, HTTPException
import requests
import datetime
from typing import List, Dict, Any,Union
from pydantic import BaseModel
# ---------- Open-Meteo ----------
class WeatherData(BaseModel):
    latitude: float
    longitude: float
    temperature: float
    ingestion_timestamp: datetime.datetime
    source: str
class StandardizedRecord(BaseModel):
    latitude: float | None = None
    longitude: float | None = None
    station: str | None = None
    parameter: str
    value: Union[int, float, str, None] = None
    timestamp: datetime.datetime
    source: str

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
        "hourly": ",".join(payload.get("hourly", ["wave_height", "sea_surface_temperature"])),
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

# ---------- Fisheries (data.gov.in) ----------
class FisheriesData(BaseModel):
    year: str
    total_fish_production_lakh_tonnes: float
    marine_fish_production_lakh_tonnes: float
    inland_fish_production_lakh_tonnes: float
    total_exports_crores: float
    ingestion_timestamp: datetime.datetime
    source: str


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

# def fetch_bold(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
#     """
#     Fetch DNA barcode and specimen data from BOLD Systems API.
#     Example payloads:
#       {"endpoint": "specimen", "params": {"taxon": "Sardinella", "format": "json"}}
#       {"endpoint": "sequence", "params": {"ids": "MFLP001-12", "format": "json"}}
#     """
#     endpoint = payload.get("endpoint", "specimen")
#     params = payload.get("params", {})
#     base = "https://www.boldsystems.org/index.php/API_Public"

#     url = f"{base}/{endpoint}"
#     r = requests.get(url, params=params, timeout=30)
#     r.raise_for_status()
#     data = r.json()

#     records = []
#     for item in data:
#         record = {
#             "processid": item.get("processid"),
#             "species": item.get("species_name"),
#             "latitude": item.get("lat"),
#             "longitude": item.get("lon"),
#             "genbank_accession": item.get("genbank_accession"),
#             "collectiondate": item.get("collectiondate"),
#             "timestamp": datetime.datetime.now().isoformat(),
#             "source": f"bold/{endpoint}"
#         }
#         records.append(record)

#     return records