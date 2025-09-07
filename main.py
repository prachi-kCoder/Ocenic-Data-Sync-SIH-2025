
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Dict, Any, List
import os
from dotenv import load_dotenv
# from providers import fetch_open_meteo, fetch_fisheries, fetch_noaa , fetch_obis , fetch_worms , fetch_bold, fetch_csv, fetch_ftp
from providers.fetch_open_meteo import fetch_open_meteo
from providers.fetch_csv import fetch_csv
from providers.fetch_fisheries import fetch_fisheries
from providers.fetch_noaa import fetch_noaa
from providers.fetch_obis import fetch_obis
from providers.fetch_worms import fetch_worms
from providers.fetch_bold import fetch_bold
from providers.fetch_ftp import fetch_ftp
from providers.fetch_cmfri import fetch_cmfri

import datetime
import requests 
load_dotenv()

app = FastAPI(
    title="Scalable Data Ingestion API",
    description="Backend to fetch and standardize data from multiple providers",
    version="1.0.0",
)

database = []

DATA_GOV_API_KEY = os.environ.get("DATA_GOV_API_KEY")

PROVIDERS = {
    "open-meteo": fetch_open_meteo,
    "noaa": fetch_noaa,
    "obis": fetch_obis,
    "worms": fetch_worms, 
    "bold": fetch_bold ,
    "fisheries": lambda payload: fetch_fisheries(payload, api_key=os.environ.get("DATA_GOV_API_KEY")),
    "csv": fetch_csv,
    "ftp": fetch_ftp,
    "cmfri": fetch_cmfri,   
}

# Request model
class IngestRequest(BaseModel):
    provider: str
    payload: Dict[str, Any]


from tools.cmfritool import scrape_technical_reports
print(scrape_technical_reports("2023", limit=2))
@app.post("/ingest/")
def ingest(req: IngestRequest):
    provider = req.provider
    payload = req.payload

    if provider not in PROVIDERS:
        raise HTTPException(status_code=400, detail=f"Unknown provider: {provider}")

    try:
        records = PROVIDERS[provider](payload)
        database.extend(records)
        return {"status": "success", "records": records}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ingestion failed: {e}")


@app.get("/data/")
def get_data() -> List[Dict[str, Any]]:
    return database
