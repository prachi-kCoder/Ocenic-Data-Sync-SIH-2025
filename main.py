# main.py

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Dict, Any, List
import os
from dotenv import load_dotenv
from providers import fetch_open_meteo, fetch_fisheries, fetch_noaa , fetch_obis , fetch_worms , fetch_bold
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
    "bold": fetch_bold 
}

# Request model
class IngestRequest(BaseModel):
    provider: str
    payload: Dict[str, Any]



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
