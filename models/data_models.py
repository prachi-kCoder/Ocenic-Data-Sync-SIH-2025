from pydantic import BaseModel
from typing import Union
import datetime

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
    
class FisheriesData(BaseModel):
    year: str
    total_fish_production_lakh_tonnes: float
    marine_fish_production_lakh_tonnes: float
    inland_fish_production_lakh_tonnes: float
    total_exports_crores: float
    ingestion_timestamp: datetime.datetime
    source: str