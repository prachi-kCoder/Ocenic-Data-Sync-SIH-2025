import datetime
from typing import Dict, Any, List
import os 
import requests
from models.data_models import StandardizedRecord
from fastapi import HTTPException
from ftplib import FTP
import os

def fetch_ftp(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Fetch file(s) from an FTP server and standardize.
    Example payload:
      {
        "host": "ftp.example.com",
        "user": "anonymous",
        "passwd": "anonymous@",
        "filepath": "/data/fisheries.csv",
        "filetype": "csv"
      }
    """
    host = payload.get("host")
    user = payload.get("user", "anonymous")
    passwd = payload.get("passwd", "anonymous@")
    filepath = payload.get("filepath")
    filetype = payload.get("filetype", "csv")

    if not host or not filepath:
        raise HTTPException(status_code=400, detail="Missing 'host' or 'filepath' for FTP fetcher.")

    try:
        ftp = FTP(host)
        ftp.login(user=user, passwd=passwd)
        local_filename = os.path.basename(filepath)

        with open(local_filename, "wb") as f:
            ftp.retrbinary(f"RETR {filepath}", f.write)
        ftp.quit()

        # Once file is downloaded → delegate to appropriate parser
        if filetype == "csv":
            return fetch_csv({"path": local_filename})
        else:
            raise ValueError(f"Unsupported filetype from FTP: {filetype}")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"FTP ingestion failed: {e}")