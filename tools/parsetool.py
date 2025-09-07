import datetime
from typing import Dict, Any, List
import os 
import requests
from models.data_models import StandardizedRecord
from fastapi import HTTPException
import pdfplumber
import camelot

def extract_text(pdf_file):
    """Extract text from a PDF file."""
    text_content = []
    with pdfplumber.open(pdf_file) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            if text:
                text_content.append(text)
    return " ".join(text_content)

def extract_tables(pdf_file):
    """Extract tables using Camelot."""
    try:
        tables = camelot.read_pdf(pdf_file, pages="all")
        return [table.df.to_dict(orient="records") for table in tables]
    except Exception as e:
        return [{"error": str(e)}]
