from tools.cmfritool import scrape_technical_reports, download_pdf
from tools.parsetool import extract_text, extract_tables
import re

def split_sections(text: str):
    """Very simple section splitter based on report keywords."""
    sections = {}
    patterns = ["Introduction", "Methodology", "Results", "Discussion", "Conclusion"]
    last = None
    buffer = []

    for line in text.split("\n"):
        if any(p in line for p in patterns):
            if last:
                sections[last] = " ".join(buffer).strip()
                buffer = []
            last = line.strip()
        else:
            buffer.append(line)

    if last:
        sections[last] = " ".join(buffer).strip()

    return sections


def fetch_cmfri(payload):
    """
    Fetch and process CMFRI Technical Reports.
    payload can contain:
    {
        "year": "2023",
        "limit": 1
    }
    """
    year = payload.get("year", "2023")
    limit = payload.get("limit", 1)

    pdf_links = scrape_technical_reports(year=year, limit=limit)

    records = []
    for pdf_url in pdf_links:
        report = download_pdf(pdf_url)
        text = extract_text(report["file"])
        tables = extract_tables(report["file"])
        sections = split_sections(text)

        record = {
            "provider": "cmfri_pdf",
            "year": year,
            "report_name": report["file"].split("/")[-1],
            "source_url": report["url"],
            "metadata": {
                "pages": text.count("\f") + 1,  # rough page count
                "length_chars": len(text),
            },
            "sections": sections,
            "tables": tables,
        }
        records.append(record)

    return records
