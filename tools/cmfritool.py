import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

EPRINTS_BASE = "https://eprints.cmfri.org.in/"

def scrape_technical_reports(year="2023", limit=5):
    """Scrape CMFRI 'Marine Fish Landings' technical report PDFs for a given year."""
    url = f"{EPRINTS_BASE}view/year/{year}.html"
    r = requests.get(url)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    pdf_links = []
    count = 0

    # Step 1: find record pages
    for link in soup.find_all("a", href=True):
        href = link['href']
        text = link.get_text(strip=True)

        if "Marine Fish Landings" in text:  # filter relevant reports
            record_url = urljoin(EPRINTS_BASE, href)

            # Step 2: visit record page → find actual PDF link
            record_res = requests.get(record_url)
            record_res.raise_for_status()
            record_soup = BeautifulSoup(record_res.text, "html.parser")

            for pdf_link in record_soup.find_all("a", href=True):
                if pdf_link['href'].endswith(".pdf"):
                    pdf_links.append(urljoin(EPRINTS_BASE, pdf_link['href']))
                    count += 1
                    break  # take first PDF per record

        if count >= limit:
            break

    return pdf_links

def download_pdf(url, folder="cmfri_reports"):
    os.makedirs(folder, exist_ok=True)
    filename = os.path.join(folder, url.split("/")[-1])
    r = requests.get(url)
    r.raise_for_status()
    with open(filename, "wb") as f:
        f.write(r.content)
    return {"file": filename, "url": url}
