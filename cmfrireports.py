import requests
from bs4 import BeautifulSoup

url = "https://www.cmfri.org.in/data-publications"
response = requests.get(url)
soup = BeautifulSoup(response.text, "html.parser")

pdf_links = []
for link in soup.find_all("a", href=True):
    if link['href'].endswith(".pdf"):
        pdf_links.append(link['href'])

print(pdf_links[:5])  # check first 5 PDF URLs

import os

def download_pdf(url, folder="cmfri_reports"):
    os.makedirs(folder, exist_ok=True)
    filename = os.path.join(folder, url.split("/")[-1])
    r = requests.get(url)
    with open(filename, "wb") as f:
        f.write(r.content)
    return filename

# Example
for pdf_url in pdf_links[:3]:  # first 3 for demo
    download_pdf(pdf_url)
