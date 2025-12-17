import os
import csv
import json
import hashlib
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from datetime import datetime

URL = "https://dst.gov.in/geospatial"

DATA_DIR = "data"
MASTER_CSV = os.path.join(DATA_DIR, "dst_geospatial_master.csv")
NEW_JSON = os.path.join(DATA_DIR, "dst_geospatial_new_entries.json")

FIELDS = [
    "id",
    "title",
    "pdf_link",
    "pdf_filename",
    "file_size",
    "source_page",
    "scraped_at"
]

os.makedirs(DATA_DIR, exist_ok=True)


def make_id(title, pdf_link):
    return hashlib.sha1(f"{title}|{pdf_link}".encode()).hexdigest()


def load_existing_ids():
    if not os.path.exists(MASTER_CSV):
        return set()

    with open(MASTER_CSV, newline="", encoding="utf-8") as f:
        return {row["id"] for row in csv.DictReader(f)}


def scrape_geospatial_div():
    response = requests.get(URL, timeout=30)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")

    container = soup.find("div", class_="field-content")
    if not container:
        raise RuntimeError("Target div (field-content) not found")

    results = []

    for p in container.find_all("p"):
        a = p.find("a", href=True)
        if not a:
            continue

        href = a["href"]
        if ".pdf" not in href.lower():
            continue  # ignore non-pdf links

        pdf_link = urljoin(URL, href)
        pdf_filename = pdf_link.split("/")[-1]

        # Title = visible strong/a text without extra junk
        title = a.get_text(" ", strip=True)

        # File size if present
        size_span = p.find("span", class_="file-size")
        file_size = size_span.get_text(strip=True) if size_span else None

        results.append({
            "title": title,
            "pdf_link": pdf_link,
            "pdf_filename": pdf_filename,
            "file_size": file_size,
            "source_page": URL,
            "scraped_at": datetime.utcnow().isoformat()
        })

    return results


def append_to_csv(rows):
    exists = os.path.exists(MASTER_CSV)

    with open(MASTER_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        if not exists:
            writer.writeheader()
        writer.writerows(rows)


def main():
    print("[INFO] DST Geospatial watcher started")

    existing_ids = load_existing_ids()
    scraped_items = scrape_geospatial_div()

    new_entries = []

    for item in scraped_items:
        item_id = make_id(item["title"], item["pdf_link"])
        if item_id in existing_ids:
            continue

        item["id"] = item_id
        new_entries.append(item)

    if new_entries:
        print(f"[INFO] New entries found: {len(new_entries)}")
        append_to_csv(new_entries)

        with open(NEW_JSON, "w", encoding="utf-8") as f:
            json.dump(new_entries, f, indent=2, ensure_ascii=False)
    else:
        print("[INFO] No new entries found")
        with open(NEW_JSON, "w") as f:
            json.dump([], f)

    print("[DONE]")


if __name__ == "__main__":
    main()
