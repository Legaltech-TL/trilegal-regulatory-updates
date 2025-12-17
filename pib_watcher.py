import requests
from bs4 import BeautifulSoup
import csv
import json
import os
import time
import datetime
import re
import logging
from urllib.parse import urljoin, urlparse, parse_qs

# ================= CONFIG =================

URL = "https://www.pib.gov.in/allRel.aspx?reg=3&lang=1"
HEADERS = {"User-Agent": "Mozilla/5.0"}

DATA_DIR = "data"
CSV_FILE = os.path.join(DATA_DIR, "pib_master.csv")
JSON_FILE = os.path.join(DATA_DIR, "pib_new_entries.json")

REQUEST_DELAY = 1.2

# ================= LOGGING =================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

# ================= HELPERS =================

def ensure_data_dir():
    os.makedirs(DATA_DIR, exist_ok=True)


def extract_prid(url: str):
    qs = parse_qs(urlparse(url).query)
    return qs.get("PRID", [None])[0]


def extract_date_from_content(content: str):
    """
    Extracts date like: 17 DEC 2025
    Works for English + Hindi, multiline-safe
    """
    if not content:
        return None

    text = " ".join(content.split())
    match = re.search(r"\b\d{1,2}\s+[A-Z]{3}\s+\d{4}\b", text)
    return match.group(0) if match else None


def load_existing_ids():
    if not os.path.exists(CSV_FILE):
        return set()

    with open(CSV_FILE, newline="", encoding="utf-8") as f:
        return {row["id"] for row in csv.DictReader(f)}


def write_csv(rows):
    write_header = not os.path.exists(CSV_FILE)

    with open(CSV_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=rows[0].keys(),
            quoting=csv.QUOTE_ALL
        )
        if write_header:
            writer.writeheader()
        writer.writerows(rows)


def write_json(rows):
    with open(JSON_FILE, "w", encoding="utf-8") as f:
        json.dump(rows, f, indent=2, ensure_ascii=False)

# ================= SCRAPERS =================

def scrape_view_page():
    logging.info("Fetching PIB listing page")
    r = requests.get(URL, headers=HEADERS, timeout=30)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")
    results = []

    for h3 in soup.find_all("h3", class_="font104"):
        ministry = h3.get_text(strip=True)
        ul = h3.find_next_sibling("ul")
        if not ul:
            continue

        for a in ul.select("a[href*='PRID=']"):
            detail_page = urljoin(URL, a["href"])
            prid = extract_prid(detail_page)

            if not prid:
                continue

            results.append({
                "id": prid,
                "ministry": ministry,
                "title": a.get_text(strip=True),  # DO NOT MODIFY
                "detail_page": detail_page
            })

    logging.info("Found %d total releases on listing page", len(results))
    return results


def scrape_detail_page(url):
    logging.debug("Fetching detail page: %s", url)
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")
    content_div = soup.select_one("div.content-area")

    content = (
        content_div.get_text("\n", strip=True)
        if content_div
        else soup.get_text(" ", strip=True)
    )

    date = extract_date_from_content(content)
    return content, date

# ================= MAIN =================

def main():
    ensure_data_dir()

    existing_ids = load_existing_ids()
    logging.info("Loaded %d existing IDs", len(existing_ids))

    view_items = scrape_view_page()
    new_entries = []

    for item in view_items:
        if item["id"] in existing_ids:
            continue

        logging.info("New entry detected: %s", item["id"])
        content, date = scrape_detail_page(item["detail_page"])

        row = {
            "id": item["id"],
            "ministry": item["ministry"],
            "title": item["title"],
            "detail_page": item["detail_page"],
            "date": date,
            "content": content,
            "scraped_at": datetime.datetime.utcnow().isoformat()
        }

        new_entries.append(row)
        existing_ids.add(item["id"])
        time.sleep(REQUEST_DELAY)

    if new_entries:
        logging.info("Writing %d new entries", len(new_entries))
        write_csv(new_entries)
        write_json(new_entries)
    else:
        logging.info("No new entries found")
        write_json([])


if __name__ == "__main__":
    main()
