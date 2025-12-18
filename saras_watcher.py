import csv
import json
import logging
import os
import re
import hashlib
from datetime import datetime, timezone
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

# ================= CONFIG =================

BASE_URL = "https://www.saras.gov.in/main/index"
BASE_DOMAIN = "https://www.saras.gov.in/main/"

DATA_DIR = "data"
MASTER_CSV = os.path.join(DATA_DIR, "saras_master.csv")
NEW_JSON = os.path.join(DATA_DIR, "saras_new_entries.json")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Regulatory Watcher; SARAS)",
    "Accept-Language": "en-US,en;q=0.9",
}

# ================= LOGGING =================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

# ================= UTIL =================

def ensure_dirs():
    os.makedirs(DATA_DIR, exist_ok=True)

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def sha1(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()

def slugify_title(title, max_words=8, max_chars=80):
    title = title.lower()
    title = re.sub(r"[^a-z0-9\s]", "", title)
    words = title.split()[:max_words]
    slug = "-".join(words)
    return slug[:max_chars].rstrip("-")

def generate_pdf_filename(title):
    return f"saras_{slugify_title(title)}.pdf"

# ================= LOAD EXISTING =================

def load_existing_ids():
    if not os.path.exists(MASTER_CSV):
        return set()

    with open(MASTER_CSV, newline="", encoding="utf-8") as f:
        return {row["id"] for row in csv.DictReader(f)}

# ================= SCRAPER =================

def fetch_latest_updates():
    logging.info("Fetching SARAS homepage")
    r = requests.get(BASE_URL, headers=HEADERS, timeout=30)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")

    container = soup.select_one("#LatestUpdates")
    if not container:
        logging.warning("LatestUpdates section not found")
        return []

    items = container.select(".media.p-lm")
    logging.info("Found %d items in Latest Updates", len(items))

    results = []

    for block in items:
        p = block.select_one(".media-body p")
        a = block.select_one("a[href]")

        if not p or not a:
            continue

        full_text = p.get_text(" ", strip=True)

        # Title = text before 'Download'
        title = full_text.split("Download")[0].strip()

        href = a.get("href").strip()
        pdf_link = urljoin(BASE_DOMAIN, href)

        # Extract file size if present
        size_match = re.search(r"\(([\d\.]+\s*MB)\)", full_text)
        file_size = size_match.group(1) if size_match else ""

        record_id = sha1(title + pdf_link)

        results.append({
            "id": record_id,
            "title": title,
            "pdf_link": pdf_link,
            "pdf_filename": generate_pdf_filename(title),
            "file_size": file_size,
            "source_page": BASE_URL,
            "scraped_at": now_iso(),
        })

    return results

# ================= SAVE =================

def append_to_master(rows):
    exists = os.path.exists(MASTER_CSV)

    with open(MASTER_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "id",
                "title",
                "pdf_link",
                "pdf_filename",
                "file_size",
                "source_page",
                "scraped_at",
            ],
        )

        if not exists:
            writer.writeheader()

        writer.writerows(rows)

def write_new_entries(rows):
    with open(NEW_JSON, "w", encoding="utf-8") as f:
        json.dump(rows, f, indent=2, ensure_ascii=False)

# ================= MAIN =================

def main():
    ensure_dirs()

    existing_ids = load_existing_ids()
    logging.info("Loaded %d existing SARAS records", len(existing_ids))

    current_items = fetch_latest_updates()

    new_items = [i for i in current_items if i["id"] not in existing_ids]

    if not new_items:
        logging.info("No new SARAS updates found")
        write_new_entries([])
        return

    logging.info("Detected %d NEW SARAS updates", len(new_items))

    append_to_master(new_items)
    write_new_entries(new_items)

    logging.info("SARAS CSV and JSON updated successfully")

if __name__ == "__main__":
    main()
