import csv
import json
import logging
import os
import re
from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse, parse_qs

import requests
from bs4 import BeautifulSoup

# ================= CONFIG =================

BASE_PAGE = "https://saralsanchar.gov.in/circulars_order.php"
POST_URL = "https://saralsanchar.gov.in/common/get_circular_list.php"
BASE_DOMAIN = "https://saralsanchar.gov.in"

LICENSES = ["UL", "UL_VNO", "WPC", "SACFA", "WANI", "M2M"]

DATA_DIR = "data"
MASTER_CSV = os.path.join(DATA_DIR, "saralsanchar_master.csv")
NEW_JSON = os.path.join(DATA_DIR, "saralsanchar_new_entries.json")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Regulatory Watcher; SaralSanchar)",
    "Accept": "*/*",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "Referer": BASE_PAGE,
}

LICENSE_MAP = {
    "UL": "Unified License",
    "UL_VNO": "Unified License VNO",
    "WPC": "Wireless Planning & Coordination (WPC)",
    "SACFA": "Standing Advisory Committee on Frequency Allocation (SACFA)",
    "WANI": "Wi-Fi Access Network Interface (WANI)",
    "M2M": "Machine-to-Machine (M2M)",
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

def slugify(text, max_words=8, max_chars=80):
    text = text.lower()
    text = re.sub(r"[^a-z0-9\s]", "", text)
    words = text.split()[:max_words]
    slug = "-".join(words)
    return slug[:max_chars].rstrip("-")

def generate_pdf_filename(license_code, title, doc_id):
    license_full = LICENSE_MAP.get(license_code, license_code)
    license_slug = slugify(license_full)
    title_slug = slugify(title)
    return f"saralsanchar_{license_slug}_{title_slug}_{doc_id}.pdf"


# ================= LOAD EXISTING =================

def load_existing_ids():
    if not os.path.exists(MASTER_CSV):
        return set()

    with open(MASTER_CSV, newline="", encoding="utf-8") as f:
        return {row["id"] for row in csv.DictReader(f)}

# ================= FETCH =================

def fetch_for_license(session, license_code):
    logging.info("Fetching circulars for license: %s", license_code)

    payload = {
        "circular_type": license_code
    }

    r = session.post(POST_URL, data=payload, headers=HEADERS, timeout=30)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")
    rows = soup.select("table tbody tr")

    results = []

    for row in rows:
        cols = row.find_all("td")
        if len(cols) < 4:
            continue

        date = cols[1].get_text(strip=True)
        title = cols[2].get_text(strip=True)

        link_tag = cols[3].find("a", href=True)
        if not link_tag:
            continue

        pdf_href = link_tag["href"].strip()
        pdf_link = urljoin(BASE_DOMAIN, pdf_href)

        parsed = urlparse(pdf_href)
        f_param = parse_qs(parsed.query).get("f", [""])[0]

        if not f_param:
            continue

        record_id = f"{license_code}_{f_param}"

        results.append({
            "id": record_id,
            "license": LICENSE_MAP.get(license_code, license_code),
            "date": date,
            "title": title,
            "pdf_link": pdf_link,
            "pdf_filename": generate_pdf_filename(license_code, title, f_param),
            "source_page": BASE_PAGE,
            "scraped_at": now_iso(),
        })

    logging.info("Found %d entries for %s", len(results), license_code)
    return results

# ================= SAVE =================

def append_to_master(rows):
    exists = os.path.exists(MASTER_CSV)

    with open(MASTER_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "id",
                "license",
                "date",
                "title",
                "pdf_link",
                "pdf_filename",
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
    logging.info("Loaded %d existing Saral Sanchar records", len(existing_ids))

    session = requests.Session()

    # 🔥 REQUIRED: establish session & cookies
    logging.info("Initializing session with base page")
    session.get(BASE_PAGE, headers=HEADERS, timeout=30)

    all_new = []

    for license_code in LICENSES:
        items = fetch_for_license(session, license_code)
        new_items = [i for i in items if i["id"] not in existing_ids]
        all_new.extend(new_items)

    if not all_new:
        logging.info("No new Saral Sanchar circulars found")
        write_new_entries([])
        return

    logging.info("Detected %d NEW Saral Sanchar circulars", len(all_new))

    append_to_master(all_new)
    write_new_entries(all_new)

    logging.info("Saral Sanchar CSV and JSON updated successfully")

if __name__ == "__main__":
    main()

