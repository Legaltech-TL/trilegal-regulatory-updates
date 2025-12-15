#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
MIB Updates Scraper — FINAL STABLE VERSION

✔ Covers ONLY server-rendered pages
✔ Notices
✔ Acts / Policy / Guidelines
✔ Other Communication
✔ Top 10 entries per page
"""

from pathlib import Path
import csv
import json
import hashlib
import re
from datetime import datetime, UTC
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

# -------------------------------------------------
# CONFIG
# -------------------------------------------------
CATEGORIES = {
    "notices": "https://mib.gov.in/documents/notification/notices",
    "acts_policy_guidelines": "https://mib.gov.in/documents/notification/acts-policy-guidelines",
    "other_communication": "https://mib.gov.in/documents/notification/Other-communication",
}

MAX_ITEMS_PER_CATEGORY = 10

ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
MASTER_CSV = DATA_DIR / "mib_master.csv"
NEW_JSON = DATA_DIR / "mib_new_entries.json"
DATA_DIR.mkdir(exist_ok=True)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0 Safari/537.36"
    )
}

CSV_FIELDS = [
    "id",
    "date",
    "title",
    "pdf_link",
    "detail_page_link",
    "pdf_filename",
    "wing_category",
    "file_info",
    "category",
    "created_at",
]

# -------------------------------------------------
# HELPERS
# -------------------------------------------------
def make_id(title, date, category, link):
    raw = f"{title}|{date}|{category}|{link}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()

def normalize_date(date_str):
    parts = re.split(r"[./-]", date_str)
    if len(parts) == 3:
        return f"{parts[2]}-{parts[1].zfill(2)}-{parts[0].zfill(2)}"
    return date_str or "unknown-date"

def make_pdf_filename(title, date):
    slug = re.sub(r"[^\w\s-]", "", title.lower())
    slug = re.sub(r"\s+", "-", slug).strip("-")[:80]
    return f"{normalize_date(date)}_{slug}.pdf"

def ensure_master_csv():
    if not MASTER_CSV.exists():
        with open(MASTER_CSV, "w", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=CSV_FIELDS).writeheader()

def load_existing_ids():
    if not MASTER_CSV.exists():
        return set()
    with open(MASTER_CSV, newline="", encoding="utf-8") as f:
        return {row["id"] for row in csv.DictReader(f)}

# -------------------------------------------------
# PARSER (COMMON FOR ALL 3 PAGES)
# -------------------------------------------------
def parse_table_row(row, category, base_url):
    cols = row.find_all("td")
    if len(cols) < 5:
        return None

    title = cols[1].get_text(" ", strip=True)
    date = cols[2].get_text(strip=True)
    wing_category = cols[3].get_text(strip=True)
    file_info = cols[-2].get_text(strip=True)

    # PDF link always in LAST column
    pdf_link = None
    for a in cols[-1].find_all("a", href=True):
        href = a["href"].strip()
        if href.lower().endswith(".pdf"):
            pdf_link = urljoin(base_url, href)

    # Detail page (sometimes present)
    detail_page_link = None
    title_tag = cols[1].find("a", href=True)
    if title_tag:
        detail_page_link = urljoin(base_url, title_tag["href"].strip())

    entry_id = make_id(
        title,
        date,
        category,
        pdf_link or detail_page_link or ""
    )

    return {
        "id": entry_id,
        "date": date,
        "title": title,
        "pdf_link": pdf_link,
        "detail_page_link": detail_page_link,
        "pdf_filename": make_pdf_filename(title, date) if pdf_link else None,
        "wing_category": wing_category,
        "file_info": file_info,
        "category": category,
        "created_at": datetime.now(UTC).isoformat(),
    }

# -------------------------------------------------
# SCRAPER
# -------------------------------------------------
def scrape_category(category, url):
    res = requests.get(url, headers=HEADERS, timeout=30)
    res.raise_for_status()

    soup = BeautifulSoup(res.text, "html.parser")
    rows = soup.select("table tr")[1:MAX_ITEMS_PER_CATEGORY + 1]

    print(f"[DEBUG] Found {len(rows)} rows in {category}")

    entries = []
    for row in rows:
        parsed = parse_table_row(row, category, url)
        if parsed:
            entries.append(parsed)

    return entries

# -------------------------------------------------
# MAIN
# -------------------------------------------------
def main():
    print("[INFO] Starting MIB scraper — STABLE (3 pages only)")

    ensure_master_csv()
    existing_ids = load_existing_ids()
    new_entries = []

    for category, url in CATEGORIES.items():
        print(f"[INFO] Scraping {category}")
        entries = scrape_category(category, url)

        for entry in entries:
            if entry["id"] not in existing_ids:
                new_entries.append(entry)
                existing_ids.add(entry["id"])

    if new_entries:
        with open(MASTER_CSV, "a", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=CSV_FIELDS).writerows(new_entries)

    with open(NEW_JSON, "w", encoding="utf-8") as f:
        json.dump(new_entries, f, ensure_ascii=False, indent=2)

    print(f"[INFO] New entries added: {len(new_entries)}")

if __name__ == "__main__":
    main()
