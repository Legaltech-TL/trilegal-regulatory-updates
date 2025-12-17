#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
IRDAI Watcher (FINAL)
--------------------
Monitors IRDAI:
- Acts
- Rules
- Regulations
- Circulars

Features:
- Checks ONLY top 10 entries per page (page 1)
- Stable documentId-based change detection
- Writes master CSV + delta JSON
- Timezone-aware UTC timestamps
- Clear per-page logging
"""

from pathlib import Path
from bs4 import BeautifulSoup
import requests
import csv
import json
import hashlib
from datetime import datetime, timezone

# ================= CONFIG =================

BASE_DIR = Path(__file__).resolve().parent

MASTER_CSV = BASE_DIR / "data/irdai_master.csv"
NEW_JSON = BASE_DIR / "data/irdai_new_entries.json"

PAGES = {
    "Acts": "https://irdai.gov.in/acts",
    "Rules": "https://irdai.gov.in/rules",
    "Regulations": "https://irdai.gov.in/consolidated-gazette-notified-regulated-regulations"
    if False else "https://irdai.gov.in/consolidated-gazette-notified-regulations",
    "Circulars": "https://irdai.gov.in/circulars",
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

TOP_N = 10

# =========================================


def load_existing_ids():
    """Load existing document IDs from master CSV"""
    ids = set()
    if MASTER_CSV.exists():
        with open(MASTER_CSV, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                ids.add(row["id"])
    return ids


def fetch_page(url):
    """Fetch page 1 with default delta"""
    params = {
        "_com_irdai_document_media_IRDAIDocumentMediaPortlet_delta": "20",
        "_com_irdai_document_media_IRDAIDocumentMediaPortlet_cur": "1",
    }
    r = requests.get(url, headers=HEADERS, params=params, timeout=30)
    r.raise_for_status()
    return r.text


def extract_document_id(tr):
    """Extract stable documentId"""
    checkbox = tr.select_one("input.checkSingle")
    if checkbox and checkbox.get("value"):
        return checkbox["value"]

    # Fallback (extremely rare)
    raw = tr.get_text(strip=True)
    return hashlib.sha1(raw.encode()).hexdigest()


def parse_table(html, category, source_url):
    """Parse table and return (entries, total_rows)"""
    soup = BeautifulSoup(html, "html.parser")
    table = soup.select_one("table.table")

    if not table:
        return [], 0

    rows = table.select("tbody tr")
    total_rows = len(rows)

    results = []

    for tr in rows[:TOP_N]:
        tds = tr.find_all("td")
        if len(tds) < 7:
            continue

        doc_id = extract_document_id(tr)

        short_desc = tds[2].get_text(strip=True)
        last_updated = tds[3].get_text(strip=True)
        reference_no = tds[5].get_text(strip=True)

        detail_link = None
        detail_a = tds[4].select_one("a[href]")
        if detail_a:
            detail_link = detail_a["href"]

        pdf_link = None
        pdf_filename = None
        file_size = None

        pdf_a = tds[6].select_one("a[href*='download=true']")
        if pdf_a:
            pdf_link = pdf_a["href"]
            pdf_filename = pdf_a.get_text(strip=True)

            size_p = tds[6].select_one("p.text-muted")
            if size_p:
                file_size = size_p.get_text(strip=True)

        results.append({
            "id": doc_id,
            "category": category,
            "short_description": short_desc,
            "reference_no": reference_no,
            "last_updated": last_updated,
            "detail_page": detail_link,
            "pdf_link": pdf_link,
            "pdf_filename": pdf_filename,
            "file_size": file_size,
            "source_page": source_url,
            "scraped_at": datetime.now(timezone.utc).isoformat(),
        })

    return results, total_rows


def append_to_csv(rows):
    """Append new rows to master CSV"""
    file_exists = MASTER_CSV.exists()
    with open(MASTER_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        if not file_exists:
            writer.writeheader()
        writer.writerows(rows)


def main():
    print("[INFO] Starting IRDAI watcher")

    existing_ids = load_existing_ids()
    new_entries = []

    for category, url in PAGES.items():
        print(f"[INFO] Scraping {category}")

        html = fetch_page(url)
        entries, total_rows = parse_table(html, category, url)

        checked = len(entries)
        new_count = 0

        for entry in entries:
            if entry["id"] not in existing_ids:
                new_entries.append(entry)
                existing_ids.add(entry["id"])
                new_count += 1

        print(
            f"[INFO] {category}: "
            f"total available = {total_rows}, "
            f"checked = {checked}, "
            f"new = {new_count}"
        )

    if new_entries:
        print(f"[INFO] Total new entries added: {len(new_entries)}")

        append_to_csv(new_entries)

        with open(NEW_JSON, "w", encoding="utf-8") as f:
            json.dump(new_entries, f, ensure_ascii=False, indent=2)
    else:
        print("[INFO] No new entries found")

    print("[INFO] IRDAI watcher finished")


if __name__ == "__main__":
    main()
