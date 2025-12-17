#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
IRDAI Watcher (FINAL – CLEAN ROW FILTERING)
------------------------------------------
- Skips blank / invalid table rows
- Checks ONLY top 10 entries per page
- CSV columns exactly match JSON
- Outputs stored in /data folder
- UTC timezone-aware timestamps
--------------------------------------------
Made by - Bhanu Tak
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
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

MASTER_CSV = DATA_DIR / "irdai_master.csv"
NEW_JSON = DATA_DIR / "irdai_new_entries.json"

PAGES = {
    "Acts": "https://irdai.gov.in/acts",
    "Rules": "https://irdai.gov.in/rules",
    "Regulations": "https://irdai.gov.in/consolidated-gazette-notified-regulations",
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
    ids = set()
    if MASTER_CSV.exists():
        with open(MASTER_CSV, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                ids.add(row["id"])
    return ids


def fetch_page(url):
    params = {
        "_com_irdai_document_media_IRDAIDocumentMediaPortlet_delta": "20",
        "_com_irdai_document_media_IRDAIDocumentMediaPortlet_cur": "1",
    }
    r = requests.get(url, headers=HEADERS, params=params, timeout=30)
    r.raise_for_status()
    return r.text


def extract_document_id(tr):
    checkbox = tr.select_one("input.checkSingle")
    if checkbox and checkbox.get("value"):
        return checkbox["value"]

    # Fallback (only used if row is valid but checkbox missing)
    raw = tr.get_text(strip=True)
    return hashlib.sha1(raw.encode()).hexdigest()


def parse_table(html, category, source_url):
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

        short_desc = tds[2].get_text(strip=True)

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

        # 🚨 STRICT ROW VALIDATION
        if not short_desc or not (detail_link or pdf_link):
            continue

        doc_id = extract_document_id(tr)

        results.append({
            "id": doc_id,
            "category": category,
            "short_description": short_desc,
            "reference_no": tds[5].get_text(strip=True),
            "last_updated": tds[3].get_text(strip=True),
            "detail_page": detail_link,
            "pdf_link": pdf_link,
            "pdf_filename": pdf_filename,
            "file_size": file_size,
            "source_page": source_url,
            "scraped_at": datetime.now(timezone.utc).isoformat(),
        })

    return results, total_rows


def append_to_csv(rows):
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
        append_to_csv(new_entries)

        with open(NEW_JSON, "w", encoding="utf-8") as f:
            json.dump(new_entries, f, ensure_ascii=False, indent=2)

        print(f"[INFO] Total new entries added: {len(new_entries)}")
    else:
        print("[INFO] No new entries found")

    print("[INFO] IRDAI watcher finished")


if __name__ == "__main__":
    main()
