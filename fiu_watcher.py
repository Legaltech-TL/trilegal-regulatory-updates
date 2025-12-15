#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import json
import hashlib
from pathlib import Path
from datetime import datetime
from urllib.parse import urljoin, urlparse, unquote

import requests
from bs4 import BeautifulSoup

# ---------- CONFIG ----------
URL = "https://fiuindia.gov.in/files/Compliance_Orders/orders.html"
BASE = "https://fiuindia.gov.in/files/Compliance_Orders/"

TOP_N = 10

ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
DATA_DIR.mkdir(exist_ok=True)

MASTER_CSV = DATA_DIR / "fiu_master.csv"
NEW_JSON = DATA_DIR / "fiu_new_entries.json"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

CSV_FIELDS = [
    "id",
    "sr_no",
    "date",
    "title",
    "pdf_link",
    "pdf_filename",
    "file_size",
    "created_at"
]

# ---------- HELPERS ----------
def make_id(title, pdf_link):
    raw = f"{title}|{pdf_link}".encode("utf-8")
    return hashlib.sha1(raw).hexdigest()


def pdf_filename_from_url(url):
    if not url:
        return ""
    return unquote(Path(urlparse(url).path).name)


def load_existing_ids():
    if not MASTER_CSV.exists():
        return set()

    with open(MASTER_CSV, newline="", encoding="utf-8") as f:
        return {row["id"] for row in csv.DictReader(f)}


def append_to_master(rows):
    write_header = not MASTER_CSV.exists()

    with open(MASTER_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        if write_header:
            writer.writeheader()
        writer.writerows(rows)


# ---------- SCRAPER ----------
def scrape_top_10():
    r = requests.get(URL, headers=HEADERS, timeout=30)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")

    collected = []

    # FIU page has MULTIPLE tables (year-wise)
    for table in soup.find_all("table"):
        tbody = table.find("tbody")
        if not tbody:
            continue

        for tr in tbody.find_all("tr", recursive=False):
            tds = tr.find_all("td", recursive=False)
            if len(tds) < 5:
                continue  # skip empty/header rows

            sr_no = tds[0].get_text(strip=True)
            date = tds[1].get_text(strip=True)
            title = tds[2].get_text(strip=True)
            file_size = tds[3].get_text(strip=True)

            a = tds[4].find("a")
            pdf_link = urljoin(BASE, a["href"]) if a else ""
            pdf_filename = pdf_filename_from_url(pdf_link)

            entry_id = make_id(title, pdf_link)

            collected.append({
                "id": entry_id,
                "sr_no": sr_no,
                "date": date,
                "title": title,
                "pdf_link": pdf_link,
                "pdf_filename": pdf_filename,
                "file_size": file_size,
                "created_at": datetime.utcnow().isoformat()
            })

            # ✅ Stop after top 10 valid entries
            if len(collected) >= TOP_N:
                return collected

    return collected


# ---------- MAIN ----------
def main():
    print("[INFO] Scraping FIU India Compliance Orders (Top 10)")

    scraped = scrape_top_10()
    existing_ids = load_existing_ids()

    new_entries = [e for e in scraped if e["id"] not in existing_ids]

    if not new_entries:
        print("[INFO] No new entries found")
        NEW_JSON.write_text("[]", encoding="utf-8")
        return

    append_to_master(new_entries)

    with open(NEW_JSON, "w", encoding="utf-8") as f:
        json.dump(new_entries, f, indent=2, ensure_ascii=False)

    print(f"[OK] {len(new_entries)} new entries saved")
    print(f" → CSV  : {MASTER_CSV}")
    print(f" → JSON : {NEW_JSON}")


if __name__ == "__main__":
    main()
