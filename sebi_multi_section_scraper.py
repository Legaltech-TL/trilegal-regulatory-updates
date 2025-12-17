#!/usr/bin/env python3
"""
SEBI Multi-Section Scraper
GitHub Actions SAFE (Playwright Sync)
"""

import os
import csv
import json
import time
import hashlib
import logging
from datetime import datetime
from pathlib import Path

# ---------- CRITICAL FOR GITHUB ACTIONS ----------
os.environ.setdefault("PLAYWRIGHT_BROWSERS_PATH", "0")

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

# ---------- PATHS ----------
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

MASTER_CSV = DATA_DIR / "sebi_master.csv"
NEW_JSON = DATA_DIR / "sebi_new_entries.json"

# ---------- LOGGING ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("SEBI")

# ---------- CONFIG ----------
SEBI_URL = "https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListing=yes&sid=1&ssid=0&smid=0"
TOP_LIMIT = 10

FIELDS = [
    "id",
    "title",
    "date",
    "pdf_link",
    "section",
    "source_page",
    "scraped_at",
]

# ---------- HELPERS ----------
def make_id(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()

def load_existing_ids():
    if not MASTER_CSV.exists():
        return set()

    with open(MASTER_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return {row["id"] for row in reader if row.get("id")}

def append_to_master(rows):
    write_header = not MASTER_CSV.exists()

    with open(MASTER_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS, quoting=csv.QUOTE_ALL)
        if write_header:
            writer.writeheader()
        for r in rows:
            writer.writerow(r)

# ---------- SCRAPER ----------
def scrape():
    new_rows = []
    seen_ids = load_existing_ids()

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
            ],
        )

        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            )
        )

        page = context.new_page()
        log.info("Opening SEBI page")
        page.goto(SEBI_URL, timeout=60000, wait_until="domcontentloaded")
        page.wait_for_timeout(3000)

        # Example selector – adjust if SEBI changes DOM
        rows = page.locator("table tbody tr").all()[:TOP_LIMIT]
        log.info("Found %d rows", len(rows))

        for row in rows:
            try:
                title = row.locator("a").inner_text().strip()
                pdf_link = row.locator("a").get_attribute("href") or ""
                date_text = row.locator("td").nth(1).inner_text().strip()

                uid = make_id(title + date_text)

                if uid in seen_ids:
                    continue

                record = {
                    "id": uid,
                    "title": title,
                    "date": date_text,
                    "pdf_link": pdf_link,
                    "section": "General",
                    "source_page": SEBI_URL,
                    "scraped_at": datetime.utcnow().isoformat(),
                }

                new_rows.append(record)

            except PlaywrightTimeoutError:
                log.warning("Timeout while parsing row")
            except Exception as e:
                log.error("Row parse error: %s", e)

        browser.close()

    return new_rows

# ---------- MAIN ----------
def main():
    log.info("Starting SEBI scraper")

    new_entries = scrape()

    if not new_entries:
        log.info("No new entries found")
        return

    append_to_master(new_entries)

    with open(NEW_JSON, "w", encoding="utf-8") as f:
        json.dump(new_entries, f, ensure_ascii=False, indent=2)

    log.info("Added %d new entries", len(new_entries))

if __name__ == "__main__":
    main()
