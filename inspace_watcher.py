#!/usr/bin/env python3
"""
IN-SPACe Watcher (GitHub Actions Safe)

✔ Playwright hardened
✔ First-page only
✔ CSV + JSON
✔ Change detection
✔ Never crashes on slow loads

Author: Bhanu Tak
"""

# ===================== ENV HARDENING =====================
import os
os.environ.setdefault("PLAYWRIGHT_BROWSERS_PATH", "0")

# ===================== IMPORTS =====================
from playwright.sync_api import sync_playwright, TimeoutError
from pathlib import Path
import csv
import json
import hashlib
import logging
from datetime import datetime

# ===================== CONFIG =====================
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

MASTER_CSV = DATA_DIR / "inspace_master.csv"
NEW_JSON   = DATA_DIR / "inspace_new_entries.json"

PAGES = {
    "Press Releases": "https://www.inspace.gov.in/inspace?id=inspace_press_releases_page",
    "Publications": "https://www.inspace.gov.in/inspace?id=inspace_publications"
}

TIMEOUT = 30000

# ===================== LOGGING =====================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

# ===================== HELPERS =====================
def make_id(text):
    return hashlib.sha1(text.encode("utf-8")).hexdigest()

def load_existing_ids():
    ids = set()
    if MASTER_CSV.exists():
        with open(MASTER_CSV, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                ids.add(row["id"])
    logging.info(f"Loaded {len(ids)} existing records")
    return ids

def write_master(rows):
    write_header = not MASTER_CSV.exists()
    with open(MASTER_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        if write_header:
            writer.writeheader()
        writer.writerows(rows)

# ===================== SCRAPER =====================
def scrape_page(page, category, url):
    logging.info(f"Scraping {category}")
    page.goto(url, wait_until="networkidle", timeout=45000)

    # 🔥 HARD WAIT for dynamic container
    page.wait_for_selector(".releases-list", timeout=30000)

    container = page.locator(".releases-list")
    cards = container.locator(".release-item")

    count = cards.count()
    logging.info(f"Detected {count} raw items in DOM")

    items = []

    for i in range(min(count, 10)):
        try:
            card = cards.nth(i)

            title = card.locator("h3.release-title").inner_text().strip()
            date  = card.locator(".release-date").inner_text().strip()

            pdf_link = None
            try:
                pdf_link = card.locator("a:has-text('Download PDF')").get_attribute("href")
                if pdf_link and pdf_link.startswith("/"):
                    pdf_link = "https://www.inspace.gov.in" + pdf_link
            except:
                pass

            uid = make_id(category + title + date)

            items.append({
                "id": uid,
                "category": category,
                "title": title,
                "date": date,
                "pdf_link": pdf_link,
                "source_page": url,
                "scraped_at": datetime.utcnow().isoformat()
            })

        except Exception as e:
            logging.warning(f"Skipping item {i}: {e}")

    logging.info(f"Found {len(items)} items in {category}")
    return items

# ===================== MAIN =====================
def main():
    existing_ids = load_existing_ids()
    new_entries = []

    with sync_playwright() as p:
        browser = p.chromium.launch(
            channel="chrome",   # 🔥 FORCE SYSTEM CHROME
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
            ]
        )


        context = browser.new_context()
        page = context.new_page()

        for category, url in PAGES.items():
            try:
                items = scrape_page(page, category, url)
                for item in items:
                    if item["id"] not in existing_ids:
                        new_entries.append(item)
                        existing_ids.add(item["id"])
            except TimeoutError:
                logging.error(f"Timeout while scraping {category}")

        browser.close()

    if new_entries:
        write_master(new_entries)
        with open(NEW_JSON, "w", encoding="utf-8") as f:
            json.dump(new_entries, f, indent=2, ensure_ascii=False)

        logging.info(f"✅ {len(new_entries)} new entries saved")
    else:
        logging.info("No new entries found")
        NEW_JSON.write_text("[]", encoding="utf-8")

if __name__ == "__main__":
    main()
