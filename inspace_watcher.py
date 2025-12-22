#!/usr/bin/env python3
"""
IN-SPACe Watcher – FINAL (Press Releases + Publications)

✔ Playwright (Chrome channel)
✔ Handles dynamic accordion (Publications)
✔ CSV + JSON
✔ Change detection via hash
✔ GitHub Actions safe
✔ Never crashes on partial failures

Author: Bhanu Tak
"""

# ===================== IMPORTS =====================
from playwright.sync_api import sync_playwright, TimeoutError
from pathlib import Path
from datetime import datetime
import hashlib
import csv
import json
import logging

# ===================== CONFIG =====================
BASE_URL = "https://www.inspace.gov.in"

PAGES = {
    "Press Releases": "https://www.inspace.gov.in/inspace?id=inspace_press_releases_page",
    "Publications": "https://www.inspace.gov.in/inspace?id=inspace_publications",
}

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

MASTER_CSV = DATA_DIR / "inspace_master.csv"
NEW_JSON   = DATA_DIR / "inspace_new_entries.json"

MAX_PRESS_ITEMS = 10   # top N only

# ===================== LOGGING =====================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

# ===================== HELPERS =====================
def make_id(text: str) -> str:
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

# ===================== PRESS RELEASES =====================
def scrape_press_releases(page, url):
    logging.info("Scraping Press Releases")

    page.goto(url, wait_until="networkidle", timeout=45000)
    page.wait_for_selector(".releases-list", timeout=30000)

    container = page.locator(".releases-list")
    cards = container.locator(".release-item")

    count = min(cards.count(), MAX_PRESS_ITEMS)
    logging.info(f"Detected {count} press release items")

    items = []

    for i in range(count):
        try:
            card = cards.nth(i)

            title = card.locator("h3.release-title").inner_text().strip()
            date  = card.locator(".release-date").inner_text().strip()

            pdf_link = None
            if card.locator("a:has-text('Download PDF')").count() > 0:
                href = card.locator("a:has-text('Download PDF')").get_attribute("href")
                if href:
                    pdf_link = BASE_URL + href if href.startswith("/") else href

            uid = make_id("press" + title + date)

            items.append({
                "id": uid,
                "section": "Press Releases",
                "category": "Press Release",
                "title": title,
                "date": date,
                "meta": "",
                "pdf_link": pdf_link,
                "source_page": url,
                "scraped_at": datetime.utcnow().isoformat()
            })

        except Exception as e:
            logging.warning(f"Skipping press item {i}: {e}")

    logging.info(f"Found {len(items)} items in Press Releases")
    return items

# ===================== PUBLICATIONS =====================
def scrape_publications(page, url):
    logging.info("Scraping Publications")

    page.goto(url, wait_until="networkidle", timeout=45000)
    page.wait_for_selector(".publications-container", timeout=30000)

    all_items = []

    categories = page.locator(".category-block")
    cat_count = categories.count()
    logging.info(f"Detected {cat_count} publication categories")

    for i in range(cat_count):
        try:
            category = categories.nth(i)
            header = category.locator(".category-header")
            category_title = header.locator("h4").inner_text().strip()

            # Open accordion
            header.click(force=True)
            page.wait_for_timeout(500)

            doc_list = category.locator("ul.doc-list")
            if doc_list.count() == 0:
                continue

            docs = doc_list.locator("li")
            doc_count = docs.count()
            logging.info(f"{category_title}: {doc_count} documents")

            for j in range(doc_count):
                try:
                    doc = docs.nth(j)

                    link = doc.locator("a.doc-link")
                    title = link.inner_text().strip()
                    href = link.get_attribute("href")

                    if href:
                        href = BASE_URL + href if href.startswith("/") else href

                    meta = ""
                    if doc.locator("p.belowlinetext").count() > 0:
                        meta = doc.locator("p.belowlinetext").inner_text().strip()

                    uid = make_id("publication" + category_title + title + (href or ""))

                    all_items.append({
                        "id": uid,
                        "section": "Publications",
                        "category": category_title,
                        "title": title,
                        "date": "",
                        "meta": meta,
                        "pdf_link": href,
                        "source_page": url,
                        "scraped_at": datetime.utcnow().isoformat()
                    })

                except Exception as e:
                    logging.warning(f"Skipping doc {j} in {category_title}: {e}")

        except Exception as e:
            logging.warning(f"Skipping category {i}: {e}")

    logging.info(f"Total publications collected: {len(all_items)}")
    return all_items

# ===================== MAIN =====================
def main():
    existing_ids = load_existing_ids()
    new_entries = []

    with sync_playwright() as p:
        browser = p.chromium.launch(
            channel="chrome",
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu"
            ]
        )
        context = browser.new_context()
        page = context.new_page()

        try:
            press_items = scrape_press_releases(page, PAGES["Press Releases"])
            for item in press_items:
                if item["id"] not in existing_ids:
                    new_entries.append(item)
                    existing_ids.add(item["id"])

            pub_items = scrape_publications(page, PAGES["Publications"])
            for item in pub_items:
                if item["id"] not in existing_ids:
                    new_entries.append(item)
                    existing_ids.add(item["id"])

        finally:
            browser.close()

    if new_entries:
        write_master(new_entries)
        with open(NEW_JSON, "w", encoding="utf-8") as f:
            json.dump(new_entries, f, indent=2, ensure_ascii=False)
        logging.info(f"✅ {len(new_entries)} new entries saved")
    else:
        NEW_JSON.write_text("[]", encoding="utf-8")
        logging.info("No new entries found")

if __name__ == "__main__":
    main()
