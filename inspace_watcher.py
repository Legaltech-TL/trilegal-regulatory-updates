#!/usr/bin/env python3
"""
IN-SPACe Watcher – FINAL (Press Releases + Publications)

✔ Playwright (Chrome channel)
✔ Accordion handling for Publications
✔ CSV + JSON
✔ Change detection via hash
✔ pdf_filename extracted
✔ GitHub Actions safe

Author: Bhanu Tak
"""

# ===================== IMPORTS =====================
from playwright.sync_api import sync_playwright
from pathlib import Path
from datetime import datetime
import hashlib
import csv
import json
import logging
from urllib.parse import urlparse, parse_qs

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

MAX_PRESS_ITEMS = 10

# ===================== LOGGING =====================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

# ===================== HELPERS =====================
def make_id(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()

def extract_pdf_filename(url: str | None) -> str:
    if not url:
        return ""
    parsed = urlparse(url)

    # ServiceNow attachment
    if "sys_attachment.do" in parsed.path:
        qs = parse_qs(parsed.query)
        sys_id = qs.get("sys_id", [""])[0]
        return f"{sys_id}.pdf" if sys_id else ""

    # Normal file URL
    name = Path(parsed.path).name
    return name if name.lower().endswith(".pdf") else ""

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

    cards = page.locator(".releases-list .release-item")
    count = min(cards.count(), MAX_PRESS_ITEMS)

    items = []

    for i in range(count):
        card = cards.nth(i)

        title = card.locator("h3.release-title").inner_text().strip()
        date  = card.locator(".release-date").inner_text().strip()

        pdf_link = ""
        if card.locator("a:has-text('Download PDF')").count() > 0:
            href = card.locator("a:has-text('Download PDF')").get_attribute("href")
            if href:
                pdf_link = BASE_URL + href if href.startswith("/") else href

        pdf_filename = extract_pdf_filename(pdf_link)

        uid = make_id("press" + title + date)

        items.append({
            "id": uid,
            "section": "Press Releases",
            "category": "Press Release",
            "title": title,
            "date": date,
            "meta": "",
            "pdf_link": pdf_link,
            "pdf_filename": pdf_filename,
            "source_page": url,
            "scraped_at": datetime.utcnow().isoformat()
        })

    logging.info(f"Found {len(items)} items in Press Releases")
    return items

# ===================== PUBLICATIONS =====================
def scrape_publications(page, url):
    logging.info("Scraping Publications")

    page.goto(url, wait_until="networkidle", timeout=45000)
    page.wait_for_selector(".publications-container", timeout=30000)

    all_items = []

    categories = page.locator(".category-block")
    logging.info(f"Detected {categories.count()} publication categories")

    for i in range(categories.count()):
        category = categories.nth(i)
        header = category.locator(".category-header")
        category_title = header.locator("h4").inner_text().strip()

        header.click(force=True)
        page.wait_for_timeout(500)

        docs = category.locator("ul.doc-list li")

        for j in range(docs.count()):
            doc = docs.nth(j)

            link = doc.locator("a.doc-link")
            title = link.inner_text().strip()
            href = link.get_attribute("href")

            if href:
                href = BASE_URL + href if href.startswith("/") else href

            meta = ""
            if doc.locator("p.belowlinetext").count() > 0:
                meta = doc.locator("p.belowlinetext").inner_text().strip()

            pdf_filename = extract_pdf_filename(href)

            uid = make_id("publication" + category_title + title + (href or ""))

            all_items.append({
                "id": uid,
                "section": "Publications",
                "category": category_title,
                "title": title,
                "date": "",
                "meta": meta,
                "pdf_link": href,
                "pdf_filename": pdf_filename,
                "source_page": url,
                "scraped_at": datetime.utcnow().isoformat()
            })

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
            args=["--no-sandbox", "--disable-dev-shm-usage"]
        )
        page = browser.new_page()

        for item in scrape_press_releases(page, PAGES["Press Releases"]):
            if item["id"] not in existing_ids:
                new_entries.append(item)
                existing_ids.add(item["id"])

        for item in scrape_publications(page, PAGES["Publications"]):
            if item["id"] not in existing_ids:
                new_entries.append(item)
                existing_ids.add(item["id"])

        browser.close()

    if new_entries:
        write_master(new_entries)
        NEW_JSON.write_text(json.dumps(new_entries, indent=2, ensure_ascii=False), encoding="utf-8")
        logging.info(f"✅ {len(new_entries)} new entries saved")
    else:
        NEW_JSON.write_text("[]", encoding="utf-8")
        logging.info("No new entries found")

if __name__ == "__main__":
    main()
