#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NPCI Press Releases + Media Coverage Scraper (CI-HARDENED)

✔ Works in GitHub Actions
✔ Handles NPCI geo / CDN DOM differences
✔ Press Releases optional (never crash)
✔ Media Coverage always scraped
✔ PDF + WEBP supported
✔ CSV + JSON in data/
"""

import asyncio
import csv
import json
import hashlib
import logging
from pathlib import Path
from datetime import datetime
from urllib.parse import urlparse

from playwright.async_api import async_playwright, TimeoutError

# ---------------- CONFIG ----------------
URL = "https://www.npci.org.in/media/press-release"
TOP_N = 10

ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
DATA_DIR.mkdir(exist_ok=True)

MASTER_CSV = DATA_DIR / "npci_master.csv"
NEW_JSON = DATA_DIR / "npci_new_entries.json"
LOG_FILE = DATA_DIR / "npci_scraper.log"

# ---------------- LOGGING ----------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger("NPCI")

# ---------------- HELPERS ----------------
def make_id(title: str, url: str) -> str:
    return hashlib.sha1(f"{title}|{url}".encode()).hexdigest()

def safe_filename(url: str) -> str:
    name = Path(urlparse(url).path).name
    return name if name else "file"

def is_pdf_response(response) -> bool:
    ct = response.headers.get("content-type", "").lower()
    cd = response.headers.get("content-disposition", "").lower()
    url = response.url.lower()
    return "pdf" in ct or ".pdf" in url or ".pdf" in cd

# ---------------- ROW SCRAPER ----------------
async def scrape_row(page, row, section_key):
    title_el = await row.query_selector("div.circulars-cell-body p")
    if not title_el:
        return None

    title = (await title_el.inner_text()).strip()
    log.info(f"[{section_key}] {title}")

    buttons = await row.query_selector_all("div.circulars-cell-buttons button")
    if not buttons:
        return None

    try:
        async with page.expect_response(
            lambda r: (
                is_pdf_response(r)
                or "image/webp" in r.headers.get("content-type", "").lower()
            ),
            timeout=8000
        ):
            await buttons[0].click()

        response = await page.wait_for_event("response", timeout=1000)
        url = response.url
        ctype = response.headers.get("content-type", "").lower()

    except TimeoutError:
        log.warning("No PDF / WEBP detected")
        return None

    entry = {
        "id": make_id(title, url),
        "section": section_key,
        "title": title,
        "pdf_link": None,
        "media_image_link": None,
        "filename": safe_filename(url),
        "scraped_at": datetime.utcnow().isoformat()
    }

    if is_pdf_response(response):
        entry["pdf_link"] = url
    elif "image/webp" in ctype:
        entry["media_image_link"] = url

    return entry

# ---------------- MAIN SCRAPER ----------------
async def scrape():
    collected = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"]
        )
        context = await browser.new_context()
        page = await context.new_page()

        log.info("Opening NPCI page")
        await page.goto(URL, wait_until="domcontentloaded", timeout=60000)

        # ---------- PRESS RELEASES (OPTIONAL) ----------
        log.info("Attempting Press Releases scrape")

        press_container = await page.query_selector("div.press-release-body")
        if press_container:
            rows = await press_container.query_selector_all("div.circulars-cell")
            log.info(f"Press Releases: {len(rows)} rows found")

            for row in rows[:TOP_N]:
                entry = await scrape_row(page, row, "press_release")
                if entry:
                    collected.append(entry)
        else:
            log.warning("Press Releases DOM not available (CI / geo restriction)")

        # ---------- MEDIA COVERAGE ----------
        log.info("Switching to Media Coverage tab")
        try:
            await page.click("text=Media Coverage")
            await page.wait_for_timeout(2000)
        except Exception:
            log.warning("Media Coverage tab click failed")

        media_container = await page.query_selector("ul.press-release-body")
        if media_container:
            rows = await media_container.query_selector_all(
                "li.circulars-cell-container"
            )
            log.info(f"Media Coverage: {len(rows)} rows found")

            for row in rows[:TOP_N]:
                entry = await scrape_row(page, row, "media_coverage")
                if entry:
                    collected.append(entry)
        else:
            log.error("Media Coverage DOM not found")

        await browser.close()
        log.info(f"Total entries collected: {len(collected)}")

    return collected

# ---------------- STORAGE ----------------
def ensure_master_csv():
    if MASTER_CSV.exists():
        return
    with open(MASTER_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "id",
                "section",
                "title",
                "pdf_link",
                "media_image_link",
                "filename",
                "scraped_at"
            ]
        )
        writer.writeheader()

def load_existing_ids():
    if not MASTER_CSV.exists():
        return set()
    with open(MASTER_CSV, newline="", encoding="utf-8") as f:
        return {row["id"] for row in csv.DictReader(f)}

def append_csv(rows):
    with open(MASTER_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "id",
                "section",
                "title",
                "pdf_link",
                "media_image_link",
                "filename",
                "scraped_at"
            ]
        )
        writer.writerows(rows)

# ---------------- ENTRYPOINT ----------------
def main():
    ensure_master_csv()

    data = asyncio.run(scrape())
    existing = load_existing_ids()
    new_entries = [d for d in data if d["id"] not in existing]

    NEW_JSON.write_text(json.dumps(new_entries, indent=2), encoding="utf-8")

    if new_entries:
        append_csv(new_entries)
        log.info(f"Added {len(new_entries)} new entries")
    else:
        log.info("No new entries found")

if __name__ == "__main__":
    main()
