#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NPCI Press Releases + Media Coverage Scraper
✔ Outputs stored in data/ directory
✔ Handles PDF (application/pdf)
✔ Handles Media Coverage images (image/webp)
✔ Top 10 per section
✔ CSV + JSON
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

SECTIONS = {
    "Press Releases": "press_release",
    "Media Coverage": "media_coverage"
}

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

# ---------------- SCRAPER ----------------
async def scrape():
    collected = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()

        log.info("Opening NPCI page")
        await page.goto(URL, wait_until="networkidle")

        for tab_text, section_key in SECTIONS.items():

            if section_key == "media_coverage":
                log.info("Switching to Media Coverage tab")
                await page.click("text=Media Coverage")
                await page.wait_for_timeout(2000)
            else:
                log.info("Using default Press Releases view")

            rows = page.locator("ul.press-release-body li.circulars-cell-container")
            total = await rows.count()
            log.info(f"{tab_text}: {total} rows found")

            for i in range(min(total, TOP_N)):
                row = rows.nth(i)

                title = (await row.locator("div.circulars-cell-body p").inner_text()).strip()
                log.info(f"[{tab_text}] Row {i+1}: {title}")

                buttons = row.locator("div.circulars-cell-buttons button")
                if await buttons.count() == 0:
                    continue

                captured_url = None
                captured_type = None

                try:
                    async with page.expect_response(
                        lambda r: (
                            "application/pdf" in r.headers.get("content-type", "")
                            or "image/webp" in r.headers.get("content-type", "")
                        ),
                        timeout=8000
                    ) as resp_info:
                        await buttons.first.click(force=True)

                    response = await resp_info.value
                    captured_url = response.url
                    captured_type = response.headers.get("content-type", "")

                except TimeoutError:
                    log.warning("No PDF or WEBP detected")
                    continue

                entry = {
                    "id": make_id(title, captured_url),
                    "section": section_key,
                    "title": title,
                    "pdf_link": None,
                    "media_image_link": None,
                    "filename": safe_filename(captured_url),
                    "scraped_at": datetime.utcnow().isoformat()
                }

                if "application/pdf" in captured_type:
                    entry["pdf_link"] = captured_url
                    log.info(f"PDF captured: {captured_url}")

                elif "image/webp" in captured_type:
                    entry["media_image_link"] = captured_url
                    log.info(f"WEBP image captured: {captured_url}")

                collected.append(entry)

        await browser.close()
        log.info(f"Total files collected: {len(collected)}")

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
