#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NPCI Press Releases + Media Coverage Scraper (CI-SAFE)

✔ Works in GitHub Actions
✔ Correct selectors per tab
✔ Handles PDF (application/pdf, octet-stream)
✔ Handles Media Coverage WEBP
✔ No networkidle
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
    return (
        "pdf" in ct
        or url.endswith(".pdf")
        or ".pdf" in cd
    )

# ---------------- ROW SCRAPER ----------------
async def scrape_row(page, row, section_key):
    title_el = row.locator("div.circulars-cell-body p")
    if await title_el.count() == 0:
        return None

    title = (await title_el.inner_text()).strip()
    log.info(f"[{section_key}] {title}")

    buttons = row.locator("div.circulars-cell-buttons button")
    if await buttons.count() == 0:
        return None

    try:
        async with page.expect_response(
            lambda r: (
                is_pdf_response(r)
                or "image/webp" in r.headers.get("content-type", "").lower()
            ),
            timeout=8000
        ) as resp_info:
            await buttons.first.click(force=True)

        response = await resp_info.value
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
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu"
            ]
        )

        context = await browser.new_context()
        page = await context.new_page()

        log.info("Opening NPCI page")
        await page.goto(URL, wait_until="domcontentloaded", timeout=60000)

        # ---------- PRESS RELEASES ----------
        log.info("Scraping Press Releases")

        await page.wait_for_selector("div.press-release-body", timeout=60000)
        rows = page.locator("div.press-release-body div.circulars-cell")
        total = await rows.count()
        log.info(f"Press Releases: {total} rows found")

        for i in range(min(total, TOP_N)):
            entry = await scrape_row(page, rows.nth(i), "press_release")
            if entry:
                collected.append(entry)

        # ---------- MEDIA COVERAGE ----------
        log.info("Switching to Media Coverage tab")
        await page.click("text=Media Coverage")
        await page.wait_for_timeout(2000)

        await page.wait_for_selector("ul.press-release-body", timeout=60000)
        rows = page.locator("ul.press-release-body li.circulars-cell-container")
        total = await rows.count()
        log.info(f"Media Coverage: {total} rows found")

        for i in range(min(total, TOP_N)):
            entry = await scrape_row(page, rows.nth(i), "media_coverage")
            if entry:
                collected.append(entry)

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

    NEW_JSON.write_text(
        json.dumps(new_entries, indent=2),
        encoding="utf-8"
    )

    if new_entries:
        append_csv(new_entries)
        log.info(f"Added {len(new_entries)} new entries")
    else:
        log.info("No new entries found")

if __name__ == "__main__":
    main()
