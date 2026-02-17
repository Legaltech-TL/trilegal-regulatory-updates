#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NPCI Press Releases + Media Coverage Scraper (FINAL)

✔ React SPA safe
✔ Press Releases → PDF
✔ Media Coverage → PDF or WEBP
✔ Network-based capture (robust)
✔ CSV always created
✔ JSON only new entries
✔ Single merged asset_link column
"""

import asyncio
import csv
import json
import hashlib
import logging
from pathlib import Path
from datetime import datetime
from urllib.parse import urlparse

from playwright.async_api import async_playwright

# ---------------- CONFIG ----------------
URL = "https://www.npci.org.in/media/press-release"
TOP_N = 10

ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "npci"
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
    return Path(urlparse(url).path).name

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
                "asset_link",
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
                "asset_link",
                "filename",
                "scraped_at"
            ]
        )
        writer.writerows(rows)

# ---------------- ROW SCRAPER ----------------
async def scrape_row(page, row, section_key):
    title_el = await row.query_selector("div.circulars-cell p")
    if not title_el:
        return None

    title = (await title_el.inner_text()).strip()
    log.info(f"[{section_key}] {title}")

    button = await row.query_selector("button[aria-label^='download pdf']")
    if not button:
        log.warning("Download button not found")
        return None

    pdf_link = None
    image_link = None

    def capture_asset(response):
        nonlocal pdf_link, image_link
        url = response.url.lower()
        if "/uploads/" in url:
            if url.endswith(".pdf"):
                pdf_link = response.url
            elif url.endswith(".webp"):
                image_link = response.url

    page.on("response", capture_asset)

    try:
        await button.click()
        await page.wait_for_timeout(3000)
    finally:
        page.remove_listener("response", capture_asset)

    if not pdf_link and not image_link:
        log.warning("No PDF or image asset captured")
        return None

    final_url = pdf_link or image_link

    return {
        "id": make_id(title, final_url),
        "section": section_key,
        "title": title,
        "asset_link": final_url,
        "filename": safe_filename(final_url),
        "scraped_at": datetime.utcnow().isoformat()
    }

# ---------------- MAIN SCRAPER ----------------
async def scrape():
    collected = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"]
        )
        context = await browser.new_context(accept_downloads=True)
        page = await context.new_page()

        log.info("Opening NPCI page")
        await page.goto(URL, wait_until="domcontentloaded", timeout=60000)

        await page.wait_for_selector(
            "li.circulars-cell-container div.circulars-cell p",
            timeout=30000
        )

        # ---------- PRESS RELEASES (2026 DEFAULT) ----------
        log.info("Attempting Press Releases scrape (2026)")
        rows = await page.query_selector_all("li.circulars-cell-container")
        log.info(f"Press Releases 2026: {len(rows)} rows found")

        for row in rows[:TOP_N]:
            entry = await scrape_row(page, row, "press_release")
            if entry:
                collected.append(entry)

        # ---------- PRESS RELEASES (2025) ----------
        log.info("Switching Press Releases year to 2025")

        try:
            await page.click("div.press-year-dropdown button", timeout=5000)
            await page.wait_for_timeout(500)

            await page.evaluate("""
                () => {
                    const buttons = Array.from(
                        document.querySelectorAll('ul.dropdown-menu button')
                    );
                    const btn = buttons.find(b => b.textContent.trim() === '2025');
                    if (btn) btn.click();
                }
            """)

            await page.wait_for_timeout(2000)
            await page.wait_for_selector(
                "li.circulars-cell-container div.circulars-cell p",
                timeout=30000
            )

            rows = await page.query_selector_all("li.circulars-cell-container")
            log.info(f"Press Releases 2025: {len(rows)} rows found")

            for row in rows[:TOP_N]:
                entry = await scrape_row(page, row, "press_release")
                if entry:
                    collected.append(entry)

        except Exception as e:
            log.warning(f"Press Releases 2025 failed: {e}")

        # ---------- MEDIA COVERAGE ----------
        log.info("Switching to Media Coverage tab")
        try:
            await page.click("text=Media Coverage")
            await page.wait_for_selector(
                "li.circulars-cell-container div.circulars-cell p",
                timeout=30000
            )

            rows = await page.query_selector_all("li.circulars-cell-container")
            log.info(f"Media Coverage: {len(rows)} rows found")

            for row in rows[:TOP_N]:
                entry = await scrape_row(page, row, "media_coverage")
                if entry:
                    collected.append(entry)

        except Exception:
            log.warning("Media Coverage tab not available")

        await browser.close()
        log.info(f"Total entries collected: {len(collected)}")

    return collected

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

