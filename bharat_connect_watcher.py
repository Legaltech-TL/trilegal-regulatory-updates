#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Bharat Connect Circulars Watcher
FINAL – SSR + Bot-aware + Non-idle Network Site
"""

import asyncio
import csv
import json
import hashlib
from pathlib import Path
from datetime import datetime, timezone
from urllib.parse import urlparse, unquote

from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

# ================= CONFIG =================

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

MASTER_CSV = DATA_DIR / "bharat_connect_master.csv"
NEW_JSON = DATA_DIR / "bharat_connect_new_entries.json"

SOURCE_URL = "https://www.bharat-connect.com/circulars/"
TOP_N = 10

REAL_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

# =========================================


def sha1(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()


def extract_filename(url: str) -> str:
    return unquote(Path(urlparse(url).path).name)


def load_existing_ids():
    if not MASTER_CSV.exists():
        return set()
    with open(MASTER_CSV, newline="", encoding="utf-8") as f:
        return {row["id"] for row in csv.DictReader(f)}


async def fetch_html():
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,  # REQUIRED
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
            ],
        )

        context = await browser.new_context(
            user_agent=REAL_UA,
            viewport={"width": 1280, "height": 800},
            locale="en-IN",
            timezone_id="Asia/Kolkata",
        )

        page = await context.new_page()

        # ✅ CORRECT LOAD STRATEGY
        await page.goto(SOURCE_URL, wait_until="domcontentloaded", timeout=60000)
        await page.wait_for_timeout(5000)  # allow JS + SSR hydration

        html = await page.content()

        await browser.close()
        return html


def parse_items(html):
    soup = BeautifulSoup(html, "lxml")

    items = soup.select("li.js-listItem")
    print(f"[INFO] Raw DOM items found: {len(items)}")

    results = []

    for li in items[:TOP_N]:
        title_el = li.select_one("h2.circulars__listItemTitle")
        date_el = li.select_one("p.circulars__listItemDate")
        pdf_el = li.select_one("a[href$='.pdf']")

        if not title_el or not pdf_el:
            continue

        pdf_link = pdf_el.get("href")

        results.append({
            "id": sha1(pdf_link),
            "source": "Bharat Connect",
            "category": "Circulars",
            "title": title_el.get_text(strip=True),
            "date": date_el.get_text(strip=True) if date_el else None,
            "pdf_link": pdf_link,
            "pdf_filename": extract_filename(pdf_link),
            "source_page": SOURCE_URL,
            "scraped_at": datetime.now(timezone.utc).isoformat(),
        })

    return results, len(items)


def append_to_csv(rows):
    exists = MASTER_CSV.exists()
    with open(MASTER_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        if not exists:
            writer.writeheader()
        writer.writerows(rows)


def main():
    print("[INFO] Starting Bharat Connect circulars watcher (FINAL)")

    existing_ids = load_existing_ids()
    html = asyncio.run(fetch_html())

    entries, total = parse_items(html)

    new_entries = []
    for row in entries:
        if row["id"] not in existing_ids:
            new_entries.append(row)
            existing_ids.add(row["id"])

    print(
        f"[INFO] Bharat Connect: total available = {total}, "
        f"checked = {len(entries)}, new = {len(new_entries)}"
    )

    if new_entries:
        append_to_csv(new_entries)
        with open(NEW_JSON, "w", encoding="utf-8") as f:
            json.dump(new_entries, f, ensure_ascii=False, indent=2)
    else:
        print("[INFO] No new entries found")

    print("[INFO] Bharat Connect watcher finished")


if __name__ == "__main__":
    main()
