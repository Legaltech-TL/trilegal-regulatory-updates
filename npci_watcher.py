#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import json
import hashlib
from pathlib import Path
from datetime import datetime
from urllib.parse import urlparse, unquote

from playwright.sync_api import sync_playwright

# ---------- CONFIG ----------
URL = "https://www.npci.org.in/media/press-release"
TOP_N = 10

ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
DATA_DIR.mkdir(exist_ok=True)

MASTER_CSV = DATA_DIR / "npci_master.csv"
NEW_JSON = DATA_DIR / "npci_new_entries.json"

CSV_FIELDS = [
    "id",
    "category",
    "title",
    "date",
    "pdf_link",
    "pdf_filename",
    "created_at"
]

# ---------- HELPERS ----------
def make_id(title, date, category):
    return hashlib.sha1(f"{title}|{date}|{category}".encode("utf-8")).hexdigest()


def pdf_filename_from_url(url):
    if not url:
        return ""
    return unquote(Path(urlparse(url).path).name)


def load_existing_ids():
    if not MASTER_CSV.exists():
        return set()
    with open(MASTER_CSV, newline="", encoding="utf-8") as f:
        return {r["id"] for r in csv.DictReader(f)}


def append_to_master(rows):
    write_header = not MASTER_CSV.exists()
    with open(MASTER_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        if write_header:
            writer.writeheader()
        writer.writerows(rows)


# ---------- SCRAPER ----------
def scrape_visible_items(page, category):
    """
    Scrape top 10 NPCI items and extract PDF via 'view pdf' button popup.
    """
    page.wait_for_load_state("networkidle")
    page.wait_for_timeout(800)

    cells = page.locator("div.circulars-cell").filter(has_text="")
    count = min(cells.count(), TOP_N)

    results = []

    for i in range(count):
        cell = cells.nth(i)

        text = cell.locator("p").inner_text().strip()

        date = ""
        title = text
        m = text.match if False else None  # placeholder to keep logic clear

        # Parse "26-03 Title..."
        if text[:5].count("-") == 1:
            date = text[:5]
            title = text[6:].strip()

        pdf_link = ""
        pdf_filename = ""

        # ---- Capture PDF popup ----
        try:
            view_btn = cell.locator("button[aria-label^='view pdf']").first
            with page.expect_popup(timeout=5000) as popup_info:
                view_btn.click()
            popup = popup_info.value
            popup.wait_for_load_state("domcontentloaded")
            pdf_link = popup.url
            pdf_filename = pdf_filename_from_url(pdf_link)
            popup.close()
        except Exception:
            pass

        entry_id = make_id(title, date, category)

        results.append({
            "id": entry_id,
            "category": category,
            "title": title,
            "date": date,
            "pdf_link": pdf_link,
            "pdf_filename": pdf_filename,
            "created_at": datetime.utcnow().isoformat()
        })

    return results


def scrape_npci():
    data = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(URL, timeout=60000)

        page.wait_for_load_state("networkidle")

        # ---- PRESS RELEASE ----
        page.click("text=Press Release", timeout=5000)
        page.wait_for_timeout(1000)
        data.extend(scrape_visible_items(page, "press_release"))

        # ---- MEDIA COVERAGE ----
        page.click("text=Media Coverage", timeout=5000)
        page.wait_for_timeout(1000)
        data.extend(scrape_visible_items(page, "media_coverage"))

        browser.close()

    return data


# ---------- MAIN ----------
def main():
    print("[INFO] Scraping NPCI Press Release + Media Coverage")

    scraped = scrape_npci()
    print(f"[DEBUG] Scraped items: {len(scraped)}")

    if not scraped:
        print("[WARN] No items scraped")
        NEW_JSON.write_text("[]", encoding="utf-8")
        return

    master_exists = MASTER_CSV.exists()
    existing_ids = load_existing_ids()

    if not master_exists:
        append_to_master(scraped)
        with open(NEW_JSON, "w", encoding="utf-8") as f:
            json.dump(scraped, f, indent=2, ensure_ascii=False)
        print(f"[OK] {len(scraped)} entries saved (initial load)")
        return

    new_entries = [e for e in scraped if e["id"] not in existing_ids]

    if not new_entries:
        print("[INFO] No new entries found")
        NEW_JSON.write_text("[]", encoding="utf-8")
        return

    append_to_master(new_entries)
    with open(NEW_JSON, "w", encoding="utf-8") as f:
        json.dump(new_entries, f, indent=2, ensure_ascii=False)

    print(f"[OK] {len(new_entries)} new entries saved")


if __name__ == "__main__":
    main()
