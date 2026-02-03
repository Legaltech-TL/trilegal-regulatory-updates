#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import json
import logging
import re
import os
from datetime import datetime, timezone
from urllib.parse import urljoin

from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup

# ================= CONFIG =================

BASE_URL = "https://www.mtcte.tec.gov.in/"
DATA_DIR = "data"

MASTER_CSV = os.path.join(DATA_DIR, "mtcte_master.csv")
NEW_JSON = os.path.join(DATA_DIR, "mtcte_new_entries.json")

# ================= LOGGING =================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

# ================= UTIL =================

def ensure_dirs():
    os.makedirs(DATA_DIR, exist_ok=True)

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def slugify_title(title, max_words=8, max_chars=80):
    title = title.lower()
    title = re.sub(r"[^a-z0-9\s]", "", title)
    words = title.split()[:max_words]
    slug = "-".join(words)
    return slug[:max_chars].rstrip("-")

def generate_pdf_filename(title):
    slug = slugify_title(title)
    return f"{slug}.pdf"

# ================= LOAD EXISTING =================

def load_existing_links():
    if not os.path.exists(MASTER_CSV):
        return set()

    with open(MASTER_CSV, newline="", encoding="utf-8") as f:
        return {row["pdf_link"] for row in csv.DictReader(f)}

# ================= SCRAPER =================

def fetch_updates():
    logging.info("Launching browser (Playwright)")
    items = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        logging.info("Opening MTCTE homepage")
        page.goto(BASE_URL, wait_until="networkidle", timeout=60000)

        page.wait_for_selector("body", timeout=30000)

        html = page.content()
        browser.close()

    soup = BeautifulSoup(html, "html.parser")

    collected = {}
    
    # -------- SOURCE 1: WHAT'S NEW MARQUEE --------
    marquee_links = soup.select("#marquee1 ul#myNewsList li a")

    for a in marquee_links:
        href = a.get("href", "").strip()
        title = a.get_text(strip=True)

        if not href or not title:
            continue

        pdf_link = urljoin(BASE_URL, href)

        collected[pdf_link] = {
            "title": title,
            "pdf_link": pdf_link,
            "source_section": "whats_new_marquee",
        }

    # -------- SOURCE 2: VISION HEAD (POLICY DOCS) --------
    vision_links = soup.select("h2.visionHead a")

    for a in vision_links:
        href = a.get("href", "").strip()
        title = a.get_text(" ", strip=True)

        if not href or not title:
            continue

        # Skip dashboards / internal navigation
        if href.lower().startswith(("monitoring", "voluntary", "cab", "#")):
            continue

        pdf_link = urljoin(BASE_URL, href)

        collected[pdf_link] = {
            "title": title,
            "pdf_link": pdf_link,
            "source_section": "policy_vision_head",
        }

    logging.info("Total unique links collected: %d", len(collected))

    for pdf_link, data in collected.items():
        items.append({
            "id": re.sub(r"\W+", "_", data["title"].lower())[:50],
            "title": data["title"],
            "pdf_link": data["pdf_link"],
            "pdf_filename": generate_pdf_filename(data["title"]),
            "source_page": BASE_URL,
            "source_section": data["source_section"],
            "scraped_at": now_iso(),
        })

    return items

# ================= SAVE =================

def append_to_master(rows):
    exists = os.path.exists(MASTER_CSV)

    with open(MASTER_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "id",
                "title",
                "pdf_link",
                "pdf_filename",
                "source_page",
                "source_section",
                "scraped_at",
            ],
        )

        if not exists:
            writer.writeheader()

        writer.writerows(rows)

def write_new_entries(rows):
    with open(NEW_JSON, "w", encoding="utf-8") as f:
        json.dump(rows, f, indent=2, ensure_ascii=False)

# ================= MAIN =================

def main():
    ensure_dirs()

    existing_links = load_existing_links()
    logging.info("Loaded %d existing records", len(existing_links))

    items = fetch_updates()

    new_items = [i for i in items if i["pdf_link"] not in existing_links]

    if not new_items:
        logging.info("No new MTCTE updates found")
        write_new_entries([])
        return

    logging.info("Detected %d NEW MTCTE updates", len(new_items))

    append_to_master(new_items)
    write_new_entries(new_items)

    logging.info("CSV and JSON updated successfully")

if __name__ == "__main__":
    main()
