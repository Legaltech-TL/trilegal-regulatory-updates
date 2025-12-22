#!/usr/bin/env python3
"""
SEBI Multi-Section Scraper (FULL-PROOF)

✔ GitHub Actions safe
✔ Playwright hardened
✔ Timeout-tolerant
✔ Backward-compatible with old CSVs
✔ Never crashes on SEBI slow pages
✔ Year-only date normalization
✔ Structured logging

made by BHANU TAK
"""

# ===================== ENV HARDENING =====================
import os
os.environ.setdefault("PLAYWRIGHT_BROWSERS_PATH", "0")

# ===================== IMPORTS =====================
from playwright.sync_api import sync_playwright
from urllib.parse import urljoin, urlparse
from pathlib import Path
import csv
import hashlib
import re
import datetime
import json
import time
import logging

# ===================== CONFIG =====================
NUM_ENTRIES = 10
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

MASTER_CSV = DATA_DIR / "sebi_master.csv"
NEW_JSON   = DATA_DIR / "sebi_new_entries.json"

DETAIL_PAGE_TIMEOUT = 20000  # 20s (SEBI-safe)
DETAIL_PAGE_DELAY = 0.7

SECTIONS = {
    "https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListing=yes&sid=1&ssid=1&smid=0": "Act",
    "https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListing=yes&sid=1&ssid=2&smid=0": "Rule",
    "https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListing=yes&sid=1&ssid=3&smid=0": "Regulation",
    "https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListing=yes&sid=1&ssid=4&smid=0": "General_Order",
    "https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListing=yes&sid=1&ssid=5&smid=0": "Guideline",
    "https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListing=yes&sid=1&ssid=6&smid=0": "Master_Circular",
    "https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListing=yes&sid=1&ssid=7&smid=0": "Circular",
}

HEADERS = [
    "id", "date", "title", "link", "pdf_link",
    "pdf_filename", "pdf_downloaded",
    "created_at", "source_commit", "category", "error"
]

# ===================== LOGGING =====================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("SEBI-SCRAPER")

# ===================== HELPERS =====================
def sha_id(*parts):
    return hashlib.sha1("|".join(parts).encode()).hexdigest()

def normalize_link(url):
    if not url:
        return ""
    p = urlparse(url)
    return f"{p.scheme or 'https'}://{p.netloc}{p.path.rstrip('/')}"

def safe_filename(text):
    text = re.sub(r'[\/\\:*?"<>|]+', "_", text or "document")
    text = re.sub(r"\s+", " ", text).strip()[:150]
    if not text.lower().endswith(".pdf"):
        text += ".pdf"
    return text

def normalize_date(date_str: str) -> str:
    """
    If date is only a year (e.g. '2025'), convert to '01-01-2025'
    Otherwise return as-is.
    """
    if not date_str:
        return ""

    d = date_str.strip()

    if re.fullmatch(r"\d{4}", d):
        return f"01-01-{d}"

    return d

def load_master():
    if not MASTER_CSV.exists():
        return []
    with open(MASTER_CSV, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def write_master(rows):
    with open(MASTER_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=HEADERS, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        writer.writerows(rows)

# ===================== EXTRACTION =====================
def extract_listing(page, base_url):
    items = []
    for tr in page.query_selector_all("table tr"):
        tds = tr.query_selector_all("td")
        a = tr.query_selector("a")
        if len(tds) >= 2 and a:
            items.append({
                "date": normalize_date(tds[0].inner_text()),
                "title": a.inner_text().strip(),
                "link": urljoin(base_url, a.get_attribute("href") or "")
            })
    return items

def find_pdf(page):
    for sel in ["a[href*='.pdf']", "iframe[src*='.pdf']", "embed[src*='.pdf']"]:
        el = page.query_selector(sel)
        if el:
            for attr in ("href", "src"):
                v = el.get_attribute(attr)
                if v and ".pdf" in v.lower():
                    return urljoin(page.url, v)
    return ""

# ===================== MAIN =====================
def main():
    logger.info("Starting SEBI multi-section scraper")

    github_sha = os.getenv("GITHUB_SHA", "")
    master = load_master()
    logger.info("Loaded %d existing records", len(master))

    existing = set()
    for r in master:
        title = (r.get("title") or "").lower().strip()
        link = normalize_link(r.get("link"))
        if title and link:
            existing.add((title, link))

    new_entries = []

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
            ],
        )
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120 Safari/537.36"
        )

        for list_url, category in SECTIONS.items():
            logger.info("Scraping section: %s", category)

            page = context.new_page()
            try:
                page.goto(list_url, wait_until="domcontentloaded", timeout=30000)
            except Exception as ex:
                logger.warning("Failed to load list page [%s]: %s", category, ex)
                page.close()
                continue

            rows = extract_listing(page, list_url)[:NUM_ENTRIES]
            page.close()

            for e in rows:
                key = (e["title"].lower(), normalize_link(e["link"]))
                if key in existing:
                    continue

                logger.info("New entry found: %s", e["title"][:80])

                pdf_link = ""
                error_msg = ""

                detail = context.new_page()
                try:
                    detail.goto(
                        e["link"],
                        wait_until="domcontentloaded",
                        timeout=DETAIL_PAGE_TIMEOUT
                    )
                    pdf_link = find_pdf(detail)
                except Exception as ex:
                    error_msg = f"detail_timeout: {str(ex)[:160]}"
                    logger.warning("Detail page failed: %s", e["link"])
                finally:
                    detail.close()
                    time.sleep(DETAIL_PAGE_DELAY)

                row = {
                    "id": sha_id(e["date"], e["title"], e["link"]),
                    "date": e["date"],
                    "title": e["title"],
                    "link": e["link"],
                    "pdf_link": pdf_link,
                    "pdf_filename": f"{category}_{safe_filename(e['title'])}",
                    "pdf_downloaded": "no",
                    "created_at": datetime.datetime.utcnow().isoformat() + "Z",
                    "source_commit": github_sha,
                    "category": category,
                    "error": error_msg,
                }

                master.append(row)
                new_entries.append(row)
                existing.add(key)

            logger.info("Completed section: %s", category)

        browser.close()

    write_master(master)

    with open(NEW_JSON, "w", encoding="utf-8") as f:
        json.dump(new_entries, f, indent=2, ensure_ascii=False)

    logger.info("SEBI scrape completed | New entries: %d", len(new_entries))

# ===================== ENTRY =====================
if __name__ == "__main__":
    main()
