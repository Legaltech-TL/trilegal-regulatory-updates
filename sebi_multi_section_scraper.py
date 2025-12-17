#!/usr/bin/env python3
"""
sebi_multi_section_scraper.py
Improved for GitHub Actions + Playwright stability
made by BHANU TAK
"""

import os
# ---------- CRITICAL FOR GITHUB ACTIONS ----------
os.environ.setdefault("PLAYWRIGHT_BROWSERS_PATH", "0")

from playwright.sync_api import sync_playwright
from urllib.parse import urljoin, urlparse, parse_qs, unquote
from pathlib import Path
import csv, hashlib, re, datetime, json, time

# ----------------- CONFIG -----------------
NUM_ENTRIES = 10
MASTER_CSV = "data/sebi_master.csv"
NEW_JSON   = "data/sebi_new_entries.json"
CSV_DELIM = ","

Path("data").mkdir(parents=True, exist_ok=True)

DETAIL_PAGE_DELAY = 0.8
DETAIL_PAGE_RETRIES = 2
RETRY_BACKOFF_BASE = 1.0

SECTIONS = {
    "https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListing=yes&sid=1&ssid=1&smid=0": "Act",
    "https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListing=yes&sid=1&ssid=2&smid=0": "Rule",
    "https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListing=yes&sid=1&ssid=3&smid=0": "Regulation",
    "https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListing=yes&sid=1&ssid=4&smid=0": "General_Order",
    "https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListing=yes&sid=1&ssid=5&smid=0": "Guideline",
    "https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListing=yes&sid=1&ssid=6&smid=0": "Master_Circular",
    "https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListing=yes&sid=1&ssid=96&smid=0": "Advisory",
    "https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListing=yes&sid=1&ssid=7&smid=0": "Circular",
    "https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListing=yes&sid=1&ssid=82&smid=0": "Gazette",
    "https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListing=yes&sid=1&ssid=85&smid=0": "Guidance_note",
}

# ----------------- HELPERS -----------------
def normalize_date(s):
    if s and re.fullmatch(r"\d{4}", s.strip()):
        return f"{s}-01-01"
    return s or ""

def safe_filename(s, fallback="document.pdf"):
    s = (s or fallback).replace("\n", " ").strip()
    s = re.sub(r'[\/\\:*?"<>|]+', "_", s)
    s = re.sub(r"\s+", " ", s)[:150]
    if not s.lower().endswith(".pdf"):
        s += ".pdf"
    return s.strip("_ ")

def make_id(date, title, link):
    return hashlib.sha1(f"{date}|{title}|{link}".encode()).hexdigest()

def normalize_link(link):
    if not link:
        return ""
    p = urlparse(link)
    return f"{p.scheme or 'https'}://{p.netloc}{p.path.rstrip('/')}"

# ----------------- PDF EXTRACTION -----------------
def find_pdf_url_on_page(page):
    for sel in ["a[href*='.pdf']", "iframe[src*='.pdf']", "embed[src*='.pdf']"]:
        el = page.query_selector(sel)
        if el:
            for attr in ("href", "src"):
                v = el.get_attribute(attr)
                if v and ".pdf" in v.lower():
                    return urljoin(page.url, v)
    return None

# ----------------- LISTING EXTRACTION -----------------
def extract_entries_from_listing(page, base_url):
    rows = []
    for tr in page.query_selector_all("table tr"):
        tds = tr.query_selector_all("td")
        if len(tds) < 2:
            continue
        a = tr.query_selector("a")
        if not a:
            continue
        rows.append({
            "date": tds[0].inner_text().strip(),
            "title": a.inner_text().strip(),
            "link": urljoin(base_url, a.get_attribute("href") or "")
        })
    return rows

# ----------------- I/O -----------------
def load_master():
    if not os.path.exists(MASTER_CSV):
        return []
    with open(MASTER_CSV, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def write_csv(path, rows):
    headers = rows[0].keys() if rows else []
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        w.writerows(rows)

# ----------------- MAIN -----------------
def main():
    github_sha = os.getenv("GITHUB_SHA", "")
    master = load_master()

    existing = {(r["title"].lower(), normalize_link(r["link"])) for r in master}

    new_rows = []

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

        for url, category in SECTIONS.items():
            page = context.new_page()
            page.goto(url, wait_until="domcontentloaded", timeout=45000)
            page.wait_for_timeout(600)

            entries = extract_entries_from_listing(page, url)[:NUM_ENTRIES]
            page.close()

            for e in entries:
                key = (e["title"].lower(), normalize_link(e["link"]))
                if key in existing:
                    continue

                detail = context.new_page()
                pdf = ""
                try:
                    detail.goto(e["link"], wait_until="domcontentloaded", timeout=45000)
                    pdf = find_pdf_url_on_page(detail) or ""
                finally:
                    detail.close()
                    time.sleep(DETAIL_PAGE_DELAY)

                row = {
                    "id": make_id(e["date"], e["title"], e["link"]),
                    "date": normalize_date(e["date"]),
                    "title": e["title"],
                    "link": e["link"],
                    "pdf_link": pdf,
                    "pdf_filename": f"{category}_{safe_filename(e['title'])}",
                    "pdf_downloaded": "no",
                    "created_at": datetime.datetime.utcnow().isoformat() + "Z",
                    "source_commit": github_sha,
                    "category": category,
                    "error": "",
                }

                master.append(row)
                new_rows.append(row)
                existing.add(key)

        browser.close()

    write_csv(MASTER_CSV, master)

    with open(NEW_JSON, "w", encoding="utf-8") as f:
        json.dump(new_rows, f, indent=2, ensure_ascii=False)

    print(f"Added {len(new_rows)} new SEBI entries.")

if __name__ == "__main__":
    main()
