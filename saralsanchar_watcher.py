import csv
import json
import logging
import os
import re
from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse, parse_qs

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

# ============================================================
# CONFIG
# ============================================================

BASE_URL = "https://saralsanchar.gov.in/circulars_order.php"
BASE_DOMAIN = "https://saralsanchar.gov.in"
XHR_ENDPOINT = "/common/get_circular_list.php"

LICENSES = ["UL", "UL_VNO", "WPC", "SACFA", "WANI", "M2M"]

LICENSE_MAP = {
    "UL": "Unified License",
    "UL_VNO": "Unified License VNO",
    "WPC": "Wireless Planning & Coordination (WPC)",
    "SACFA": "Standing Advisory Committee on Frequency Allocation (SACFA)",
    "WANI": "Wi-Fi Access Network Interface (WANI)",
    "M2M": "Machine-to-Machine (M2M)",
}

DATA_DIR = "data"
MASTER_CSV = os.path.join(DATA_DIR, "saralsanchar_master.csv")
NEW_JSON = os.path.join(DATA_DIR, "saralsanchar_new_entries.json")

# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

# ============================================================
# UTILITIES
# ============================================================

def ensure_dirs():
    os.makedirs(DATA_DIR, exist_ok=True)

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def slugify(text, max_words=8, max_chars=80):
    text = text.lower()
    text = re.sub(r"[^a-z0-9\s]", "", text)
    words = text.split()[:max_words]
    slug = "-".join(words)
    return slug[:max_chars].rstrip("-")

def generate_pdf_filename(license_code, title, doc_id):
    license_full = LICENSE_MAP.get(license_code, license_code)
    return (
        f"saralsanchar_"
        f"{slugify(license_full)}_"
        f"{slugify(title)}_"
        f"{doc_id}.pdf"
    )

def load_existing_ids():
    if not os.path.exists(MASTER_CSV):
        return set()
    with open(MASTER_CSV, newline="", encoding="utf-8") as f:
        return {row["id"] for row in csv.DictReader(f)}

# ============================================================
# CORE SCRAPER (XHR via page.evaluate)
# ============================================================

def fetch_html_via_browser(page, license_code):
    """
    Executes fetch() INSIDE browser JS context.
    This is the key to bypass CI blocking.
    """
    logging.info("Fetching backend HTML for license: %s", license_code)

    html = page.evaluate(
        """
        async ({endpoint, license}) => {
            const resp = await fetch(endpoint, {
                method: "POST",
                headers: {
                    "Content-Type": "application/x-www-form-urlencoded"
                },
                body: "circular_type=" + encodeURIComponent(license)
            });
            return await resp.text();
        }
        """,
        {"endpoint": XHR_ENDPOINT, "license": license_code}
    )

    return html

def parse_html(html, license_code):
    soup = BeautifulSoup(html, "html.parser")
    rows = soup.select("table tbody tr")

    records = []

    for row in rows:
        cols = row.find_all("td")
        if len(cols) < 4:
            continue

        date = cols[1].get_text(strip=True)
        title = cols[2].get_text(strip=True)

        link = cols[3].find("a", href=True)
        if not link:
            continue

        pdf_href = link["href"].strip()
        pdf_link = urljoin(BASE_DOMAIN, pdf_href)

        parsed = urlparse(pdf_href)
        f_param = parse_qs(parsed.query).get("f", [""])[0]
        if not f_param:
            continue

        record_id = f"{license_code}_{f_param}"

        records.append({
            "id": record_id,
            "license": LICENSE_MAP.get(license_code, license_code),
            "date": date,
            "title": title,
            "pdf_link": pdf_link,
            "pdf_filename": generate_pdf_filename(
                license_code, title, f_param
            ),
            "source_page": BASE_URL,
            "scraped_at": now_iso(),
        })

    logging.info("Parsed %d rows for %s", len(records), license_code)
    return records

# ============================================================
# SAVE
# ============================================================

def append_to_master(rows):
    exists = os.path.exists(MASTER_CSV)

    with open(MASTER_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "id",
                "license",
                "date",
                "title",
                "pdf_link",
                "pdf_filename",
                "source_page",
                "scraped_at",
            ],
        )

        if not exists:
            writer.writeheader()

        writer.writerows(rows)

def write_new_entries(rows):
    with open(NEW_JSON, "w", encoding="utf-8") as f:
        json.dump(rows, f, indent=2, ensure_ascii=False)

# ============================================================
# MAIN
# ============================================================

def main():
    ensure_dirs()

    existing_ids = load_existing_ids()
    logging.info("Loaded %d existing Saral Sanchar records", len(existing_ids))

    all_scraped = []

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled"]
        )
        context = browser.new_context()
        page = context.new_page()

        logging.info("Opening Saral Sanchar page")
        page.goto(BASE_URL, wait_until="domcontentloaded", timeout=60000)

        for license_code in LICENSES:
            html = fetch_html_via_browser(page, license_code)
            records = parse_html(html, license_code)
            all_scraped.extend(records)

        browser.close()

    new_items = [r for r in all_scraped if r["id"] not in existing_ids]

    if not new_items:
        logging.info("No new Saral Sanchar circulars found")
        write_new_entries([])
        return

    logging.info("Detected %d NEW Saral Sanchar circulars", len(new_items))

    append_to_master(new_items)
    write_new_entries(new_items)

    logging.info("Saral Sanchar CSV and JSON updated successfully")

if __name__ == "__main__":
    main()
