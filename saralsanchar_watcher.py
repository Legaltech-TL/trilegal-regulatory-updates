import csv
import json
import logging
import os
import re
import time
from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse, parse_qs

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

# ============================================================
# CONFIG
# ============================================================

BASE_URL = "https://saralsanchar.gov.in/circulars_order.php"
BASE_DOMAIN = "https://saralsanchar.gov.in"

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
# PLAYWRIGHT SCRAPER
# ============================================================

def scrape_with_playwright():
    results = []

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
            logging.info("Processing license: %s", license_code)

            try:
                # Select license
                page.select_option("#circular_type", license_code)

                # Click Get List
                page.click("#Submit")

                # Wait for table rows to load
                page.wait_for_selector(
                    "table tbody tr",
                    timeout=30000
                )

                # Small buffer for DataTables render
                time.sleep(1.5)

                rows = page.query_selector_all("table tbody tr")
                logging.info("Found %d rows for %s", len(rows), license_code)

                for row in rows:
                    cols = row.query_selector_all("td")
                    if len(cols) < 4:
                        continue

                    date = cols[1].inner_text().strip()
                    title = cols[2].inner_text().strip()

                    link_el = cols[3].query_selector("a[href]")
                    if not link_el:
                        continue

                    pdf_href = link_el.get_attribute("href")
                    pdf_link = urljoin(BASE_DOMAIN, pdf_href)

                    parsed = urlparse(pdf_href)
                    f_param = parse_qs(parsed.query).get("f", [""])[0]
                    if not f_param:
                        continue

                    record_id = f"{license_code}_{f_param}"

                    results.append({
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

            except PWTimeout:
                logging.warning(
                    "Timeout while processing license %s — skipping",
                    license_code
                )
                continue

            # Defensive reset before next license
            time.sleep(1)

        browser.close()

    return results

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

    scraped = scrape_with_playwright()

    new_items = [i for i in scraped if i["id"] not in existing_ids]

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
