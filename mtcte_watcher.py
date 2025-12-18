import csv
import json
import logging
import re
import os
from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse, parse_qs

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
    """
    Convert title into filesystem-safe slug
    """
    # Lowercase
    title = title.lower()

    # Remove non-alphanumeric (keep spaces)
    title = re.sub(r"[^a-z0-9\s]", "", title)

    # Collapse spaces
    words = title.split()

    # Limit words
    words = words[:max_words]

    slug = "-".join(words)

    # Safety trim by chars
    return slug[:max_chars].rstrip("-")

def generate_pdf_filename(item_id, title):
    slug = slugify_title(title)
    return f"{item_id}_{slug}.pdf"


def extract_filename(url):
    parsed = urlparse(url)
    qs = parse_qs(parsed.query)
    return qs.get("name", [""])[0]

# ================= LOAD EXISTING =================

def load_existing_ids():
    if not os.path.exists(MASTER_CSV):
        return set()

    with open(MASTER_CSV, newline="", encoding="utf-8") as f:
        return {row["id"] for row in csv.DictReader(f)}

# ================= SCRAPER =================

def fetch_whats_new():
    logging.info("Launching browser (Playwright)")
    items = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        logging.info("Opening MTCTE homepage")
        page.goto(BASE_URL, wait_until="networkidle", timeout=60000)

        # Wait explicitly for the marquee
        page.wait_for_selector("#marquee1", timeout=30000)

        html = page.content()
        browser.close()

    soup = BeautifulSoup(html, "html.parser")

    links = soup.select("#marquee1 ul#myNewsList li a")
    logging.info("Found %d items in What's New card", len(links))

    for a in links:
        item_id = a.get("id", "").strip()
        title = a.get_text(strip=True)
        href = a.get("href", "").strip()

        if not item_id or not href:
            continue

        pdf_link = urljoin(BASE_URL, href)

        items.append({
            "id": item_id,
            "title": title,
            "pdf_link": pdf_link,
            "pdf_filename": generate_pdf_filename(item_id, title),
            "source_page": BASE_URL,
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

    existing_ids = load_existing_ids()
    logging.info("Loaded %d existing records", len(existing_ids))

    items = fetch_whats_new()

    new_items = [i for i in items if i["id"] not in existing_ids]

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
