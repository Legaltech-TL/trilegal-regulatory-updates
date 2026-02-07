from playwright.sync_api import sync_playwright
from pathlib import Path
from datetime import datetime
import hashlib
import csv
import json
import re
import time


# ================= CONFIG =================

DATA_DIR = Path("data")
CSV_FILE = DATA_DIR / "dot_master.csv"
JSON_FILE = DATA_DIR / "dot_new.json"

SECTIONS = [
    ("ORDERS_AND_NOTICES", "https://www.dot.gov.in/documents/orders-and-notices?page=", True),
    ("REPORTS", "https://www.dot.gov.in/documents?page=", True),
    ("ACTS_AND_POLICIES", "https://www.dot.gov.in/documents/acts-and-policies?page=", True),
    ("PUBLICATIONS", "https://www.dot.gov.in/documents/publications?page=", True),
    ("PRESS_RELEASE", "https://www.dot.gov.in/documents/press-release?page=", True),
    ("GUIDELINES", "https://www.dot.gov.in/documents/guidelines?page=", True),
    ("GAZETTES_NOTIFICATIONS", "https://www.dot.gov.in/documents/gazettes-notifications?page=", False),
]

VIEWPORT = {"width": 1400, "height": 900}


# ================= HELPERS =================

def normalize_date(s):
    for fmt in ("%d.%m.%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(s, fmt).strftime("%m/%d/%Y")
        except:
            pass
    return s or ""


def extract_year(s):
    m = re.search(r"(20\d{2})", s or "")
    return int(m.group(1)) if m else None


def make_id(pdf_url):
    return hashlib.sha1(pdf_url.encode()).hexdigest()[:16]


def find_ctx(page, timeout=70000):
    """Find frame/page where announcement cards render"""
    start = time.time()

    while (time.time() - start) * 1000 < timeout:
        for ctx in [page] + page.frames:
            try:
                if ctx.query_selector("div.announcementbox"):
                    return ctx
            except:
                pass
        page.wait_for_timeout(1000)

    return None


# ================= SCRAPER =================

def scrape_section(page, category, base_url, year_stop):

    rows = []
    page_no = 1

    print(f"\n========== {category} ==========")

    while True:

        url = base_url + str(page_no)
        print("Opening:", url)

        page.goto(url, timeout=90000)
        page.wait_for_load_state("domcontentloaded")
        page.wait_for_load_state("networkidle")
        page.wait_for_timeout(3500)

        ctx = find_ctx(page)

        if not ctx:
            print("No content context — stop section")
            break

        cards = ctx.query_selector_all("div.announcementbox")
        print("Cards found:", len(cards))

        if not cards:
            break

        for c in cards:

            try:
                title = c.query_selector("p.mb-0").inner_text().strip()

                date_el = c.query_selector("small.ptype")
                date_raw = date_el.inner_text().strip() if date_el else ""

                pdf_el = c.query_selector("a.download-btn")
                pdf = pdf_el.get_attribute("href") if pdf_el else ""

            except:
                continue

            if not pdf:
                continue

            # ensure absolute URL
            if pdf.startswith("/"):
                pdf = "https://www.dot.gov.in" + pdf

            if year_stop and date_raw:
                y = extract_year(date_raw)
                if y and y < 2026:
                    print("🛑 Hit 2025 — stop section")
                    return rows

            row = {
                "id": make_id(pdf),
                "title": title,
                "publish_date": normalize_date(date_raw),
                "pdf_url": pdf,
                "category": category,
                "scraped_at": datetime.utcnow().strftime("%m/%d/%Y"),
            }

            rows.append(row)
            print("✓", title[:80])

        page_no += 1

    return rows


# ================= CSV / JSON =================

def ensure_csv():
    DATA_DIR.mkdir(exist_ok=True)

    if not CSV_FILE.exists():
        with CSV_FILE.open("w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow([
                "id",
                "title",
                "publish_date",
                "pdf_url",
                "category",
                "scraped_at",
            ])


def load_existing_ids():
    if not CSV_FILE.exists():
        return set()

    with CSV_FILE.open() as f:
        return {r["id"] for r in csv.DictReader(f)}


def append_csv(rows):
    with CSV_FILE.open("a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        for r in rows:
            w.writerow([
                r["id"],
                r["title"],
                r["publish_date"],
                r["pdf_url"],
                r["category"],
                r["scraped_at"],
            ])


# ================= MAIN =================

def main():

    ensure_csv()
    existing_ids = load_existing_ids()

    scraped = []

    with sync_playwright() as p:

        # headful but hidden off-screen
        browser = p.chromium.launch(
            headless=False,
            args=[
                "--window-position=-2000,-2000",
                "--window-size=1400,900",
            ]
        )

        page = browser.new_page(viewport=VIEWPORT)

        for category, base, year_stop in SECTIONS:
            scraped.extend(scrape_section(page, category, base, year_stop))

        browser.close()

    # ---------- dedupe vs master ----------

    new_rows = [r for r in scraped if r["id"] not in existing_ids]

    print("\n====================")
    print("New entries:", len(new_rows))

    if new_rows:
        append_csv(new_rows)

    JSON_FILE.write_text(
        json.dumps({
            "generated_at": datetime.utcnow().isoformat(),
            "count": len(new_rows),
            "items": new_rows
        }, indent=2),
        encoding="utf-8"
    )

    print("CSV + JSON updated")


if __name__ == "__main__":
    main()
