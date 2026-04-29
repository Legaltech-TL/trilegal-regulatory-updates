from playwright.sync_api import sync_playwright
from pathlib import Path
from datetime import datetime
import hashlib
import csv
import json
import re


# ================= CONFIG =================

DATA_DIR = Path("pfrda")
CSV_FILE = DATA_DIR / "pfrda_master.csv"
JSON_FILE = DATA_DIR / "pfrda_new.json"

BASE_URL = "https://url.uk.m.mimecastprotect.com/s/OpYYCMjvrhj9kWof4fXT8iQ7t?domain=pfrda.org.in"
YEAR_CUTOFF = 2026
PAGE_DELTA = 60

SECTIONS = [
    ("CIRCULAR",          "/web/pfrda/regulatory-framework/circulars/active-circulars"),
    ("MASTER_CIRCULAR",   "/web/pfrda/regulatory-framework/master-circulars/active-master-circulars"),
    ("NOTIFICATION",      "/web/pfrda/regulatory-framework/notifications"),
    ("GUIDELINE",         "/web/pfrda/regulatory-framework/guidelines"),
    ("REGULATION",        "/web/pfrda/regulatory-framework/regulations"),
    ("GENERAL_ORDER",     "/web/pfrda/regulatory-framework/orders/general-orders"),
    ("ENFORCEMENT_ORDER", "/web/pfrda/regulatory-framework/orders/enforcement-orders"),
    ("NOTICE",            "/web/pfrda/regulatory-framework/orders/notices"),
]

VIEWPORT = {"width": 1400, "height": 900}


# ================= HELPERS =================

def normalize_date(s):
    s = (s or "").strip()
    for fmt in ("%d-%m-%Y", "%d/%m/%Y", "%d.%m.%Y"):
        try:
            return datetime.strptime(s, fmt).strftime("%m/%d/%Y")
        except Exception:
            pass
    return s


def extract_year(s):
    m = https://url.uk.m.mimecastprotect.com/s/1dNWCGZmlswqOGyUXSjTBRFHt?domain=re.search("(20[0-9][0-9])", s or "")
    return int(m.group(1)) if m else None


def make_id(url):
    return hashlib.sha1(url.encode()).hexdigest()[:16]


# ================= LIST PAGE SCRAPER =================

def scrape_list_page(page, category):
    items = []

    h3_elements = page.query_selector_all("h3")

    for h3 in h3_elements:
        try:
            href = h3.evaluate(
                "el => { let a = el.closest('a'); return a ? a.getAttribute('href') : null; }"
            )
            if not href:
                continue

            if "/w/" not in href and "/regulatory-framework/" not in href:
                continue

            title = h3.inner_text().strip()
            if not title:
                continue

            container_text = h3.evaluate(
                "el => { let a = el.closest('a'); let p = a ? a.parentElement : el.parentElement; return p ? p.innerText : ''; }"
            )

            date_m = https://url.uk.m.mimecastprotect.com/s/1dNWCGZmlswqOGyUXSjTBRFHt?domain=re.search("([0-9][0-9]-[0-9][0-9]-20[0-9][0-9])", container_text or "")
            date_raw = date_m.group(1) if date_m else ""

            clean_href = href.split("?")[0]
            if clean_href.startswith("/"):
                detail_url = BASE_URL + clean_href
            else:
                detail_url = clean_href

            items.append({
                "detail_url": detail_url,
                "title": title,
                "date_raw": date_raw,
                "category": category,
            })

        except Exception as e:
            print("  [warn] entry parse error: " + str(e))
            continue

    return items


def scrape_section(page, category, section_path):
    all_items = []
    start = 0

    print("\n========== " + category + " ==========")

    while True:
        url = BASE_URL + section_path + "?delta=" + str(PAGE_DELTA) + "&start=" + str(start)
        print("Opening: " + url)

        page.goto(url, timeout=90000)
        page.wait_for_load_state("domcontentloaded")

        try:
            page.wait_for_selector("h3", timeout=20000)
        except Exception:
            print("  No h3 found — stopping section")
            break

        page.wait_for_timeout(2000)

        items = scrape_list_page(page, category)
        print("Items found: " + str(len(items)))

        if not items:
            break

        hit_cutoff = False
        for item in items:
            yr = extract_year(item["date_raw"])
            if yr and yr < YEAR_CUTOFF:
                hit_cutoff = True
                break

        all_items.extend(items)

        if hit_cutoff:
            print("  Reached pre-" + str(YEAR_CUTOFF) + " item — stopping section")
            break

        start += PAGE_DELTA

    return all_items


# ================= DETAIL PAGE PDF FETCH =================

def get_pdf_url(page, detail_url):
    try:
        page.goto(detail_url, timeout=60000)
        page.wait_for_load_state("domcontentloaded")
        page.wait_for_timeout(2000)

        el = page.query_selector("a[href*='/documents/'][href$='.pdf']")
        if el:
            href = el.get_attribute("href") or ""
            if href.startswith("/"):
                return BASE_URL + href
            return href

        el = page.query_selector("a[href$='.pdf']")
        if el:
            href = el.get_attribute("href") or ""
            if href.startswith("/"):
                return BASE_URL + href
            return href

    except Exception as e:
        print("  [warn] PDF fetch failed: " + str(e))

    return ""


# ================= CSV / JSON =================

def ensure_csv():
    DATA_DIR.mkdir(exist_ok=True)
    if not CSV_FILE.exists():
        with CSV_FILE.open("w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(["id", "title", "publish_date", "pdf_url", "category", "scraped_at"])


def load_existing_ids():
    if not CSV_FILE.exists():
        return set()
    with CSV_FILE.open(encoding="utf-8") as f:
        return {r["id"] for r in csv.DictReader(f)}


def append_csv(rows):
    with CSV_FILE.open("a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        for r in rows:
            w.writerow([r["id"], r["title"], r["publish_date"], r["pdf_url"], r["category"], r["scraped_at"]])


# ================= MAIN =================

def main():
    ensure_csv()
    existing_ids = load_existing_ids()
    print("Existing IDs in master CSV: " + str(len(existing_ids)))

    with sync_playwright() as p:

        browser = p.chromium.launch(
            headless=False,
            args=["--window-position=-2000,-2000", "--window-size=1400,900"],
        )
        context = browser.new_context(
            viewport=VIEWPORT,
            extra_http_headers={"Accept-Language": "en-US,en;q=0.9"},
        )
        page = context.new_page()

        list_items = []
        for category, section_path in SECTIONS:
            list_items.extend(scrape_section(page, category, section_path))

        print("\n" + "="*50)
        print("Total list items collected: " + str(len(list_items)))

        seen_urls = set()
        unique_items = []
        for item in list_items:
            if item["detail_url"] not in seen_urls:
                seen_urls.add(item["detail_url"])
                unique_items.append(item)

        new_rows = []
        for item in unique_items:
            item_id = make_id(item["detail_url"])
            if item_id in existing_ids:
                continue

            print("Fetching PDF: " + item["title"][:70])
            pdf_url = get_pdf_url(page, item["detail_url"])

            row = {
                "id": item_id,
                "title": item["title"],
                "publish_date": normalize_date(item["date_raw"]),
                "pdf_url": pdf_url,
                "category": item["category"],
                "scraped_at": datetime.utcnow().strftime("%m/%d/%Y"),
            }
            new_rows.append(row)
            print("  done: " + item["title"][:60])

        browser.close()

    print("\n" + "="*50)
    print("New entries: " + str(len(new_rows)))

    if new_rows:
        append_csv(new_rows)

    JSON_FILE.write_text(
        json.dumps({"generated_at": datetime.utcnow().isoformat(), "count": len(new_rows), "items": new_rows}, indent=2),
        encoding="utf-8",
    )

    print("pfrda_master.csv and pfrda_new.json updated")


if __name__ == "__main__":
    main()

