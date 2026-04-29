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

BASE_URL = "https://url.uk.m.mimecastprotect.com/s/FYu5CZ4MLIzx8yyhzf8TBWmxs?domain=pfrda.org.in"
YEAR_CUTOFF = 2026   # stop scraping items from years before this
PAGE_DELTA = 60      # items per list page request

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
    m = "https://url.uk.m.mimecastprotect.com/s/RPOwC1jZyh8OBKKUGhnTVPb__?domain=re.search(r"(20\d{2})", s or "")
    return int(m.group(1)) if m else None


def make_id(url):
    return hashlib.sha1(url.encode()).hexdigest()[:16]


# ================= LIST PAGE SCRAPER =================

def scrape_list_page(page, category):
    """
    Extract all entry items visible on the current page.
    PFRDA Liferay list pages: each item is an <a href="/web/pfrda/w/..."> containing an <h3>.
    Date is in the sibling/parent text as DD-MM-YYYY.
    Returns a list of dicts: {detail_url, title, date_raw, category}.
    """
    items = []

    # Each entry is an anchor that wraps an h3 title
    anchors = page.query_selector_all("a:has(h3)")

    for anchor in anchors:
        try:
            href = (anchor.get_attribute("href") or "").strip()

            # Skip nav/menu links — only content detail links
            if "/w/" not in href and "/regulatory-framework/" not in href:
                continue
            if href in ("/web/pfrda/regulatory-framework/circulars", "/web/pfrda/"):
                continue

            h3 = anchor.query_selector("h3")
            title = (h3.inner_text() if h3 else anchor.inner_text()).strip()
            if not title:
                continue

            # Walk up to a container that holds the metadata text
            container_text = anchor.evaluate(
                "el => {"
                "  let p = el.parentElement;"
                "  for (let i = 0; i < 4; i++) {"
                "    if (p && p.innerText && p.innerText.match(/\\d{2}-\\d{2}-20\\d{2}/)) return p.innerText;"
                "    if (p) p = p.parentElement; else break;"
                "  }"
                "  return el.parentElement ? el.parentElement.innerText : '';"
                "}"
            )

            date_m = https://url.uk.m.mimecastprotect.com/s/RPOwC1jZyh8OBKKUGhnTVPb__?domain=re.search(r"(\d{2}-\d{2}-20\d{2})", container_text or "")
            date_raw = date_m.group(1) if date_m else ""

            detail_url = (BASE_URL + href) if href.startswith("/") else href

            items.append({
                "detail_url": detail_url,
                "title": title,
                "date_raw": date_raw,
                "category": category,
            })

        except Exception as e:
            print(f"  [warn] entry parse error: {e}")
            continue

    return items


def get_next_page_url(page):
    """Return the href of the pagination 'Next' link, or None if on the last page."""
    try:
        for selector in [
            "https://url.uk.m.mimecastprotect.com/s/O2DgC2RZ0h76ZAAHBiPT5X1wa?domain=a.next",
            "https://url.uk.m.mimecastprotect.com/s/XBSsC3l8ATZx9VVHDskTQwyp8?domain=li.next > a",
            "a[aria-label='Next']",
            "a[rel='next']",
            ".pagination-next a",
            "a:text('Next')",
            "a:text('›')",
            "a:text('»')",
        ]:
            el = page.query_selector(selector)
            if el:
                href = el.get_attribute("href") or ""
                if href and href != "#":
                    return (BASE_URL + href) if href.startswith("/") else href
    except Exception:
        pass
    return None


def scrape_section(page, category, section_path):
    """Paginate through all list pages for one section. Returns combined list of items."""
    all_items = []
    url = f"{BASE_URL}{section_path}?delta={PAGE_DELTA}&start=0"

    print(f"\n========== {category} ==========")

    while url:
        print(f"Opening: {url}")
        page.goto(url, timeout=90000)
        page.wait_for_load_state("domcontentloaded")
        page.wait_for_load_state("networkidle")
        page.wait_for_timeout(2500)

        items = scrape_list_page(page, category)
        print(f"Items found: {len(items)}")

        if not items:
            print("No items — stopping section")
            break

        # Year cutoff: stop when we hit items older than YEAR_CUTOFF
        hit_cutoff = False
        for item in items:
            yr = extract_year(item["date_raw"])
            if yr and yr < YEAR_CUTOFF:
                hit_cutoff = True
                break

        all_items.extend(items)

        if hit_cutoff:
            print(f"  Reached pre-{YEAR_CUTOFF} item — stopping section")
            break

        next_url = get_next_page_url(page)
        url = next_url  # None exits the loop

    return all_items


# ================= DETAIL PAGE PDF FETCH =================

def get_pdf_url(page, detail_url):
    """Visit a circular/notification detail page and return the PDF download URL."""
    try:
        page.goto(detail_url, timeout=60000)
        page.wait_for_load_state("domcontentloaded")
        page.wait_for_timeout(2000)

        # Primary: Liferay document library PDFs
        el = page.query_selector("a[href*='/documents/'][href$='.pdf']")
        if el:
            href = el.get_attribute("href") or ""
            return (BASE_URL + href) if href.startswith("/") else href

        # Fallback: any .pdf link on the page
        el = page.query_selector("a[href$='.pdf']")
        if el:
            href = el.get_attribute("href") or ""
            return (BASE_URL + href) if href.startswith("/") else href

    except Exception as e:
        print(f"  [warn] PDF fetch failed for {detail_url}: {e}")

    return ""


# ================= CSV / JSON =================

def ensure_csv():
    DATA_DIR.mkdir(exist_ok=True)
    if not CSV_FILE.exists():
        with CSV_FILE.open("w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow([
                "id", "title", "publish_date", "pdf_url", "category", "scraped_at",
            ])


def load_existing_ids():
    if not CSV_FILE.exists():
        return set()
    with CSV_FILE.open(encoding="utf-8") as f:
        return {r["id"] for r in csv.DictReader(f)}


def append_csv(rows):
    with CSV_FILE.open("a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        for r in rows:
            w.writerow([
                r["id"], r["title"], r["publish_date"],
                r["pdf_url"], r["category"], r["scraped_at"],
            ])


# ================= MAIN =================

def main():
    ensure_csv()
    existing_ids = load_existing_ids()
    print(f"Existing IDs in master CSV: {len(existing_ids)}")

    with sync_playwright() as p:

        browser = p.chromium.launch(
            headless=False,
            args=[
                "--window-position=-2000,-2000",
                "--window-size=1400,900",
            ],
        )
        context = browser.new_context(
            viewport=VIEWPORT,
            extra_http_headers={"Accept-Language": "en-US,en;q=0.9"},
        )
        page = context.new_page()

        # ---- Pass 1: collect list items from all sections ----
        list_items = []
        for category, section_path in SECTIONS:
            list_items.extend(scrape_section(page, category, section_path))

        print(f"\n{'='*50}")
        print(f"Total list items collected: {len(list_items)}")

        # Deduplicate list items by detail URL (handles same item appearing in multiple runs)
        seen_urls = set()
        unique_items = []
        for item in list_items:
            if item["detail_url"] not in seen_urls:
                seen_urls.add(item["detail_url"])
                unique_items.append(item)

        # ---- Pass 2: fetch PDF URLs only for genuinely new items ----
        new_rows = []
        for item in unique_items:
            item_id = make_id(item["detail_url"])
            if item_id in existing_ids:
                continue  # already in master CSV, skip detail page fetch

            print(f"Fetching PDF: {item['title'][:70]}")
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
            print(f"  ✓ {item['title'][:60]}")

        browser.close()

    # ---- Write outputs ----
    print(f"\n{'='*50}")
    print(f"New entries: {len(new_rows)}")

    if new_rows:
        append_csv(new_rows)

    JSON_FILE.write_text(
        json.dumps({
            "generated_at": datetime.utcnow().isoformat(),
            "count": len(new_rows),
            "items": new_rows,
        }, indent=2),
        encoding="utf-8",
    )

    print("pfrda_master.csv and pfrda_new.json updated")


if __name__ == "__main__":
    main()

