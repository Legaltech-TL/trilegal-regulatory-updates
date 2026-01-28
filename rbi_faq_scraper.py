#!/usr/bin/env python3
# rbi_faq_scraper.py
# FAST VERSION — ONLY NEW ENTRIES (NO UPDATE CHECK)

from pathlib import Path
from urllib.parse import urlparse
import requests
import lxml.html
import csv
import json
import re
import time
import datetime
from dateutil import parser as date_parser   # pip install python-dateutil

BASE = "https://rbi.org.in"
LISTING_URL = "https://rbi.org.in/Scripts/FAQDisplay.aspx"

OUT_DIR = Path("data")
OUT_DIR.mkdir(parents=True, exist_ok=True)

MASTER_CSV = OUT_DIR / "rbi_faq_master.csv"
NEW_JSON = OUT_DIR / "rbi_faq_new_entries.json"

HEADERS = {"User-Agent": "rbi-faq-watcher/fast-simple"}

REQUEST_DELAY = 1.0  # only applies to NEW entries fetch


# ----------- Utilities -----------

def slugify(name, maxlen=100):
    name = name.lower()
    name = re.sub(r"[^\w\s-]", "", name)
    name = re.sub(r"[\s-]+", "_", name).strip("_")
    return name[:maxlen]


def safe_pdf_filename(faq_id, title, pdf_url):
    suffix = Path(urlparse(pdf_url).path).suffix or ".pdf"
    slug = slugify(title)
    return f"{faq_id}_{slug}{suffix}"


def parse_pub_date(raw):
    """Extract published date from start of text, return ISO or empty."""
    m = re.match(r"^\s*([A-Za-z]{3,9}\s+\d{1,2},\s*\d{4})", raw)
    if not m:
        return ""
    try:
        return date_parser.parse(m.group(1)).date().isoformat()
    except:
        return ""


def load_existing_ids():
    if not MASTER_CSV.exists():
        return set()
    ids = set()
    with MASTER_CSV.open(encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            ids.add(row["faq_id"])
    return ids


# ----------- Listing Table Extraction -----------

def extract_listing_table(html):
    """
    Reads the main FAQ listing table.
    Detects category header rows + normal rows.
    Extracts only listing-level info (no detail).
    Returns list of dicts with:
       faq_id, title_text, published_date, category, url, pdf_link
    """
    doc = lxml.html.fromstring(html)
    doc.make_links_absolute(BASE)

    table_nodes = doc.xpath("//div[@id='ctl00_ContentPlaceHolder1_pnlFAQ']//table")
    if not table_nodes:
        table_nodes = doc.xpath("//table")
    if not table_nodes:
        return []

    table = table_nodes[0]
    rows = []
    current_category = ""

    for tr in table.xpath(".//tr"):
        tds = tr.xpath("./td|./th")
        if not tds:
            continue

        # Category header row
        if len(tds) == 1:
            txt = tds[0].text_content().strip()
            if txt:
                current_category = txt
            continue

        a = tr.xpath(".//a[contains(@href,'FAQDisplay.aspx?Id=')]")
        if not a:
            continue
        a = a[0]

        raw = a.get("href") or ""

        if raw.lower().startswith("faqdisplay.aspx"):
            url = f"https://rbi.org.in/Scripts/{raw}"
        elif raw.lower().startswith("/scripts/"):
            url = "https://rbi.org.in" + raw
        else:
            url = raw

        row_text = tr.text_content().strip()

        m = re.search(r"Id=(\d+)", url)
        if not m:
            continue
        faq_id = m.group(1)

        published_date = parse_pub_date(row_text)
        title_text = a.text_content().strip()

        pdf_link = ""
        pdf_a = tr.xpath(".//a[contains(translate(@href,'PDF','pdf'),'.pdf')]")
        if pdf_a:
            pdf_link = pdf_a[0].get("href")

        rows.append({
            "faq_id": faq_id,
            "title_text": title_text,
            "published_date": published_date,
            "category": current_category,
            "url": url,
            "pdf_link": pdf_link
        })

    return rows


# ----------- Detail Page Extraction (ONLY for NEW ENTRIES) -----------

def extract_detail_page(url):
    r = requests.get(url, headers=HEADERS)
    r.raise_for_status()
    doc = lxml.html.fromstring(r.text)
    doc.make_links_absolute(url)

    # 🔧 FIX: remove script/style/noscript to avoid JS pollution
    for bad in doc.xpath("//script|//style|//noscript"):
        bad.drop_tree()

    whole_text = doc.text_content()

    m = re.search(
        r"(Last Updated|Last reviewed|Last Reviewed)\s*[:\-]?\s*([A-Za-z0-9 ,]{4,50})",
        whole_text,
        flags=re.IGNORECASE
    )
    last_updated = m.group(2).strip() if m else ""

    content_nodes = doc.xpath("//div[@id='ctl00_ContentPlaceHolder1_pnlFAQ']") or \
                    doc.xpath("//div[contains(@class,'faqcontent')]") or \
                    doc.xpath("//body")

    text = "\n\n".join(n.text_content() for n in content_nodes)
    text = re.sub(r"\n{3,}", "\n\n", text).strip()

    pdf_link = ""
    a = doc.xpath("//a[contains(translate(@href,'PDF','pdf'), '.pdf')]")
    if a:
        pdf_link = a[0].get("href")

    return text, last_updated, pdf_link


# ----------- MAIN -----------

def main():
    print("Running RBI FAQ Watcher (FAST MODE: Only New IDs)")

    existing_ids = load_existing_ids()
    print(f"Loaded {len(existing_ids)} existing IDs")

    listing_html = requests.get(LISTING_URL, headers=HEADERS).text
    listing_rows = extract_listing_table(listing_html)
    print(f"Found {len(listing_rows)} listing rows")

    new_items = []
    now_iso = datetime.datetime.now().isoformat()

    csv_exists = MASTER_CSV.exists()
    f = MASTER_CSV.open("a", newline="", encoding="utf-8")
    writer = csv.DictWriter(f, fieldnames=[
        "faq_id",
        "title_text",
        "published_date",
        "category",
        "url",
        "last_updated",
        "full_text",
        "pdf_link",
        "pdf_filename",
        "scraped_at"
    ])
    if not csv_exists:
        writer.writeheader()

    for row in listing_rows:
        faq_id = row["faq_id"]

        if faq_id in existing_ids:
            continue

        print(f"NEW ENTRY FOUND: {faq_id} — Fetching detail page...")

        time.sleep(REQUEST_DELAY)

        full_text, last_updated, page_pdf_link = extract_detail_page(row["url"])

        pdf_link = row["pdf_link"] or page_pdf_link or ""
        pdf_filename = safe_pdf_filename(faq_id, row["title_text"], pdf_link) if pdf_link else ""

        item = {
            "faq_id": faq_id,
            "title_text": row["title_text"],
            "published_date": row["published_date"],
            "category": row["category"],
            "url": row["url"],
            "last_updated": last_updated,
            "full_text": full_text,
            "pdf_link": pdf_link,
            "pdf_filename": pdf_filename,
            "scraped_at": now_iso
        }

        writer.writerow(item)
        new_items.append(item)

    f.close()

    with NEW_JSON.open("w", encoding="utf-8") as jf:
        json.dump({"new_items": new_items}, jf, indent=2, ensure_ascii=False)

    print(f"Completed. New entries found: {len(new_items)}")
    print(f"CSV updated: {MASTER_CSV}")
    print(f"New JSON: {NEW_JSON}")


if __name__ == "__main__":
    main()
