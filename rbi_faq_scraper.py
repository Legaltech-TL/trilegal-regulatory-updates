#!/usr/bin/env python3
# rbi_faq_scraper.py
# Python 3.11+
# Dependencies: requests, lxml
# pip install requests lxml

from pathlib import Path
from urllib.parse import urljoin, urlparse
import requests
import lxml.html
import csv
import json
import hashlib
import re
import time
import datetime
import sys
from dateutil import parser as date_parser  # pip install python-dateutil

BASE = "https://rbi.org.in"
LISTING_URL = "https://rbi.org.in/Scripts/FAQDisplay.aspx"
OUT_DIR = Path("data")
OUT_DIR.mkdir(parents=True, exist_ok=True)
MASTER_CSV = OUT_DIR / "rbi_faq_master.csv"
NEW_JSON = OUT_DIR / "rbi_faq_new_entries.json"

HEADERS = {"User-Agent": "rbi-faq-watcher/1.0 (+https://example.com)"}
REQUEST_DELAY = 1.2  # seconds between requests (polite)

CSV_FIELDS = [
    "faq_id", "title_text", "published_date", "category", "url", "last_updated",
    "full_text", "pdf_link", "pdf_filename", "scraped_at", "content_hash"
]

# Regex to find a date at start like "Sep 17, 2025" or "Oct 01, 2025"
DATE_AT_START_RE = re.compile(r"^\s*([A-Za-z]{3,9}\s+\d{1,2},\s*\d{4})\s*(.+)$")

def slugify_for_filename(s, maxlen=120):
    s = s.lower()
    s = re.sub(r"[^\w\s-]", "", s)
    s = re.sub(r"[\s-]+", "_", s).strip("_")
    return s[:maxlen]

def safe_pdf_filename(faq_id, title, pdf_url):
    parsed = urlparse(pdf_url)
    ext = Path(parsed.path).suffix or ".pdf"
    slug = slugify_for_filename(title)
    name = f"{faq_id}_{slug}{ext}"
    return name[:150]

def sha256_text(text: str):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()

def load_existing_master(csv_path):
    existing = {}
    if not csv_path.exists():
        return existing
    with csv_path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            existing[r["faq_id"]] = r
    return existing

def save_master_csv(rows, csv_path):
    tmp = csv_path.with_suffix(".tmp")
    with tmp.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in CSV_FIELDS})
    tmp.replace(csv_path)

def get_listing_page():
    r = requests.get(LISTING_URL, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.text

def parse_date_iso(maybe_date_str):
    if not maybe_date_str:
        return ""
    try:
        dt = date_parser.parse(maybe_date_str)
        return dt.date().isoformat()
    except Exception:
        return ""

def extract_listing_links(html_text):
    """
    Traverse the listing table row-by-row. Detect category header rows (e.g. with bold text/background)
    and assign that category to subsequent rows until next header. For each regular row extract:
      - faq_id (from href ?Id=)
      - url (absolute)
      - title_text (title without leading date)
      - published_date (ISO)
      - possible pdf link on the row
    Returns dict keyed by faq_id -> metadata dict.
    """
    doc = lxml.html.fromstring(html_text)
    doc.make_links_absolute(BASE)
    links = {}

    # Try to find the main table containing the FAQ listing
    # Common container in RBI listing pages: div id=ctl00_ContentPlaceHolder1_pnlFAQ
    table_nodes = doc.xpath("//div[@id='ctl00_ContentPlaceHolder1_pnlFAQ']//table | //table[contains(@class,'table') or contains(@class,'faqtable')]")
    table = table_nodes[0] if table_nodes else doc.xpath("//table")[0] if doc.xpath("//table") else None

    current_category = ""
    if table is None:
        # fallback: find all anchors containing FAQDisplay.aspx
        for a in doc.xpath("//a[contains(@href,'FAQDisplay.aspx?Id=')]"):
            href = a.get("href")
            m = re.search(r"Id=(\d+)", href or "")
            if not m:
                continue
            faq_id = m.group(1)
            raw_text = (a.text_content() or "").strip()
            published_date = ""
            title_text = raw_text
            dm = DATE_AT_START_RE.match(raw_text)
            if dm:
                published_date = parse_date_iso(dm.group(1))
                title_text = dm.group(2).strip()
            links[faq_id] = {"url": href, "title_text": title_text, "category": "", "published_date": published_date}
        return links

    # iterate rows
    for tr in table.xpath(".//tr"):
        # If row looks like a category header: one td/th spanning row and styled (bold or bgcolor)
        tds = tr.xpath("./th|./td")
        if not tds:
            continue

        # Heuristic: a category header often has a single cell spanning many columns and contains bold text
        if len(tds) == 1:
            text = tds[0].text_content().strip()
            # treat as category if non-empty and looks like header (bold or background style)
            style = tds[0].get("style") or ""
            cls = tds[0].get("class") or ""
            if text and (("background" in style.lower()) or "bold" in style.lower() or "font-weight" in style.lower() or "faq" in cls.lower() or len(text.split()) <= 5):
                current_category = text
                continue  # category row
        # else regular row: try find anchor and optional pdf icon in this row
        a = tr.xpath(".//a[contains(@href,'FAQDisplay.aspx?Id=')]")
        if not a:
            # sometimes the link is inside a cell not caught: skip
            continue
        a = a[0]
        href = a.get("href")
        raw_title = (a.text_content() or "").strip()
        # sometimes the date appears in a sibling text node or same cell but not within <a>
        # get the whole row text for date parse
        row_text = tr.text_content().strip()
        # prefer date from leftmost text (often at start). Try regex on row_text first
        published_date = ""
        title_text = raw_title

        dm = DATE_AT_START_RE.match(row_text)
        if dm:
            published_date = parse_date_iso(dm.group(1))
            # The title with date removed may include trailing extras; prefer anchor text for title if anchor text is descriptive
            # But ensure we remove date prefix from anchor text if present
            am = DATE_AT_START_RE.match(raw_title)
            if am:
                title_text = am.group(2).strip()
            else:
                # remove date prefix from the row_text and try to match anchor text inside remainder
                remainder = dm.group(2).strip()
                if raw_title and raw_title in remainder:
                    title_text = raw_title
                else:
                    title_text = remainder
        else:
            # no date at start, maybe anchor itself starts with date
            am = DATE_AT_START_RE.match(raw_title)
            if am:
                published_date = parse_date_iso(am.group(1))
                title_text = am.group(2).strip()
            else:
                title_text = raw_title

        # find pdf link/icon in the same row (href that endswith .pdf)
        pdf_link = ""
        pdf_a = tr.xpath(".//a[contains(translate(@href,'PDF','pdf'), '.pdf')]")
        if pdf_a:
            pdf_link = pdf_a[0].get("href")

        # extract faq_id
        m = re.search(r"Id=(\d+)", href or "")
        if not m:
            continue
        faq_id = m.group(1)
        links[faq_id] = {
            "url": href,
            "title_text": title_text,
            "category": current_category,
            "published_date": published_date,
            "pdf_link": pdf_link
        }

    return links

def fetch_detail_page(url):
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.text

def extract_text_and_pdf(html_text, page_url):
    doc = lxml.html.fromstring(html_text)
    doc.make_links_absolute(page_url)
    last_updated = ""
    whole_text = doc.text_content()
    m = re.search(r"(Last Updated|Last reviewed|Last Reviewed|Last updated)\s*[:\-]?\s*([A-Za-z0-9 ,]{4,50})",
                  whole_text, flags=re.IGNORECASE)
    if m:
        last_updated = m.group(2).strip()

    content_candidates = [
        "//div[@id='ctl00_ContentPlaceHolder1_pnlFAQ']",
        "//div[contains(@class,'faqcontent')]",
        "//div[@id='ctl00_ContentPlaceHolder1_ContentPanel']",
        "//div[@id='ContentPlaceHolder1']",
        "//body"
    ]
    content_text = ""
    for xp in content_candidates:
        nodes = doc.xpath(xp)
        if nodes:
            content_text = "\n\n".join([n.text_content() for n in nodes])
            if content_text.strip():
                break
    if not content_text.strip():
        content_text = whole_text

    content_text = re.sub(r"\r\n", "\n", content_text)
    content_text = re.sub(r"\n[ \t]+", "\n", content_text)
    content_text = re.sub(r"[ \t]{2,}", " ", content_text)
    content_text = re.sub(r"\n{3,}", "\n\n", content_text).strip()

    pdf_link = ""
    for a in doc.xpath("//a[contains(translate(@href,'PDF','pdf'), '.pdf')]"):
        href = a.get("href")
        if href:
            pdf_link = href
            break

    return content_text, last_updated, pdf_link

def main():
    print("Starting rbi_faq_watcher (table-aware)...")
    now_iso = datetime.datetime.now().isoformat()
    existing = load_existing_master(MASTER_CSV)
    listing_html = get_listing_page()
    time.sleep(REQUEST_DELAY)
    links = extract_listing_links(listing_html)
    print(f"Found {len(links)} FAQ links on listing page.")

    all_rows = []
    new_items = []

    for faq_id in sorted(links.keys(), key=lambda x: int(x)):
        meta = links[faq_id]
        url = meta["url"]
        title_text = meta.get("title_text", "") or ""
        category = meta.get("category", "") or ""
        published_date = meta.get("published_date", "") or ""
        # If the listing provided a pdf_link, use it; otherwise will search the detail page
        listing_pdf_link = meta.get("pdf_link", "")

        try:
            html = fetch_detail_page(url)
        except Exception as e:
            print(f"Failed to fetch {url}: {e}", file=sys.stderr)
            continue

        content_text, last_updated, page_pdf_link = extract_text_and_pdf(html, url)
        # prefer pdf found on listing row if any; otherwise page_pdf_link
        pdf_link = listing_pdf_link or page_pdf_link or ""
        content_hash = sha256_text(content_text)
        pdf_filename = safe_pdf_filename(faq_id, title_text or "faq", pdf_link) if pdf_link else ""

        scraped_at = now_iso

        row = {
            "faq_id": faq_id,
            "title_text": title_text,
            "published_date": published_date,
            "category": category,
            "url": url,
            "last_updated": last_updated,
            "full_text": content_text,
            "pdf_link": pdf_link,
            "pdf_filename": pdf_filename,
            "scraped_at": scraped_at,
            "content_hash": content_hash
        }

        all_rows.append(row)

        prev = existing.get(faq_id)
        prev_hash = prev["content_hash"] if prev and "content_hash" in prev else None

        if prev is None:
            reason = "new"
            is_new = True
        elif prev_hash != content_hash:
            reason = "updated"
            is_new = True
        else:
            reason = "unchanged"
            is_new = False

        if is_new:
            new_items.append({
                "faq_id": faq_id,
                "title_text": title_text,
                "published_date": published_date,
                "category": category,
                "url": url,
                "last_updated": last_updated,
                "pdf_link": pdf_link,
                "pdf_filename": pdf_filename,
                "scraped_at": scraped_at,
                "content_hash": content_hash,
                "full_text": content_text
            })
            print(f"[{reason}] {faq_id} - {title_text} ({category})")
        else:
            print(f"[{reason}] {faq_id} - {title_text} ({category})")

        time.sleep(REQUEST_DELAY)

    save_master_csv(all_rows, MASTER_CSV)
    print(f"Saved master CSV to {MASTER_CSV}")

    with NEW_JSON.open("w", encoding="utf-8") as f:
        json.dump({"new_items": new_items}, f, indent=2, ensure_ascii=False)
    print(f"Wrote new entries JSON to {NEW_JSON} ({len(new_items)} items)")

if __name__ == "__main__":
    main()
