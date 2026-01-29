#!/usr/bin/env python3

from pathlib import Path
from urllib.parse import urlparse, urljoin
import requests
import lxml.html
import csv
import json
import re
import time
import datetime
from dateutil import parser as date_parser
import sys

csv.field_size_limit(sys.maxsize)

BASE = "https://rbi.org.in"
LISTING_URL = "https://rbi.org.in/Scripts/FAQDisplay.aspx"

OUT_DIR = Path("data")
OUT_DIR.mkdir(parents=True, exist_ok=True)

MASTER_CSV = OUT_DIR / "rbi_faq_master.csv"
NEW_JSON = OUT_DIR / "rbi_faq_new_entries.json"

HEADERS = {"User-Agent": "rbi-faq-watcher"}
REQUEST_DELAY = 1.0
TIMEOUT = 30


# ---------- utilities ----------

def slugify(name):
    name = re.sub(r"[^\w\s-]", "", name.lower())
    return re.sub(r"[\s-]+", "_", name).strip("_")


def safe_pdf_filename(fid, title, url):
    suffix = Path(urlparse(url).path).suffix or ".pdf"
    return f"{fid}_{slugify(title)}{suffix}"


def parse_pub_date(raw):
    m = re.search(r"([A-Za-z]{3,9}\s+\d{1,2},\s*\d{4})", raw)
    if not m:
        return ""
    try:
        return date_parser.parse(m.group(1)).date().isoformat()
    except:
        return ""


def load_existing_rows():
    if not MASTER_CSV.exists():
        return []
    with MASTER_CSV.open(encoding="utf-8") as f:
        return list(csv.DictReader(f))


def load_existing_ids(rows):
    return {r["faq_id"] for r in rows if r.get("faq_id")}


# ---------- listing extraction ----------

def normalize_faq_url(url):
    if "Scripts/" not in url:
        url = url.replace("rbi.org.in/", "rbi.org.in/Scripts/")
    return url


def extract_listing_table(html):
    doc = lxml.html.fromstring(html)

    tables = doc.xpath("//div[@id='ctl00_ContentPlaceHolder1_pnlFAQ']//table") \
             or doc.xpath("//table")

    if not tables:
        return []

    table = tables[0]
    rows = []
    cat = ""

    for tr in table.xpath(".//tr"):
        tds = tr.xpath("./td|./th")
        if not tds:
            continue

        if len(tds) == 1:
            cat = tds[0].text_content().strip()
            continue

        a = tr.xpath(".//a[contains(@href,'FAQDisplay.aspx?Id=')]")
        if not a:
            continue
        a = a[0]

        url = normalize_faq_url(urljoin(BASE, a.get("href")))
        m = re.search(r"Id=(\d+)", url)
        if not m:
            continue

        fid = m.group(1)

        pdf_link = ""
        pdf_a = tr.xpath(".//a[contains(translate(@href,'PDF','pdf'),'.pdf')]")
        if pdf_a:
            pdf_link = urljoin(BASE, pdf_a[0].get("href"))

        rows.append({
            "faq_id": fid,
            "title_text": a.text_content().strip(),
            "published_date": parse_pub_date(tr.text_content()),
            "category": cat,
            "url": url,
            "pdf_link": pdf_link
        })

    return rows


# ---------- content extraction ----------

def extract_tablebg_text(doc):
    tables = doc.xpath(".//table[contains(@class,'tablebg')]")
    if not tables:
        return ""
    return "\n\n".join(t.text_content() for t in tables).strip()


def extract_child_links(doc, base):
    return list({
        urljoin(base, a.get("href"))
        for a in doc.xpath("//a[contains(@class,'link2') and contains(@href,'FAQDisplay.aspx')]")
    })


def extract_detail_page(url):
    url = normalize_faq_url(url)

    r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()

    doc = lxml.html.fromstring(r.text)

    for bad in doc.xpath("//script|//style|//noscript"):
        bad.drop_tree()

    content = extract_tablebg_text(doc)

    if not content:
        texts = []
        for link in extract_child_links(doc, url):
            time.sleep(REQUEST_DELAY)
            try:
                rr = requests.get(normalize_faq_url(link), headers=HEADERS, timeout=TIMEOUT)
                dd = lxml.html.fromstring(rr.text)
                for bad in dd.xpath("//script|//style|//noscript"):
                    bad.drop_tree()
                t = extract_tablebg_text(dd)
                if t:
                    texts.append(t)
            except:
                pass
        content = "\n\n".join(texts)

    return content


# ---------- main ----------

def main():
    now = datetime.datetime.now().isoformat()

    existing_rows = load_existing_rows()
    existing_ids = load_existing_ids(existing_rows)

    for r in existing_rows:
        r["last_updated"] = now

    listing_html = requests.get(LISTING_URL, headers=HEADERS, timeout=TIMEOUT).text
    rows = extract_listing_table(listing_html)

    new_items = []

    for row in rows:
        if row["faq_id"] in existing_ids:
            continue

        print("NEW:", row["faq_id"])
        time.sleep(REQUEST_DELAY)

        pdf_link = row["pdf_link"]

        if pdf_link:
            full_text = ""
            pdf_filename = safe_pdf_filename(row["faq_id"], row["title_text"], pdf_link)
        else:
            full_text = extract_detail_page(row["url"])
            pdf_filename = ""

        item = {
            **row,
            "last_updated": now,
            "full_text": full_text,
            "pdf_filename": pdf_filename,
            "scraped_at": now
        }

        existing_rows.append(item)
        new_items.append(item)

    with MASTER_CSV.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "faq_id","title_text","published_date","category","url",
            "last_updated","full_text","pdf_link","pdf_filename","scraped_at"
        ])
        writer.writeheader()
        writer.writerows(existing_rows)

    NEW_JSON.write_text(json.dumps({"new_items": new_items}, indent=2))


if __name__ == "__main__":
    main()
