import requests
from bs4 import BeautifulSoup
import pandas as pd
from urllib.parse import urljoin
import time
import hashlib
import json
import re
from pathlib import Path


BASE = "https://ipindia.gov.in"
BASE_LIST = "https://ipindia.gov.in/Home/Latestnews/1?CatID=1"

DATA_DIR = Path("data")
CSV_FILE = DATA_DIR / "ipindia_master.csv"
NEW_JSON = DATA_DIR / "ipindia_new_entries.json"

HEADERS = {"User-Agent": "Mozilla/5.0"}


# -------------------------
# Stable ID (CCI style)
# -------------------------
def make_id(title, link):
    return hashlib.sha1(
        f"{title}|{link}".encode()
    ).hexdigest()[:16]


# -------------------------
# PDF filename from title
# first 5 real words only
# -------------------------
def make_pdf_filename(title, pdf_link):
    if not pdf_link:
        return ""

    words = re.findall(r"[A-Za-z0-9]+", title)[:5]
    base = "_".join(words)

    return base + ".pdf"


# -------------------------
# Clean breadcrumb text
# -------------------------
def clean_content(text):
    remove_lines = {"Home", "Media", "Latest News", "News Detail"}
    lines = text.splitlines()
    lines = [l.strip() for l in lines if l.strip() and l.strip() not in remove_lines]
    return "\n".join(lines)


# -------------------------
# Listing page
# -------------------------
def get_listing_page(page_no):

    if page_no == 1:
        url = BASE_LIST
    else:
        url = f"{BASE_LIST}&pg={page_no}"

    print("Listing:", url)

    r = requests.get(url, headers=HEADERS, timeout=30)
    soup = BeautifulSoup(r.text, "lxml")

    items = []

    for li in soup.select("#news-container li"):
        link_tag = li.select_one("a[id^=rpNews_hlTitle_]")
        date_tag = li.select_one("p[class^='newsDate']")

        if not link_tag:
            continue

        items.append({
            "title": link_tag.get_text(strip=True),
            "date": date_tag.get_text(strip=True) if date_tag else "",
            "detail_link": urljoin(BASE, link_tag["href"])
        })

    return items


# -------------------------
# Detail page
# -------------------------
def extract_detail(url):

    try:
        r = requests.get(url, headers=HEADERS, timeout=30)
        soup = BeautifulSoup(r.text, "lxml")

        content_div = soup.select_one("div.contentArea")
        content = content_div.get_text("\n", strip=True) if content_div else ""
        content = clean_content(content)

        pdf_link = ""

        # primary selector
        pdf_tag = soup.select_one("a[id^=lnkTitle_]")
        if pdf_tag and pdf_tag.get("href"):
            pdf_link = urljoin(BASE, pdf_tag["href"])
        else:
            news_links = soup.select_one("#NewsLinks")
            if news_links:
                a = news_links.find("a", href=lambda x: x and x.lower().endswith(".pdf"))
                if a:
                    pdf_link = urljoin(BASE, a["href"])

        return content, pdf_link

    except Exception as e:
        print("Detail error:", url, e)
        return "", ""


# -------------------------
# Scraper runner
# -------------------------
def run_scraper():

    results = []
    page = 1
    stop_all = False

    while True:

        listing = get_listing_page(page)

        if not listing:
            break

        print("Page", page, "items:", len(listing))

        for item in listing:

            # stop at non-2026
            if not item["date"].endswith("2026"):
                stop_all = True
                break

            content, pdf = extract_detail(item["detail_link"])

            uid = make_id(item["title"], item["detail_link"])
            pdf_name = make_pdf_filename(item["title"], pdf)

            results.append({
                "id": uid,
                "title": item["title"],
                "date": item["date"],
                "detail_link": item["detail_link"],
                "pdf_link": pdf,
                "pdf_filename": pdf_name,
                "content": content
            })

            time.sleep(0.6)

        if stop_all:
            break

        page += 1
        time.sleep(1)

    print("TOTAL SCRAPED:", len(results))
    return results


# -------------------------
# CCI-STYLE SAVE LOGIC
# -------------------------
def save_outputs(data):

    DATA_DIR.mkdir(exist_ok=True)

    new_df = pd.DataFrame(data)

    if new_df.empty:
        print("No data scraped — JSON not written")
        new_df.to_json(NEW_JSON, orient="records", indent=2)
        return

    # -------- FIRST RUN --------
    if not CSV_FILE.exists():
        new_df.to_csv(CSV_FILE, index=False)
        new_df.to_json(NEW_JSON, orient="records", indent=2)
        print("First run — all entries new:", len(new_df))
        return

    # -------- NORMAL RUN --------
    old_df = pd.read_csv(CSV_FILE)

    if "id" not in old_df.columns:
        old_ids = set()
    else:
        old_ids = set(old_df["id"].astype(str))

    new_entries = new_df[~new_df["id"].isin(old_ids)]

    # overwrite master CSV with full latest scrape
    new_df.to_csv(CSV_FILE, index=False)

    # write only delta to JSON
    new_entries.to_json(NEW_JSON, orient="records", indent=2)

    print("New entries:", len(new_entries))


# -------------------------
if __name__ == "__main__":

    scraped = run_scraper()
    save_outputs(scraped)

    print("\n✅ DONE")
    print("CSV:", CSV_FILE)
    print("JSON new:", NEW_JSON)
