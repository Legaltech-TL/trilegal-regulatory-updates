from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import re
import time
import hashlib
import pandas as pd
from dateutil import parser as dtparser
from pathlib import Path


WHATS_NEW = "https://cci.gov.in/whats-new"
PUBLIC_NOTICES = "https://cci.gov.in/public-notices"
PRESS_RELEASE = "https://cci.gov.in/media-gallery/press-release"

CSV_FILE = "cci_all_sections.csv"
NEW_JSON = "cci_new_entries.json"


# --------------------------------------------------
# Robust table wait with retries
# --------------------------------------------------

def wait_for_table_rows(page, retries=3):
    for i in range(retries):
        try:
            page.wait_for_selector("#datatable_ajax", timeout=20000)
            page.wait_for_selector("#datatable_ajax tbody tr", timeout=20000)
            return
        except:
            print("Table load retry", i + 1)
            page.reload(wait_until="domcontentloaded")
    raise Exception("Table failed to load")


# --------------------------------------------------
# Date from title
# --------------------------------------------------

def extract_date_from_title(title):
    m = re.search(r"\d{1,2}[./-]\d{1,2}[./-]\d{2,4}", title)
    if not m:
        return ""
    try:
        return dtparser.parse(m.group()).date().isoformat()
    except:
        return ""


# --------------------------------------------------
# Stable ID
# --------------------------------------------------

def make_id(section, title, link):
    return hashlib.sha1(
        f"{section}|{title}|{link}".encode()
    ).hexdigest()[:16]


# --------------------------------------------------
# Press PDF resolver
# --------------------------------------------------

def get_press_pdf_link(detail_page, url, retries=3):

    for attempt in range(retries):
        try:
            detail_page.goto(url, wait_until="domcontentloaded", timeout=60000)
            detail_page.wait_for_selector("a[onclick]", timeout=15000)

            soup = BeautifulSoup(detail_page.content(), "html.parser")

            for a in soup.find_all("a", onclick=True):
                m = re.search(
                    r"(?:DownloadFile|viewPdf)\('([^']+)'\)",
                    a["onclick"]
                )
                if m:
                    time.sleep(0.6)
                    return m.group(1)

        except Exception:
            print("Retry", attempt + 1, "failed:", url)
            time.sleep(2)

    print("PDF resolve failed:", url)
    return ""


# --------------------------------------------------
# Parse table
# --------------------------------------------------

def parse_table(page, detail_page, section,
                has_date_col=False,
                press_mode=False):

    soup = BeautifulSoup(page.content(), "html.parser")
    table = soup.find("table", id="datatable_ajax")
    if not table:
        return []

    out = []

    for r in table.find("tbody").find_all("tr"):
        cols = r.find_all("td")
        if len(cols) < 3:
            continue

        no = cols[0].get_text(strip=True)
        title = cols[1].get_text(strip=True)

        if has_date_col:
            try:
                date_val = dtparser.parse(
                    cols[2].get_text(strip=True)
                ).date().isoformat()
            except:
                date_val = ""
            link_col = cols[3]
        else:
            date_val = extract_date_from_title(title)
            link_col = cols[2]

        a = link_col.find("a")
        link = a["href"] if a else ""

        if press_mode and link:
            link = get_press_pdf_link(detail_page, link)

        uid = make_id(section, title, link)

        out.append({
            "id": uid,
            "section": section,
            "no": no,
            "title": title,
            "date": date_val,
            "pdf_link": link
        })

    return out


# --------------------------------------------------
# Pagination
# --------------------------------------------------

def paginate(page, detail_page, section,
             has_date_col=False,
             keep_only_year=None,
             press_mode=False):

    results = []

    while True:
        wait_for_table_rows(page)

        rows = parse_table(
            page,
            detail_page,
            section,
            has_date_col,
            press_mode
        )

        if keep_only_year:
            page_has_target = False
            for r in rows:
                if r["date"] and int(r["date"][:4]) == keep_only_year:
                    results.append(r)
                    page_has_target = True
            if not page_has_target:
                break
        else:
            results.extend(rows)

        next_btn = page.query_selector("#datatable_ajax_next")
        if not next_btn:
            break

        if "disabled" in (next_btn.get_attribute("class") or ""):
            break

        next_btn.click()
        time.sleep(1)

    return results


# --------------------------------------------------
# Scraper runner
# --------------------------------------------------

def run_scraper():

    all_data = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)

        page = browser.new_page()
        detail_page = browser.new_page()

        page.set_default_navigation_timeout(60000)

        print("Whats New")
        page.goto(WHATS_NEW, wait_until="domcontentloaded")
        wait_for_table_rows(page)
        all_data.extend(parse_table(page, detail_page, "whats_new"))

        time.sleep(2)

        print("Public Notices")
        page.goto(PUBLIC_NOTICES, wait_until="domcontentloaded")
        all_data.extend(
            paginate(page, detail_page, "public_notices")
        )

        time.sleep(2)

        print("Press Releases")
        page.goto(PRESS_RELEASE, wait_until="domcontentloaded")
        all_data.extend(
            paginate(
                page,
                detail_page,
                "press_release",
                has_date_col=True,
                keep_only_year=2026,
                press_mode=True
            )
        )

        browser.close()

    return all_data


# --------------------------------------------------
# Save CSV + new JSON
# --------------------------------------------------

def save_outputs(data):

    new_df = pd.DataFrame(data)

    if Path(CSV_FILE).exists():
        old_ids = set(pd.read_csv(CSV_FILE)["id"].astype(str))
    else:
        old_ids = set()

    new_entries = new_df[~new_df["id"].isin(old_ids)]

    new_df.to_csv(CSV_FILE, index=False)
    new_entries.to_json(NEW_JSON, orient="records", indent=2)

    print("New entries:", len(new_entries))


# --------------------------------------------------

if __name__ == "__main__":
    scraped = run_scraper()
    save_outputs(scraped)

    print("\n✅ DONE")
    print("CSV:", CSV_FILE)
    print("JSON new:", NEW_JSON)
