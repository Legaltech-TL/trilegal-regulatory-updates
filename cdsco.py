import requests
from bs4 import BeautifulSoup
import hashlib
import pandas as pd
import re
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


BASE = "https://cdsco.gov.in"

SECTIONS = {

    "public_notices":
        "https://cdsco.gov.in/opencms/opencms/en/Notifications/Public-Notices/",

    "gazette_notifications":
        "https://cdsco.gov.in/opencms/opencms/en/Notifications/Gazette-Notifications/",

    "circulars":
        "https://cdsco.gov.in/opencms/opencms/en/Notifications/Circulars/"
}


DATA_DIR = Path("cdsco")

CSV_FILE = DATA_DIR / "cdsco_master.csv"

NEW_JSON = DATA_DIR / "cdsco_new_entries.json"


YEAR_FILTER = 2026


# --------------------------------------------------
# Create robust session
# --------------------------------------------------

def create_session():

    session = requests.Session()

    retry = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )

    adapter = HTTPAdapter(max_retries=retry)

    session.mount("http://", adapter)
    session.mount("https://", adapter)

    session.headers.update({
        "User-Agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0 Safari/537.36",

        "Accept":
        "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",

        "Accept-Language":
        "en-IN,en;q=0.9",

        "Connection":
        "keep-alive"
    })

    return session


SESSION = create_session()


# --------------------------------------------------
# Stable ID
# --------------------------------------------------

def make_id(category, title, link):

    return hashlib.sha1(
        f"{category}|{title}|{link}".encode()
    ).hexdigest()[:16]


# --------------------------------------------------
# Filename generator
# --------------------------------------------------

def make_filename(title):

    cleaned = re.sub(r'[^A-Za-z0-9\s]', '', title)

    words = cleaned.split()[:7]

    return "_".join(words)


# --------------------------------------------------
# Extract year
# --------------------------------------------------

def extract_year(date):

    try:
        return int(date.split("-")[0])
    except:
        return 0


# --------------------------------------------------
# Fetch page safely
# --------------------------------------------------

def fetch_page(url):

    for attempt in range(5):

        try:

            print(f"Connecting → {url}")

            res = SESSION.get(url, timeout=60)

            res.raise_for_status()

            return res.text

        except Exception as e:

            print(f"Attempt {attempt+1} failed:", e)

            time.sleep(5)

    raise Exception("Failed after retries")


# --------------------------------------------------
# Scrape section
# --------------------------------------------------

def scrape_section(category, url):

    print(f"\nScraping {category}...")

    html = fetch_page(url)

    soup = BeautifulSoup(html, "html.parser")

    table = soup.find("table")

    if not table:

        print("No table found")
        return []

    rows = table.find("tbody").find_all("tr")

    scraped_time = datetime.utcnow().isoformat()

    results = []

    for row in rows:

        cols = row.find_all("td")

        if len(cols) < 4:
            continue

        sno = cols[0].get_text(strip=True)

        title = cols[1].get_text(strip=True)

        date = cols[2].get_text(strip=True)

        year = extract_year(date)

        if year < YEAR_FILTER:
            continue

        link_tag = cols[3].find("a")

        if not link_tag:
            continue

        pdf_link = urljoin(BASE, link_tag["href"])

        uid = make_id(category, title, pdf_link)

        filename = make_filename(title)

        results.append({

            "id": uid,
            "category": category,
            "sno": sno,
            "title": title,
            "date": date,
            "pdf_link": pdf_link,
            "filename": filename,
            "scraped_time": scraped_time
        })

    print(f"{category}: {len(results)} records")

    return results


# --------------------------------------------------
# Run scraper
# --------------------------------------------------

def run_scraper():

    all_data = []

    for category, url in SECTIONS.items():

        section_data = scrape_section(category, url)

        all_data.extend(section_data)

        # Prevent blocking
        time.sleep(5)

    print("\nTOTAL:", len(all_data))

    return all_data


# --------------------------------------------------
# Save outputs
# --------------------------------------------------

def save_outputs(data):

    DATA_DIR.mkdir(exist_ok=True)

    new_df = pd.DataFrame(data)

    if new_df.empty:

        print("No data scraped")

        new_df.to_json(NEW_JSON, orient="records", indent=2)

        return


    if not CSV_FILE.exists():

        new_df.to_csv(CSV_FILE, index=False)

        new_df.to_json(NEW_JSON, orient="records", indent=2)

        print("First run: all entries new")

        return


    old_df = pd.read_csv(CSV_FILE)

    old_ids = set(old_df["id"].astype(str))

    new_entries = new_df[~new_df["id"].isin(old_ids)]

    combined = pd.concat([old_df, new_entries]).drop_duplicates("id")

    combined.to_csv(CSV_FILE, index=False)

    new_entries.to_json(NEW_JSON, orient="records", indent=2)

    print("New entries:", len(new_entries))


# --------------------------------------------------
# MAIN
# --------------------------------------------------

if __name__ == "__main__":

    scraped = run_scraper()

    save_outputs(scraped)

    print("\nDONE")
    print("CSV:", CSV_FILE)
    print("JSON:", NEW_JSON)
