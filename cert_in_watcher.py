from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By

from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
from urllib.parse import urljoin

import requests
import PyPDF2
import io
import time
import hashlib
from pathlib import Path


# ============================
# CONFIG
# ============================

BASE = "https://www.cert-in.org.in/"

DATA_DIR = Path("data")
CSV_FILE = DATA_DIR / "certin_master.csv"
NEW_JSON = DATA_DIR / "certin_new_entries.json"

TARGET_SECTIONS = [
    "Latest Security Alert",
    "Current Activities",
    "ITNews",
    "virus alert",
    "Virus Alerts"
]

TARGET_YEARS = ["2025", "2026"]


# ============================
# UNIQUE ID
# ============================

def make_id(category, title, link):

    return hashlib.sha1(
        f"{category}|{title}|{link}".encode()
    ).hexdigest()[:16]


# ============================
# CONTENT EXTRACTORS
# ============================

def extract_html_content(url):

    try:
        res = requests.get(url, timeout=30)
        soup = BeautifulSoup(res.text, "html.parser")

        div = soup.find("div", id="print_content")

        if div:
            return div.get_text("\n", strip=True)

    except:
        pass

    return ""


def extract_pdf_content(url):

    try:
        res = requests.get(url, timeout=30)

        reader = PyPDF2.PdfReader(io.BytesIO(res.content))

        text = ""

        for page in reader.pages:
            page_text = page.extract_text()
            if page_text:
                text += page_text + "\n"

        return text.strip()

    except:
        return ""


def extract_content(url):

    if url.lower().endswith(".pdf"):
        return extract_pdf_content(url)

    return extract_html_content(url)


# ============================
# SCRAPER
# ============================

def scrape_certin():

    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options
    )

    driver.get(BASE)

    time.sleep(3)

    frames = driver.find_elements(By.TAG_NAME, "frame")
    driver.switch_to.frame(frames[-1])

    soup = BeautifulSoup(driver.page_source, "html.parser")

    driver.quit()

    scraped_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    data = []

    headers = soup.find_all("img", alt=True)

    print("Headers found:", len(headers))

    for header in headers:

        category = header.get("alt").strip()

        if category not in TARGET_SECTIONS:
            continue

        print("\nProcessing:", category)

        element = header

        while True:

            element = element.find_next()

            if not element:
                break

            if element.name == "img" and element.get("alt") in TARGET_SECTIONS:
                break

            if element.name == "span" and "DateContent" in element.get("class", []):

                raw_date = element.text.strip()

                if not any(y in raw_date for y in TARGET_YEARS):
                    continue

                date_text = (
                    raw_date
                    .replace("(", "")
                    .replace(")", "")
                    .replace("CERT-In", "")
                    .replace("-", "")
                )

                date_text = " ".join(date_text.split())

                link_tag = element.find_previous("a")

                if not link_tag:
                    continue

                title = link_tag.text.strip()

                link = urljoin(BASE, link_tag.get("href"))

                uid = make_id(category, title, link)

                print("Extracting content:", title)

                content = extract_content(link)

                data.append({
                    "id": uid,
                    "title": title,
                    "date": date_text,
                    "link": link,
                    "category": category,
                    "content": content,
                    "scraped_at": scraped_at
                })

    print("\nTotal scraped:", len(data))

    return data


# ============================
# SAVE OUTPUTS
# ============================

def save_outputs(data):

    DATA_DIR.mkdir(exist_ok=True)

    new_df = pd.DataFrame(data)

    if new_df.empty:
        print("No data scraped")
        return

    # FIRST RUN
    if not CSV_FILE.exists():

        new_df.to_csv(CSV_FILE, index=False)
        new_df.to_json(NEW_JSON, orient="records", indent=2)

        print("First run — saved:", len(new_df))
        return


    # NORMAL RUN

    old_df = pd.read_csv(CSV_FILE)

    old_ids = set(old_df["id"].astype(str))

    new_entries = new_df[~new_df["id"].isin(old_ids)]

    # Append to master CSV
    updated_df = pd.concat([old_df, new_entries], ignore_index=True)

    updated_df.to_csv(CSV_FILE, index=False)

    # Save ONLY new entries JSON
    new_entries.to_json(NEW_JSON, orient="records", indent=2)

    print("New entries:", len(new_entries))
    print("Total master:", len(updated_df))


# ============================
# RUN
# ============================

if __name__ == "__main__":

    scraped = scrape_certin()

    save_outputs(scraped)

    print("\nDONE")
    print("Master CSV:", CSV_FILE)
    print("New JSON:", NEW_JSON)
