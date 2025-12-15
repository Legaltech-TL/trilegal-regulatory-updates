from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import pandas as pd
import json
from urllib.parse import urljoin
from pathlib import Path
import hashlib

# ---------------- CONFIG ----------------
BASE_URL = "https://www.isro.gov.in"
PRESS_URL = "https://www.isro.gov.in/Press.html"

MAX_ENTRIES_TO_CHECK = 5

DATA_DIR = Path("data")
DATA_DIR.mkdir(parents=True, exist_ok=True)

MASTER_CSV = DATA_DIR / "isro_master.csv"
NEW_JSON = DATA_DIR / "isro_new_entries.json"

# ---------------- HELPERS ----------------
def generate_id(value: str) -> str:
    return hashlib.sha1(value.encode("utf-8")).hexdigest()

# ---------------- LOAD MASTER CSV ----------------
if MASTER_CSV.exists():
    master_df = pd.read_csv(MASTER_CSV)
    existing_links = set(master_df["page_link"].astype(str))
else:
    master_df = pd.DataFrame(
        columns=["id", "title", "page_link", "page_content", "date"]
    )
    existing_links = set()

print(f"[+] Loaded {len(existing_links)} existing records")

new_entries = []

# ---------------- SCRAPER ----------------
with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page()

    page.goto(PRESS_URL, wait_until="domcontentloaded", timeout=60000)
    soup = BeautifulSoup(page.content(), "html.parser")

    rows = soup.select("table tbody tr")[:MAX_ENTRIES_TO_CHECK]

    for row in rows:
        link_tag = row.select_one("a")
        if not link_tag:
            continue

        title = " ".join(link_tag.get_text(strip=True).split())
        page_link = urljoin(BASE_URL, link_tag.get("href"))

        # ---- Date = last column ----
        cells = row.find_all("td")
        date = cells[-1].get_text(strip=True) if len(cells) >= 3 else None

        if page_link in existing_links:
            continue

        print(f"[+] New press: {title}")

        # ---- Open detail page ----
        detail = browser.new_page()
        try:
            detail.goto(page_link, wait_until="domcontentloaded", timeout=30000)
        except Exception:
            print(f"[!] Skipped (slow/broken): {page_link}")
            detail.close()
            continue

        detail_soup = BeautifulSoup(detail.content(), "html.parser")
        detail.close()

        # ---- CORRECT CONTENT EXTRACTION (ISRO-specific) ----
        content_blocks = detail_soup.select("p.pageContent")

        page_content = "\n".join(
            p.get_text(strip=True)
            for p in content_blocks
            if p.get_text(strip=True)
        )

        record = {
            "id": generate_id(page_link),
            "title": title,
            "page_link": page_link,
            "page_content": page_content,
            "date": date
        }

        new_entries.append(record)
        master_df.loc[len(master_df)] = record

    browser.close()

# ---------------- WRITE OUTPUTS ----------------
master_df.to_csv(MASTER_CSV, index=False)

with open(NEW_JSON, "w", encoding="utf-8") as f:
    json.dump(new_entries, f, indent=2, ensure_ascii=False)

print(f"[✓] New entries found: {len(new_entries)}")
print(f"[✓] Master CSV updated: {MASTER_CSV}")
print(f"[✓] New entries JSON written: {NEW_JSON}")
