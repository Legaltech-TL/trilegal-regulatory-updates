import requests
import urllib3
from bs4 import BeautifulSoup
from pathlib import Path
from datetime import datetime
import hashlib
import csv
import json
import re
import io

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

try:
    import pdfplumber
    HAS_PDFPLUMBER = True
except ImportError:
    HAS_PDFPLUMBER = False

# ================= CONFIG =================

DATA_DIR   = Path("aptel")
CSV_FILE   = DATA_DIR / "aptel_master.csv"
JSON_FILE  = DATA_DIR / "aptel_new.json"

BASE_URL   = "https://aptel.gov.in"
ORDERS_URL = f"{BASE_URL}/en/old-judgement-data"

MAX_PDF_CHARS = 12000   # ~3 000 words — plenty for Claude
MAX_PDF_PAGES = 20

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; TrilegalBot/1.0)",
    "Accept-Language": "en-US,en;q=0.9",
}


# ================= HELPERS =================

def make_id(url: str) -> str:
    return hashlib.sha1(url.encode()).hexdigest()[:16]


def clean(el) -> str:
    """Strip tags and normalise whitespace from a BS4 element."""
    return re.sub(r"\s+", " ", el.get_text(" ", strip=True))


def extract_pdf_text(pdf_url: str) -> str:
    if not HAS_PDFPLUMBER:
        return ""
    try:
        resp = requests.get(pdf_url, headers=HEADERS, timeout=40, verify=False)
        if resp.status_code != 200:
            return ""
        text_parts = []
        with pdfplumber.open(io.BytesIO(resp.content)) as pdf:
            for page in pdf.pages[:MAX_PDF_PAGES]:
                t = page.extract_text()
                if t:
                    text_parts.append(t)
        return "\n".join(text_parts).strip()[:MAX_PDF_CHARS]
    except Exception as e:
        print(f"  [PDF extract error] {pdf_url}: {e}")
        return ""


# ================= SCRAPER =================

def scrape_orders():
    resp = requests.get(ORDERS_URL, headers=HEADERS, timeout=30, verify=False)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")

    # Find the judgements table — it has "APPEAL/PETITION" in the header
    target_table = None
    for table in soup.find_all("table"):
        header_text = table.get_text()
        if "APPEAL/PETITION" in header_text or "CAUSE TITLE" in header_text:
            target_table = table
            break

    if not target_table:
        print("ERROR: judgements table not found on page")
        return []

    results = []
    for tr in target_table.find_all("tr"):
        cells = tr.find_all("td")
        if len(cells) < 5:
            continue  # skip header rows and malformed rows

        # cell[1] — petition/appeal number, with PDF <a href>
        petition_cell = cells[1]
        pdf_tag = petition_cell.find("a", href=True)
        if not pdf_tag:
            continue

        pdf_href = pdf_tag["href"].strip()
        if not pdf_href.lower().endswith(".pdf"):
            continue

        # Make absolute URL
        if pdf_href.startswith("http"):
            pdf_url = pdf_href
        elif pdf_href.startswith("/"):
            pdf_url = BASE_URL + pdf_href
        else:
            pdf_url = BASE_URL + "/" + pdf_href.lstrip("/")

        petition_no  = clean(petition_cell)
        cause_title  = clean(cells[2])
        bench        = clean(cells[3])
        date_cell_txt = clean(cells[4])

        # Date of decision — first DD.MM.YYYY in the cell
        dates = re.findall(r"\d{2}\.\d{2}\.\d{4}", date_cell_txt)
        date_of_decision = dates[0] if dates else ""
        date_uploaded    = dates[1] if len(dates) > 1 else date_of_decision

        results.append({
            "petition_no":      petition_no,
            "cause_title":      cause_title,
            "bench":            bench,
            "date_of_decision": date_of_decision,
            "date_uploaded":    date_uploaded,
            "pdf_url":          pdf_url,
        })

    return results


# ================= CSV =================

def ensure_csv():
    DATA_DIR.mkdir(exist_ok=True)
    if not CSV_FILE.exists():
        with CSV_FILE.open("w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(["id", "petition_no", "pdf_url", "scraped_at"])


def load_existing_ids() -> set:
    if not CSV_FILE.exists():
        return set()
    with CSV_FILE.open(encoding="utf-8") as f:
        return {r["id"] for r in csv.DictReader(f)}


def append_csv(rows):
    with CSV_FILE.open("a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        for r in rows:
            w.writerow([r["id"], r["petition_no"], r["pdf_url"], r["scraped_at"]])


# ================= MAIN =================

def main():
    ensure_csv()
    existing_ids = load_existing_ids()

    print("Scraping APTEL judgements/orders …")
    scraped = scrape_orders()
    print(f"  {len(scraped)} entries on page")

    new_rows = []
    for entry in scraped:
        item_id = make_id(entry["pdf_url"])
        if item_id in existing_ids:
            continue

        print(f"  NEW: {entry['petition_no'][:80]}")
        print(f"       Extracting PDF text …")
        pdf_text = extract_pdf_text(entry["pdf_url"])
        print(f"       {len(pdf_text)} chars extracted")

        new_rows.append({
            "id":               item_id,
            "petition_no":      entry["petition_no"],
            "cause_title":      entry["cause_title"],
            "bench":            entry["bench"],
            "date_of_decision": entry["date_of_decision"],
            "date_uploaded":    entry["date_uploaded"],
            "pdf_url":          entry["pdf_url"],
            "pdf_text":         pdf_text,
            "scraped_at":       datetime.utcnow().isoformat(),
        })

    print(f"\nNew entries: {len(new_rows)}")

    if new_rows:
        append_csv(new_rows)

    JSON_FILE.write_text(
        json.dumps(
            {
                "generated_at": datetime.utcnow().isoformat(),
                "count":        len(new_rows),
                "items":        new_rows,
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    print("Done.")


if __name__ == "__main__":
    main()
