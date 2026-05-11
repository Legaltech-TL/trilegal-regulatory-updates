import requests
from bs4 import BeautifulSoup
from pathlib import Path
from datetime import datetime
import hashlib
import csv
import json
import re
import io

try:
    import pdfplumber
    HAS_PDFPLUMBER = True
except ImportError:
    HAS_PDFPLUMBER = False


# ================= CONFIG =================

DATA_DIR         = Path("cerc")
ORDERS_CSV       = DATA_DIR / "cerc_orders_master.csv"
ORDERS_JSON      = DATA_DIR / "cerc_orders_new.json"
REGS_CSV         = DATA_DIR / "cerc_regs_master.csv"
REGS_JSON        = DATA_DIR / "cerc_regs_new.json"

CERC_BASE        = "https://www.cercind.gov.in"
ORDERS_URL_TMPL  = f"{CERC_BASE}/recent_orders{{year}}.html"
REGS_URL         = f"{CERC_BASE}/current_reg.html"

MAX_PDF_CHARS    = 12000
MAX_PDF_PAGES    = 20

HEADERS = {
    "User-Agent":      "Mozilla/5.0 (compatible; TrilegalBot/1.0)",
    "Accept-Language": "en-US,en;q=0.9",
}


# ================= HELPERS =================

def make_id(url: str) -> str:
    return hashlib.sha1(url.encode()).hexdigest()[:16]


def clean(el) -> str:
    return re.sub(r"\s+", " ", el.get_text(" ", strip=True))


def absolute_url(href: str) -> str:
    href = href.strip()
    if href.startswith("http"):
        return href
    return CERC_BASE + "/" + href.lstrip("/")


def extract_pdf_text(pdf_url: str) -> str:
    if not HAS_PDFPLUMBER:
        return ""
    try:
        resp = requests.get(pdf_url, headers=HEADERS, timeout=40)
        if resp.status_code != 200:
            return ""
        parts = []
        with pdfplumber.open(io.BytesIO(resp.content)) as pdf:
            for page in pdf.pages[:MAX_PDF_PAGES]:
                t = page.extract_text()
                if t:
                    parts.append(t)
        return "\n".join(parts).strip()[:MAX_PDF_CHARS]
    except Exception as e:
        print(f"  [PDF extract error] {pdf_url}: {e}")
        return ""


# ================= ORDERS SCRAPER =================

def resolve_orders_url() -> tuple[str, int]:
    """Return (url, year) for the most recent available orders page."""
    current_year = datetime.utcnow().year
    for year in [current_year, current_year - 1]:
        url = ORDERS_URL_TMPL.format(year=year)
        try:
            resp = requests.get(url, headers=HEADERS, timeout=15)
            if resp.status_code == 200 and len(resp.content) > 5000:
                print(f"  Using orders page: {url}")
                return url, year
        except Exception:
            pass
    raise RuntimeError("Could not resolve a valid CERC orders URL")


def scrape_orders() -> list[dict]:
    url, year = resolve_orders_url()
    resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")

    # Find table with "Petition No." in header
    target = None
    for table in soup.find_all("table"):
        if "Petition No." in table.get_text() or "Petition" in table.get_text():
            target = table
            break

    if not target:
        print("ERROR: CERC orders table not found")
        return []

    results = []
    for tr in target.find_all("tr"):
        cells = tr.find_all("td")
        if len(cells) < 6:
            continue

        # cell[2] contains: <a href="YYYY/orders/XXX.pdf">Subject text</a>
        subject_cell = cells[2]
        pdf_tag = subject_cell.find("a", href=True)
        if not pdf_tag:
            continue

        pdf_href = pdf_tag.get("href", "").strip()
        if not pdf_href.lower().endswith(".pdf"):
            continue

        pdf_url     = absolute_url(pdf_href)
        petition_no = clean(cells[1])
        subject     = clean(subject_cell)
        date_order  = clean(cells[3])
        date_posted = clean(cells[4])
        category    = clean(cells[5])

        if not petition_no:
            continue

        results.append({
            "petition_no": petition_no,
            "subject":     subject,
            "date_order":  date_order,
            "date_posted": date_posted,
            "category":    category,
            "pdf_url":     pdf_url,
        })

    return results


# ================= REGULATIONS SCRAPER =================

_GAZ_PATTERNS  = ["gaz", "gazette", "-gz-", "/gz-"]
_SKIP_PATTERNS = _GAZ_PATTERNS + ["sor", "statement-of", "corri", "errata", "addendum",
                                   "consolidated", "amendment_2007", "amendment_2008"]


def _pick_main_pdf(reg_cell) -> tuple[str, str]:
    """
    Return (main_pdf_url, gazette_pdf_url).
    Main PDF: the actual regulation/notification document.
    We try three passes so old inconsistent naming still works.
    """
    pdf_links = reg_cell.find_all("a", href=lambda h: h and h.lower().endswith(".pdf"))
    if not pdf_links:
        return "", ""

    gazette_url = ""
    for a in pdf_links:
        href_lower = a["href"].lower()
        if any(p in href_lower for p in _GAZ_PATTERNS):
            gazette_url = absolute_url(a["href"])
            break

    # Pass 1 — explicit "Noti" in filename (recent naming convention)
    for a in pdf_links:
        if "noti" in a["href"].lower():
            return absolute_url(a["href"]), gazette_url

    # Pass 2 — skip gazette / SOR / editorial artefacts, take first remainder
    for a in pdf_links:
        href_lower = a["href"].lower()
        if not any(p in href_lower for p in _SKIP_PATTERNS):
            return absolute_url(a["href"]), gazette_url

    # Pass 3 — absolute fallback: first PDF in cell
    return absolute_url(pdf_links[0]["href"]), gazette_url


def scrape_regulations() -> list[dict]:
    resp = requests.get(REGS_URL, headers=HEADERS, timeout=30)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")

    target = None
    for table in soup.find_all("table"):
        if "CERC Regulations" in table.get_text() or "Gazette" in table.get_text():
            target = table
            break

    if not target:
        print("ERROR: CERC regulations table not found")
        return []

    results = []
    for tr in target.find_all("tr"):
        cells = tr.find_all("td")
        if len(cells) < 4:
            continue

        sl_no_text = clean(cells[0]).rstrip(".")
        if not sl_no_text.isdigit():
            continue
        sl_no = int(sl_no_text)

        reg_cell     = cells[1]
        noti_url, gazette_url = _pick_main_pdf(reg_cell)

        if not noti_url:
            continue

        # Regulation name: cell text stripped of link labels
        reg_name_raw = reg_cell.get_text(" ", strip=True)
        reg_name = re.split(r"\d\.\s+(?:Gazette|Notification|Guidelines)", reg_name_raw)[0].strip()
        reg_name = re.sub(r"\s+", " ", reg_name)

        gazette_no   = clean(cells[2])
        gazette_date = clean(cells[3])

        results.append({
            "sl_no":        sl_no,
            "reg_name":     reg_name,
            "gazette_no":   gazette_no,
            "gazette_date": gazette_date,
            "noti_pdf_url": noti_url,
            "gaz_pdf_url":  gazette_url,
        })

    return results


# ================= CSV HELPERS =================

def ensure_csv(csv_path: Path, fieldnames: list[str]):
    DATA_DIR.mkdir(exist_ok=True)
    if not csv_path.exists():
        with csv_path.open("w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(fieldnames)


def load_ids(csv_path: Path) -> set:
    if not csv_path.exists():
        return set()
    with csv_path.open(encoding="utf-8") as f:
        return {r["id"] for r in csv.DictReader(f)}


def append_to_csv(csv_path: Path, rows: list[dict], fieldnames: list[str]):
    with csv_path.open("a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writerows(rows)


def write_json(json_path: Path, items: list[dict]):
    json_path.write_text(
        json.dumps(
            {
                "generated_at": datetime.utcnow().isoformat(),
                "count":        len(items),
                "items":        items,
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )


# ================= MAIN =================

def main():
    # ---- ORDERS ----
    orders_fields = ["id", "petition_no", "pdf_url", "scraped_at"]
    ensure_csv(ORDERS_CSV, orders_fields)
    existing_order_ids = load_ids(ORDERS_CSV)

    print("Scraping CERC orders …")
    scraped_orders = scrape_orders()
    print(f"  {len(scraped_orders)} entries on page")

    new_orders = []
    for entry in scraped_orders:
        item_id = make_id(entry["pdf_url"])
        if item_id in existing_order_ids:
            continue

        print(f"  NEW order: {entry['petition_no']}")
        pdf_text = extract_pdf_text(entry["pdf_url"])
        print(f"  {len(pdf_text)} chars extracted")

        new_orders.append({
            "id":          item_id,
            "petition_no": entry["petition_no"],
            "subject":     entry["subject"],
            "date_order":  entry["date_order"],
            "date_posted": entry["date_posted"],
            "category":    entry["category"],
            "pdf_url":     entry["pdf_url"],
            "pdf_text":    pdf_text,
            "scraped_at":  datetime.utcnow().isoformat(),
        })

    print(f"  New orders: {len(new_orders)}")
    if new_orders:
        append_to_csv(ORDERS_CSV, new_orders, orders_fields)
    write_json(ORDERS_JSON, new_orders)

    # ---- REGULATIONS ----
    regs_fields = ["id", "sl_no", "reg_name", "noti_pdf_url", "scraped_at"]
    ensure_csv(REGS_CSV, regs_fields)
    existing_reg_ids = load_ids(REGS_CSV)

    print("\nScraping CERC regulations …")
    scraped_regs = scrape_regulations()
    print(f"  {len(scraped_regs)} entries on page")

    new_regs = []
    for entry in scraped_regs:
        item_id = make_id(entry["noti_pdf_url"])
        if item_id in existing_reg_ids:
            continue

        print(f"  NEW regulation: sl_no={entry['sl_no']} | {entry['reg_name'][:70]}")
        pdf_text = extract_pdf_text(entry["noti_pdf_url"])
        print(f"  {len(pdf_text)} chars extracted")

        new_regs.append({
            "id":           item_id,
            "sl_no":        entry["sl_no"],
            "reg_name":     entry["reg_name"],
            "gazette_no":   entry["gazette_no"],
            "gazette_date": entry["gazette_date"],
            "noti_pdf_url": entry["noti_pdf_url"],
            "gaz_pdf_url":  entry["gaz_pdf_url"],
            "pdf_text":     pdf_text,
            "scraped_at":   datetime.utcnow().isoformat(),
        })

    print(f"  New regulations: {len(new_regs)}")
    if new_regs:
        append_to_csv(REGS_CSV, new_regs, regs_fields)
    write_json(REGS_JSON, new_regs)

    print("\nAll done.")


if __name__ == "__main__":
    main()
