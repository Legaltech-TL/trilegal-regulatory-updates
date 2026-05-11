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

DATA_DIR        = Path("cerc")
ORDERS_CSV      = DATA_DIR / "cerc_orders_master.csv"
ORDERS_JSON     = DATA_DIR / "cerc_orders_new.json"
REGS_CSV        = DATA_DIR / "cerc_regs_master.csv"
REGS_JSON       = DATA_DIR / "cerc_regs_new.json"

CERC_BASE       = "https://www.cercind.gov.in"
ORDERS_URL_TMPL = f"{CERC_BASE}/recent_orders{{year}}.html"
REGS_URL        = f"{CERC_BASE}/current_reg.html"

HARD_CAP        = 80_000
MAX_PDF_PAGES   = 40

HEADERS = {
    "User-Agent":      "Mozilla/5.0 (compatible; TrilegalBot/1.0)",
    "Accept-Language": "en-US,en;q=0.9",
}

SUMMARY_QUERIES = [
    "What is the final order, decision, or direction issued?",
    "What are the main legal issues and questions decided in this case?",
    "What is the reasoning and legal basis for the decision?",
    "Who are the parties and what is the nature of the dispute or petition?",
]

_st_model = None


# ================= SEMANTIC EXTRACTION =================

def _get_model():
    global _st_model
    if _st_model is None:
        from sentence_transformers import SentenceTransformer
        print("  Loading semantic model …")
        _st_model = SentenceTransformer("all-MiniLM-L6-v2")
        print("  Model ready.")
    return _st_model


def semantic_extract(full_text: str) -> str:
    chunks = [c.strip() for c in re.split(r"\n{2,}", full_text) if len(c.strip()) > 80]

    if not chunks:
        return full_text[:HARD_CAP]

    total = sum(len(c) for c in chunks)
    if total <= HARD_CAP:
        return full_text

    try:
        import numpy as np
        model = _get_model()

        chunk_embs = model.encode(chunks,          show_progress_bar=False, batch_size=64)
        query_embs = model.encode(SUMMARY_QUERIES, show_progress_bar=False)

        chunk_unit = chunk_embs / (np.linalg.norm(chunk_embs, axis=1, keepdims=True) + 1e-8)
        scores     = np.zeros(len(chunks))

        for q_emb in query_embs:
            q_unit = q_emb / (np.linalg.norm(q_emb) + 1e-8)
            scores = np.maximum(scores, chunk_unit @ q_unit)

        # Keep every chunk scoring >= 35% of the top score.
        # The document's own relevance distribution sets the cutoff — not a character budget.
        threshold = 0.35 * scores.max()
        selected  = []
        used      = 0

        for idx in range(len(chunks)):
            if scores[idx] >= threshold and used + len(chunks[idx]) <= HARD_CAP:
                selected.append(idx)
                used += len(chunks[idx])

        # Fallback: if threshold too aggressive, take top 10 chunks
        if not selected:
            selected = sorted(int(i) for i in np.argsort(scores)[::-1][:10])
            used = sum(len(chunks[i]) for i in selected)

        result = "\n\n".join(chunks[i] for i in selected)
        print(f"  Semantic selection: {len(selected)}/{len(chunks)} chunks "
              f"({used:,} chars from {total:,} total, threshold={threshold:.3f})")
        return result

    except Exception as e:
        print(f"  [Semantic extract fallback] {e}")
        return full_text[:HARD_CAP]


# ================= PDF EXTRACTION =================

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
        full_text = "\n\n".join(parts).strip()
        return semantic_extract(full_text)
    except Exception as e:
        print(f"  [PDF extract error] {pdf_url}: {e}")
        return ""


# ================= HELPERS =================

def make_id(url: str) -> str:
    return hashlib.sha1(url.encode()).hexdigest()[:16]


def clean(el) -> str:
    return re.sub(r"\s+", " ", el.get_text(" ", strip=True))


def absolute_url(href: str) -> str:
    href = href.strip()
    return href if href.startswith("http") else CERC_BASE + "/" + href.lstrip("/")


# ================= ORDERS SCRAPER =================

def resolve_orders_url() -> tuple:
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


def scrape_orders() -> list:
    url, _ = resolve_orders_url()
    resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")
    target = next(
        (t for t in soup.find_all("table") if "Petition No." in t.get_text()),
        None,
    )
    if not target:
        print("ERROR: CERC orders table not found")
        return []

    results = []
    for tr in target.find_all("tr"):
        cells = tr.find_all("td")
        if len(cells) < 6:
            continue
        subject_cell = cells[2]
        pdf_tag = subject_cell.find("a", href=True)
        if not pdf_tag:
            continue
        pdf_href = pdf_tag.get("href", "").strip()
        if not pdf_href.lower().endswith(".pdf"):
            continue

        results.append({
            "petition_no": clean(cells[1]),
            "subject":     clean(subject_cell),
            "date_order":  clean(cells[3]),
            "date_posted": clean(cells[4]),
            "category":    clean(cells[5]),
            "pdf_url":     absolute_url(pdf_href),
        })

    return results


# ================= REGULATIONS SCRAPER =================

_GAZ_PATTERNS  = ["gaz", "gazette", "-gz-", "/gz-"]
_SKIP_PATTERNS = _GAZ_PATTERNS + ["sor", "statement-of", "corri", "errata",
                                   "addendum", "consolidated", "amendment_2007",
                                   "amendment_2008"]


def _pick_main_pdf(reg_cell) -> tuple:
    pdf_links = reg_cell.find_all("a", href=lambda h: h and h.lower().endswith(".pdf"))
    if not pdf_links:
        return "", ""

    gazette_url = next(
        (absolute_url(a["href"]) for a in pdf_links
         if any(p in a["href"].lower() for p in _GAZ_PATTERNS)),
        "",
    )

    for a in pdf_links:
        if "noti" in a["href"].lower():
            return absolute_url(a["href"]), gazette_url

    for a in pdf_links:
        if not any(p in a["href"].lower() for p in _SKIP_PATTERNS):
            return absolute_url(a["href"]), gazette_url

    return absolute_url(pdf_links[0]["href"]), gazette_url


def scrape_regulations() -> list:
    resp = requests.get(REGS_URL, headers=HEADERS, timeout=30)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")
    target = next(
        (t for t in soup.find_all("table") if "Gazette" in t.get_text()),
        None,
    )
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

        noti_url, gazette_url = _pick_main_pdf(cells[1])
        if not noti_url:
            continue

        reg_name_raw = cells[1].get_text(" ", strip=True)
        reg_name = re.split(r"\d\.\s+(?:Gazette|Notification|Guidelines)", reg_name_raw)[0].strip()
        reg_name = re.sub(r"\s+", " ", reg_name)

        results.append({
            "sl_no":        int(sl_no_text),
            "reg_name":     reg_name,
            "gazette_no":   clean(cells[2]),
            "gazette_date": clean(cells[3]),
            "noti_pdf_url": noti_url,
            "gaz_pdf_url":  gazette_url,
        })

    return results


# ================= CSV HELPERS =================

def ensure_csv(csv_path: Path, fieldnames: list):
    DATA_DIR.mkdir(exist_ok=True)
    if not csv_path.exists():
        with csv_path.open("w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(fieldnames)


def load_ids(csv_path: Path) -> set:
    if not csv_path.exists():
        return set()
    with csv_path.open(encoding="utf-8") as f:
        return {r["id"] for r in csv.DictReader(f)}


def append_to_csv(csv_path: Path, rows: list, fieldnames: list):
    with csv_path.open("a", newline="", encoding="utf-8") as f:
        csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore").writerows(rows)


def write_json(json_path: Path, items: list):
    json_path.write_text(
        json.dumps(
            {"generated_at": datetime.utcnow().isoformat(),
             "count":        len(items),
             "items":        items},
            indent=2, ensure_ascii=False,
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

        print(f"\n  NEW order: {entry['petition_no']}")
        pdf_text = extract_pdf_text(entry["pdf_url"])

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

    print(f"\n  New orders: {len(new_orders)}")
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

        print(f"\n  NEW regulation: [{entry['sl_no']}] {entry['reg_name'][:70]}")
        pdf_text = extract_pdf_text(entry["noti_pdf_url"])

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

    print(f"\n  New regulations: {len(new_regs)}")
    if new_regs:
        append_to_csv(REGS_CSV, new_regs, regs_fields)
    write_json(REGS_JSON, new_regs)

    print("\nAll done.")


if __name__ == "__main__":
    main()
