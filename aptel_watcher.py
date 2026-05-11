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

HARD_CAP      = 80_000  # safety net for extremely long PDFs
MAX_PDF_PAGES = 40      # page cap for extraction

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; TrilegalBot/1.0)",
    "Accept-Language": "en-US,en;q=0.9",
}

SUMMARY_QUERIES = [
    "What is the final order, decision, or direction issued by the tribunal?",
    "What are the main legal issues and questions decided in this case?",
    "What is the reasoning and legal basis for the decision?",
    "Who are the parties and what is the nature of the dispute or petition?",
]

# Lazy-loaded — one model instance per scraper run
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
        resp = requests.get(pdf_url, headers=HEADERS, timeout=40, verify=False)
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


# ================= SCRAPER =================

def scrape_orders():
    resp = requests.get(ORDERS_URL, headers=HEADERS, timeout=30, verify=False)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")

    target_table = None
    for table in soup.find_all("table"):
        if "APPEAL/PETITION" in table.get_text() or "CAUSE TITLE" in table.get_text():
            target_table = table
            break

    if not target_table:
        print("ERROR: judgements table not found on page")
        return []

    results = []
    for tr in target_table.find_all("tr"):
        cells = tr.find_all("td")
        if len(cells) < 5:
            continue

        petition_cell = cells[1]
        pdf_tag = petition_cell.find("a", href=True)
        if not pdf_tag:
            continue

        pdf_href = pdf_tag["href"].strip()
        if not pdf_href.lower().endswith(".pdf"):
            continue

        if pdf_href.startswith("http"):
            pdf_url = pdf_href
        elif pdf_href.startswith("/"):
            pdf_url = BASE_URL + pdf_href
        else:
            pdf_url = BASE_URL + "/" + pdf_href.lstrip("/")

        def clean(el):
            return re.sub(r"\s+", " ", el.get_text(" ", strip=True))

        petition_no   = clean(petition_cell)
        cause_title   = clean(cells[2])
        bench         = clean(cells[3])
        date_cell_txt = clean(cells[4])
        dates         = re.findall(r"\d{2}\.\d{2}\.\d{4}", date_cell_txt)

        results.append({
            "petition_no":      petition_no,
            "cause_title":      cause_title,
            "bench":            bench,
            "date_of_decision": dates[0] if dates else "",
            "date_uploaded":    dates[1] if len(dates) > 1 else (dates[0] if dates else ""),
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
        item_id = hashlib.sha1(entry["pdf_url"].encode()).hexdigest()[:16]
        if item_id in existing_ids:
            continue

        print(f"\n  NEW: {entry['petition_no'][:80]}")
        print(f"  Extracting + selecting PDF text …")
        pdf_text = extract_pdf_text(entry["pdf_url"])

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
            {"generated_at": datetime.utcnow().isoformat(),
             "count":        len(new_rows),
             "items":        new_rows},
            indent=2, ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    print("Done.")


if __name__ == "__main__":
    main()
