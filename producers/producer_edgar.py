"""
producers/producer_edgar.py
Fetches SEC Edgar 8-K, 10-K, 10-Q filings and publishes to Kafka topic: sec-filings
"""
import os, json, time, re, requests
from datetime import datetime
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC           = "sec-filings"
SEC_HEADERS     = {"User-Agent": "Group7-FinIntelligence yuchunwu@columbia.edu"}
BATCH_INDEX     = int(os.getenv("TICKER_BATCH_INDEX", "-1"))
BATCH_SIZE      = int(os.getenv("TICKER_BATCH_SIZE",  "10"))
FETCH_TEXT      = os.getenv("FETCH_FILING_TEXT", "true").lower() == "true"
MAX_CHARS       = int(os.getenv("MAX_FILING_CHARS", "500000"))
MAX_FILINGS     = int(os.getenv("MAX_FILINGS_PER_TICKER", "200"))
FORM_TYPES      = ["8-K", "10-K", "10-Q", "8-K/A", "10-K/A", "20-F"]

TARGET_TICKERS = list(dict.fromkeys([
    "AAPL","MSFT","GOOGL","AMZN","META","NVDA","TSLA","NFLX","ORCL","ADBE",
    "CRM","INTC","AMD","QCOM","TXN","CSCO","IBM","SNOW","PLTR","NOW",
    "JPM","BAC","GS","MS","WFC","C","BLK","AXP","V","MA",
    "SCHW","USB","PNC","TFC","COF","MET","PRU","ALL","CB","MMC",
    "JNJ","PFE","UNH","ABBV","MRK","TMO","ABT","LLY","BMY","GILD",
    "XOM","CVX","COP","SLB","EOG","PSX","VLO","MPC","OXY","HAL",
    "WMT","KO","PEP","MCD","NKE","SBUX","TGT","COST","HD","LOW",
    "BA","CAT","HON","GE","MMM","UPS","FDX","LMT","RTX","DE",
    "T","VZ","CMCSA","DIS","CHTR","AMT","PLD","NEE","DUK","SO",
]))

if BATCH_INDEX >= 0:
    start = BATCH_INDEX * BATCH_SIZE
    TARGET_TICKERS = TARGET_TICKERS[start:start + BATCH_SIZE]

def get_cik(ticker):
    try:
        r = requests.get("https://www.sec.gov/files/company_tickers.json",
                         headers=SEC_HEADERS, timeout=10)
        for entry in r.json().values():
            if entry.get("ticker","").upper() == ticker.upper():
                return str(entry["cik_str"]).zfill(10)
    except Exception as e:
        print(f"    ✗ CIK {ticker}: {e}")
    return None

def get_filings(cik, company):
    results = []
    try:
        r = requests.get(f"https://data.sec.gov/submissions/CIK{cik}.json",
                         headers=SEC_HEADERS, timeout=10)
        data = r.json()

        def extract(batch):
            forms   = batch.get("form", [])
            dates   = batch.get("filingDate", [])
            accs    = batch.get("accessionNumber", [])
            docs    = batch.get("primaryDocument", [])
            periods = batch.get("reportDate", [])
            for i, form in enumerate(forms):
                if form not in FORM_TYPES: continue
                ac = accs[i].replace("-","")
                doc_url = (f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{ac}/{docs[i]}"
                           if i < len(docs) and docs[i] else None)
                results.append({
                    "ticker": None, "company_name": company, "cik": cik,
                    "form_type": form,
                    "filed_at": dates[i] if i < len(dates) else None,
                    "period":   periods[i] if i < len(periods) else None,
                    "accession_number": accs[i],
                    "document_url": doc_url,
                    "is_material_event": form == "8-K",
                })
                if len(results) >= MAX_FILINGS: return True
            return False

        recent = data.get("filings",{}).get("recent",{})
        if not extract(recent):
            for f in data.get("filings",{}).get("files",[]):
                if len(results) >= MAX_FILINGS: break
                try:
                    r2 = requests.get(f"https://data.sec.gov/submissions/{f['name']}",
                                      headers=SEC_HEADERS, timeout=10)
                    if extract(r2.json()): break
                    time.sleep(0.1)
                except Exception: pass
    except Exception as e:
        print(f"    ✗ Filings {cik}: {e}")
    return results

def fetch_text(url):
    if not url: return ""
    try:
        r = requests.get(url, headers=SEC_HEADERS, timeout=20)
        if r.status_code != 200: return ""
        text = r.text
        # Follow index links if this is just a wrapper page
        if len(text) < 5000:
            links = re.findall(r'href=["\']([^"\']*\.(?:htm|txt))["\']', text, re.I)
            for link in links:
                if not link.startswith("http"):
                    link = url.rsplit("/",1)[0] + "/" + link
                try:
                    r2 = requests.get(link, headers=SEC_HEADERS, timeout=20)
                    if r2.status_code == 200 and len(r2.text) > len(text):
                        text = r2.text; break
                except Exception: continue
        text = re.sub(r'<[^>]+>', ' ', text)
        text = re.sub(r'\s+', ' ', text).strip()
        return text[:MAX_CHARS]
    except Exception:
        return ""

def run():
    print(f"[Edgar Producer] Connecting to Kafka at {KAFKA_BOOTSTRAP}...")
    print(f"  Tickers: {TARGET_TICKERS}")
    print(f"  Fetch text: {FETCH_TEXT} | Max chars: {MAX_CHARS:,}")

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        max_request_size=10485760,
        buffer_memory=67108864,
    )

    total = 0
    for ticker in TARGET_TICKERS:
        print(f"  {ticker}...", end=" ", flush=True)
        cik = get_cik(ticker)
        if not cik:
            print("✗ CIK not found"); continue

        filings = get_filings(cik, ticker)
        for f in filings:
            f["ticker"] = ticker
            f["fetched_at"] = datetime.utcnow().isoformat()
            if FETCH_TEXT and f.get("document_url"):
                f["full_text"] = fetch_text(f["document_url"])
                f["text_chars"] = len(f["full_text"])
                time.sleep(0.12)
            else:
                f["full_text"] = ""
                f["text_chars"] = 0
            producer.send(TOPIC, value=f)
            total += 1

        print(f"✓ {len(filings)} filings")
        time.sleep(0.15)

    producer.flush()
    producer.close()
    print(f"[Edgar Producer] ✓ Published {total} filings to '{TOPIC}'")

if __name__ == "__main__":
    run()
