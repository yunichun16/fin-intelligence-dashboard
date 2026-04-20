"""
pipeline.py — Financial Intelligence Platform ETL
Group 7: Ce Zhang · Cai Gao · Yuchun Wu · Yanji Li

Runs the full pipeline: Extract → Transform → Load
Called by GitHub Actions daily, or manually anytime.

Usage:
    pip install requests pandas psycopg2-binary pymongo python-dotenv
    python pipeline.py

Environment variables (set in GitHub Actions Secrets or .env):
    NEWS_API_KEY, FRED_API_KEY,
    SUPABASE_HOST, SUPABASE_PORT, SUPABASE_DB, SUPABASE_USER, SUPABASE_PASSWORD,
    MONGO_URI
"""

import os, json, time, requests
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

# ── Credentials ───────────────────────────────────────────────────────────────
NEWS_API_KEY = os.environ["NEWS_API_KEY"]
FRED_API_KEY = os.environ["FRED_API_KEY"]

PG_CONFIG = {
    "host":     os.environ["SUPABASE_HOST"],
    "port":     int(os.getenv("SUPABASE_PORT", "5432")),
    "dbname":   os.getenv("SUPABASE_DB", "postgres"),
    "user":     os.environ["SUPABASE_USER"],
    "password": os.environ["SUPABASE_PASSWORD"],
    "sslmode":  "require",
}
MONGO_URI = os.environ["MONGO_URI"]

# ── Config ────────────────────────────────────────────────────────────────────
SEARCH_KEYWORDS = [
    "SEC filing earnings report",
    "earnings report quarterly results",
    "stock market financial results",
    "revenue profit loss quarterly",
    "EPS beat miss analyst expectations",
    "company acquisition merger deal",
    "IPO initial public offering stock",
    "stock buyback share repurchase",
    "dividend announcement increase cut",
    "layoffs restructuring workforce reduction",
    "Federal Reserve interest rates decision",
    "inflation CPI consumer prices",
    "unemployment jobs report labor market",
    "GDP economic growth recession",
    "Treasury yield bond market rates",
    "big tech FAANG earnings results",
    "banking financial sector JPMorgan Goldman",
    "energy oil price crude market",
    "healthcare pharma FDA approval",
    "semiconductor chip AI nvidia market",
]

TARGET_TICKERS = [
    "AAPL","MSFT","GOOGL","AMZN","META","NVDA","TSLA","NFLX","ORCL","ADBE",
    "JPM","BAC","GS","MS","WFC","C","BLK","AXP","V","MA",
    "JNJ","PFE","UNH","ABBV","MRK",
    "XOM","CVX","COP",
    "WMT","KO","PEP","MCD","NKE",
    "BA","CAT","HON",
]

FRED_SERIES = {
    "SP500":        "S&P 500 index (daily)",
    "DFF":          "Federal funds rate (daily)",
    "GS10":         "10-Year Treasury yield (daily)",
    "VIXCLS":       "CBOE VIX volatility index (daily)",
    "CPIAUCSL":     "Consumer Price Index (monthly)",
    "UNRATE":       "Unemployment rate (monthly)",
    "GDP":          "US GDP (quarterly)",
    "GDPC1":        "Real GDP (quarterly)",
    "WTISPLC":      "WTI crude oil price (monthly)",
    "M2SL":         "M2 money supply (monthly)",
    "MORTGAGE30US": "30-Year mortgage rate (weekly)",
    "UMCSENT":      "Consumer sentiment (monthly)",
}

FRED_START = "2010-01-01"
NEWS_LOOKBACK_DAYS = 7  # fetch last 7 days of news each run


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 1 — NewsAPI
# ══════════════════════════════════════════════════════════════════════════════
def fetch_news(query, from_date=None, page_size=100):
    if from_date is None:
        from_date = (datetime.utcnow() - timedelta(days=NEWS_LOOKBACK_DAYS)).strftime("%Y-%m-%d")
    params = {
        "q": query, "from": from_date, "sortBy": "publishedAt",
        "language": "en", "pageSize": page_size, "apiKey": NEWS_API_KEY,
    }
    try:
        r = requests.get("https://newsapi.org/v2/everything", params=params, timeout=10)
        r.raise_for_status()
        return [
            {
                "source_name":  a.get("source", {}).get("name"),
                "title":        a.get("title"),
                "description":  a.get("description"),
                "url":          a.get("url"),
                "published_at": a.get("publishedAt"),
                "content":      a.get("content"),
                "search_query": query,
                "fetched_at":   datetime.utcnow().isoformat(),
            }
            for a in r.json().get("articles", [])
        ]
    except Exception as e:
        print(f"  ✗ NewsAPI '{query}': {e}")
        return []


def run_news_ingestion():
    print("\n── Section 1: NewsAPI ──────────────────────────────")
    all_articles = []
    for kw in SEARCH_KEYWORDS:
        print(f"  Fetching: {kw[:50]}...")
        all_articles.extend(fetch_news(kw, page_size=100))
        time.sleep(1)
    df = pd.DataFrame(all_articles).drop_duplicates(subset=["url"]).dropna(subset=["title","url"])

    # Simple keyword sentiment
    POS = {"surge","soar","beat","record","growth","profit","strong","gain","rise","rally","exceed","boost","expand"}
    NEG = {"fall","drop","crash","miss","loss","weak","cut","layoff","decline","warn","risk","default","bankrupt","fraud"}

    def tag_category(title):
        t = str(title).lower()
        if any(w in t for w in ["earn","revenue","profit","eps"]): return "earnings"
        if any(w in t for w in ["acqui","merger","takeover","deal"]): return "m&a"
        if any(w in t for w in ["fed","rate","inflation","cpi","gdp"]): return "macro"
        if any(w in t for w in ["sec","filing","disclosure"]): return "regulatory"
        return "general"

    df["category"] = df["title"].apply(tag_category)
    print(f"  ✓ {len(df)} unique articles")
    return df.to_dict(orient="records")


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 2 — SEC Edgar
# ══════════════════════════════════════════════════════════════════════════════
SEC_HEADERS = {"User-Agent": "Group7-FinIntelligence yuchunwu@columbia.edu"}

def get_cik(ticker):
    try:
        r = requests.get("https://www.sec.gov/files/company_tickers.json", headers=SEC_HEADERS, timeout=10)
        for entry in r.json().values():
            if entry.get("ticker","").upper() == ticker.upper():
                return str(entry["cik_str"]).zfill(10)
    except Exception as e:
        print(f"    ✗ CIK {ticker}: {e}")
    return None

def get_filings(cik, form_types=["8-K","10-K"], max_filings=10):
    try:
        r = requests.get(f"https://data.sec.gov/submissions/CIK{cik}.json", headers=SEC_HEADERS, timeout=10)
        data   = r.json()
        company = data.get("name","Unknown")
        recent  = data.get("filings",{}).get("recent",{})
        forms   = recent.get("form",[])
        dates   = recent.get("filingDate",[])
        acc     = recent.get("accessionNumber",[])
        docs    = recent.get("primaryDocument",[])
        periods = recent.get("reportDate",[])
        results = []
        for i, form in enumerate(forms):
            if form in form_types:
                ac = acc[i].replace("-","")
                doc_url = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{ac}/{docs[i]}" if i < len(docs) and docs[i] else None
                results.append({
                    "company_name": company, "cik": cik, "form_type": form,
                    "filed_at": dates[i] if i < len(dates) else None,
                    "period":   periods[i] if i < len(periods) else None,
                    "accession_number": acc[i], "document_url": doc_url,
                    "fetched_at": datetime.utcnow().isoformat(),
                })
                if len(results) >= max_filings: break
        return results
    except Exception as e:
        print(f"    ✗ Filings CIK {cik}: {e}")
        return []

def run_edgar_ingestion():
    print("\n── Section 2: SEC Edgar ────────────────────────────")
    all_filings = []
    for ticker in TARGET_TICKERS:
        print(f"  {ticker}...", end=" ", flush=True)
        cik = get_cik(ticker)
        if cik:
            filings = get_filings(cik, max_filings=10)
            for f in filings: f["ticker"] = ticker
            all_filings.extend(filings)
            print(f"✓ {len(filings)}")
        else:
            print("✗ CIK not found")
        time.sleep(0.15)
    print(f"  Total: {len(all_filings)} filings")
    return all_filings


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 3 — FRED API
# ══════════════════════════════════════════════════════════════════════════════
def fetch_fred(series_id, start_date=FRED_START):
    params = {"series_id": series_id, "observation_start": start_date,
              "api_key": FRED_API_KEY, "file_type": "json", "sort_order": "asc"}
    try:
        r = requests.get("https://api.stlouisfed.org/fred/series/observations", params=params, timeout=10)
        r.raise_for_status()
        rows = []
        for obs in r.json().get("observations", []):
            val = obs.get("value",".")
            if val != ".":
                rows.append({
                    "series_code": series_id,
                    "series_name": FRED_SERIES.get(series_id, series_id),
                    "date":        obs["date"],
                    "value":       float(val),
                    "fetched_at":  datetime.utcnow().isoformat(),
                })
        return rows
    except Exception as e:
        print(f"  ✗ FRED {series_id}: {e}")
        return []

def run_fred_ingestion():
    print("\n── Section 3: FRED ─────────────────────────────────")
    all_market = []
    for sid, desc in FRED_SERIES.items():
        print(f"  {sid}: {desc[:40]}...", end=" ", flush=True)
        rows = fetch_fred(sid)
        all_market.extend(rows)
        print(f"✓ {len(rows)}")
        time.sleep(0.3)
    print(f"  Total: {len(all_market)} data points")
    return all_market


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 4 — PostgreSQL load
# ══════════════════════════════════════════════════════════════════════════════
def get_pg():
    import psycopg2
    return psycopg2.connect(**PG_CONFIG, connect_timeout=10)

def create_tables(conn):
    ddl = """
    CREATE TABLE IF NOT EXISTS market_data (
        id SERIAL PRIMARY KEY, series_code VARCHAR(50) NOT NULL,
        series_name VARCHAR(200), date DATE NOT NULL, value NUMERIC(18,6),
        fetched_at TIMESTAMPTZ, UNIQUE (series_code, date)
    );
    CREATE TABLE IF NOT EXISTS sec_filings (
        id SERIAL PRIMARY KEY, ticker VARCHAR(10), company_name VARCHAR(200),
        cik VARCHAR(20), form_type VARCHAR(20), filed_at DATE, period DATE,
        accession_number VARCHAR(50) UNIQUE, document_url TEXT,
        is_material_event BOOLEAN, fetched_at TIMESTAMPTZ
    );
    CREATE TABLE IF NOT EXISTS news_sentiment (
        id SERIAL PRIMARY KEY, url TEXT UNIQUE, title VARCHAR(500),
        source_name VARCHAR(100), published_at TIMESTAMPTZ, date_only DATE,
        category VARCHAR(50), fetched_at TIMESTAMPTZ
    );
    CREATE INDEX IF NOT EXISTS idx_market_date     ON market_data(date);
    CREATE INDEX IF NOT EXISTS idx_market_series   ON market_data(series_code);
    CREATE INDEX IF NOT EXISTS idx_filings_ticker  ON sec_filings(ticker);
    CREATE INDEX IF NOT EXISTS idx_filings_date    ON sec_filings(filed_at);
    CREATE INDEX IF NOT EXISTS idx_news_date       ON news_sentiment(date_only);
    CREATE INDEX IF NOT EXISTS idx_news_category   ON news_sentiment(category);
    """
    with conn.cursor() as cur: cur.execute(ddl)
    conn.commit()
    print("  ✓ Tables ready")

def load_market(conn, records):
    from psycopg2.extras import execute_values
    if not records: return
    rows = [(r["series_code"],r.get("series_name"),r["date"],r["value"],r.get("fetched_at")) for r in records]
    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO market_data (series_code,series_name,date,value,fetched_at)
            VALUES %s ON CONFLICT (series_code,date) DO NOTHING
        """, rows, page_size=500)
    conn.commit()
    print(f"  ✓ {len(rows)} market records loaded")

def load_filings(conn, records):
    from psycopg2.extras import execute_values
    if not records: return
    rows = [(r.get("ticker"),r.get("company_name"),r.get("cik"),r.get("form_type"),
             r.get("filed_at"),r.get("period"),r.get("accession_number"),
             r.get("document_url"),r.get("form_type")=="8-K",r.get("fetched_at")) for r in records]
    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO sec_filings (ticker,company_name,cik,form_type,filed_at,period,
                                     accession_number,document_url,is_material_event,fetched_at)
            VALUES %s ON CONFLICT (accession_number) DO NOTHING
        """, rows, page_size=200)
    conn.commit()
    print(f"  ✓ {len(rows)} filing records loaded")

def load_news_pg(conn, articles):
    from psycopg2.extras import execute_values
    if not articles: return
    rows = []
    for a in articles:
        pub = a.get("published_at")
        try: date_only = pub[:10] if pub else None
        except: date_only = None
        rows.append((a.get("url"),str(a.get("title",""))[:500],a.get("source_name"),
                     pub,date_only,a.get("category"),a.get("fetched_at")))
    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO news_sentiment (url,title,source_name,published_at,date_only,category,fetched_at)
            VALUES %s ON CONFLICT (url) DO NOTHING
        """, rows, page_size=200)
    conn.commit()
    print(f"  ✓ {len(rows)} news metadata records loaded")

def run_pg_load(articles, filings, market):
    print("\n── Section 4: PostgreSQL (Supabase) ────────────────")
    conn = get_pg()
    print(f"  ✓ Connected to {PG_CONFIG['host']}")
    create_tables(conn)
    load_market(conn, market)
    load_filings(conn, filings)
    load_news_pg(conn, articles)
    conn.close()
    print("  ✓ PostgreSQL load complete")


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 5 — MongoDB load
# ══════════════════════════════════════════════════════════════════════════════
def run_mongo_load(articles, filings):
    from pymongo import MongoClient, UpdateOne
    print("\n── Section 5: MongoDB (Atlas) ──────────────────────")
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    client.admin.command("ping")
    print("  ✓ Connected to Atlas")
    db = client["fin_intelligence"]
    news_col    = db["news_articles"]
    filings_col = db["sec_filing_documents"]
    news_col.create_index("url", unique=True)
    filings_col.create_index("accession_number", unique=True)

    if articles:
        ops = [UpdateOne({"url":a["url"]},{"$set":a},upsert=True) for a in articles if a.get("url")]
        r = news_col.bulk_write(ops, ordered=False)
        print(f"  ✓ News: {r.upserted_count} inserted, {r.modified_count} updated")

    if filings:
        ops = [UpdateOne({"accession_number":f["accession_number"]},{"$set":f},upsert=True) for f in filings if f.get("accession_number")]
        r = filings_col.bulk_write(ops, ordered=False)
        print(f"  ✓ Filings: {r.upserted_count} inserted, {r.modified_count} updated")

    client.close()
    print("  ✓ MongoDB load complete")


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    start = datetime.utcnow()
    print("=" * 55)
    print("  FINANCIAL INTELLIGENCE PIPELINE")
    print(f"  Started: {start.strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 55)

    articles = run_news_ingestion()
    filings  = run_edgar_ingestion()
    market   = run_fred_ingestion()
    run_pg_load(articles, filings, market)
    run_mongo_load(articles, filings)

    elapsed = (datetime.utcnow() - start).seconds
    print(f"\n{'='*55}")
    print(f"  DONE in {elapsed}s")
    print(f"  News: {len(articles)} · Filings: {len(filings)} · Market: {len(market)}")
    print(f"{'='*55}")
