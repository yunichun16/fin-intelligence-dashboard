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

# Alpaca (free data API — sign up at alpaca.markets)
ALPACA_KEY    = os.environ.get("ALPACA_API_KEY", "")
ALPACA_SECRET = os.environ.get("ALPACA_SECRET_KEY", "")


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

TARGET_TICKERS = list(dict.fromkeys([
    # Big Tech (20)
    "AAPL","MSFT","GOOGL","AMZN","META","NVDA","TSLA","NFLX","ORCL","ADBE",
    "CRM","INTC","AMD","QCOM","TXN","CSCO","IBM","SNOW","PLTR","NOW",
    # Finance (20)
    "JPM","BAC","GS","MS","WFC","C","BLK","AXP","V","MA",
    "SCHW","USB","PNC","TFC","COF","MET","PRU","ALL","CB","MMC",
    # Healthcare (10)
    "JNJ","PFE","UNH","ABBV","MRK","TMO","ABT","LLY","BMY","GILD",
    # Energy (10)
    "XOM","CVX","COP","SLB","EOG","PSX","VLO","MPC","OXY","HAL",
    # Consumer (10)
    "WMT","KO","PEP","MCD","NKE","SBUX","TGT","COST","HD","LOW",
    # Industrial (10)
    "BA","CAT","HON","GE","MMM","UPS","FDX","LMT","RTX","DE",
    # Telecom & Media (5)
    "T","VZ","CMCSA","DIS","CHTR",
    # Real Estate & Utilities (5)
    "AMT","PLD","NEE","DUK","SO",
]))  # dict.fromkeys deduplicates while preserving order

# ── Batch support for parallel GitHub Actions matrix jobs ────────────────────
# When TICKER_BATCH_INDEX is set, only process that slice of tickers.
_TICKER_BATCH_INDEX = int(os.getenv("TICKER_BATCH_INDEX", "-1"))
_TICKER_BATCH_SIZE  = int(os.getenv("TICKER_BATCH_SIZE",  "10"))

# ── Apply ticker batch slicing if running in parallel CI mode ────────────────
if _TICKER_BATCH_INDEX >= 0:
    start = _TICKER_BATCH_INDEX * _TICKER_BATCH_SIZE
    end   = start + _TICKER_BATCH_SIZE
    TARGET_TICKERS = TARGET_TICKERS[start:end]
    print(f"[Batch mode] Processing tickers {start}–{end-1}: {TARGET_TICKERS}")


FRED_SERIES = {
    # Market & rates (daily — high volume)
    "SP500":        "S&P 500 index (daily)",
    "NASDAQCOM":    "NASDAQ Composite (daily)",
    "DJIA":         "Dow Jones Industrial Average (daily)",
    "DFF":          "Federal funds rate (daily)",
    "DGS10":        "10-Year Treasury yield (daily)",
    "DGS2":         "2-Year Treasury yield (daily)",
    "DGS30":        "30-Year Treasury yield (daily)",
    "T10Y2Y":       "10Y-2Y Treasury spread (daily)",
    "VIXCLS":       "CBOE VIX volatility index (daily)",
    "DEXUSEU":      "USD/EUR exchange rate (daily)",
    "DEXUSUK":      "USD/GBP exchange rate (daily)",
    "DEXJPUS":      "JPY/USD exchange rate (daily)",
    "DCOILWTICO":   "WTI crude oil spot (daily)",
    "DCOILBRENTEU": "Brent crude oil spot (daily)",
    "GOLDAMGBD228NLBM": "Gold price (daily)",
    # Macro (monthly/quarterly)
    "CPIAUCSL":     "Consumer Price Index (monthly)",
    "CPILFESL":     "Core CPI ex food & energy (monthly)",
    "PCEPI":        "PCE Price Index (monthly)",
    "UNRATE":       "Unemployment rate (monthly)",
    "PAYEMS":       "Nonfarm payrolls (monthly)",
    "ICSA":         "Initial jobless claims (weekly)",
    "GDP":          "US GDP (quarterly)",
    "GDPC1":        "Real GDP (quarterly)",
    "GDPPOT":       "Potential GDP (quarterly)",
    "WTISPLC":      "WTI crude oil price (monthly)",
    "M2SL":         "M2 money supply (monthly)",
    "M1SL":         "M1 money supply (monthly)",
    "MORTGAGE30US": "30-Year mortgage rate (weekly)",
    "MORTGAGE15US": "15-Year mortgage rate (weekly)",
    "UMCSENT":      "Consumer sentiment (monthly)",
    "RSXFS":        "Retail sales ex food (monthly)",
    "INDPRO":       "Industrial production (monthly)",
    "HOUST":        "Housing starts (monthly)",
    "HSN1F":        "New home sales (monthly)",
    "TOTALSL":      "Total consumer credit (monthly)",
    "BUSLOANS":     "Commercial & industrial loans (monthly)",
    "FEDFUNDS":     "Effective federal funds rate (monthly)",
    "BAA10Y":       "BAA corporate bond spread (daily)",
    "T10YIE":       "10-Year breakeven inflation (daily)",
}

FRED_START = "2000-01-01"   # push back to 2000 for more volume

# Alpaca config
ALPACA_BASE     = "https://data.alpaca.markets/v2"
ALPACA_LOOKBACK = 365 * 15  # 15 years of daily bars — major volume driver
NEWS_LOOKBACK_DAYS = 30  # fetch last 30 days of news each run


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
        if r.status_code == 429:
            print(f"  ⚠ NewsAPI 429 — daily 100 req/day cap reached. Skipping.")
            return []
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
    rate_limited = False
    for kw in SEARCH_KEYWORDS:
        if rate_limited:
            break
        print(f"  Fetching: {kw[:50]}...")
        results = fetch_news(kw, page_size=100)
        if not results and not rate_limited:
            # Check if we got nothing due to rate limit vs just no results
            pass
        all_articles.extend(results)
        time.sleep(1.5)  # slightly longer sleep to stay under rate limit
    df = pd.DataFrame(all_articles).drop_duplicates(subset=["url"]).dropna(subset=["title","url"]) if all_articles else pd.DataFrame()
    if df.empty:
        print("  ⚠ No articles fetched — NewsAPI may be rate limited (100 req/day cap)")
        print("  ℹ Try again tomorrow or reduce SEARCH_KEYWORDS count")
        return []

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

def get_filings(cik, form_types=["8-K","10-K","10-Q"], max_filings=100):
    """
    Fetch filings from SEC EDGAR. Pulls 'recent' batch first,
    then follows pagination files[] array to get older filings too.
    This is crucial for volume — 'recent' only has ~1000 entries.
    """
    try:
        r = requests.get(f"https://data.sec.gov/submissions/CIK{cik}.json", headers=SEC_HEADERS, timeout=10)
        data    = r.json()
        company = data.get("name", "Unknown")
        results = []

        def extract_from_batch(batch):
            forms   = batch.get("form", [])
            dates   = batch.get("filingDate", [])
            acc     = batch.get("accessionNumber", [])
            docs    = batch.get("primaryDocument", [])
            periods = batch.get("reportDate", [])
            for i, form in enumerate(forms):
                if form in form_types:
                    ac = acc[i].replace("-", "")
                    doc_url = (
                        f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{ac}/{docs[i]}"
                        if i < len(docs) and docs[i] else None
                    )
                    results.append({
                        "company_name":     company,
                        "cik":              cik,
                        "form_type":        form,
                        "filed_at":         dates[i] if i < len(dates) else None,
                        "period":           periods[i] if i < len(periods) else None,
                        "accession_number": acc[i],
                        "document_url":     doc_url,
                        "fetched_at":       datetime.utcnow().isoformat(),
                    })
                    if len(results) >= max_filings:
                        return True  # stop signal
            return False

        # Pull recent filings (most recent ~1000)
        recent = data.get("filings", {}).get("recent", {})
        if extract_from_batch(recent):
            return results

        # Follow pagination — older filings are in separate JSON files
        older_files = data.get("filings", {}).get("files", [])
        for file_info in older_files:
            if len(results) >= max_filings:
                break
            fname = file_info.get("name", "")
            if not fname:
                continue
            try:
                r2 = requests.get(
                    f"https://data.sec.gov/submissions/{fname}",
                    headers=SEC_HEADERS, timeout=10
                )
                if extract_from_batch(r2.json()):
                    break
                time.sleep(0.1)
            except Exception:
                pass

        return results
    except Exception as e:
        print(f"    ✗ Filings CIK {cik}: {e}")
        return []

def fetch_filing_text(document_url: str, max_chars: int = 500000) -> str:
    """
    Download the actual text content of an SEC filing document.
    Caps at 500KB — 10-Ks can be 5MB+ so we take the first 500KB of useful text.
    On Flex (5GB) this stores ~2-4GB of filing text across all companies.
    Returns plain text stripped of HTML tags.
    """
    if not document_url:
        return ""
    try:
        import re
        r = requests.get(document_url, headers=SEC_HEADERS, timeout=20)
        r.raise_for_status()
        text = r.text

        # If this looks like an index page rather than the actual document,
        # try to find the primary .htm/.txt document link within it
        if len(text) < 5000 and (".htm" in text.lower() or ".txt" in text.lower()):
            links = re.findall(r'href=["\']([^"\']*\.(?:htm|txt))["\']', text, re.IGNORECASE)
            for link in links:
                if not link.startswith("http"):
                    base = document_url.rsplit("/", 1)[0]
                    link = f"{base}/{link}"
                try:
                    r2 = requests.get(link, headers=SEC_HEADERS, timeout=20)
                    if r2.status_code == 200 and len(r2.text) > len(text):
                        text = r2.text
                        break
                except Exception:
                    continue

        # Strip HTML tags
        text = re.sub(r'<[^>]+>', ' ', text)
        text = re.sub(r'\s+', ' ', text).strip()
        return text[:max_chars]
    except Exception:
        return ""


def run_edgar_ingestion(fetch_text: bool = None):
    """
    fetch_text=True  → downloads full document text (slow, for weekly deep run)
    fetch_text=False → metadata only (fast, for daily run)
    fetch_text=None  → reads FETCH_FILING_TEXT env var (default False for daily CI)
    """
    if fetch_text is None:
        fetch_text = os.getenv("FETCH_FILING_TEXT", "false").lower() == "true"

    mode = "FULL TEXT" if fetch_text else "METADATA ONLY"
    print(f"\n── Section 2: SEC Edgar [{mode}] ──────────────────")
    if fetch_text:
        print("  ⚠ Full text mode — this will take 2-4 hours for all tickers")
    else:
        print("  Fast mode — metadata only, no HTTP text downloads")

    # Deduplicate tickers
    seen = set()
    unique_tickers = [t for t in TARGET_TICKERS if t not in seen and not seen.add(t)]

    all_filings = []
    for ticker in unique_tickers:
        print(f"  {ticker}...", end=" ", flush=True)
        cik = get_cik(ticker)
        if cik:
            # Daily: last 20 filings only. Full text: all 200
            max_f = 200 if fetch_text else 20
            filings = get_filings(
                cik,
                form_types=["8-K","10-K","10-Q","8-K/A","10-K/A","20-F"],
                max_filings=max_f
            )
            for f in filings:
                f["ticker"] = ticker
                if fetch_text and f.get("document_url"):
                    f["full_text"] = fetch_filing_text(f["document_url"])
                    f["text_chars"] = len(f.get("full_text", ""))
                    time.sleep(0.12)  # polite to SEC — 8 req/sec max
                else:
                    f["full_text"] = ""
                    f["text_chars"] = 0
            all_filings.extend(filings)
            if fetch_text:
                total_mb = sum(f.get("text_chars", 0) for f in all_filings) / 1024 / 1024
                print(f"✓ {len(filings)} filings | {total_mb:.1f} MB total")
            else:
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
# SECTION 4 — Alpaca: stock price bars
# ══════════════════════════════════════════════════════════════════════════════
def fetch_alpaca_bars(tickers, lookback_days=ALPACA_LOOKBACK):
    """
    Fetch daily OHLCV bars for all tickers in a single API call.
    Uses the IEX free data feed — no exchange license needed.

    Args:
        tickers      : list of ticker symbols
        lookback_days: how many calendar days of history to fetch

    Returns:
        List of bar dicts ready for PostgreSQL
    """
    if not ALPACA_KEY or not ALPACA_SECRET:
        print("  ⚠ ALPACA_API_KEY not set — skipping stock price ingestion")
        return []

    from datetime import timezone
    start_dt = (datetime.utcnow() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    headers  = {
        "APCA-API-KEY-ID":     ALPACA_KEY,
        "APCA-API-SECRET-KEY": ALPACA_SECRET,
    }

    all_bars = []
    # Alpaca accepts up to 100 symbols per request
    batch_size = 100
    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i+batch_size]
        params = {
            "symbols":    ",".join(batch),
            "timeframe":  "1Day",
            "start":      start_dt,
            "feed":       "iex",       # free feed — no license fee
            "limit":      10000,
            "adjustment": "all",       # split & dividend adjusted
        }
        try:
            r = requests.get(f"{ALPACA_BASE}/stocks/bars",
                             headers=headers, params=params, timeout=15)
            r.raise_for_status()
            bars_by_ticker = r.json().get("bars", {})
            for ticker, bars in bars_by_ticker.items():
                for bar in bars:
                    all_bars.append({
                        "ticker":     ticker,
                        "date":       bar["t"][:10],   # ISO date string
                        "open":       bar["o"],
                        "high":       bar["h"],
                        "low":        bar["l"],
                        "close":      bar["c"],
                        "volume":     bar["v"],
                        "vwap":       bar.get("vw"),
                        "fetched_at": datetime.utcnow().isoformat(),
                    })
        except Exception as e:
            print(f"  ✗ Alpaca batch {batch[:3]}...: {e}")

    return all_bars


def run_alpaca_ingestion():
    print("\n── Section 4: Alpaca Stock Prices ─────────────────")
    if not ALPACA_KEY:
        print("  ⚠ Skipping — add ALPACA_API_KEY and ALPACA_SECRET_KEY to env")
        return []
    bars = fetch_alpaca_bars(TARGET_TICKERS)
    print(f"  ✓ {len(bars)} daily bars across {len(TARGET_TICKERS)} tickers")
    return bars


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 5 — PostgreSQL load
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

    CREATE TABLE IF NOT EXISTS stock_prices (
        id         SERIAL PRIMARY KEY,
        ticker     VARCHAR(10)  NOT NULL,
        date       DATE         NOT NULL,
        open       NUMERIC(12,4),
        high       NUMERIC(12,4),
        low        NUMERIC(12,4),
        close      NUMERIC(12,4),
        volume     BIGINT,
        vwap       NUMERIC(12,4),
        fetched_at TIMESTAMPTZ,
        UNIQUE (ticker, date)
    );
    CREATE INDEX IF NOT EXISTS idx_prices_ticker ON stock_prices(ticker);
    CREATE INDEX IF NOT EXISTS idx_prices_date   ON stock_prices(date);
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

def load_stock_prices(conn, bars):
    """Upsert Alpaca daily OHLCV bars into stock_prices table."""
    from psycopg2.extras import execute_values
    if not bars: return
    rows = [(b["ticker"],b["date"],b["open"],b["high"],b["low"],
             b["close"],b["volume"],b.get("vwap"),b.get("fetched_at")) for b in bars]
    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO stock_prices (ticker,date,open,high,low,close,volume,vwap,fetched_at)
            VALUES %s ON CONFLICT (ticker,date) DO UPDATE SET
                close=EXCLUDED.close, high=EXCLUDED.high, low=EXCLUDED.low,
                volume=EXCLUDED.volume, vwap=EXCLUDED.vwap, fetched_at=EXCLUDED.fetched_at
        """, rows, page_size=500)
    conn.commit()
    print(f"  ✓ {len(rows)} stock price bars loaded")


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

def run_pg_load(articles, filings, market, stock_bars):
    print("\n── Section 5: PostgreSQL (Supabase) ────────────────")
    conn = get_pg()
    print(f"  ✓ Connected to {PG_CONFIG['host']}")
    create_tables(conn)
    load_market(conn, market)
    load_filings(conn, filings)
    load_news_pg(conn, articles)
    load_stock_prices(conn, stock_bars)
    conn.close()
    print("  ✓ PostgreSQL load complete")


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 6 — MongoDB load
# ══════════════════════════════════════════════════════════════════════════════
def run_mongo_load(articles, filings):
    """
    Load unstructured document data into MongoDB — the correct store for this data.

    Why MongoDB and NOT PostgreSQL for this:
    - SEC filing documents have variable-length text (1KB to 5MB+ per document)
    - News articles have nested JSON fields (source, author, content)
    - No fixed schema — fields vary by filing type (8-K vs 10-K vs 10-Q)
    - MongoDB handles large text blobs efficiently via BSON
    - PostgreSQL TEXT columns work but are not optimized for document retrieval
    - This separation is the core of the polyglot persistence design
    """
    from pymongo import MongoClient, UpdateOne
    from pymongo.errors import OperationFailure, BulkWriteError
    print("\n── Section 6: MongoDB (Atlas) — document store ─────")
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        print("  ✓ Connected to Atlas")
    except Exception as e:
        print(f"  ✗ MongoDB connection failed: {e}")
        print("  ⚠ Skipping MongoDB load — data saved to PostgreSQL only")
        return

    db          = client["fin_intelligence"]
    news_col    = db["news_articles"]
    filings_col = db["sec_filing_documents"]
    news_col.create_index("url", unique=True)
    filings_col.create_index("accession_number", unique=True)

    # ── News articles ─────────────────────────────────────────────────────────
    if articles:
        try:
            # Store full article content in MongoDB (not just metadata)
            # This is the correct place for unstructured text — not PostgreSQL
            ops = [UpdateOne(
                {"url": a["url"]},
                {"$set": {
                    **a,
                    "content_length": len(str(a.get("content", "") or "")),
                    "has_full_content": bool(a.get("content")),
                }},
                upsert=True
            ) for a in articles if a.get("url")]
            r = news_col.bulk_write(ops, ordered=False)
            print(f"  ✓ News: {r.upserted_count} inserted, {r.modified_count} updated")
        except (OperationFailure, BulkWriteError) as e:
            err = str(e)
            if "space quota" in err or "8000" in err:
                print("  ✗ MongoDB Flex quota hit — check Atlas dashboard for storage usage")
            else:
                print(f"  ✗ News write error: {e}")

    # ── SEC filing documents ──────────────────────────────────────────────────
    if filings:
        # FORCE_REINDEX=true clears existing full_text so they get re-fetched at 500KB
        # Use this when upgrading from a lower max_chars setting
        force_reindex = os.getenv("FORCE_REINDEX", "false").lower() == "true"
        if force_reindex:
            print("  ⚠ FORCE_REINDEX=true — clearing existing full_text for re-fetch")
            filings_col.update_many(
                {"full_text": {"$exists": True, "$not": {"$gt": ""}}},
                {"$set": {"full_text": ""}}
            )

        # Write in small batches so a quota error doesn't lose all progress
        BATCH_SIZE = 50
        total_inserted = total_updated = total_skipped = 0

        for i in range(0, len(filings), BATCH_SIZE):
            batch = filings[i:i+BATCH_SIZE]
            ops   = []
            for f in batch:
                if not f.get("accession_number"): continue
                full_text = f.get("full_text", "")
                doc = {k:v for k,v in f.items() if k != "full_text"}

                if full_text:
                    # We fetched real text — always write it (upgrades old 50KB docs to 500KB)
                    update_op = {"$set": {**doc, "full_text": full_text}}
                else:
                    # No text this run (fast/metadata mode) — preserve existing text
                    update_op = {"$set": doc, "$setOnInsert": {"full_text": ""}}

                ops.append(UpdateOne(
                    {"accession_number": f["accession_number"]},
                    update_op,
                    upsert=True
                ))
            if not ops: continue
            try:
                r = filings_col.bulk_write(ops, ordered=False)
                total_inserted += r.upserted_count
                total_updated  += r.modified_count
            except (OperationFailure, BulkWriteError) as e:
                err = str(e)
                if "space quota" in err or "8000" in err:
                    total_skipped += len(ops)
                    print(f"  ✗ Quota exceeded at batch {i//BATCH_SIZE} — "
                          f"{total_inserted} inserted before limit hit")
                    print("  ℹ Upgrade Atlas M0 → M10 ($57/mo) for 10 GB storage")
                    print("  ℹ All metadata is safe in PostgreSQL (no size limit issue there)")
                    break
                else:
                    print(f"  ✗ Batch {i//BATCH_SIZE} error: {e}")

        print(f"  ✓ Filings: {total_inserted} inserted, {total_updated} updated"
              + (f", {total_skipped} skipped (quota)" if total_skipped else ""))

    client.close()
    print("  ✓ MongoDB section complete")


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    start = datetime.utcnow()

    # In batch mode: only batch 0 fetches news + FRED (shared data)
    # All batches fetch their own ticker slice for SEC + Alpaca
    batch_mode   = _TICKER_BATCH_INDEX >= 0
    is_primary   = _TICKER_BATCH_INDEX <= 0   # batch 0 or non-batch mode
    skip_news    = os.getenv("SKIP_NEWS",  "false").lower() == "true"
    skip_fred    = os.getenv("SKIP_FRED",  "false").lower() == "true"
    skip_alpaca  = os.getenv("SKIP_ALPACA","false").lower() == "true"

    print("=" * 55)
    print("  FINANCIAL INTELLIGENCE PIPELINE")
    print(f"  Started: {start.strftime('%Y-%m-%d %H:%M UTC')}")
    if batch_mode:
        print(f"  Batch: {_TICKER_BATCH_INDEX} | Tickers: {TARGET_TICKERS}")
    print("=" * 55)

    articles   = run_news_ingestion()   if (is_primary and not skip_news)   else []
    market     = run_fred_ingestion()   if (is_primary and not skip_fred)   else []
    filings    = run_edgar_ingestion()
    stock_bars = run_alpaca_ingestion() if not skip_alpaca                  else []
    run_pg_load(articles, filings, market, stock_bars)
    run_mongo_load(articles, filings)

    elapsed = (datetime.utcnow() - start).seconds
    print(f"\n{'='*55}")
    print(f"  DONE in {elapsed}s")
    print(f"  News: {len(articles)} · Filings: {len(filings)}")
    print(f"  Market: {len(market)} · Stock bars: {len(stock_bars)}")
    print(f"{'='*55}")
