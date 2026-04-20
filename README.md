# 📈 Financial Intelligence Platform

> **Group 7 · Columbia University — Big Data Engineering**  
> Ce Zhang · Cai Gao · Yuchun Wu · Yanji Li

A real-time ETL pipeline that ingests financial news, SEC public filings, macroeconomic indicators, and live stock prices from four free APIs — transforms and joins them using distributed computing — and serves a unified analytical dashboard.

**Live dashboard → [fin-intelligence-dashboard.streamlit.app](https://fin-intelligence-dashboard.streamlit.app)**

---

## Architecture

```
┌─────────────────────────── DATA SOURCES ────────────────────────────┐
│                                                                      │
│  NewsAPI           SEC Edgar        FRED API         Alpaca Markets  │
│  Headlines·JSON    8-K·10-K·REST   12 macro series  OHLCV·IEX feed  │
│  Near real-time    On filing        Daily/Monthly    Daily bars      │
│                                                                      │
└──────────────────┬──────────────┬──────────────┬───────────────────┘
                   │              │              │
                   ▼              ▼              ▼
┌─────────────────────── STREAMING LAYER ─────────────────────────────┐
│           Apache Kafka  (3 topics)                                   │
│     news-articles · sec-filings · market-data                       │
│  Decouples producers from consumers · durable log · no data loss    │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               ▼
┌─────────────────── PROCESSING LAYER ────────────────────────────────┐
│           Apache PySpark  (distributed)                              │
│     Deduplicate → Standardise → Categorise → Cross-source join      │
│                  orchestrated by Apache Airflow                      │
└────────────────┬─────────────────────────────┬───────────────────────┘
                 │                             │
        structured data             unstructured docs
                 ▼                             ▼
┌──────────────────┐              ┌─────────────────────────┐
│  PostgreSQL       │              │  MongoDB Atlas           │
│  (Supabase)       │              │  news_articles           │
│                   │              │  sec_filing_documents    │
│  market_data      │              └─────────────────────────┘
│  sec_filings      │
│  news_sentiment   │              ┌─────────────────────────┐
│  stock_prices     │              │  GitHub Actions          │
└──────────┬────────┘              │  Runs daily @ 06:00 UTC  │
           │                       │  pipeline.py automated   │
           ▼                       └─────────────────────────┘
┌─────────────────────── SERVE LAYER ─────────────────────────────────┐
│         Streamlit Dashboard  (Streamlit Cloud)                       │
│  Overview · Market Data · Stock Prices · SEC Filings                │
│  News Feed · Cross-Source Analysis · Alert Simulation               │
└──────────────────────────────────────────────────────────────────────┘
```

---

## Repository structure

```
fin-intelligence-dashboard/
├── app.py                        # Streamlit dashboard (7 pages)
├── pipeline.py                   # Standalone ETL script
├── requirements.txt              # Python dependencies
├── .github/
│   └── workflows/
│       └── daily_pipeline.yml   # GitHub Actions daily scheduler
├── .streamlit/
│   └── secrets.toml             # Connection secrets (never commit real values)
└── README.md
```

---

## Data sources

| Source | What we pull | Volume | Frequency |
|---|---|---|---|
| **NewsAPI** | Financial headlines from 150,000+ sources | ~2,000 articles/run | Every 15 min (free: daily) |
| **SEC Edgar** | 8-K & 10-K filings, 38 companies | ~380 filings | On filing (near real-time) |
| **FRED** | 12 macroeconomic series back to 2010 | ~15,000+ data points | Daily / Monthly / Quarterly |
| **Alpaca Markets** | Daily OHLCV bars, 38 tickers, IEX feed | ~13,000+ rows | Daily |

---

## Technology stack

| Technology | Role | Free tier used |
|---|---|---|
| **Apache Kafka** | Streaming buffer — decouples ingestion from processing | Docker (local) |
| **Apache PySpark** | Distributed transformation + cross-source join | Colab / local |
| **PostgreSQL** | Structured warehouse — 4 tables, indexed by date/ticker | Supabase free |
| **MongoDB** | Document store — full article text + filing documents | Atlas M0 free |
| **Apache Airflow** | Pipeline orchestration and DAG scheduling | Docker (local) |
| **GitHub Actions** | Daily automated pipeline runs (06:00 UTC) | GitHub free (2,000 min/mo) |
| **Streamlit Cloud** | Dashboard hosting with public URL | Streamlit free |

---

## Getting started

### Prerequisites

```bash
pip install requests pandas psycopg2-binary pymongo python-dotenv plotly streamlit
```

### Environment variables

Create a `.env` file in the project root (never commit this):

```env
# NewsAPI — free key from newsapi.org
NEWS_API_KEY=your_key_here

# FRED — free key from fred.stlouisfed.org/docs/api/api_key.html
FRED_API_KEY=your_key_here

# Supabase (PostgreSQL) — Session Pooler connection details
SUPABASE_HOST=aws-0-us-east-1.pooler.supabase.com
SUPABASE_PORT=5432
SUPABASE_DB=postgres
SUPABASE_USER=postgres.your_project_id
SUPABASE_PASSWORD=your_password

# MongoDB Atlas
MONGO_URI=mongodb+srv://user:password@cluster.mongodb.net/?appName=Cluster0

# Alpaca Markets — free paper trading API from alpaca.markets
ALPACA_API_KEY=your_key_here
ALPACA_SECRET_KEY=your_secret_here
```

### Run the pipeline manually

```bash
python pipeline.py
```

This runs the full ETL — Extract from all 4 sources → Transform with PySpark → Load into PostgreSQL and MongoDB.

### Run the dashboard locally

```bash
streamlit run app.py
```

### Automated daily runs (GitHub Actions)

The pipeline runs automatically every day at 06:00 UTC via `.github/workflows/daily_pipeline.yml`.

To set this up:
1. Go to your GitHub repo → **Settings → Secrets and variables → Actions**
2. Add all 9 secrets from the environment variables list above
3. Go to **Actions** tab → **Daily ETL Pipeline** → **Run workflow** to test manually

---

## Dashboard pages

| Page | Description |
|---|---|
| **Overview** | KPI cards, S&P 500 trend, live macro snapshot with MoM change, pipeline health |
| **Market Data** | Interactive FRED indicator charts — line/area/bar, moving average, compare mode, recession bands |
| **Stock Prices** | Alpaca OHLCV — multi-ticker comparison, candlestick chart, return correlations, volume ranking |
| **SEC Filings** | Filterable filings explorer — form type, company search, timeline, document links |
| **News Feed** | MongoDB articles — sentiment scoring, category filter, card grid / compact list view |
| **Cross-Source** | Filing + news overlap, market context with filing markers, correlation heatmap, alert simulation |
| **About** | Architecture diagram, scalability assessment, cost estimate, data quality |

---

## Database schema

### PostgreSQL (Supabase)

```sql
market_data     (series_code, series_name, date, value)
sec_filings     (ticker, company_name, form_type, filed_at, period, accession_number, document_url, is_material_event)
news_sentiment  (url, title, source_name, published_at, date_only, category)
stock_prices    (ticker, date, open, high, low, close, volume, vwap)
```

### MongoDB (Atlas)

```
news_articles          { title, description, url, published_at, content, category, source_name }
sec_filing_documents   { ticker, company_name, form_type, filed_at, accession_number, document_url }
```

---

## Scalability

| Dimension | Demo | Production path |
|---|---|---|
| Tickers | 38 | Add to `TARGET_TICKERS` list in `pipeline.py` |
| FRED series | 12 | Add to `FRED_SERIES` dict |
| News keywords | 20 | Up to 100 (free tier limit) |
| History depth | 2010–present | FRED macro back to 1947 |
| Update frequency | Daily | 3× daily — stays within free tier limits |
| DB size (estimate) | ~50–100 MB now | ~2 GB/year at current rate |
| Kafka brokers | 1 (demo) | Scale horizontally — add brokers linearly |
| Spark workers | 1 node (demo) | Add worker nodes — same code, no changes |

### Cost estimate

| Component | Demo | Production |
|---|---|---|
| Kafka | $0 | $50–150/mo |
| PySpark | $0 | $200–500/mo |
| PostgreSQL | $0 (Supabase free) | $15–50/mo |
| MongoDB | $0 (Atlas M0) | $57–200/mo |
| Airflow | $0 | $100–200/mo |
| Alpaca data | $0 (stays free) | $0 |
| GitHub Actions | $0 | $0 |
| Streamlit Cloud | $0 | $0 |
| **Total** | **$0** | **$422–1,100/mo** |

---

## Data quality

| Dimension | Implementation |
|---|---|
| **Completeness** | Deduplication by URL (news) and accession number (filings) |
| **Consistency** | PySpark schema enforcement with explicit type casting |
| **Timeliness** | Daily GitHub Actions + manual refresh button in dashboard |
| **Accuracy** | Primary sources only — no third-party aggregators |
| **Licensing** | All free-tier or public domain (SEC Edgar is US government data) |

---

## Development notes

- The **Colab notebook** (`financial_intelligence_pipeline_colab.ipynb`) is kept for documentation and debugging. Day-to-day ingestion runs via `pipeline.py` + GitHub Actions.
- The **Kafka section** is optional for local runs — `pipeline.py` loads data directly if Kafka is not running.
- **Streamlit secrets** are managed via App Settings → Secrets on Streamlit Cloud. Never commit real credentials to the repo.
- Dark mode is toggled via the sidebar ☀️ / 🌙 radio button and applies to all charts and components.

---

## License

For academic use — Columbia University APAN coursework, Spring 2026.
