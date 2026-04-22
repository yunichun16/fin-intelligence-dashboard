# 📈 Financial Intelligence Platform

> **Group 7 · Columbia University — Big Data Engineering**  
> Ce Zhang · Cai Gao · Yuchun Wu · Yanji Li

A real-time ETL pipeline that ingests financial news, SEC public filings, macroeconomic indicators, and live stock prices from four free APIs — transforms and joins them using distributed computing — and serves a unified analytical dashboard.

**Current scale:** 220,947 rows · 1.21 GB combined (48 MB PostgreSQL + 1,180 MB MongoDB) · 89 companies · 4 sources

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
| **NewsAPI** | Financial headlines from 150,000+ sources | 1,572 articles indexed | Every 15 min (free: daily) |
| **SEC Edgar** | 8-K & 10-K filings, 89 companies | 17,204 filings | On filing (near real-time) |
| **FRED** | 39 macroeconomic series back to 2010 | 109,344 data points | Daily / Monthly / Quarterly |
| **Alpaca Markets** | Daily OHLCV bars, 89 tickers, IEX feed | 92,827 rows | Daily |

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

## Scalability — Path to Enterprise

The demo runs entirely on free tiers. The architecture is intentionally layered so each component maps directly to a cloud-managed AWS equivalent — no code rewrites, only infrastructure and config changes. The table below shows how this system scales to firm-wide production serving thousands of analysts.

| Dimension | Demo (now) | Enterprise (AWS) |
|---|---|---|
| **Coverage** | 89 tickers, 4 sources | 10,000+ equities, options chains, FX, crypto, commodities, 50+ alt-data feeds |
| **Data volume** | 220,947 rows · 1.21 GB | Billions of rows · multi-TB/day ingest · petabyte data lake on S3 |
| **Update latency** | Daily batch (GitHub Actions) | Sub-second — tick-by-tick via AWS MSK (managed Kafka) + Kinesis |
| **Streaming** | 1 Kafka broker (Docker) | AWS MSK — managed multi-AZ Kafka; auto-scaling broker count |
| **Processing** | PySpark local / Colab | AWS EMR (managed Spark) + Glue serverless ETL; hundreds of worker nodes on demand |
| **Orchestration** | GitHub Actions cron | Amazon MWAA (managed Airflow) — enterprise DAGs, SLA monitoring, alerting |
| **Structured store** | PostgreSQL 48 MB (Supabase free) | Amazon Redshift — columnar MPP warehouse; petabyte-scale; 1,000s of concurrent analysts |
| **Document store** | MongoDB M0 1.18 GB (Atlas free) | MongoDB Atlas Dedicated / Amazon DocumentDB — VPC peering, 99.99% SLA |
| **Data lake** | — | AWS S3 + Lake Formation — raw/curated/aggregated zones; Parquet/Delta Lake; Athena for ad-hoc SQL |
| **Concurrency** | 1 user | Thousands of concurrent users behind AWS ALB with auto-scaling EC2 |
| **Security** | Public URL | VPC isolation · IAM roles · KMS encryption · PrivateLink · SOC 2 / FINRA-ready audit logs |
| **Compliance** | None | SEC Rule 17a-4 WORM storage · data lineage via Glue Data Catalog · full audit trail |
| **Disaster recovery** | None | Multi-region active-active · RTO < 1 hr · RPO < 5 min · automated snapshots |
| **ML / Analytics** | Dashboard charts | SageMaker for signal modeling · Bedrock for LLM filings analysis · QuickSight for BI |

### Cost estimate

| Component | Demo | Mid-size firm (~50 analysts) | Enterprise (firm-wide) |
|---|---|---|---|
| **Streaming** (MSK / Kinesis) | $0 | $800–2,000/mo | $5,000–15,000/mo |
| **Processing** (EMR / Glue) | $0 | $2,000–5,000/mo | $20,000–80,000/mo |
| **Data Warehouse** (Redshift) | $0 | $1,000–3,000/mo | $10,000–50,000/mo |
| **Document DB** (Atlas Dedicated) | $0 | $500–1,500/mo | $3,000–10,000/mo |
| **Data Lake** (S3 + Glue Catalog) | $0 | $200–800/mo | $2,000–10,000/mo |
| **Orchestration** (MWAA) | $0 | $400–800/mo | $1,500–4,000/mo |
| **Compute** (EC2 + ALB) | $0 | $500–2,000/mo | $5,000–20,000/mo |
| **ML / BI** (SageMaker + QuickSight) | $0 | $500–1,500/mo | $5,000–25,000/mo |
| **Market data feeds** | $0 (free APIs) | $2,000–10,000/mo | $50,000–200,000/mo |
| **Support + ops** | $0 | $1,000–3,000/mo | $10,000–30,000/mo |
| **Total** | **$0** | **~$9K–30K/mo** | **~$110K–440K/mo** |

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
