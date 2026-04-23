# 📈 Financial Intelligence Platform

> **Group 7 · Columbia University — Big Data Engineering**
> Ce Zhang · Cai Gao · Yuchun Wu · Yanji Li

A real-time ETL pipeline that ingests financial news, SEC public filings, macroeconomic indicators, and live stock prices from four free APIs — streams them through Apache Kafka, transforms and joins them with PySpark, and serves a unified analytical dashboard.

The entire stack runs locally via Docker Compose, orchestrated by Apache Airflow, with persistent storage in Supabase (PostgreSQL) and MongoDB Atlas.

**Current scale:** 220,947 rows · 1.21 GB combined (48 MB PostgreSQL + 1,180 MB MongoDB) · 89 companies · 4 sources · 635K+ Kafka messages processed

---

## Architecture

```
┌─────────────────────────── DATA SOURCES ────────────────────────────┐
│                                                                      │
│  NewsAPI           SEC Edgar        FRED API         Alpaca Markets  │
│  Headlines·JSON    8-K·10-K·REST   39 macro series  OHLCV·IEX feed   │
│  Near real-time    On filing        Daily/Monthly    Daily bars      │
│                                                                      │
└─────────┬─────────────┬─────────────┬─────────────┬─────────────────┘
          │             │             │             │
          ▼             ▼             ▼             ▼
┌──────────────────── PRODUCERS (Airflow Tasks) ──────────────────────┐
│                                                                      │
│  produce_news      produce_edgar    produce_market (FRED + Alpaca)  │
│  BashOperator → Python script → kafka-python client                 │
│                                                                      │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────── STREAMING LAYER ─────────────────────────────┐
│              Apache Kafka 7.5.0  (Docker container)                  │
│     4 topics:  news-articles · sec-filings · market-data · stock-prices │
│     Broker: kafka:29092 (internal) · localhost:9092 (external)      │
│     Zookeeper coordination · 3 partitions/topic                      │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               ▼
┌──────────────────── PROCESSING LAYER ───────────────────────────────┐
│         PySpark 3.5  (spark-submit --master local[*])                │
│                                                                      │
│  Batch read from Kafka → dedupe on natural keys → schema enforce →  │
│  Cross-source join (filings ± 7d window with news) →                │
│  Dual sink: JDBC to Postgres · Mongo Spark Connector to Atlas       │
│                                                                      │
│     Runs inside the Airflow scheduler container                      │
│     Packages: spark-sql-kafka-0-10 · mongo-spark-connector · postgresql │
└────────────────┬─────────────────────────────┬──────────────────────┘
                 │                             │
        structured data                 unstructured docs
                 ▼                             ▼
┌────────────────────────┐         ┌──────────────────────────┐
│  PostgreSQL             │         │  MongoDB Atlas            │
│  (Supabase · cloud)     │         │  (M0 free · cloud)        │
│                         │         │                           │
│  market_data            │         │  news_articles            │
│  sec_filings            │         │  sec_filing_documents     │
│  news_sentiment         │         │                           │
│  stock_prices           │         │  Full article text +      │
│                         │         │  filing document content  │
│  4 tables · 220,947 rows│         │  1.18 GB · flexible schema│
└──────────┬──────────────┘         └─────────────┬─────────────┘
           │                                      │
           └──────────────┬───────────────────────┘
                          │
                          ▼
┌──────────────────── ORCHESTRATION ──────────────────────────────────┐
│                  Apache Airflow 2.8.1                                │
│                                                                      │
│     DAG: finintel_pipeline  (schedule: 0 6 * * * UTC)               │
│     [produce_news, produce_edgar, produce_market] >> spark_transform│
│         >> log_complete                                              │
│                                                                      │
│     Metadata DB: PostgreSQL 15 (finintel-airflow-db container)       │
│     Webserver UI: http://localhost:8080  (admin / finintel)         │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               ▼
┌────────────────────────── SERVE LAYER ──────────────────────────────┐
│                Streamlit Dashboard  (local, port 8501)               │
│                                                                      │
│  7 pages:  Overview · Market Data · Stock Prices · SEC Filings       │
│            News Feed · Cross-Source · About                          │
│                                                                      │
│  Reads from Supabase + MongoDB Atlas directly                        │
│  Dark/Light theme toggle · 89 tickers · interactive Plotly charts   │
└──────────────────────────────────────────────────────────────────────┘
```

---

## Docker stack

All infrastructure runs in Docker containers via `docker-compose.yml`:

| Container | Image | Role | Ports |
|---|---|---|---|
| `finintel-zookeeper` | `confluentinc/cp-zookeeper:7.5.0` | Kafka coordination | 2181 |
| `finintel-kafka` | `confluentinc/cp-kafka:7.5.0` | Message broker · 4 topics | 9092 (external), 29092 (internal) |
| `finintel-airflow-db` | `postgres:15` | Airflow metadata DB | 5432 (internal) |
| `finintel-airflow-webserver` | custom (see `airflow-docker/Dockerfile`) | Airflow UI + scheduler | 8080 |
| `finintel-airflow-scheduler` | custom (same image) | DAG execution + Spark worker | — |

The custom Airflow image extends `apache/airflow:2.8.1-python3.11` with OpenJDK 17, PySpark 3.5, and `spark-submit` on PATH so `spark_transform` can run Spark jobs natively inside Airflow. The Dockerfile uses `dpkg --print-architecture` so it builds correctly on both Apple Silicon (arm64) and Intel (amd64).

---

## Repository structure

```
fin-intelligence-dashboard/
├── app.py                        # Streamlit dashboard (7 pages)
├── pipeline.py                   # Standalone Python ETL (alternative to the DAG)
├── docker-compose.yml            # 5-container stack definition
├── airflow-docker/
│   └── Dockerfile                # Custom Airflow image (adds Java + PySpark)
├── airflow/
│   └── dags/
│       └── finintel_dag.py       # Orchestration: 5-task DAG
├── producers/
│   ├── producer_news.py          # NewsAPI → Kafka
│   ├── producer_edgar.py         # SEC Edgar → Kafka
│   └── producer_market.py        # FRED + Alpaca → Kafka
├── spark/
│   └── spark_consumer.py         # Kafka → transform → Postgres + Mongo
├── requirements.txt              # Streamlit + dashboard deps
├── .streamlit/
│   └── secrets.toml              # Connection secrets (git-ignored)
├── .env                          # API keys and DB credentials (git-ignored)
└── README.md
```

---

## Data sources

| Source | What we pull | Volume | Frequency |
|---|---|---|---|
| **NewsAPI** | Financial headlines from 150,000+ sources | 1,572 articles indexed | 20 queries per run |
| **SEC Edgar** | 8-K & 10-K filings, 89 companies | 17,204 filings | On filing (polled daily) |
| **FRED** | 39 macroeconomic series back to 2000 | 109,344 data points | Daily / Monthly / Quarterly |
| **Alpaca Markets** | Daily OHLCV bars, 89 tickers, IEX feed | 92,827 rows | Daily (paper trading API) |

---

## Technology stack

| Layer | Technology | Role |
|---|---|---|
| **Streaming** | Apache Kafka 7.5.0 | Buffered message queue decoupling producers from consumers |
| **Coordination** | Apache Zookeeper 7.5.0 | Kafka cluster state management |
| **Processing** | Apache PySpark 3.5.0 | Distributed batch transformations + cross-source joins |
| **Runtime** | OpenJDK 17 | JVM for Spark |
| **Orchestration** | Apache Airflow 2.8.1 | DAG scheduling, task retry, dependency management |
| **Structured store** | PostgreSQL 15 (Supabase) | 4 relational tables, indexed by date/ticker |
| **Document store** | MongoDB Atlas M0 | Full-text filing docs + article content, flexible schema |
| **Serve** | Streamlit + Plotly | Interactive dashboard with 7 analytical views |
| **Containerisation** | Docker Compose | 5-service local deployment |

---

## Getting started

### Prerequisites

- Docker Desktop (with at least 6 GB RAM allocated)
- Python 3.11+ (for Streamlit dashboard on host)
- Free API accounts: [NewsAPI](https://newsapi.org), [FRED](https://fred.stlouisfed.org/docs/api/api_key.html), [Alpaca](https://alpaca.markets) paper trading
- Free cloud DB accounts: [Supabase](https://supabase.com), [MongoDB Atlas](https://www.mongodb.com/cloud/atlas)

### 1. Environment variables

Create a `.env` file in the project root (git-ignored):

```env
# NewsAPI
NEWS_API_KEY=your_key_here

# FRED
FRED_API_KEY=your_key_here

# Supabase (PostgreSQL) — Session Pooler details
SUPABASE_HOST=aws-1-us-east-1.pooler.supabase.com
SUPABASE_PORT=5432
SUPABASE_DB=postgres
SUPABASE_USER=postgres.your_project_id
SUPABASE_PASSWORD=your_password

# MongoDB Atlas
MONGO_URI=mongodb+srv://user:password@cluster.mongodb.net/?appName=Cluster0

# Alpaca Markets (paper trading)
ALPACA_API_KEY=your_key_here
ALPACA_SECRET_KEY=your_secret_here

# Kafka (internal Docker hostname)
KAFKA_BOOTSTRAP=kafka:29092
```

### 2. Build the custom Airflow image and start the stack

```bash
docker compose up -d --build
```

First build takes ~10 minutes (pulls ~2 GB of images, installs OpenJDK + PySpark). Subsequent starts take ~60 seconds.

Verify:

```bash
docker compose ps
```

All five containers should show `Up` and Airflow ones `(healthy)`.

### 3. Import API keys and DB credentials into Airflow

Convert `.env` to the JSON format Airflow expects, then import:

```bash
python3 -c "
import json
with open('.env') as f:
    d = {k: v for line in f if '=' in line and not line.startswith('#')
         for k, v in [line.strip().split('=', 1)]}
with open('airflow_vars.json', 'w') as f:
    json.dump(d, f, indent=2)
"

docker compose cp airflow_vars.json airflow-webserver:/tmp/airflow_vars.json
docker compose exec airflow-webserver airflow variables import /tmp/airflow_vars.json
```

Expected: `11 of 11 variables successfully updated.`

### 4. Configure Streamlit secrets

Create `.streamlit/secrets.toml` (git-ignored):

```toml
[postgres]
host     = "aws-1-us-east-1.pooler.supabase.com"
port     = 5432
dbname   = "postgres"
user     = "postgres.your_project_id"
password = "your_password"

MONGO_URI = "mongodb+srv://user:password@cluster.mongodb.net/?appName=Cluster0"
```

### 5. Trigger the pipeline

Open the Airflow UI at http://localhost:8080 (login `admin` / `finintel`), find `finintel_pipeline`, and click the ▶ trigger button.

Or from the CLI:

```bash
docker compose exec airflow-webserver airflow dags trigger finintel_pipeline
```

Takes ~10 minutes end-to-end: producers fire in parallel (~2 min), Spark resolves Maven dependencies + runs transforms (~5 min), results land in Supabase + MongoDB.

### 6. Run the dashboard

```bash
pip install -r requirements.txt
streamlit run app.py
```

Open http://localhost:8501.

---

## DAG structure

```
finintel_pipeline  (schedule: 0 6 * * * UTC)

    ┌─────────────────┐
    │  produce_news   │──┐
    │  (BashOperator) │  │
    └─────────────────┘  │
                         │
    ┌─────────────────┐  │  ┌──────────────────┐      ┌─────────────────┐
    │ produce_market  │──┼─▶│  spark_transform │─────▶│  log_complete   │
    │  (BashOperator) │  │  │  (BashOperator)  │      │ (PythonOperator)│
    └─────────────────┘  │  └──────────────────┘      └─────────────────┘
                         │
    ┌─────────────────┐  │
    │ produce_edgar   │──┘
    │  (BashOperator) │
    └─────────────────┘
```

Three producers run in parallel, each publishing to its own Kafka topic. `spark_transform` fans-in after all three complete, reads from Kafka, and lands data in both warehouses. `log_complete` posts a run summary.

---

## Spark job (spark_consumer.py)

`spark-submit` invoked from the Airflow scheduler container:

```bash
spark-submit \
  --master local[*] \
  --conf spark.jars.ivy=/tmp/ivy \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,\
             org.mongodb.spark:mongo-spark-connector_2.12:10.3.0,\
             org.postgresql:postgresql:42.7.1 \
  /opt/airflow/spark/spark_consumer.py
```

**Operations performed per Kafka topic:**

1. **Batch read** from Kafka with explicit schema (StructType per topic)
2. **Filter + dedupe** on natural keys (URL, accession_number, `series_code+date`, `ticker+date`)
3. **Type cast** date/timestamp columns
4. **Cross-source join** (filings × news within ±7 days on `datediff`)
5. **Dual sink** — JDBC write to Supabase + Mongo Spark Connector write to Atlas

Deduplication is idempotent: reruns produce the same final state.

---

## Dashboard pages

| Page | Description |
|---|---|
| **Overview** | KPI cards, S&P 500 trend (50-day MA), filings-by-company, cross-source metrics |
| **Market Data** | Interactive FRED charts — line/area/bar, moving averages, compare mode, recession bands |
| **Stock Prices** | Alpaca OHLCV — multi-ticker comparison, candlestick, return correlations, volume ranking |
| **SEC Filings** | Filterable explorer — form type, company search, timeline, document links |
| **News Feed** | MongoDB articles — sentiment scoring, category filter, card grid / compact list |
| **Cross-Source** | Filing + news overlap, market context with filing markers, correlation heatmap |
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
news_articles         { title, description, url, published_at, content, category, source_name }
sec_filing_documents  { ticker, company_name, form_type, filed_at, accession_number, full_text }
```

---

## Operations

### Inspecting Kafka

```bash
# List topics
docker compose exec kafka kafka-topics --bootstrap-server kafka:29092 --list

# Count messages per topic (lifetime offsets)
for topic in news-articles market-data sec-filings stock-prices; do
  total=$(docker compose exec kafka kafka-run-class kafka.tools.GetOffsetShell \
    --broker-list kafka:29092 --topic $topic --time -1 2>/dev/null \
    | awk -F: '{sum += $3} END {print sum}')
  echo "$topic: $total messages"
done

# Peek at actual JSON messages
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic market-data \
  --from-beginning --max-messages 3 --timeout-ms 5000
```

### Pausing the scheduled DAG

```bash
docker compose exec airflow-webserver airflow dags pause finintel_pipeline
```

### Stopping vs tearing down

- `docker compose stop` — pause containers (preserves state and Airflow Variables)
- `docker compose down` — delete containers (wipes Airflow Variables and Kafka data; you'll need to re-import and re-trigger)

### Viewing Spark logs

In the Airflow UI: click `spark_transform` task → **Logs** tab. Look for `SparkSession created`, per-topic row counts, and JDBC / Mongo write confirmations.

---

## Scalability — Path to Enterprise

The demo stack runs entirely on free tiers and local Docker. Every layer maps directly to a cloud-managed AWS equivalent — no code rewrites, only infrastructure and config changes. The table below shows how this system scales to firm-wide production serving thousands of analysts.

| Dimension | Demo (now) | Enterprise (AWS) |
|---|---|---|
| **Coverage** | 89 tickers, 4 sources | 10,000+ equities, options, FX, crypto, commodities, 50+ alt-data feeds |
| **Data volume** | 220,947 rows · 1.21 GB | Billions of rows · multi-TB/day ingest · petabyte data lake on S3 |
| **Update latency** | Daily batch (cron-scheduled DAG) | Sub-second — tick-by-tick via AWS MSK + Kinesis |
| **Streaming** | 1 Kafka broker (Docker) | AWS MSK — managed multi-AZ Kafka; auto-scaling brokers |
| **Processing** | PySpark `local[*]` in Airflow container | AWS EMR (managed Spark) + Glue serverless ETL; hundreds of workers |
| **Orchestration** | Airflow in Docker | Amazon MWAA (managed Airflow) — enterprise DAGs, SLA alerting |
| **Structured store** | Supabase free (48 MB) | Amazon Redshift — columnar MPP warehouse; petabyte-scale |
| **Document store** | Atlas M0 (1.18 GB) | Atlas Dedicated / Amazon DocumentDB — VPC peering, 99.99% SLA |
| **Data lake** | — | AWS S3 + Lake Formation — raw/curated/aggregated zones; Parquet/Delta |
| **Concurrency** | 1 user | Thousands behind AWS ALB with auto-scaling EC2 |
| **Security** | Local only | VPC isolation · IAM · KMS encryption · PrivateLink · SOC 2 / FINRA audit logs |
| **Compliance** | None | SEC Rule 17a-4 WORM storage · Glue Data Catalog lineage · full audit trail |
| **Disaster recovery** | None | Multi-region active-active · RTO < 1 hr · RPO < 5 min |
| **ML / Analytics** | Dashboard charts | SageMaker for signals · Bedrock for LLM filings analysis · QuickSight for BI |

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
| **Completeness** | Spark `dropDuplicates` on URL (news), accession_number (filings), (series_code, date) for market, (ticker, date) for prices |
| **Consistency** | PySpark explicit `StructType` schemas + type casting for every Kafka topic |
| **Timeliness** | Airflow daily cron + manual trigger · Streamlit refresh button |
| **Accuracy** | Primary sources only — no third-party aggregators |
| **Licensing** | All free-tier or public domain (SEC Edgar is US government data) |

---

## Development notes

- **Kafka data is non-persistent** — messages live only as long as the container does. Once Spark consumes a batch and writes to the warehouses, Kafka's copy is expendable. To populate a topic for demo purposes, clear and rerun the relevant producer task in Airflow.
- **Airflow Variables are wiped on `docker compose down`** — metadata DB volume is ephemeral. Use `docker compose stop` for pausing. Re-import variables after a full tear-down.
- **Ivy cache can get corrupted** on first Spark runs. The DAG works around this by pointing Ivy at `/tmp/ivy` (tmpfs) and wiping it before each run.
- **Apple Silicon / Intel compatibility** — the Dockerfile detects architecture at build time so the same repo builds cleanly on M-series Macs and x86 machines.
- **`load_dotenv()` in producers is wrapped in try/except** — inside Airflow containers there's no `.env` file (Variables pass env vars directly), but the producers still run standalone for local debugging.

---

## License

For academic use — Columbia University APAN coursework, Spring 2026.
