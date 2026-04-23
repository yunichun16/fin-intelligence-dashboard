"""
airflow/dags/finintel_dag.py
Orchestrates the full Financial Intelligence Platform pipeline:
  1. Run all 4 Kafka producers (extract → stream)
  2. Run PySpark consumer (transform → load)

Schedule: daily at 6:00 AM UTC
Airflow UI: http://localhost:8080  (admin / finintel)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# ── Default args ──────────────────────────────────────────────────────────────
default_args = {
    "owner":            "group7",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
}

# ── DAG definition ────────────────────────────────────────────────────────────
with DAG(
    dag_id="finintel_pipeline",
    description="Financial Intelligence Platform — Full ETL via Kafka + PySpark",
    default_args=default_args,
    schedule_interval="0 6 * * *",   # daily at 6 AM UTC
    start_date=days_ago(1),
    catchup=False,
    tags=["finintel", "kafka", "pyspark", "columbia"],
    doc_md="""
    ## Financial Intelligence Platform Pipeline

    **Group 7 — Columbia University Big Data Engineering**

    ### Flow
    ```
    [NewsAPI] ──→ Kafka: news-articles  ──→
    [SEC Edgar] → Kafka: sec-filings   ──→  PySpark → PostgreSQL + MongoDB
    [FRED API] ──→ Kafka: market-data  ──→
    [Alpaca] ───→ Kafka: stock-prices  ──→
    ```

    ### Technologies
    - **Kafka**: event streaming buffer (4 topics)
    - **PySpark**: distributed transformation + cross-source join
    - **Airflow**: this DAG — orchestration and scheduling
    - **PostgreSQL**: structured warehouse (Supabase)
    - **MongoDB**: document store (Atlas)
    """,
) as dag:

    # ── Task 1: Produce news to Kafka ─────────────────────────────────────────
    produce_news = BashOperator(
        task_id="produce_news",
        bash_command=(
            "cd /opt/airflow && "
            "pip install kafka-python requests python-dotenv -q && "
            "python producers/producer_news.py"
        ),
        env={
            "NEWS_API_KEY":     "{{ var.value.NEWS_API_KEY }}",
            "KAFKA_BOOTSTRAP":  "{{ var.value.KAFKA_BOOTSTRAP | default('kafka:29092') }}",
        },
        doc_md="Fetches financial news from NewsAPI and publishes to Kafka `news-articles` topic.",
    )

    # ── Task 2: Produce SEC filings to Kafka ──────────────────────────────────
    produce_edgar = BashOperator(
        task_id="produce_edgar",
        bash_command=(
            "cd /opt/airflow && "
            "pip install kafka-python requests python-dotenv -q && "
            "python producers/producer_edgar.py"
        ),
        env={
            "KAFKA_BOOTSTRAP":       "{{ var.value.KAFKA_BOOTSTRAP | default('kafka:29092') }}",
            "FETCH_FILING_TEXT":     "true",
            "MAX_FILING_CHARS":      "500000",
            "MAX_FILINGS_PER_TICKER":"20",
        },
        execution_timeout=timedelta(hours=5),
        doc_md="Fetches SEC Edgar filings with full text and publishes to Kafka `sec-filings` topic.",
    )

    # ── Task 3: Produce market data to Kafka ──────────────────────────────────
    produce_market = BashOperator(
        task_id="produce_market",
        bash_command=(
            "cd /opt/airflow && "
            "pip install kafka-python requests python-dotenv -q && "
            "python producers/producer_market.py"
        ),
        env={
            "FRED_API_KEY":    "{{ var.value.FRED_API_KEY }}",
            "ALPACA_API_KEY":  "{{ var.value.ALPACA_API_KEY }}",
            "ALPACA_SECRET_KEY":"{{ var.value.ALPACA_SECRET_KEY }}",
            "KAFKA_BOOTSTRAP": "{{ var.value.KAFKA_BOOTSTRAP | default('kafka:29092') }}",
        },
        doc_md="Fetches FRED macro series + Alpaca stock prices and publishes to Kafka topics.",
    )

    # ── Task 4: PySpark consumer — transform + load ───────────────────────────
    spark_transform = BashOperator(
        task_id="spark_transform",
        bash_command=(
            "/home/airflow/.local/bin/spark-submit "
            "--conf spark.jars.ivy=/tmp/ivy "
            "--master local[*] "
            "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0,"
            "org.postgresql:postgresql:42.7.1 "
            "/opt/airflow/spark/spark_consumer.py"
        ),
        env={
            "KAFKA_BOOTSTRAP":  "{{ var.value.KAFKA_BOOTSTRAP | default('kafka:29092') }}",
            "SUPABASE_HOST":    "{{ var.value.SUPABASE_HOST }}",
            "SUPABASE_PORT":    "{{ var.value.SUPABASE_PORT }}",
            "SUPABASE_DB":      "{{ var.value.SUPABASE_DB }}",
            "SUPABASE_USER":    "{{ var.value.SUPABASE_USER }}",
            "SUPABASE_PASSWORD":"{{ var.value.SUPABASE_PASSWORD }}",
            "MONGO_URI":        "{{ var.value.MONGO_URI }}",
        },
        execution_timeout=timedelta(hours=2),
        doc_md="Reads from all 4 Kafka topics, deduplicates, joins, and loads to PostgreSQL + MongoDB.",
    )

    # ── Task 5: Log pipeline completion ───────────────────────────────────────
    log_complete = PythonOperator(
        task_id="log_complete",
        python_callable=lambda **ctx: print(
            f"✓ Pipeline complete at {datetime.utcnow().isoformat()} | "
            f"Run ID: {ctx['run_id']}"
        ),
        doc_md="Logs pipeline completion time.",
    )

    # ── Task dependencies ─────────────────────────────────────────────────────
    # Producers run in parallel (independent sources)
    # Spark runs after all producers finish
    [produce_news, produce_edgar, produce_market] >> spark_transform >> log_complete
