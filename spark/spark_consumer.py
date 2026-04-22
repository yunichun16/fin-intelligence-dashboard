"""
spark/spark_consumer.py
Reads from all 4 Kafka topics using PySpark Structured Streaming,
transforms the data, and loads into PostgreSQL (Supabase) + MongoDB (Atlas).

Run locally:
    spark-submit \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.1 \
      spark/spark_consumer.py

Or via Docker:
    docker exec -it finintel-spark spark-submit \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
      /opt/spark-apps/spark_consumer.py
"""

import os, json
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, trim, lower, length,
    when, lit, current_timestamp, abs as spark_abs, datediff
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    LongType, BooleanType, TimestampType
)
from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), ".env"))

# ── Config ────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")

PG_URL  = (
    f"jdbc:postgresql://{os.environ['SUPABASE_HOST']}:{os.getenv('SUPABASE_PORT','5432')}"
    f"/{os.getenv('SUPABASE_DB','postgres')}?sslmode=require"
)
PG_PROPS = {
    "user":     os.environ["SUPABASE_USER"],
    "password": os.environ["SUPABASE_PASSWORD"],
    "driver":   "org.postgresql.Driver",
}
MONGO_URI = os.environ["MONGO_URI"]

# ── Spark session ─────────────────────────────────────────────────────────────
spark = (SparkSession.builder
    .appName("FinIntelligence-Consumer")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.mongodb.output.uri", MONGO_URI)
    .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("  FinIntelligence PySpark Consumer")
print(f"  Kafka: {KAFKA_BOOTSTRAP}")
print(f"  PostgreSQL: {PG_URL[:60]}...")
print("=" * 60)


# ── Schemas ───────────────────────────────────────────────────────────────────
news_schema = StructType([
    StructField("source_name",  StringType()),
    StructField("title",        StringType()),
    StructField("description",  StringType()),
    StructField("url",          StringType()),
    StructField("published_at", StringType()),
    StructField("content",      StringType()),
    StructField("search_query", StringType()),
    StructField("category",     StringType()),
    StructField("fetched_at",   StringType()),
])

filing_schema = StructType([
    StructField("ticker",            StringType()),
    StructField("company_name",      StringType()),
    StructField("cik",               StringType()),
    StructField("form_type",         StringType()),
    StructField("filed_at",          StringType()),
    StructField("period",            StringType()),
    StructField("accession_number",  StringType()),
    StructField("document_url",      StringType()),
    StructField("is_material_event", BooleanType()),
    StructField("full_text",         StringType()),
    StructField("text_chars",        LongType()),
    StructField("fetched_at",        StringType()),
])

market_schema = StructType([
    StructField("series_code", StringType()),
    StructField("series_name", StringType()),
    StructField("date",        StringType()),
    StructField("value",       DoubleType()),
    StructField("fetched_at",  StringType()),
])

price_schema = StructType([
    StructField("ticker",     StringType()),
    StructField("date",       StringType()),
    StructField("open",       DoubleType()),
    StructField("high",       DoubleType()),
    StructField("low",        DoubleType()),
    StructField("close",      DoubleType()),
    StructField("volume",     LongType()),
    StructField("vwap",       DoubleType()),
    StructField("fetched_at", StringType()),
])


def read_kafka_batch(topic, schema):
    """Read all available messages from a Kafka topic as a batch (not streaming)."""
    raw = (spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .option("maxOffsetsPerTrigger", 500000)
        .load())

    return (raw
        .select(from_json(col("value").cast("string"), schema).alias("data"))
        .select("data.*"))


def write_postgres(df, table, mode="append"):
    """Write a DataFrame to PostgreSQL via JDBC."""
    df.write.jdbc(url=PG_URL, table=table, mode=mode, properties=PG_PROPS)
    print(f"  ✓ PostgreSQL '{table}': {df.count()} rows written")


def write_mongo(df, collection):
    """Write a DataFrame to MongoDB Atlas."""
    (df.write
        .format("mongo")
        .mode("append")
        .option("uri", MONGO_URI)
        .option("database", "fin_intelligence")
        .option("collection", collection)
        .save())
    print(f"  ✓ MongoDB '{collection}': {df.count()} documents written")


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 1 — News Articles
# ══════════════════════════════════════════════════════════════════════════════
print("\n── Processing: news-articles ────────────────────────")
try:
    news_df = read_kafka_batch("news-articles", news_schema)

    # Transform
    news_clean = (news_df
        .filter(col("url").isNotNull() & col("title").isNotNull())
        .dropDuplicates(["url"])
        .withColumn("published_at", to_timestamp("published_at"))
        .withColumn("date_only",    col("published_at").cast("date"))
        .withColumn("title",        trim(col("title")))
        .withColumn("category",     when(col("category").isNull(), "general")
                                    .otherwise(col("category")))
    )

    print(f"  News records after dedup: {news_clean.count():,}")

    # Write metadata to PostgreSQL
    pg_news = news_clean.select(
        "url", "title", "source_name", "published_at", "date_only", "category"
    )
    write_postgres(pg_news, "news_sentiment")

    # Write full documents to MongoDB
    write_mongo(news_clean, "news_articles")

except Exception as e:
    print(f"  ✗ News processing failed: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 2 — SEC Filings
# ══════════════════════════════════════════════════════════════════════════════
print("\n── Processing: sec-filings ──────────────────────────")
try:
    filings_df = read_kafka_batch("sec-filings", filing_schema)

    filings_clean = (filings_df
        .filter(col("accession_number").isNotNull())
        .dropDuplicates(["accession_number"])
        .withColumn("filed_at", col("filed_at").cast("date"))
        .withColumn("period",   col("period").cast("date"))
        .withColumn("is_material_event",
                    when(col("form_type") == "8-K", True).otherwise(False))
    )

    print(f"  Filing records after dedup: {filings_clean.count():,}")

    # Cross-source join: find news articles within ±7 days of filing date
    try:
        news_for_join = news_clean.select(
            col("date_only").alias("news_date"),
            col("title").alias("news_title"),
            col("category").alias("news_category"),
        )
        cross_join = (filings_clean
            .join(news_for_join,
                  spark_abs(datediff(col("filed_at"), col("news_date"))) <= 7,
                  "left")
            .groupBy("accession_number", "ticker", "company_name",
                     "form_type", "filed_at")
            .count()
            .withColumnRenamed("count", "nearby_news_count")
        )
        print(f"  Cross-source join: {cross_join.filter(col('nearby_news_count') > 0).count():,} filings with nearby news")
    except Exception as e:
        print(f"  ⚠ Cross-source join skipped: {e}")

    # Write metadata to PostgreSQL
    pg_filings = filings_clean.select(
        "ticker", "company_name", "cik", "form_type",
        "filed_at", "period", "accession_number",
        "document_url", "is_material_event", "fetched_at"
    )
    write_postgres(pg_filings, "sec_filings")

    # Write full documents (with text) to MongoDB
    write_mongo(filings_clean, "sec_filing_documents")

except Exception as e:
    print(f"  ✗ Filings processing failed: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 3 — Market Data (FRED)
# ══════════════════════════════════════════════════════════════════════════════
print("\n── Processing: market-data (FRED) ───────────────────")
try:
    market_df = read_kafka_batch("market-data", market_schema)

    market_clean = (market_df
        .filter(col("value").isNotNull())
        .dropDuplicates(["series_code", "date"])
        .withColumn("date", col("date").cast("date"))
    )

    print(f"  Market records after dedup: {market_clean.count():,}")
    write_postgres(market_clean, "market_data")

except Exception as e:
    print(f"  ✗ Market data processing failed: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 4 — Stock Prices (Alpaca)
# ══════════════════════════════════════════════════════════════════════════════
print("\n── Processing: stock-prices (Alpaca) ────────────────")
try:
    prices_df = read_kafka_batch("stock-prices", price_schema)

    prices_clean = (prices_df
        .filter(col("close").isNotNull())
        .dropDuplicates(["ticker", "date"])
        .withColumn("date", col("date").cast("date"))
    )

    print(f"  Stock price records after dedup: {prices_clean.count():,}")
    write_postgres(prices_clean, "stock_prices")

except Exception as e:
    print(f"  ✗ Stock prices processing failed: {e}")


print("\n" + "=" * 60)
print("  PySpark Consumer Complete")
print("=" * 60)
spark.stop()
