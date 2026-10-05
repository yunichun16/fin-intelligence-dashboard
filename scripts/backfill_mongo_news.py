"""
backfill_mongo_news.py — seed a fresh MongoDB Atlas M0 cluster from PostgreSQL

The original Atlas cluster is gone, but every headline's metadata is still in
the Supabase `news_sentiment` table. This copies those rows into the
`news_articles` collection so the News Feed page isn't empty on day one.
Later pipeline runs add full NewsAPI content on top (upsert on url).

Usage:
    pip install psycopg2-binary pymongo python-dotenv
    python scripts/backfill_mongo_news.py

Reads SUPABASE_* and MONGO_URI from the environment or .env.
"""

import os
import psycopg2
from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne

load_dotenv()

pg = psycopg2.connect(
    host=os.environ["SUPABASE_HOST"],
    port=int(os.getenv("SUPABASE_PORT", "5432")),
    dbname=os.getenv("SUPABASE_DB", "postgres"),
    user=os.environ["SUPABASE_USER"],
    password=os.environ["SUPABASE_PASSWORD"],
    sslmode="require",
)
with pg.cursor() as cur:
    cur.execute("SELECT url, title, source_name, published_at, category FROM news_sentiment WHERE url IS NOT NULL")
    rows = cur.fetchall()
pg.close()
print(f"Read {len(rows)} articles from PostgreSQL")

col = MongoClient(os.environ["MONGO_URI"])["fin_intelligence"]["news_articles"]
col.create_index("url", unique=True)
ops = [
    UpdateOne(
        {"url": url},
        # $setOnInsert so richer documents written by the pipeline are never overwritten
        {"$setOnInsert": {
            "url": url, "title": title, "source_name": source,
            "published_at": published.isoformat() if published else None,
            "category": category, "description": "",
        }},
        upsert=True,
    )
    for url, title, source, published, category in rows
]
for i in range(0, len(ops), 500):
    col.bulk_write(ops[i:i + 500], ordered=False)
print(f"Done — news_articles now has {col.count_documents({})} documents")
