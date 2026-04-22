"""
producers/producer_news.py
Fetches financial news from NewsAPI and publishes to Kafka topic: news-articles
"""
import os, json, time, requests
from datetime import datetime, timedelta
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

NEWS_API_KEY    = os.environ["NEWS_API_KEY"]
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC           = "news-articles"

SEARCH_KEYWORDS = [
    "SEC filing earnings report",
    "earnings report quarterly results",
    "stock market financial results",
    "Federal Reserve interest rates decision",
    "company acquisition merger deal",
    "inflation CPI consumer prices",
    "unemployment jobs report labor market",
    "GDP economic growth recession",
    "big tech FAANG earnings results",
    "banking financial sector JPMorgan Goldman",
    "energy oil price crude market",
    "healthcare pharma FDA approval",
    "semiconductor chip AI nvidia market",
    "stock buyback share repurchase",
    "dividend announcement increase cut",
    "layoffs restructuring workforce reduction",
    "Treasury yield bond market rates",
    "IPO initial public offering stock",
    "revenue profit loss quarterly",
    "EPS beat miss analyst expectations",
]

def tag_category(title: str) -> str:
    t = str(title).lower()
    if any(w in t for w in ["earn","revenue","profit","eps","quarterly"]): return "earnings"
    if any(w in t for w in ["acqui","merger","takeover","deal","ipo"]):     return "m&a"
    if any(w in t for w in ["fed","rate","inflation","cpi","gdp","treasury"]): return "macro"
    if any(w in t for w in ["sec","filing","disclosure","lawsuit"]):        return "regulatory"
    return "general"

def fetch_news(query: str, lookback_days: int = 30, page_size: int = 100):
    from_date = (datetime.utcnow() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    params = {
        "q": query, "from": from_date, "sortBy": "publishedAt",
        "language": "en", "pageSize": page_size, "apiKey": NEWS_API_KEY,
    }
    r = requests.get("https://newsapi.org/v2/everything", params=params, timeout=10)
    if r.status_code == 429:
        print(f"  ⚠ NewsAPI rate limit — skipping '{query}'")
        return []
    r.raise_for_status()
    return r.json().get("articles", [])

def run():
    print(f"[NewsAPI Producer] Connecting to Kafka at {KAFKA_BOOTSTRAP}...")
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        max_request_size=10485760,
    )

    total = 0
    seen_urls = set()

    for keyword in SEARCH_KEYWORDS:
        print(f"  Fetching: {keyword[:50]}...")
        try:
            articles = fetch_news(keyword)
        except Exception as e:
            print(f"  ✗ {e}"); continue

        for a in articles:
            url = a.get("url", "")
            if not url or url in seen_urls:
                continue
            seen_urls.add(url)

            msg = {
                "source_name":  a.get("source", {}).get("name"),
                "title":        a.get("title"),
                "description":  a.get("description"),
                "url":          url,
                "published_at": a.get("publishedAt"),
                "content":      a.get("content"),
                "search_query": keyword,
                "category":     tag_category(a.get("title", "")),
                "fetched_at":   datetime.utcnow().isoformat(),
            }

            producer.send(TOPIC, value=msg)
            total += 1

        time.sleep(1.5)

    producer.flush()
    producer.close()
    print(f"[NewsAPI Producer] ✓ Published {total} unique articles to '{TOPIC}'")

if __name__ == "__main__":
    run()
