"""
producers/producer_market.py
Fetches FRED macro data + Alpaca stock prices and publishes to Kafka topics:
  - market-data   (FRED series)
  - stock-prices  (Alpaca OHLCV bars)
"""
import os, json, time, requests
from datetime import datetime, timedelta
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

FRED_API_KEY    = os.environ["FRED_API_KEY"]
ALPACA_KEY      = os.getenv("ALPACA_API_KEY", "")
ALPACA_SECRET   = os.getenv("ALPACA_SECRET_KEY", "")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")

FRED_TOPIC   = "market-data"
ALPACA_TOPIC = "stock-prices"
FRED_START   = "2000-01-01"
ALPACA_LOOKBACK = 365 * 15

FRED_SERIES = {
    "SP500": "S&P 500 index (daily)",
    "NASDAQCOM": "NASDAQ Composite (daily)",
    "DJIA": "Dow Jones Industrial Average (daily)",
    "DFF": "Federal funds rate (daily)",
    "DGS10": "10-Year Treasury yield (daily)",
    "DGS2": "2-Year Treasury yield (daily)",
    "DGS30": "30-Year Treasury yield (daily)",
    "T10Y2Y": "10Y-2Y Treasury spread (daily)",
    "VIXCLS": "CBOE VIX volatility index (daily)",
    "DEXUSEU": "USD/EUR exchange rate (daily)",
    "DEXUSUK": "USD/GBP exchange rate (daily)",
    "DEXJPUS": "JPY/USD exchange rate (daily)",
    "DCOILWTICO": "WTI crude oil spot (daily)",
    "GOLDAMGBD228NLBM": "Gold price (daily)",
    "CPIAUCSL": "Consumer Price Index (monthly)",
    "CPILFESL": "Core CPI ex food & energy (monthly)",
    "UNRATE": "Unemployment rate (monthly)",
    "PAYEMS": "Nonfarm payrolls (monthly)",
    "GDP": "US GDP (quarterly)",
    "GDPC1": "Real GDP (quarterly)",
    "WTISPLC": "WTI crude oil price (monthly)",
    "M2SL": "M2 money supply (monthly)",
    "MORTGAGE30US": "30-Year mortgage rate (weekly)",
    "UMCSENT": "Consumer sentiment (monthly)",
    "INDPRO": "Industrial production (monthly)",
    "HOUST": "Housing starts (monthly)",
    "BAA10Y": "BAA corporate bond spread (daily)",
    "T10YIE": "10-Year breakeven inflation (daily)",
    "FEDFUNDS": "Effective federal funds rate (monthly)",
    "ICSA": "Initial jobless claims (weekly)",
    "RSXFS": "Retail sales ex food (monthly)",
    "TOTALSL": "Total consumer credit (monthly)",
    "BUSLOANS": "Commercial & industrial loans (monthly)",
    "GDPPOT": "Potential GDP (quarterly)",
    "HSN1F": "New home sales (monthly)",
    "PCEPI": "PCE Price Index (monthly)",
    "DCOILBRENTEU": "Brent crude oil spot (daily)",
    "M1SL": "M1 money supply (monthly)",
    "MORTGAGE15US": "15-Year mortgage rate (weekly)",
}

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


def run_fred(producer):
    print("\n[FRED Producer] Fetching macro series...")
    total = 0
    for series_id, desc in FRED_SERIES.items():
        print(f"  {series_id}...", end=" ", flush=True)
        try:
            r = requests.get(
                "https://api.stlouisfed.org/fred/series/observations",
                params={
                    "series_id": series_id,
                    "observation_start": FRED_START,
                    "api_key": FRED_API_KEY,
                    "file_type": "json",
                    "sort_order": "asc",
                },
                timeout=10,
            )
            r.raise_for_status()
            observations = r.json().get("observations", [])
            count = 0
            for obs in observations:
                val = obs.get("value", ".")
                if val == ".": continue
                msg = {
                    "series_code": series_id,
                    "series_name": desc,
                    "date":        obs["date"],
                    "value":       float(val),
                    "fetched_at":  datetime.utcnow().isoformat(),
                }
                producer.send(FRED_TOPIC, value=msg)
                count += 1
                total += 1
            print(f"✓ {count} observations")
        except Exception as e:
            print(f"✗ {e}")
        time.sleep(0.3)

    print(f"[FRED Producer] ✓ Published {total} data points to '{FRED_TOPIC}'")


def run_alpaca(producer):
    if not ALPACA_KEY:
        print("\n[Alpaca Producer] ⚠ No ALPACA_API_KEY — skipping stock prices")
        return

    print("\n[Alpaca Producer] Fetching stock price bars...")
    headers = {
        "APCA-API-KEY-ID":     ALPACA_KEY,
        "APCA-API-SECRET-KEY": ALPACA_SECRET,
    }
    start_dt = (datetime.utcnow() - timedelta(days=ALPACA_LOOKBACK)).strftime("%Y-%m-%d")
    total = 0

    # Alpaca accepts up to 100 symbols per request — batch them
    batch_size = 100
    for i in range(0, len(TARGET_TICKERS), batch_size):
        batch = TARGET_TICKERS[i:i+batch_size]
        try:
            r = requests.get(
                "https://data.alpaca.markets/v2/stocks/bars",
                headers=headers,
                params={
                    "symbols":    ",".join(batch),
                    "timeframe":  "1Day",
                    "start":      start_dt,
                    "feed":       "iex",
                    "limit":      10000,
                    "adjustment": "all",
                },
                timeout=30,
            )
            r.raise_for_status()
            bars_by_ticker = r.json().get("bars", {})
            for ticker, bars in bars_by_ticker.items():
                for bar in bars:
                    msg = {
                        "ticker":     ticker,
                        "date":       bar["t"][:10],
                        "open":       bar["o"],
                        "high":       bar["h"],
                        "low":        bar["l"],
                        "close":      bar["c"],
                        "volume":     bar["v"],
                        "vwap":       bar.get("vw"),
                        "fetched_at": datetime.utcnow().isoformat(),
                    }
                    producer.send(ALPACA_TOPIC, value=msg)
                    total += 1
            print(f"  Batch {i//batch_size + 1}: ✓ {total} bars so far")
        except Exception as e:
            print(f"  Batch {i//batch_size + 1}: ✗ {e}")

    print(f"[Alpaca Producer] ✓ Published {total} bars to '{ALPACA_TOPIC}'")


def run():
    print(f"[Market Producer] Connecting to Kafka at {KAFKA_BOOTSTRAP}...")
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        max_request_size=10485760,
    )
    run_fred(producer)
    run_alpaca(producer)
    producer.flush()
    producer.close()
    print("[Market Producer] ✓ Done")


if __name__ == "__main__":
    run()
