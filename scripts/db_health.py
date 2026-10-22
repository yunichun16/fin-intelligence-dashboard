"""
Report on the Supabase Postgres database the pipeline writes to, and optionally
VACUUM its tables. Run from the "Database Health Check" workflow.

    python scripts/db_health.py            # report only
    python scripts/db_health.py --vacuum   # report, VACUUM ANALYZE, report again
"""
import os, sys, time
import psycopg2

PG_CONFIG = {
    "host":     os.environ["SUPABASE_HOST"],
    "port":     int(os.getenv("SUPABASE_PORT", "5432")),
    "dbname":   os.getenv("SUPABASE_DB", "postgres"),
    "user":     os.environ["SUPABASE_USER"],
    "password": os.environ["SUPABASE_PASSWORD"],
    "sslmode":  "require",
}
TABLES = ["stock_prices", "market_data", "sec_filings", "news_sentiment"]


def connect():
    for attempt in range(1, 6):
        try:
            conn = psycopg2.connect(**PG_CONFIG, connect_timeout=30)
            conn.autocommit = True
            return conn
        except psycopg2.OperationalError as e:
            print(f"connect attempt {attempt} failed: {str(e).strip().splitlines()[0]}")
            if attempt == 5:
                raise
            time.sleep(30)


def report(cur):
    cur.execute("SELECT pg_size_pretty(pg_database_size(current_database()))")
    print(f"\nDatabase size: {cur.fetchone()[0]}  (free tier limit: 500 MB)")

    cur.execute("""
        SELECT relname, n_live_tup, n_dead_tup,
               pg_size_pretty(pg_total_relation_size(relid)),
               last_vacuum, last_autovacuum
        FROM pg_stat_user_tables ORDER BY pg_total_relation_size(relid) DESC
    """)
    print(f"\n{'table':<22}{'live rows':>12}{'dead rows':>12}{'size':>10}  last vacuum / autovacuum")
    for name, live, dead, size, vac, autovac in cur.fetchall():
        print(f"{name:<22}{live:>12,}{dead:>12,}{size:>10}  {vac} / {autovac}")

    cur.execute("""
        SELECT pid, usename, application_name, state,
               now() - COALESCE(xact_start, query_start) AS age, left(query, 80)
        FROM pg_stat_activity
        WHERE datname = current_database() AND pid <> pg_backend_pid()
          AND state IS NOT NULL AND state <> 'idle'
        ORDER BY age DESC NULLS LAST
    """)
    rows = cur.fetchall()
    print(f"\nNon-idle sessions: {len(rows)}")
    for r in rows:
        print("  ", r)


def main():
    conn = connect()
    cur = conn.cursor()
    cur.execute("SET statement_timeout = 0")
    report(cur)
    if "--vacuum" in sys.argv:
        for t in TABLES:
            cur.execute("SELECT to_regclass(%s)", (t,))
            if cur.fetchone()[0] is None:
                continue
            start = time.time()
            print(f"\nVACUUM ANALYZE {t} ...", flush=True)
            cur.execute(f"VACUUM (ANALYZE) {t}")
            print(f"  done in {time.time() - start:.0f}s")
        report(cur)
    conn.close()


if __name__ == "__main__":
    main()
