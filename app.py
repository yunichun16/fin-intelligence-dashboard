"""
Financial Intelligence Platform — Streamlit Dashboard
Group 7: Ce Zhang · Cai Gao · Yuchun Wu · Yanji Li

Connects to:
  - PostgreSQL (Supabase) for structured data: market_data, sec_filings, news_sentiment
  - MongoDB (Atlas) for document data: news_articles, sec_filing_documents

Run locally:
    pip install streamlit psycopg2-binary pymongo pandas plotly python-dotenv
    streamlit run app.py

Run on Streamlit Cloud:
    Add secrets to .streamlit/secrets.toml (see bottom of this file)
"""

import os
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta, date

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Financial Intelligence Platform",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS ────────────────────────────────────────────────────────────────
st.markdown("""
<style>
  @import url('https://fonts.googleapis.com/css2?family=DM+Serif+Display&family=DM+Sans:wght@300;400;500&family=DM+Mono&display=swap');

  html, body, [class*="css"] {
    font-family: 'DM Sans', sans-serif;
  }
  h1, h2, h3 { font-family: 'DM Serif Display', serif; font-weight: 400; }

  .main { background: #F7F6F2; }

  /* Metric cards */
  .metric-card {
    background: #1E2761;
    border-radius: 12px;
    padding: 1.2rem 1.5rem;
    color: white;
    margin-bottom: 0.5rem;
  }
  .metric-label { font-size: 11px; letter-spacing: 0.08em; text-transform: uppercase; opacity: 0.7; margin: 0; }
  .metric-value { font-size: 32px; font-weight: 500; margin: 4px 0 0; font-family: 'DM Serif Display', serif; }
  .metric-sub   { font-size: 12px; opacity: 0.6; margin: 2px 0 0; }

  /* Section headers */
  .section-label {
    font-size: 10px; letter-spacing: 0.12em; text-transform: uppercase;
    color: #3A86FF; font-weight: 500; margin-bottom: 4px;
  }

  /* Filing pill */
  .pill-8k  { background:#FEE2E2; color:#991B1B; padding:2px 8px; border-radius:20px; font-size:11px; font-weight:500; }
  .pill-10k { background:#DBEAFE; color:#1E40AF; padding:2px 8px; border-radius:20px; font-size:11px; font-weight:500; }

  /* News card */
  .news-card {
    background: white; border-radius: 10px; padding: 14px 16px;
    border-left: 3px solid #3A86FF; margin-bottom: 10px;
  }
  .news-title  { font-size: 14px; font-weight: 500; color: #1E2761; margin: 0 0 4px; }
  .news-meta   { font-size: 11px; color: #888; margin: 0; }
  .cat-earnings   { color:#059669; font-weight:500; }
  .cat-ma         { color:#7C3AED; font-weight:500; }
  .cat-macro      { color:#D97706; font-weight:500; }
  .cat-regulatory { color:#DC2626; font-weight:500; }
  .cat-general    { color:#6B7280; font-weight:500; }

  /* Hide Streamlit branding */
  #MainMenu, footer, header { visibility: hidden; }
  .block-container { padding-top: 1.5rem; }
</style>
""", unsafe_allow_html=True)


# ── Connection helpers ────────────────────────────────────────────────────────

@st.cache_resource(show_spinner=False)
def get_pg_conn():
    """Connect to PostgreSQL (Supabase). Reads from st.secrets or env vars."""
    import psycopg2
    try:
        # Try Streamlit secrets first (for Streamlit Cloud deployment)
        if "postgres" in st.secrets:
            s = st.secrets["postgres"]
            conn = psycopg2.connect(
                host=s["host"], port=s.get("port", 5432),
                dbname=s["dbname"], user=s["user"],
                password=s["password"], sslmode="require",
                connect_timeout=10,
            )
        else:
            # Fall back to environment variables (local .env)
            conn = psycopg2.connect(
                host=os.getenv("SUPABASE_HOST"),
                port=int(os.getenv("SUPABASE_PORT", "5432")),
                dbname=os.getenv("SUPABASE_DB", "postgres"),
                user=os.getenv("SUPABASE_USER"),
                password=os.getenv("SUPABASE_PASSWORD"),
                sslmode="require",
                connect_timeout=10,
            )
        return conn
    except Exception as e:
        st.error(f"PostgreSQL connection failed: {e}")
        return None


@st.cache_resource(show_spinner=False)
def get_mongo_client():
    """Connect to MongoDB (Atlas). Reads from st.secrets or env vars."""
    from pymongo import MongoClient
    try:
        uri = st.secrets.get("MONGO_URI", os.getenv("MONGO_URI", ""))
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        return client
    except Exception as e:
        st.error(f"MongoDB connection failed: {e}")
        return None


def pg_query(sql: str, params=None) -> pd.DataFrame:
    """Run a SQL query and return a DataFrame."""
    conn = get_pg_conn()
    if conn is None:
        return pd.DataFrame()
    try:
        return pd.read_sql_query(sql, conn, params=params)
    except Exception as e:
        st.warning(f"Query error: {e}")
        return pd.DataFrame()


# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### 📊 Financial Intelligence")
    st.markdown("**Group 7** · Columbia University")
    st.markdown("---")

    page = st.radio(
        "Navigation",
        ["Overview", "Market Data", "SEC Filings", "News Feed", "Cross-Source Analysis"],
        label_visibility="collapsed",
    )

    st.markdown("---")

    # Date filter
    st.markdown("**Date range**")
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("From", value=date(2024, 1, 1), label_visibility="collapsed")
    with col2:
        end_date = st.date_input("To", value=date.today(), label_visibility="collapsed")

    # Ticker filter
    st.markdown("**Tickers**")
    ticker_options = ["All", "AAPL", "MSFT", "GOOGL", "JPM", "BAC", "GS"]
    selected_tickers = st.multiselect("", ticker_options, default=["All"], label_visibility="collapsed")
    if "All" in selected_tickers or not selected_tickers:
        ticker_filter = ticker_options[1:]  # all real tickers
    else:
        ticker_filter = selected_tickers

    st.markdown("---")
    st.markdown("""
    <div style='font-size:11px;color:#999;line-height:1.6'>
    <b>Data sources</b><br>
    NewsAPI · SEC Edgar · FRED<br><br>
    <b>Stack</b><br>
    Kafka · PySpark · PostgreSQL · MongoDB
    </div>
    """, unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE: OVERVIEW
# ══════════════════════════════════════════════════════════════════════════════
if page == "Overview":

    st.markdown('<p class="section-label">Financial Intelligence Platform</p>', unsafe_allow_html=True)
    st.title("Market Overview")
    st.markdown(f"*Data as of {date.today().strftime('%B %d, %Y')} · Supabase + MongoDB Atlas*")

    # ── KPI row ───────────────────────────────────────────────────────────────
    counts = pg_query("""
        SELECT
            (SELECT COUNT(*) FROM market_data)    AS market_rows,
            (SELECT COUNT(*) FROM sec_filings)    AS filing_rows,
            (SELECT COUNT(*) FROM news_sentiment) AS news_rows,
            (SELECT COUNT(DISTINCT series_code) FROM market_data) AS series_count,
            (SELECT COUNT(DISTINCT ticker) FROM sec_filings)      AS ticker_count
    """)

    c1, c2, c3, c4, c5 = st.columns(5)
    def kpi(col, label, value, sub=""):
        with col:
            st.markdown(f"""
            <div class="metric-card">
              <p class="metric-label">{label}</p>
              <p class="metric-value">{value}</p>
              <p class="metric-sub">{sub}</p>
            </div>""", unsafe_allow_html=True)

    if not counts.empty:
        r = counts.iloc[0]
        kpi(c1, "Market data points", f"{int(r.market_rows):,}", f"{int(r.series_count)} series")
        kpi(c2, "SEC filings",        f"{int(r.filing_rows):,}", f"{int(r.ticker_count)} companies")
        kpi(c3, "News articles",      f"{int(r.news_rows):,}",  "indexed headlines")
        kpi(c4, "Data sources",       "3", "NewsAPI · Edgar · FRED")
        kpi(c5, "Technologies",       "5", "Kafka·Spark·PG·Mongo·Airflow")

    st.markdown("---")

    # ── Two charts side by side ───────────────────────────────────────────────
    left, right = st.columns(2)

    with left:
        st.markdown('<p class="section-label">S&P 500 — recent trend</p>', unsafe_allow_html=True)
        sp = pg_query("""
            SELECT date, value FROM market_data
            WHERE series_code = 'SP500'
            ORDER BY date DESC LIMIT 90
        """)
        if not sp.empty:
            sp = sp.sort_values("date")
            fig = px.area(sp, x="date", y="value",
                          color_discrete_sequence=["#3A86FF"])
            fig.update_layout(
                margin=dict(l=0, r=0, t=10, b=0),
                plot_bgcolor="white", paper_bgcolor="white",
                xaxis=dict(showgrid=False, title=""),
                yaxis=dict(showgrid=True, gridcolor="#F0F0F0", title=""),
                height=240,
            )
            fig.update_traces(fillcolor="rgba(58,134,255,0.1)", line_width=2)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No S&P 500 data yet — run the pipeline first")

    with right:
        st.markdown('<p class="section-label">Filings by company</p>', unsafe_allow_html=True)
        filings_by_co = pg_query("""
            SELECT ticker, COUNT(*) AS filings
            FROM sec_filings
            GROUP BY ticker ORDER BY filings DESC LIMIT 8
        """)
        if not filings_by_co.empty:
            fig2 = px.bar(filings_by_co, x="ticker", y="filings",
                          color_discrete_sequence=["#1E2761"])
            fig2.update_layout(
                margin=dict(l=0, r=0, t=10, b=0),
                plot_bgcolor="white", paper_bgcolor="white",
                xaxis=dict(showgrid=False, title=""),
                yaxis=dict(showgrid=True, gridcolor="#F0F0F0", title="Count"),
                height=240, showlegend=False,
            )
            st.plotly_chart(fig2, use_container_width=True)
        else:
            st.info("No filings data yet")

    # ── Recent filings table ──────────────────────────────────────────────────
    st.markdown("---")
    st.markdown('<p class="section-label">Most recent SEC filings</p>', unsafe_allow_html=True)
    recent_filings = pg_query("""
        SELECT ticker, company_name, form_type, filed_at, period, is_material_event
        FROM sec_filings
        ORDER BY filed_at DESC LIMIT 10
    """)
    if not recent_filings.empty:
        st.dataframe(
            recent_filings.rename(columns={
                "ticker": "Ticker", "company_name": "Company",
                "form_type": "Form", "filed_at": "Filed",
                "period": "Period", "is_material_event": "Material Event"
            }),
            use_container_width=True, hide_index=True,
        )


# ══════════════════════════════════════════════════════════════════════════════
# PAGE: MARKET DATA
# ══════════════════════════════════════════════════════════════════════════════
elif page == "Market Data":

    st.markdown('<p class="section-label">FRED Economic Indicators</p>', unsafe_allow_html=True)
    st.title("Market & Economic Data")

    # Series selector
    series_df = pg_query("SELECT DISTINCT series_code, series_name FROM market_data ORDER BY series_code")
    if series_df.empty:
        st.info("No market data yet — run the pipeline first")
        st.stop()

    series_options = {row["series_name"]: row["series_code"] for _, row in series_df.iterrows()}
    selected_series_name = st.selectbox("Select indicator", list(series_options.keys()))
    selected_code = series_options[selected_series_name]

    # Fetch selected series
    data = pg_query("""
        SELECT date, value FROM market_data
        WHERE series_code = %s AND date BETWEEN %s AND %s
        ORDER BY date
    """, params=(selected_code, start_date, end_date))

    if data.empty:
        st.warning("No data for selected range")
    else:
        # Stats row
        latest = data.iloc[-1]["value"]
        oldest = data.iloc[0]["value"]
        change = latest - oldest
        pct    = (change / oldest * 100) if oldest else 0

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Latest value",    f"{latest:,.4f}")
        c2.metric("Start of period", f"{oldest:,.4f}")
        c3.metric("Change",          f"{change:+,.4f}")
        c4.metric("% Change",        f"{pct:+.2f}%")

        st.markdown("---")

        # Main chart
        fig = px.line(data, x="date", y="value",
                      title=selected_series_name,
                      color_discrete_sequence=["#1E2761"])
        fig.update_layout(
            plot_bgcolor="white", paper_bgcolor="white",
            xaxis=dict(showgrid=False, title=""),
            yaxis=dict(showgrid=True, gridcolor="#F0F0F0", title="Value"),
            title_font=dict(family="DM Serif Display", size=18),
            margin=dict(l=0, r=0, t=40, b=0), height=380,
        )
        fig.update_traces(line_width=2)
        st.plotly_chart(fig, use_container_width=True)

        # Data table toggle
        if st.checkbox("Show raw data table"):
            st.dataframe(data.sort_values("date", ascending=False), use_container_width=True, hide_index=True)

    # ── All series summary ────────────────────────────────────────────────────
    st.markdown("---")
    st.markdown('<p class="section-label">All indicators — latest values</p>', unsafe_allow_html=True)

    summary = pg_query("""
        SELECT series_code, series_name,
               MAX(date)   AS latest_date,
               ROUND(MAX(value) FILTER (WHERE date = (SELECT MAX(d2.date) FROM market_data d2 WHERE d2.series_code = d1.series_code))::numeric, 4) AS latest_value,
               COUNT(*)    AS data_points
        FROM market_data d1
        GROUP BY series_code, series_name
        ORDER BY series_code
    """)
    if not summary.empty:
        st.dataframe(summary.rename(columns={
            "series_code": "Code", "series_name": "Indicator",
            "latest_date": "Latest Date", "latest_value": "Latest Value",
            "data_points": "Data Points"
        }), use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE: SEC FILINGS
# ══════════════════════════════════════════════════════════════════════════════
elif page == "SEC Filings":

    st.markdown('<p class="section-label">SEC Edgar</p>', unsafe_allow_html=True)
    st.title("SEC Filings Explorer")

    # Filters row
    f1, f2, f3 = st.columns(3)
    with f1:
        form_filter = st.selectbox("Form type", ["All", "8-K", "10-K"])
    with f2:
        sort_by = st.selectbox("Sort by", ["Filed date (newest)", "Filed date (oldest)", "Ticker A-Z"])
    with f3:
        material_only = st.checkbox("Material events (8-K) only", value=False)

    # Build query
    conditions = ["filed_at BETWEEN %s AND %s"]
    params = [start_date, end_date]

    if ticker_filter:
        placeholders = ",".join(["%s"] * len(ticker_filter))
        conditions.append(f"ticker IN ({placeholders})")
        params.extend(ticker_filter)

    if form_filter != "All":
        conditions.append("form_type = %s")
        params.append(form_filter)

    if material_only:
        conditions.append("is_material_event = TRUE")

    order = {"Filed date (newest)": "filed_at DESC",
             "Filed date (oldest)": "filed_at ASC",
             "Ticker A-Z":          "ticker ASC"}[sort_by]

    where = " AND ".join(conditions)
    filings = pg_query(f"""
        SELECT ticker, company_name, form_type, filed_at, period,
               is_material_event, document_url
        FROM sec_filings
        WHERE {where}
        ORDER BY {order}
    """, params=tuple(params))

    # Summary stats
    if not filings.empty:
        c1, c2, c3 = st.columns(3)
        c1.metric("Filings found",   len(filings))
        c2.metric("8-K (material)",  int(filings["is_material_event"].sum()))
        c3.metric("Companies",       filings["ticker"].nunique())

        st.markdown("---")

        # Form type breakdown chart
        breakdown = filings["form_type"].value_counts().reset_index()
        breakdown.columns = ["Form Type", "Count"]
        fig = px.pie(breakdown, names="Form Type", values="Count",
                     color_discrete_sequence=["#1E2761", "#3A86FF", "#CADCFC"])
        fig.update_layout(margin=dict(l=0, r=0, t=10, b=0), height=200,
                          showlegend=True, paper_bgcolor="white")
        left, right = st.columns([1, 2])
        with left:
            st.plotly_chart(fig, use_container_width=True)
        with right:
            st.markdown('<p class="section-label">Filing timeline</p>', unsafe_allow_html=True)
            timeline = filings.copy()
            timeline["filed_at"] = pd.to_datetime(timeline["filed_at"])
            timeline_count = timeline.groupby(["filed_at", "form_type"]).size().reset_index(name="count")
            fig2 = px.bar(timeline_count, x="filed_at", y="count", color="form_type",
                          color_discrete_map={"8-K": "#1E2761", "10-K": "#3A86FF"},
                          barmode="stack")
            fig2.update_layout(
                margin=dict(l=0, r=0, t=10, b=0), height=200,
                plot_bgcolor="white", paper_bgcolor="white",
                xaxis=dict(showgrid=False, title=""), yaxis=dict(title="Count"),
                legend_title="Form", showlegend=True,
            )
            st.plotly_chart(fig2, use_container_width=True)

        st.markdown("---")
        st.markdown('<p class="section-label">Filing records</p>', unsafe_allow_html=True)

        # Display with clickable URLs
        display_df = filings[["ticker", "company_name", "form_type", "filed_at", "period", "is_material_event"]].copy()
        display_df.columns = ["Ticker", "Company", "Form", "Filed", "Period", "Material"]
        st.dataframe(display_df, use_container_width=True, hide_index=True)

        # Show document links separately
        with st.expander("Document links"):
            for _, row in filings.iterrows():
                if row["document_url"]:
                    badge = "🔴 8-K" if row["form_type"] == "8-K" else "🔵 10-K"
                    st.markdown(f"{badge} **{row['ticker']}** — [{row['company_name']}]({row['document_url']}) ({row['filed_at']})")
    else:
        st.info("No filings match your filters")


# ══════════════════════════════════════════════════════════════════════════════
# PAGE: NEWS FEED
# ══════════════════════════════════════════════════════════════════════════════
elif page == "News Feed":

    st.markdown('<p class="section-label">NewsAPI</p>', unsafe_allow_html=True)
    st.title("Financial News Feed")

    # Category filter
    cat_options = ["All", "earnings", "m&a", "macro", "regulatory", "general"]
    cat_filter = st.selectbox("Filter by category", cat_options)

    # Fetch from MongoDB
    mongo = get_mongo_client()
    if mongo is None:
        st.error("MongoDB not connected")
        st.stop()

    news_col = mongo["fin_intelligence"]["news_articles"]

    query = {}
    if cat_filter != "All":
        query["category"] = cat_filter

    articles = list(
        news_col.find(query, {"title": 1, "source_name": 1, "published_at": 1,
                              "description": 1, "url": 1, "category": 1, "_id": 0})
        .sort("published_at", -1)
        .limit(50)
    )

    if not articles:
        st.info("No articles found — run the pipeline to ingest news")
        st.stop()

    # Category counts
    cat_counts = pg_query("SELECT category, COUNT(*) AS n FROM news_sentiment GROUP BY category ORDER BY n DESC")
    if not cat_counts.empty:
        fig = px.bar(cat_counts, x="category", y="n",
                     color="category",
                     color_discrete_map={
                         "earnings": "#059669", "m&a": "#7C3AED",
                         "macro": "#D97706", "regulatory": "#DC2626", "general": "#6B7280"
                     })
        fig.update_layout(
            margin=dict(l=0, r=0, t=10, b=0), height=180,
            plot_bgcolor="white", paper_bgcolor="white",
            xaxis=dict(showgrid=False, title=""), yaxis=dict(title="Articles"),
            showlegend=False,
        )
        st.plotly_chart(fig, use_container_width=True)

    st.markdown(f"**{len(articles)} articles** · sorted by most recent")
    st.markdown("---")

    cat_colors = {
        "earnings": "cat-earnings", "m&a": "cat-ma",
        "macro": "cat-macro", "regulatory": "cat-regulatory", "general": "cat-general"
    }

    for a in articles:
        cat   = a.get("category", "general")
        title = a.get("title", "Untitled")
        src   = a.get("source_name", "Unknown")
        desc  = a.get("description", "") or ""
        pub   = a.get("published_at", "")[:10] if a.get("published_at") else ""
        url   = a.get("url", "#")
        css   = cat_colors.get(cat, "cat-general")

        st.markdown(f"""
        <div class="news-card">
          <p class="news-title"><a href="{url}" target="_blank" style="color:#1E2761;text-decoration:none">{title}</a></p>
          <p class="news-meta">
            <span class="{css}">{cat.upper()}</span> &nbsp;·&nbsp;
            {src} &nbsp;·&nbsp; {pub}
          </p>
          <p style="font-size:12px;color:#555;margin:6px 0 0;line-height:1.5">{desc[:160]}{"..." if len(desc) > 160 else ""}</p>
        </div>
        """, unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE: CROSS-SOURCE ANALYSIS
# ══════════════════════════════════════════════════════════════════════════════
elif page == "Cross-Source Analysis":

    st.markdown('<p class="section-label">Multi-source intelligence</p>', unsafe_allow_html=True)
    st.title("Cross-Source Analysis")
    st.markdown("*The core value of the platform: joining news, filings, and market data on the same timeline.*")

    # ── Co-occurrence: filings + news on same date ────────────────────────────
    st.markdown("---")
    st.markdown('<p class="section-label">SEC filings + news co-occurrence</p>', unsafe_allow_html=True)
    st.markdown("Companies where an SEC filing and a news article appeared on the same calendar date.")

    cross = pg_query("""
        SELECT
            f.ticker, f.company_name, f.form_type, f.filed_at,
            COUNT(n.id) AS news_same_day
        FROM   sec_filings f
        LEFT   JOIN news_sentiment n ON n.date_only = f.filed_at
        GROUP  BY f.ticker, f.company_name, f.form_type, f.filed_at
        HAVING COUNT(n.id) > 0
        ORDER  BY f.filed_at DESC
        LIMIT  30
    """)

    if cross.empty:
        st.info("No date overlaps found yet. Try running the pipeline with a wider date range in Sections 1 and 2 of the notebook.")
    else:
        fig = px.scatter(cross,
                         x="filed_at", y="ticker",
                         size="news_same_day", color="form_type",
                         color_discrete_map={"8-K": "#DC2626", "10-K": "#1E2761"},
                         hover_data=["company_name", "news_same_day"],
                         title="Filing dates with concurrent news coverage (bubble = article count)")
        fig.update_layout(
            plot_bgcolor="white", paper_bgcolor="white",
            xaxis=dict(showgrid=False, title="Filing date"),
            yaxis=dict(showgrid=True, gridcolor="#F0F0F0", title="Ticker"),
            margin=dict(l=0, r=0, t=40, b=0), height=380,
        )
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(cross.rename(columns={
            "ticker": "Ticker", "company_name": "Company",
            "form_type": "Form", "filed_at": "Filed",
            "news_same_day": "News articles same day"
        }), use_container_width=True, hide_index=True)

    # ── Market data around filing dates ───────────────────────────────────────
    st.markdown("---")
    st.markdown('<p class="section-label">Market context around filing dates</p>', unsafe_allow_html=True)
    st.markdown("S&P 500 trend with SEC filing dates overlaid as markers.")

    sp500 = pg_query("""
        SELECT date, value FROM market_data
        WHERE series_code = 'SP500'
        ORDER BY date
    """)
    filing_dates = pg_query("""
        SELECT DISTINCT filed_at AS date, ticker, form_type
        FROM sec_filings
        ORDER BY filed_at
    """)

    if not sp500.empty:
        fig3 = go.Figure()
        fig3.add_trace(go.Scatter(
            x=sp500["date"], y=sp500["value"],
            mode="lines", name="S&P 500",
            line=dict(color="#1E2761", width=2),
        ))
        if not filing_dates.empty:
            filing_dates["date"] = pd.to_datetime(filing_dates["date"])
            sp500["date"] = pd.to_datetime(sp500["date"])
            merged = pd.merge_asof(
                filing_dates.sort_values("date"),
                sp500.sort_values("date"),
                on="date", direction="nearest"
            )
            fig3.add_trace(go.Scatter(
                x=merged["date"], y=merged["value"],
                mode="markers", name="Filing date",
                marker=dict(color="#DC2626", size=10, symbol="diamond"),
                text=merged["ticker"] + " " + merged["form_type"],
                hovertemplate="%{text}<br>%{x}<extra></extra>",
            ))
        fig3.update_layout(
            plot_bgcolor="white", paper_bgcolor="white",
            xaxis=dict(showgrid=False, title=""),
            yaxis=dict(showgrid=True, gridcolor="#F0F0F0", title="S&P 500"),
            legend=dict(orientation="h", y=1.1),
            margin=dict(l=0, r=0, t=20, b=0), height=360,
        )
        st.plotly_chart(fig3, use_container_width=True)

    # ── News category breakdown over time ─────────────────────────────────────
    st.markdown("---")
    st.markdown('<p class="section-label">News category distribution over time</p>', unsafe_allow_html=True)

    cat_time = pg_query("""
        SELECT date_only, category, COUNT(*) AS n
        FROM news_sentiment
        WHERE date_only BETWEEN %s AND %s
        GROUP BY date_only, category
        ORDER BY date_only
    """, params=(start_date, end_date))

    if not cat_time.empty:
        fig4 = px.bar(cat_time, x="date_only", y="n", color="category",
                      color_discrete_map={
                          "earnings": "#059669", "m&a": "#7C3AED",
                          "macro": "#D97706", "regulatory": "#DC2626", "general": "#6B7280"
                      }, barmode="stack")
        fig4.update_layout(
            plot_bgcolor="white", paper_bgcolor="white",
            xaxis=dict(showgrid=False, title="Date"),
            yaxis=dict(title="Articles"), margin=dict(l=0, r=0, t=10, b=0),
            height=300, legend_title="Category",
        )
        st.plotly_chart(fig4, use_container_width=True)
    else:
        st.info("No news data in selected date range")
