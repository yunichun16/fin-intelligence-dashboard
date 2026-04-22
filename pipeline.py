"""
Financial Intelligence Platform — Streamlit Dashboard
Group 7: Ce Zhang · Cai Gao · Yuchun Wu · Yanji Li
Columbia University — Big Data Engineering
"""

import os
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import date, timedelta

st.set_page_config(
    page_title="Financial Intelligence Platform",
    page_icon="📈", layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
  @import url('https://fonts.googleapis.com/css2?family=Syne:wght@400;600;700&family=Inter:wght@300;400;500&family=JetBrains+Mono:wght@400;500&display=swap');

  /* ── Base typography ── */
  html, body, [class*="css"] { font-family: 'Inter', sans-serif; }
  h1, h2, h3 { font-family: 'Syne', sans-serif !important; font-weight: 700 !important; letter-spacing: -0.02em; }

  /* ── Sidebar — always dark ── */
  [data-testid="stSidebar"] { background: #080E1D !important; }
  [data-testid="stSidebar"] * { color: #CBD5E1 !important; }
  [data-testid="stSidebar"] hr { border-color: #1E293B !important; }
  [data-testid="stSidebar"] .stSelectbox > div { background: #0F1729 !important; border-color: #1E3A5F !important; }
  [data-testid="stSidebar"] .stMultiSelect > div { background: #0F1729 !important; border-color: #1E3A5F !important; }

  /* ── Light mode (default) ── */
  .main { background: #F8FAFF; }
  .block-container { padding-top: 1.5rem; }

  /* ── Dark mode overrides ── */
  @media (prefers-color-scheme: dark) {
    .main { background: #0D1117; }
    .news-card { background: #161B27 !important; border-color: #1E3A5F !important; }
    .news-title { color: #E2E8F0 !important; }
  }
  [data-theme="dark"] .main { background: #0D1117; }
  [data-theme="dark"] .news-card { background: #161B27 !important; }
  [data-theme="dark"] .news-title { color: #E2E8F0 !important; }

  /* ── KPI cards — dark always, bright accent numbers ── */
  .kpi { background: linear-gradient(135deg,#080E1D,#162040); border-radius:14px;
         padding:1.3rem 1.5rem; color:white; border:1px solid #1E3A5F; margin-bottom:.5rem; }
  .kpi-l { font-size:9px; letter-spacing:.14em; text-transform:uppercase; opacity:.5; margin:0; }
  .kpi-v { font-family:'Syne',sans-serif; font-size:34px; font-weight:700; margin:4px 0 2px;
            letter-spacing:-.03em; color:#3BFFA0; text-shadow: 0 0 20px rgba(59,255,160,.3); }
  .kpi-v-blue  { color:#5BA4FF; text-shadow: 0 0 20px rgba(91,164,255,.3); }
  .kpi-v-amber { color:#FFD166; text-shadow: 0 0 20px rgba(255,209,102,.3); }
  .kpi-v-coral { color:#FF6B6B; text-shadow: 0 0 20px rgba(255,107,107,.3); }
  .kpi-v-teal  { color:#3BFFA0; text-shadow: 0 0 20px rgba(59,255,160,.3); }
  .kpi-s { font-size:11px; opacity:.45; margin:0; }

  /* ── Section labels ── */
  .sec { font-size:9px; letter-spacing:.14em; text-transform:uppercase; color:#3A86FF; font-weight:600; margin-bottom:2px; }

  /* ── News cards ── */
  .news-card { background:white; border-radius:12px; padding:14px 16px;
               border-left:3px solid #3A86FF; margin-bottom:10px; box-shadow:0 1px 4px rgba(0,0,0,.06); }
  .news-title { font-size:13px; font-weight:500; color:#080E1D; margin:0 0 4px; line-height:1.4; }
  .news-meta  { font-size:11px; color:#94A3B8; margin:0; }

  /* ── Badges ── */
  .badge { display:inline-block; padding:1px 8px; border-radius:20px; font-size:9px; font-weight:700; letter-spacing:.06em; text-transform:uppercase; }
  .be { background:#D1FAE5; color:#065F46; }
  .bm { background:#EDE9FE; color:#4C1D95; }
  .bk { background:#FEF3C7; color:#92400E; }
  .br { background:#FEE2E2; color:#991B1B; }
  .bg { background:#F1F5F9; color:#475569; }
  .b8 { background:#FEE2E2; color:#991B1B; }
  .b10{ background:#DBEAFE; color:#1E40AF; }

  /* ── Ticker pill ── */
  .tk { font-family:'JetBrains Mono',monospace; font-size:11px; background:#EFF6FF;
        color:#1D4ED8; padding:1px 7px; border-radius:5px; font-weight:500; }

  /* ── Insight box ── */
  .insight { background:#EFF6FF; border:1px solid #BFDBFE; border-radius:12px;
             padding:12px 16px; margin-bottom:1rem; font-size:13px; color:#1E40AF; line-height:1.6; }

  /* ── Macro metric cards ── */
  .macro-card {
    background: white; border:1px solid #E2E8F0; border-radius:12px;
    padding:14px 16px; margin-bottom:8px;
  }
  .macro-label { font-size:10px; letter-spacing:.1em; text-transform:uppercase; color:#94A3B8; margin:0; }
  .macro-value { font-family:'Syne',sans-serif; font-size:24px; font-weight:700; margin:4px 0 2px; color:#080E1D; }
  .macro-up   { color:#10B981; font-size:13px; font-weight:600; }
  .macro-down { color:#EF4444; font-size:13px; font-weight:600; }
  .macro-flat { color:#94A3B8; font-size:13px; }

  /* ── Hide Streamlit chrome ── */
  #MainMenu, footer, header { visibility:hidden; }
</style>
""", unsafe_allow_html=True)

# ── Dark/light mode toggle in sidebar ─────────────────────────────────────────
# Streamlit natively respects system preference, but we also offer a manual toggle
_mode_col1, _mode_col2 = st.sidebar.columns([3,1])
with _mode_col1:
    st.sidebar.markdown('<p style="font-size:9px;color:#475569;letter-spacing:.1em;margin-bottom:4px">APPEARANCE</p>', unsafe_allow_html=True)
with _mode_col2:
    pass
_theme = st.sidebar.radio("", ["☀️ Light", "🌙 Dark"], horizontal=True, label_visibility="collapsed")
DARK = "Dark" in _theme

if DARK:
    st.markdown("""<style>
      /* ── Main backgrounds ── */
      .main,
      [data-testid="stAppViewContainer"],
      [data-testid="stApp"],
      section[data-testid="stMainBlockContainer"],
      section[data-testid="stMainBlockContainer"] > div,
      [data-testid="block-container"]
        { background: #0D1117 !important; }

      /* ── Text ── */
      h1,h2,h3,h4,h5,h6 { color: #F1F5F9 !important; }
      p, span, label, div { color: #CBD5E1 !important; }
      .sec { color: #3A86FF !important; }
      .kpi-l, .kpi-s { color: rgba(203,213,225,.5) !important; }
      [data-testid="stMarkdownContainer"] p { color: #CBD5E1 !important; }
      [data-testid="stCaptionContainer"] { color: #64748B !important; }

      /* ── Metrics ── */
      [data-testid="stMetric"] { background: #161B27 !important; border-radius:10px; padding:10px; }
      [data-testid="stMetricValue"] > div { color: #E2E8F0 !important; }
      [data-testid="stMetricLabel"] > div { color: #64748B !important; }
      [data-testid="stMetricDelta"] > div { color: #10B981 !important; }

      /* ── Dataframes / tables ── */
      [data-testid="stDataFrame"] { background: #161B27 !important; border-radius:10px; overflow:hidden; }
      [data-testid="stDataFrame"] iframe { background: #161B27 !important; }
      .stDataFrame > div { background: #161B27 !important; }
      /* Force the internal Streamlit dataframe iframe content */
      [data-testid="stDataFrame"] [class*="dataframe"] { background: #161B27 !important; color: #E2E8F0 !important; }

      /* ── Form inputs ── */
      [data-testid="stSelectbox"] > div > div,
      [data-testid="stMultiSelect"] > div > div,
      [data-testid="stTextInput"] > div > div
        { background: #1F2937 !important; color: #E2E8F0 !important; border-color: #374151 !important; }
      [data-testid="stSelectbox"] svg,
      [data-testid="stMultiSelect"] svg { fill: #94A3B8 !important; }

      /* ── Tabs ── */
      [data-testid="stTabs"] [role="tab"] { color: #94A3B8 !important; }
      [data-testid="stTabs"] [role="tab"][aria-selected="true"] { color: #3A86FF !important; border-bottom-color: #3A86FF !important; }
      [data-testid="stTabsContent"] { background: #0D1117 !important; }

      /* ── Expanders ── */
      [data-testid="stExpander"] { background: #161B27 !important; border-color: #1E3A5F !important; }
      [data-testid="stExpander"] summary { color: #E2E8F0 !important; }

      /* ── Checkboxes and sliders ── */
      [data-testid="stCheckbox"] label { color: #CBD5E1 !important; }
      [data-testid="stSlider"] label { color: #CBD5E1 !important; }
      [data-testid="stRadio"] label { color: #CBD5E1 !important; }

      /* ── Custom cards ── */
      .news-card { background: #161B27 !important; border-left-color: #3A86FF !important; }
      .news-title { color: #E2E8F0 !important; }
      .news-meta  { color: #64748B !important; }
      .macro-card { background: #161B27 !important; border-color: #1E3A5F !important; }
      .macro-value { color: #E2E8F0 !important; }
      .macro-label { color: #64748B !important; }

      /* ── Alerts / info boxes ── */
      [data-testid="stAlert"] { background: #1F2937 !important; color: #E2E8F0 !important; }
      [data-testid="stInfo"] { background: #1E3A5F !important; color: #BFDBFE !important; }
      [data-testid="stSuccess"] { background: #064E3B !important; color: #A7F3D0 !important; }
      [data-testid="stWarning"] { background: #78350F !important; color: #FDE68A !important; }

      /* ── Plotly charts — override white bg via wrapper ── */
      [data-testid="stPlotlyChart"] { background: #111827 !important; border-radius:10px; }
      .js-plotly-plot .plotly .bg { fill: #111827 !important; }

      /* ── Dividers ── */
      hr { border-color: #1E293B !important; }

      /* ── Dataframe — match main dark background exactly ── */
      [data-testid="stDataFrame"] { border: none !important; }
      [data-testid="stDataFrame"] > div {
        background: #0D1117 !important;
        border: 1px solid #1E293B !important;
        border-radius: 10px !important;
      }
      /* Remove the default white iframe background */
      [data-testid="stDataFrame"] iframe {
        background: transparent !important;
      }
      /* Header row */
      [data-testid="stDataFrame"] th {
        background: #111827 !important;
        color: #64748B !important;
        font-size: 11px !important;
        letter-spacing: 0.05em !important;
        border-bottom: 1px solid #1E293B !important;
        font-weight: 600 !important;
      }
      /* Data cells */
      [data-testid="stDataFrame"] td {
        color: #CBD5E1 !important;
        background: #0D1117 !important;
        border-bottom: 1px solid #111827 !important;
        font-size: 12px !important;
      }
      /* Hover row highlight */
      [data-testid="stDataFrame"] tr:hover td {
        background: #111827 !important;
      }
      /* Index column */
      [data-testid="stDataFrame"] th:first-child,
      [data-testid="stDataFrame"] td:first-child {
        background: #111827 !important;
        color: #475569 !important;
      }

    </style>""", unsafe_allow_html=True)

NAVY = "#080E1D"
BLUE = "#3A86FF"
COLORS = [BLUE, "#FF6B6B", "#FFD166", "#06D6A0", "#8338EC", "#FB5607", "#3A86FF", "#FFBE0B"]

def fig_style(fig, h=300, dark=False):
    bg   = "#111827" if dark else "white"
    grid = "#1F2937" if dark else "#F1F5F9"
    line = "#374151" if dark else "#E2E8F0"
    txt  = "#E2E8F0" if dark else "#334155"
    fig.update_layout(
        plot_bgcolor=bg, paper_bgcolor=bg, font_family="Inter", font_color=txt,
        margin=dict(l=8,r=8,t=28,b=8), height=h,
        xaxis=dict(showgrid=False, showline=True, linecolor=line, title="", color=txt),
        yaxis=dict(showgrid=True, gridcolor=grid, title="", color=txt),
        legend=dict(bgcolor="rgba(0,0,0,0)", borderwidth=0, font_color=txt),
        hoverlabel=dict(bgcolor=bg, font_size=12, font_color=txt),
    )
    return fig


# ── Connections ────────────────────────────────────────────────────────────────
def _pg_creds():
    """Read PostgreSQL credentials from secrets or env."""
    if hasattr(st,"secrets") and "postgres" in st.secrets:
        s = st.secrets["postgres"]
        return (str(s["host"]).strip(), int(s.get("port",5432)),
                str(s["dbname"]).strip(), str(s["user"]).strip(),
                str(s["password"]).strip())
    from dotenv import load_dotenv; load_dotenv()
    return (os.getenv("SUPABASE_HOST","").strip(),
            int(os.getenv("SUPABASE_PORT","5432")),
            os.getenv("SUPABASE_DB","postgres"),
            os.getenv("SUPABASE_USER","").strip(),
            os.getenv("SUPABASE_PASSWORD","").strip())

def pg():
    """
    Open a fresh PostgreSQL connection every call.
    No caching — avoids stale/closed connection errors after
    Atlas upgrades, Supabase restarts, or long idle periods.
    """
    import psycopg2
    try:
        h,port,db,u,pw = _pg_creds()
        if not h: st.error("SUPABASE_HOST empty"); return None
        return psycopg2.connect(
            host=h, port=port, dbname=db, user=u, password=pw,
            sslmode="require", connect_timeout=10,
            keepalives=1, keepalives_idle=30,
            keepalives_interval=10, keepalives_count=5,
        )
    except Exception as e:
        st.error(f"PostgreSQL failed: {e}"); return None

def mongo():
    """Connect to MongoDB — no caching so secrets are always read fresh."""
    from pymongo import MongoClient

    # Build URI — try every possible way Streamlit might expose it
    uri = ""
    try:
        # Method 1: top-level key
        uri = str(st.secrets["MONGO_URI"]).strip()
    except Exception:
        pass

    if not uri:
        try:
            # Method 2: nested inside [postgres] block (common Streamlit TOML issue)
            uri = str(st.secrets["postgres"]["MONGO_URI"]).strip()
        except Exception:
            pass

    if not uri:
        try:
            # Method 3: via .get()
            uri = str(st.secrets.get("MONGO_URI", "")).strip()
        except Exception:
            pass

    if not uri:
        # Method 4: env var fallback for local dev
        try:
            from dotenv import load_dotenv; load_dotenv()
        except Exception:
            pass
        uri = os.getenv("MONGO_URI", "").strip()

    if not uri:
        st.error("MONGO_URI not found — check Streamlit Secrets")
        return None

    try:
        c = MongoClient(uri, serverSelectionTimeoutMS=8000)
        c.admin.command("ping")
        return c
    except Exception as e:
        err = str(e)
        if "bad auth" in err or "Authentication" in err:
            st.error(
                "MongoDB authentication failed — the Atlas upgrade may have reset "
                "your database user password. Go to Atlas → Database Access → "
                "Edit your user → Reset password, then update MONGO_URI in "
                "Streamlit Secrets and GitHub Secrets with the new password."
            )
        else:
            st.error(f"MongoDB connection failed: {e}")
        return None

@st.cache_data(ttl=300, show_spinner=False)
def html_table(df: "pd.DataFrame", dark: bool = False) -> str:
    """Render a DataFrame as a styled HTML table that works in both light and dark mode."""
    bg     = "#0D1117" if dark else "#FFFFFF"
    hdr_bg = "#111827" if dark else "#F8FAFF"
    hdr_c  = "#64748B" if dark else "#64748B"
    row_c  = "#CBD5E1" if dark else "#1E293B"
    brd    = "#1E293B" if dark else "#E2E8F0"
    alt_bg = "#111827" if dark else "#F8FAFF"

    cols = df.columns.tolist()
    header = "".join(
        f'<th style="background:{hdr_bg};color:{hdr_c};padding:8px 12px;'
        f'text-align:left;font-size:11px;font-weight:600;letter-spacing:.05em;'
        f'border-bottom:1px solid {brd};white-space:nowrap">{c}</th>'
        for c in cols
    )
    rows = ""
    for i, (_, row) in enumerate(df.iterrows()):
        rbg = alt_bg if i % 2 == 0 else bg
        cells = "".join(
            f'<td style="background:{rbg};color:{row_c};padding:7px 12px;'
            f'font-size:12px;border-bottom:1px solid {brd};white-space:nowrap">'
            f'{str(v)}</td>'
            for v in row.values
        )
        rows += f"<tr>{cells}</tr>"

    return (
        f'<div style="overflow-x:auto;border-radius:10px;border:1px solid {brd};margin-bottom:1rem">'
        f'<table style="width:100%;border-collapse:collapse;background:{bg}">'
        f"<thead><tr>{header}</tr></thead>"
        f"<tbody>{rows}</tbody>"
        f"</table></div>"
    )


def q(sql, params=None):
    """Execute a SQL query, always opening and closing a fresh connection."""
    conn = pg()
    if conn is None: return pd.DataFrame()
    try:
        result = pd.read_sql_query(sql, conn, params=params)
        return result
    except Exception as e:
        st.warning(f"Query: {e}")
        return pd.DataFrame()
    finally:
        try: conn.close()
        except Exception: pass


# ── Sidebar ────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
    <div style='padding:6px 0 14px'>
      <p style='font-family:Syne;font-size:15px;font-weight:700;color:#E2E8F0;margin:0'>📈 FinIntel</p>
      <p style='font-size:10px;color:#475569;margin:2px 0 0'>Group 7 · Columbia University</p>
    </div>""", unsafe_allow_html=True)

    page = st.radio("", ["🏠 Overview","📊 Market Data","📈 Stock Prices",
                         "📁 SEC Filings","📰 News Feed","🔗 Cross-Source","ℹ️ About"],
                    label_visibility="collapsed")
    page = page.split(" ",1)[1]

    st.markdown("---")
    st.markdown('<p style="font-size:9px;color:#475569;letter-spacing:.1em">DATE RANGE</p>', unsafe_allow_html=True)
    preset = st.selectbox("", ["Last 30 days","Last 90 days","Last 1 year","Last 5 years","Since 2010","Custom"],
                          index=4,   # default to "Since 2010"
                          label_visibility="collapsed")
    today = date.today()
    presets = {"Last 30 days":30,"Last 90 days":90,"Last 1 year":365,"Last 5 years":1825}
    if preset in presets:
        sd, ed = today-timedelta(days=presets[preset]), today
    elif preset == "Since 2010":
        sd, ed = date(2010,1,1), today
    else:
        sd = st.date_input("From", value=date(2020,1,1))
        ed = st.date_input("To",   value=today)

    st.markdown("---")
    st.markdown('<p style="font-size:9px;color:#475569;letter-spacing:.1em">SECTOR FILTER</p>', unsafe_allow_html=True)
    sectors = {
        "All": ["AAPL","MSFT","GOOGL","AMZN","META","NVDA","TSLA","NFLX","ORCL","ADBE","JPM","BAC","GS","MS","WFC","C","BLK","AXP","V","MA","JNJ","PFE","UNH","ABBV","MRK","XOM","CVX","COP","WMT","KO","PEP","MCD","NKE","BA","CAT","HON"],
        "Big Tech": ["AAPL","MSFT","GOOGL","AMZN","META","NVDA","TSLA","NFLX","ORCL","ADBE"],
        "Finance":  ["JPM","BAC","GS","MS","WFC","C","BLK","AXP","V","MA"],
        "Healthcare":["JNJ","PFE","UNH","ABBV","MRK"],
        "Energy":   ["XOM","CVX","COP"],
        "Consumer": ["WMT","KO","PEP","MCD","NKE"],
        "Industrial":["BA","CAT","HON"],
    }
    sec_sel = st.selectbox("", list(sectors.keys()), index=0, label_visibility="collapsed")
    pool = sectors[sec_sel]
    tickers = st.multiselect("Tickers", pool, default=pool)
    if not tickers: tickers = pool

    st.markdown("---")
    st.markdown('<p style="font-size:9px;color:#475569;letter-spacing:.1em">LIVE MODE</p>', unsafe_allow_html=True)
    auto_refresh = st.toggle("Auto-refresh (30s)", value=False)
    if auto_refresh:
        import time as _time
        st.markdown('<p style="font-size:10px;color:#3A86FF;margin:0">🔴 Live · refreshing every 30s</p>', unsafe_allow_html=True)

    if st.button("🔄 Refresh data", use_container_width=True):
        st.cache_data.clear()
        st.toast("✓ Data refreshed from database", icon="🔄")
        st.rerun()

    st.markdown('''<p style="font-size:9px;color:#475569;line-height:1.6;margin-top:4px">
    Refresh pulls latest data from Supabase + Atlas.<br>
    New articles ingested daily at 06:00 UTC via GitHub Actions.
    </p>''', unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# OVERVIEW
# ══════════════════════════════════════════════════════════════════════════════
if "DARK" not in dir(): DARK = False  # fallback
if page == "Overview":
    st.markdown('<p class="sec">Financial Intelligence Platform</p>', unsafe_allow_html=True)
    st.title("Market Overview")

    # Auto-refresh handler
    if auto_refresh:
        import time as _t
        st.caption(f"🔴 Live · {today.strftime('%B %d, %Y')} · auto-refreshing every 30s")
        _t.sleep(30)
        st.cache_data.clear()
        st.rerun()
    else:
        st.caption(f"Live data · {today.strftime('%B %d, %Y')} · Supabase + MongoDB Atlas")

    counts = q("""SELECT
        (SELECT COUNT(*) FROM market_data) market_rows,
        (SELECT COUNT(*) FROM sec_filings) filing_rows,
        (SELECT COUNT(*) FROM news_sentiment) news_rows,
        (SELECT COUNT(DISTINCT series_code) FROM market_data) series_count,
        (SELECT COUNT(DISTINCT ticker) FROM sec_filings) ticker_count""")

    c1,c2,c3,c4,c5 = st.columns(5)
    def kpi(col,lbl,val,sub="",accent="kpi-v"):
        with col:
            st.markdown(f'<div class="kpi"><p class="kpi-l">{lbl}</p><p class="{accent}">{val}</p><p class="kpi-s">{sub}</p></div>', unsafe_allow_html=True)
    if not counts.empty:
        r = counts.iloc[0]
        kpi(c1,"Market data points",f"{int(r.market_rows):,}",f"{int(r.series_count)} FRED series","kpi-v kpi-v-teal")
        kpi(c2,"SEC filings",f"{int(r.filing_rows):,}",f"{int(r.ticker_count)} companies","kpi-v kpi-v-blue")
        kpi(c3,"News articles",f"{int(r.news_rows):,}","indexed","kpi-v kpi-v-amber")
        kpi(c4,"Data sources","3","NewsAPI · Edgar · FRED","kpi-v kpi-v-coral")
        kpi(c5,"Technologies","5","Kafka·Spark·PG·Mongo·Airflow","kpi-v kpi-v-teal")

    # Live data volume counter
    vol = q("""SELECT
        (SELECT COUNT(*) FROM market_data)    AS m,
        (SELECT COUNT(*) FROM sec_filings)    AS f,
        (SELECT COUNT(*) FROM news_sentiment) AS n,
        (SELECT COUNT(*) FROM stock_prices)   AS sp,
        (SELECT pg_size_pretty(
            pg_total_relation_size('market_data') +
            pg_total_relation_size('sec_filings') +
            pg_total_relation_size('news_sentiment') +
            pg_total_relation_size('stock_prices')
        )) AS pg_size,
        (SELECT pg_database_size(current_database())) AS pg_bytes
    """)

    # Get MongoDB size estimate separately
    mongo_size_mb = 0
    try:
        mc = mongo()
        if mc:
            db_stats = mc["fin_intelligence"].command("dbStats", scale=1024*1024)
            mongo_size_mb = round(db_stats.get("dataSize", 0) + db_stats.get("indexSize", 0), 1)
            mc.close()
    except Exception:
        pass

    if not vol.empty:
        r = vol.iloc[0]
        pg_mb    = round(int(r["pg_bytes"]) / 1024 / 1024, 1)
        total_mb = round(pg_mb + mongo_size_mb, 1)
        total_gb = round(total_mb / 1024, 2)
        total_pg_rows = int(r["m"]) + int(r["f"]) + int(r["n"]) + int(r["sp"])

        size_label = f"{total_gb} GB" if total_gb >= 0.1 else f"{total_mb} MB"
        pct_of_1gb = min(100, round(total_mb / 1024 * 100, 1))

        st.markdown(f"""
        <div style="background:linear-gradient(90deg,#080E1D,#162040);border-radius:12px;
                    padding:16px 24px;margin:12px 0;border:1px solid #1E3A5F;color:white;">
          <div style="display:flex;align-items:center;gap:32px;flex-wrap:wrap">
            <div>
              <p style="font-size:9px;letter-spacing:.12em;opacity:.5;margin:0">TOTAL RECORDS (PostgreSQL)</p>
              <p style="font-family:Syne,sans-serif;font-size:28px;font-weight:700;margin:2px 0;
                        letter-spacing:-.02em;color:#3BFFA0">
                {total_pg_rows:,} <span style="font-size:13px;opacity:.5;font-weight:400">rows</span>
              </p>
            </div>
            <div style="width:1px;height:44px;background:rgba(255,255,255,.08)"></div>
            <div><p style="font-size:9px;opacity:.5;margin:0">MARKET DATA</p><p style="font-size:16px;font-weight:600;margin:0;color:#5BA4FF">{int(r["m"]):,}</p></div>
            <div><p style="font-size:9px;opacity:.5;margin:0">SEC FILINGS</p><p style="font-size:16px;font-weight:600;margin:0;color:#5BA4FF">{int(r["f"]):,}</p></div>
            <div><p style="font-size:9px;opacity:.5;margin:0">STOCK PRICES</p><p style="font-size:16px;font-weight:600;margin:0;color:#5BA4FF">{int(r["sp"]):,}</p></div>
            <div><p style="font-size:9px;opacity:.5;margin:0">NEWS ARTICLES</p><p style="font-size:16px;font-weight:600;margin:0;color:#5BA4FF">{int(r["n"]):,}</p></div>
            <div style="width:1px;height:44px;background:rgba(255,255,255,.08)"></div>
            <div>
              <p style="font-size:9px;opacity:.5;margin:0">POSTGRESQL SIZE</p>
              <p style="font-size:16px;font-weight:600;margin:0;color:#FFD166">{r["pg_size"]}</p>
            </div>
            <div>
              <p style="font-size:9px;opacity:.5;margin:0">MONGODB SIZE</p>
              <p style="font-size:16px;font-weight:600;margin:0;color:#FF6B6B">{mongo_size_mb} MB</p>
            </div>
            <div style="width:1px;height:44px;background:rgba(255,255,255,.08)"></div>
            <div>
              <p style="font-size:9px;opacity:.5;margin:0">COMBINED STORAGE</p>
              <p style="font-family:Syne,sans-serif;font-size:20px;font-weight:700;margin:2px 0;
                        color:#FFD166">{size_label}</p>
              <p style="font-size:9px;opacity:.45;margin:0">{pct_of_1gb}% of 1 GB target</p>
            </div>
          </div>
          <!-- Progress bar toward 1GB -->
          <div style="margin-top:10px;background:rgba(255,255,255,.06);border-radius:4px;height:4px">
            <div style="background:linear-gradient(90deg,#3BFFA0,#3A86FF);
                        width:{pct_of_1gb}%;height:4px;border-radius:4px;
                        transition:width .3s"></div>
          </div>
          <p style="font-size:8px;opacity:.35;margin:4px 0 0;text-align:right">
            {size_label} of 1 GB target · PostgreSQL {r["pg_size"]} · MongoDB {mongo_size_mb} MB
          </p>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("---")
    col1,col2 = st.columns([3,2])
    with col1:
        st.markdown('<p class="sec">S&P 500</p>', unsafe_allow_html=True)
        sp = q("SELECT date,value FROM market_data WHERE series_code='SP500' ORDER BY date")
        if not sp.empty:
            sp["date"] = pd.to_datetime(sp["date"])
            show_ma = st.checkbox("50-day MA", value=True)
            fig = go.Figure()
            fig.add_trace(go.Scatter(x=sp["date"],y=sp["value"],name="S&P 500",
                                     line=dict(color=BLUE,width=2),fill="tozeroy",
                                     fillcolor="rgba(58,134,255,0.06)"))
            if show_ma and len(sp)>=50:
                sp["ma"]=sp["value"].rolling(50).mean()
                fig.add_trace(go.Scatter(x=sp["date"],y=sp["ma"],name="50d MA",
                                         line=dict(color="#FF6B6B",width=1.5,dash="dot")))
            fig_style(fig, 290, dark=DARK); fig.update_xaxes(rangeslider_visible=True,rangeslider_thickness=0.05)
            st.plotly_chart(fig,use_container_width=True)
        else:
            st.info("No S&P 500 data — run pipeline first")

    with col2:
        st.markdown('<p class="sec">Filings by company</p>', unsafe_allow_html=True)
        ph=",".join(["%s"]*len(tickers))
        fb=q(f"SELECT ticker,form_type,COUNT(*) n FROM sec_filings WHERE ticker IN ({ph}) GROUP BY ticker,form_type ORDER BY ticker",params=tuple(tickers))
        if not fb.empty:
            fig2=px.bar(fb,x="ticker",y="n",color="form_type",barmode="stack",
                        color_discrete_map={"8-K":"#FF6B6B","10-K":BLUE})
            fig_style(fig2, 290, dark=DARK); st.plotly_chart(fig2,use_container_width=True)

    st.markdown("---")
    col3,col4 = st.columns([2,3])
    with col3:
        st.markdown('<p class="sec">Live macro snapshot</p>', unsafe_allow_html=True)
        # Fetch latest AND previous period for each series to compute MoM change
        macro_latest = q("""
            SELECT DISTINCT ON (series_code) series_code, value AS latest, date AS latest_date
            FROM market_data
            WHERE series_code IN ('DFF','CPIAUCSL','UNRATE','GS10','VIXCLS')
            ORDER BY series_code, date DESC
        """)
        macro_prev = q("""
            SELECT series_code, value AS prev, date AS prev_date
            FROM (
                SELECT series_code, value, date,
                       ROW_NUMBER() OVER (PARTITION BY series_code ORDER BY date DESC) AS rn
                FROM market_data
                WHERE series_code IN ('DFF','CPIAUCSL','UNRATE','GS10','VIXCLS')
            ) t WHERE rn = 2
        """)
        labels={"DFF":"Fed Funds Rate","CPIAUCSL":"CPI","UNRATE":"Unemployment","GS10":"10Y Treasury","VIXCLS":"VIX"}
        units ={"DFF":"%","CPIAUCSL":"","UNRATE":"%","GS10":"%","VIXCLS":""}
        if not macro_latest.empty and not macro_prev.empty:
            macro_merged = macro_latest.merge(macro_prev, on="series_code", how="left")
            for _,row in macro_merged.iterrows():
                code = row["series_code"]
                label = labels.get(code, code)
                unit  = units.get(code, "")
                val   = row["latest"]
                prev  = row.get("prev", None)
                val_str = f"{val:.2f}{unit}"
                if prev and prev != 0:
                    chg = val - prev
                    pct = (chg / abs(prev)) * 100
                    arrow = "↑" if chg > 0 else "↓"
                    chg_color = "#10B981" if chg > 0 else "#EF4444"
                    chg_str = f"{arrow} {abs(pct):.2f}%"
                    st.markdown(f"""
                    <div class="macro-card">
                      <p class="macro-label">{label}</p>
                      <div style="display:flex;align-items:baseline;gap:10px">
                        <p class="macro-value">{val_str}</p>
                        <span style="color:{chg_color};font-size:13px;font-weight:600">{chg_str}</span>
                      </div>
                      <p style="font-size:10px;color:#94A3B8;margin:0">vs previous: {prev:.2f}{unit}</p>
                    </div>
                    """, unsafe_allow_html=True)
                else:
                    st.markdown(f"""
                    <div class="macro-card">
                      <p class="macro-label">{label}</p>
                      <p class="macro-value">{val_str}</p>
                    </div>
                    """, unsafe_allow_html=True)

    with col4:
        st.markdown('<p class="sec">Most recent filings</p>', unsafe_allow_html=True)
        rf=q("SELECT ticker,company_name,form_type,filed_at FROM sec_filings ORDER BY filed_at DESC LIMIT 12")
        if not rf.empty:
            rf["filed_at"]=pd.to_datetime(rf["filed_at"].astype(str)).dt.strftime("%Y-%m-%d")
            rf_display = rf.rename(columns={"ticker":"Ticker","company_name":"Company","form_type":"Form","filed_at":"Filed"})
            st.markdown(html_table(rf_display, dark=DARK), unsafe_allow_html=True)

    # Pipeline health strip
    st.markdown("---")
    st.markdown('<p class="sec">Pipeline health</p>', unsafe_allow_html=True)
    h1,h2,h3,h4 = st.columns(4)
    last_market = q("SELECT MAX(fetched_at) AS t FROM market_data")
    last_filing = q("SELECT MAX(fetched_at) AS t FROM sec_filings")
    last_news   = q("SELECT MAX(fetched_at) AS t FROM news_sentiment")
    newest_filing = q("SELECT MAX(filed_at) AS t FROM sec_filings")

    def fmt_time(df):
        if df.empty or df.iloc[0]["t"] is None: return "No data"
        try: return pd.to_datetime(str(df.iloc[0]["t"])).strftime("%b %d %H:%M UTC")
        except: return "Unknown"

    h1.metric("Last market ingest",  fmt_time(last_market))
    h2.metric("Last filing ingest",  fmt_time(last_filing))
    h3.metric("Last news ingest",    fmt_time(last_news))
    h4.metric("Newest filing date",  fmt_time(newest_filing))


# ══════════════════════════════════════════════════════════════════════════════
# MARKET DATA
# ══════════════════════════════════════════════════════════════════════════════
elif page == "Market Data":
    st.markdown('<p class="sec">FRED Economic Indicators</p>', unsafe_allow_html=True)
    st.title("Market & Economic Data")

    sdf=q("SELECT DISTINCT series_code,series_name FROM market_data ORDER BY series_code")
    if sdf.empty: st.info("No market data yet"); st.stop()

    opts={r["series_name"]:r["series_code"] for _,r in sdf.iterrows()}
    c1,c2,c3,c4 = st.columns(4)
    with c1: sel_name=st.selectbox("Indicator",list(opts.keys()))
    with c2: chart_t=st.selectbox("Chart type",["Line","Area","Bar"])
    with c3: log_s=st.checkbox("Log scale")
    with c4: compare=st.checkbox("Compare mode")

    code=opts[sel_name]
    data=q("SELECT date,value FROM market_data WHERE series_code=%s AND date BETWEEN %s AND %s ORDER BY date",params=(code,sd,ed))

    if data.empty: st.warning("No data for this range"); st.stop()
    data["date"]=pd.to_datetime(data["date"])

    lat,old=data.iloc[-1]["value"],data.iloc[0]["value"]
    chg=lat-old; pct=(chg/old*100) if old else 0
    m1,m2,m3,m4,m5=st.columns(5)
    m1.metric("Latest",f"{lat:,.4f}")
    m2.metric("Period start",f"{old:,.4f}")
    m3.metric("Change",f"{chg:+,.4f}",delta=f"{pct:+.2f}%")
    m4.metric("High",f"{data['value'].max():,.4f}")
    m5.metric("Low",f"{data['value'].min():,.4f}")

    st.markdown("---")

    if compare:
        comp_sel=st.multiselect("Compare indicators",list(opts.keys()),default=list(opts.keys())[:4])
        fig=go.Figure()
        for nm in comp_sel:
            cd=opts[nm]
            df=q("SELECT date,value FROM market_data WHERE series_code=%s AND date BETWEEN %s AND %s ORDER BY date",params=(cd,sd,ed))
            if not df.empty:
                df["date"]=pd.to_datetime(df["date"]); base=df.iloc[0]["value"]
                df["norm"]=df["value"]/base*100
                fig.add_trace(go.Scatter(x=df["date"],y=df["norm"],name=nm[:30],mode="lines",line=dict(width=2)))
        fig_style(fig, 380, dark=DARK); fig.update_layout(title="Normalized to 100 at period start",yaxis_title="Index")
        st.plotly_chart(fig,use_container_width=True)
    else:
        ma_w=st.slider("Moving average (days, 0=off)",0,200,0,10)
        rec=st.checkbox("Show recession bands (2001, 2008, 2020)")
        fig=go.Figure()
        if chart_t=="Area":
            fig.add_trace(go.Scatter(x=data["date"],y=data["value"],name=sel_name,fill="tozeroy",fillcolor="rgba(58,134,255,0.08)",line=dict(color=BLUE,width=2)))
        elif chart_t=="Bar":
            fig.add_trace(go.Bar(x=data["date"],y=data["value"],name=sel_name,marker_color=BLUE,opacity=0.7))
        else:
            fig.add_trace(go.Scatter(x=data["date"],y=data["value"],name=sel_name,line=dict(color=BLUE,width=2)))
        if ma_w>0:
            data["ma"]=data["value"].rolling(ma_w).mean()
            fig.add_trace(go.Scatter(x=data["date"],y=data["ma"],name=f"{ma_w}d MA",line=dict(color="#FF6B6B",width=1.5,dash="dot")))
        if rec:
            for rs,re,lbl in [("2001-03-01","2001-11-01","Dot-com"),("2007-12-01","2009-06-01","GFC"),("2020-02-01","2020-04-01","COVID")]:
                fig.add_vrect(x0=rs,x1=re,fillcolor="rgba(255,107,107,0.1)",line_width=0,annotation_text=lbl,annotation_font_size=10)
        if log_s: fig.update_yaxes(type="log")
        fig_style(fig, 400, dark=DARK); fig.update_layout(title=sel_name)
        fig.update_xaxes(rangeslider_visible=True,rangeslider_thickness=0.05)
        st.plotly_chart(fig,use_container_width=True)

    st.markdown("---")
    a1,a2=st.columns(2)
    with a1:
        st.markdown('<p class="sec">Value distribution</p>', unsafe_allow_html=True)
        fh=px.histogram(data,x="value",nbins=50,color_discrete_sequence=[BLUE])
        fig_style(fh, 200, dark=DARK); st.plotly_chart(fh,use_container_width=True)
    with a2:
        st.markdown('<p class="sec">Period-over-period % change</p>', unsafe_allow_html=True)
        data["pct"]=data["value"].pct_change()*100
        fc=px.bar(data.dropna(),x="date",y="pct",color="pct",
                  color_continuous_scale=["#FF6B6B","#F8FAFF",BLUE],color_continuous_midpoint=0)
        fig_style(fc, 200, dark=DARK); fc.update_coloraxes(showscale=False)
        st.plotly_chart(fc,use_container_width=True)

    if st.checkbox("Show raw data + download"):
        st.markdown(html_table(data.sort_values("date",ascending=False).head(500), dark=DARK), unsafe_allow_html=True)
        st.download_button("Download CSV",data.to_csv(index=False),f"{code}.csv","text/csv")


# ══════════════════════════════════════════════════════════════════════════════
# SEC FILINGS
# ══════════════════════════════════════════════════════════════════════════════
elif page == "Stock Prices":
    st.markdown('<p class="sec">Alpaca Markets · IEX feed</p>', unsafe_allow_html=True)
    st.title("Stock Price Explorer")
    st.caption("Daily OHLCV bars · adjusted for splits & dividends · powered by Alpaca free tier")

    # ── Ticker selector ──────────────────────────────────────────────────────
    sp_col1, sp_col2, sp_col3 = st.columns([2,1,1])
    with sp_col1:
        sp_tickers = st.multiselect("Compare tickers", tickers, default=tickers[:5])
    with sp_col2:
        sp_metric = st.selectbox("Show", ["Close price","Open","High","Low","Volume","VWAP"])
    with sp_col3:
        sp_norm = st.checkbox("Normalize to 100", value=False,
                              help="Rebases all series to 100 at start of period for easy comparison")

    metric_col = {"Close price":"close","Open":"open","High":"high",
                  "Low":"low","Volume":"volume","VWAP":"vwap"}[sp_metric]

    if not sp_tickers:
        st.info("Select at least one ticker above")
        st.stop()

    # ── Fetch data ───────────────────────────────────────────────────────────
    ph = ",".join(["%s"]*len(sp_tickers))
    price_df = q(f"""
        SELECT ticker, date, open, high, low, close, volume, vwap
        FROM stock_prices
        WHERE ticker IN ({ph}) AND date BETWEEN %s AND %s
        ORDER BY date, ticker
    """, params=tuple(sp_tickers) + (sd, ed))

    if price_df.empty:
        st.warning("No stock price data yet — add ALPACA_API_KEY + ALPACA_SECRET_KEY to GitHub Secrets and re-run the pipeline")
        st.code("""# Add to GitHub Actions Secrets:
ALPACA_API_KEY    = your_key_here
ALPACA_SECRET_KEY = your_secret_here

# Then go to Actions tab → Daily ETL Pipeline → Run workflow""")
        st.stop()

    price_df["date"] = pd.to_datetime(price_df["date"].astype(str))

    # ── KPI row — latest close per ticker ────────────────────────────────────
    # Build latest and previous close per ticker safely
    latest_dict = {}
    prev_dict   = {}
    for tkr in sp_tickers:
        td = price_df[price_df["ticker"] == tkr].sort_values("date")
        if len(td) >= 2:
            latest_dict[tkr] = td.iloc[-1]["close"]
            prev_dict[tkr]   = td.iloc[-2]["close"]
        elif len(td) == 1:
            latest_dict[tkr] = td.iloc[-1]["close"]
            prev_dict[tkr]   = td.iloc[-1]["close"]

    n_cols = min(len(sp_tickers), 5)
    kpi_cols = st.columns(n_cols)
    for i, tkr in enumerate(sp_tickers[:n_cols]):
        lat  = latest_dict.get(tkr, 0)
        prev = prev_dict.get(tkr, lat)
        chg  = lat - prev
        pct  = (chg / prev * 100) if prev else 0
        arrow = "↑" if chg >= 0 else "↓"
        color = "#10B981" if chg >= 0 else "#EF4444"
        accent = "teal" if chg >= 0 else "coral"
        with kpi_cols[i]:
            st.markdown(f"""
            <div class="kpi">
              <p class="kpi-l">{tkr}</p>
              <p class="kpi-v kpi-v-{accent}">${lat:.2f}</p>
              <p class="kpi-s" style="color:{color}">{arrow} {abs(pct):.2f}% vs prev day</p>
            </div>""", unsafe_allow_html=True)

    st.markdown("---")

    # ── Main price chart ─────────────────────────────────────────────────────
    fig = go.Figure()
    for ticker in sp_tickers:
        td = price_df[price_df["ticker"] == ticker].copy()
        if td.empty: continue
        y = td[metric_col]
        if sp_norm and y.iloc[0] != 0:
            y = y / y.iloc[0] * 100
        fig.add_trace(go.Scatter(
            x=td["date"], y=y, name=ticker, mode="lines", line=dict(width=2)
        ))
    fig_style(fig, 400, dark=DARK)
    title = f"{sp_metric} — {'Normalized (100 = period start)' if sp_norm else 'Absolute'}"
    fig.update_layout(title=title, hovermode="x unified")
    fig.update_xaxes(rangeslider_visible=True, rangeslider_thickness=0.05)
    st.plotly_chart(fig, use_container_width=True)

    # ── Candlestick chart (single ticker) ─────────────────────────────────────
    st.markdown("---")
    st.markdown('<p class="sec">Candlestick chart</p>', unsafe_allow_html=True)
    candle_ticker = st.selectbox("Select ticker for candlestick", sp_tickers)
    candle_df = price_df[price_df["ticker"] == candle_ticker]

    if not candle_df.empty:
        fig_c = go.Figure(data=go.Candlestick(
            x=candle_df["date"],
            open=candle_df["open"], high=candle_df["high"],
            low=candle_df["low"],   close=candle_df["close"],
            name=candle_ticker,
            increasing_line_color="#10B981", decreasing_line_color="#EF4444",
        ))
        # Add volume bars on secondary axis
        fig_c.add_trace(go.Bar(
            x=candle_df["date"], y=candle_df["volume"],
            name="Volume", marker_color="rgba(58,134,255,0.2)",
            yaxis="y2"
        ))
        fig_c.update_layout(
            yaxis2=dict(overlaying="y", side="right", showgrid=False, title="Volume"),
            xaxis_rangeslider_visible=False,
        )
        fig_style(fig_c, 400, dark=DARK)
        fig_c.update_layout(title=f"{candle_ticker} — OHLCV candlestick")
        st.plotly_chart(fig_c, use_container_width=True)

    # ── Correlation heatmap between selected stocks ───────────────────────────
    if len(sp_tickers) > 1:
        st.markdown("---")
        st.markdown('<p class="sec">Price correlation between selected tickers</p>', unsafe_allow_html=True)
        pivot = price_df.pivot_table(index="date", columns="ticker", values="close")
        returns = pivot.pct_change().dropna()
        corr = returns.corr()
        fig_corr = px.imshow(corr, color_continuous_scale=["#EF4444","white",BLUE],
                              color_continuous_midpoint=0, text_auto=".2f", aspect="auto")
        fig_corr.update_layout(
            margin=dict(l=0,r=0,t=20,b=0), height=350,
            paper_bgcolor="#111827" if DARK else "white",
            font_color="#E2E8F0" if DARK else "#334155",
            font_family="Inter",
        )
        st.plotly_chart(fig_corr, use_container_width=True)
        st.caption("Correlation of daily returns — +1 = move together, -1 = move opposite")

    # ── Volume leaders ────────────────────────────────────────────────────────
    st.markdown("---")
    st.markdown('<p class="sec">Average daily volume ranking</p>', unsafe_allow_html=True)
    vol_rank = price_df.groupby("ticker")["volume"].mean().reset_index()
    vol_rank.columns = ["Ticker","Avg Daily Volume"]
    vol_rank = vol_rank.sort_values("Avg Daily Volume", ascending=True)
    fig_vol = px.bar(vol_rank, x="Avg Daily Volume", y="Ticker",
                     orientation="h", color_discrete_sequence=[BLUE])
    fig_style(fig_vol, max(200, len(sp_tickers)*35), dark=DARK)
    fig_vol.update_layout(title="Average daily volume (shares traded)")
    st.plotly_chart(fig_vol, use_container_width=True)

    # ── Raw data download ─────────────────────────────────────────────────────
    if st.checkbox("Show raw price data"):
        st.markdown(html_table(price_df.sort_values(["ticker","date"], ascending=[True,False]), dark=DARK), unsafe_allow_html=True)
        st.download_button("Download CSV", price_df.to_csv(index=False),
                           "stock_prices.csv", "text/csv")


elif page == "SEC Filings":
    st.markdown('<p class="sec">SEC Edgar</p>', unsafe_allow_html=True)
    st.title("SEC Filings Explorer")

    f1,f2,f3,f4=st.columns(4)
    with f1: form_f=st.selectbox("Form type",["All","8-K","10-K"])
    with f2: sort_f=st.selectbox("Sort",["Filed (newest)","Filed (oldest)","Ticker A-Z"])
    with f3: mat_f=st.checkbox("8-K only")
    with f4: co_s=st.text_input("Search company","")

    conds=["filed_at BETWEEN %s AND %s"]; params=[sd,ed]
    ph=",".join(["%s"]*len(tickers)); conds.append(f"ticker IN ({ph})"); params.extend(tickers)
    if form_f!="All": conds.append("form_type=%s"); params.append(form_f)
    if mat_f: conds.append("is_material_event=TRUE")
    if co_s: conds.append("LOWER(company_name) LIKE %s"); params.append(f"%{co_s.lower()}%")
    order={"Filed (newest)":"filed_at DESC","Filed (oldest)":"filed_at ASC","Ticker A-Z":"ticker ASC"}[sort_f]

    fil=q(f"SELECT ticker,company_name,form_type,filed_at::date AS filed_at,period::date AS period,is_material_event,document_url FROM sec_filings WHERE {' AND '.join(conds)} ORDER BY {order}",params=tuple(params))

    if fil.empty: st.info("No filings match filters"); st.stop()

    k1,k2,k3,k4=st.columns(4)
    k1.metric("Total",len(fil)); k2.metric("8-K",int(fil["is_material_event"].sum()))
    k3.metric("10-K",int((fil["form_type"]=="10-K").sum())); k4.metric("Companies",fil["ticker"].nunique())

    st.markdown("---")
    ch1,ch2,ch3=st.columns(3)
    with ch1:
        st.markdown('<p class="sec">By form type</p>', unsafe_allow_html=True)
        bd=fil["form_type"].value_counts().reset_index(); bd.columns=["Form","n"]
        fp=px.pie(bd,names="Form",values="n",color_discrete_sequence=[BLUE,"#FF6B6B"],hole=0.5)
        fp.update_layout(
            margin=dict(l=0,r=0,t=10,b=0), height=200,
            paper_bgcolor="#111827" if DARK else "white",
            plot_bgcolor="#111827" if DARK else "white",
            font_color="#E2E8F0" if DARK else "#334155",
            legend=dict(font=dict(color="#E2E8F0" if DARK else "#334155")),
        )
        st.plotly_chart(fp,use_container_width=True)
    with ch2:
        st.markdown('<p class="sec">Monthly timeline</p>', unsafe_allow_html=True)
        tl=fil.copy(); tl["filed_at"]=pd.to_datetime(tl["filed_at"].astype(str)); tl["month"]=tl["filed_at"].dt.strftime("%Y-%m")
        ml=tl.groupby(["month","form_type"]).size().reset_index(name="n")
        ft=px.bar(ml,x="month",y="n",color="form_type",barmode="stack",color_discrete_map={"8-K":"#FF6B6B","10-K":BLUE})
        fig_style(ft, 200, dark=DARK); st.plotly_chart(ft,use_container_width=True)
    with ch3:
        st.markdown('<p class="sec">By ticker</p>', unsafe_allow_html=True)
        bt=fil.groupby("ticker").size().reset_index(name="n").sort_values("n",ascending=True)
        fb2=px.bar(bt,x="n",y="ticker",orientation="h",color_discrete_sequence=[BLUE])
        fig_style(fb2, 200, dark=DARK); st.plotly_chart(fb2,use_container_width=True)

    st.markdown("---")
    disp=fil[["ticker","company_name","form_type","filed_at","period","is_material_event"]].copy()
    disp.columns=["Ticker","Company","Form","Filed","Period","Material"]
    st.markdown(html_table(disp, dark=DARK), unsafe_allow_html=True)
    st.download_button("Download CSV",fil.to_csv(index=False),"filings.csv","text/csv")

    with st.expander(f"Document links ({fil['document_url'].notna().sum()} available)"):
        for _,row in fil[fil["document_url"].notna()].iterrows():
            ico="🔴" if row["form_type"]=="8-K" else "🔵"
            st.markdown(f"{ico} **{row['ticker']}** [{row['company_name']}]({row['document_url']}) · {row['filed_at']}")


# ══════════════════════════════════════════════════════════════════════════════
# NEWS FEED
# ══════════════════════════════════════════════════════════════════════════════
elif page == "News Feed":
    st.markdown('<p class="sec">NewsAPI</p>', unsafe_allow_html=True)
    st.title("Financial News Feed")

    c1,c2,c3=st.columns(3)
    with c1: cat_f=st.selectbox("Category",["All","earnings","m&a","macro","regulatory","general"])
    with c2: srch=st.text_input("Search title/source","")
    with c3: n_art=st.slider("Articles",10,100,30,10)

    view=st.radio("Layout",["Card grid","Compact list"],horizontal=True)

    cat_df=q("SELECT category,COUNT(*) n FROM news_sentiment GROUP BY category ORDER BY n DESC")
    if not cat_df.empty:
        fc=px.bar(cat_df,x="category",y="n",color="category",
                  color_discrete_map={"earnings":"#10B981","m&a":"#8B5CF6","macro":"#F59E0B","regulatory":"#EF4444","general":"#94A3B8"})
        fig_style(fc, 140, dark=DARK); fc.update_layout(showlegend=False,margin=dict(l=0,r=0,t=0,b=0))
        st.plotly_chart(fc,use_container_width=True)

    mc=mongo()
    if mc is None: st.stop()
    nc=mc["fin_intelligence"]["news_articles"]
    qry={}
    if cat_f!="All": qry["category"]=cat_f
    arts=list(nc.find(qry,{"title":1,"source_name":1,"published_at":1,"description":1,"url":1,"category":1,"_id":0}).sort("published_at",-1).limit(n_art))
    if srch:
        t=srch.lower()
        arts=[a for a in arts if t in str(a.get("title","")).lower() or t in str(a.get("source_name","")).lower()]

    # Simple keyword-based sentiment scoring (no ML needed)
    POS_WORDS = {"surge","soar","beat","record","growth","profit","strong","gain","rise","rally","upgrade","buy","bullish","exceed","outperform","boost","innovation","breakthrough","expand","hire"}
    NEG_WORDS = {"fall","drop","crash","miss","loss","weak","cut","layoff","decline","warn","downgrade","sell","bearish","below","underperform","risk","debt","default","bankrupt","investigation","fine","lawsuit","fraud","recall"}

    def sentiment_score(title, desc=""):
        text = (str(title) + " " + str(desc)).lower()
        words = set(text.split())
        pos = len(words & POS_WORDS)
        neg = len(words & NEG_WORDS)
        if pos > neg: return "🟢 Positive", "#D1FAE5", "#065F46"
        elif neg > pos: return "🔴 Negative", "#FEE2E2", "#991B1B"
        else: return "⚪ Neutral", "#F1F5F9", "#475569"

    for a in arts:
        a["_sentiment"], a["_sent_bg"], a["_sent_color"] = sentiment_score(
            a.get("title",""), a.get("description","")
        )

    # Sentiment summary bar
    sentiments = [a["_sentiment"] for a in arts]
    sent_counts = {"🟢 Positive": sentiments.count("🟢 Positive"),
                   "⚪ Neutral":  sentiments.count("⚪ Neutral"),
                   "🔴 Negative": sentiments.count("🔴 Negative")}
    s1,s2,s3 = st.columns(3)
    s1.metric("🟢 Positive", sent_counts["🟢 Positive"])
    s2.metric("⚪ Neutral",  sent_counts["⚪ Neutral"])
    s3.metric("🔴 Negative", sent_counts["🔴 Negative"])

    st.markdown(f"**{len(arts)} articles** · most recent first")
    st.markdown("---")

    bmap={"earnings":("be","EARNINGS"),"m&a":("bm","M&A"),"macro":("bk","MACRO"),"regulatory":("br","REGULATORY"),"general":("bg","GENERAL")}

    if view=="Card grid":
        l,r=st.columns(2)
        for i,a in enumerate(arts):
            cat=a.get("category","general"); css,lbl=bmap.get(cat,("bg","GENERAL"))
            title=a.get("title","")[:100]; src=a.get("source_name","?")
            desc=(a.get("description","") or "")[:130]; pub=str(a.get("published_at",""))[:10]; url=a.get("url","#")
            sent=a.get("_sentiment","⚪ Neutral"); sent_bg=a.get("_sent_bg","#F1F5F9"); sent_col=a.get("_sent_color","#475569")
            border_color = "#10B981" if "Positive" in sent else "#EF4444" if "Negative" in sent else "#94A3B8"
            html=f'''<div class="news-card" style="border-left-color:{border_color}">
              <div style="display:flex;gap:6px;align-items:center">
                <span class="badge {css}">{lbl}</span>
                <span style="font-size:10px;background:{sent_bg};color:{sent_col};padding:1px 8px;border-radius:20px;font-weight:600">{sent}</span>
              </div>
              <p class="news-title" style="margin-top:7px"><a href="{url}" target="_blank" style="color:#080E1D;text-decoration:none">{title}</a></p>
              <p class="news-meta">{src} · {pub}</p>
              <p style="font-size:12px;color:#64748B;margin:5px 0 0;line-height:1.5">{desc}{"..." if len(desc)==130 else ""}</p>
            </div>'''
            (l if i%2==0 else r).markdown(html,unsafe_allow_html=True)
    else:
        for a in arts:
            cat=a.get("category","general"); css,lbl=bmap.get(cat,("bg","GENERAL"))
            sent=a.get("_sentiment","⚪ Neutral")
            st.markdown(f'<div style="display:flex;align-items:center;gap:10px;padding:8px 0;border-bottom:1px solid #F1F5F9"><span class="badge {css}" style="min-width:72px;text-align:center">{lbl}</span><span style="font-size:11px;white-space:nowrap">{sent}</span><a href="{a.get("url","#")}" target="_blank" style="font-size:13px;color:#080E1D;text-decoration:none;flex:1">{a.get("title","")}</a><span style="font-size:10px;color:#94A3B8;white-space:nowrap">{a.get("source_name","?")} · {str(a.get("published_at",""))[:10]}</span></div>',unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# CROSS-SOURCE
# ══════════════════════════════════════════════════════════════════════════════
elif page == "Cross-Source":
    st.markdown('<p class="sec">Multi-source intelligence</p>', unsafe_allow_html=True)
    st.title("Cross-Source Analysis")
    st.caption("Joining SEC filings, news headlines, and FRED macro data on a unified timeline.")

    tab1,tab2,tab3,tab4,tab5=st.tabs(["📌 Filing + News","📈 Market context","🔥 Correlations","🏢 Company drill-down","🚨 Alert Simulation"])

    with tab1:
        cross=q("""SELECT f.ticker,f.company_name,f.form_type,f.filed_at,
                          COUNT(n.id) AS news_same_day,
                          MIN(ABS(f.filed_at - n.date_only)) AS min_days_apart
                   FROM sec_filings f
                   LEFT JOIN news_sentiment n
                     ON n.date_only BETWEEN f.filed_at - INTERVAL '7 days'
                                        AND f.filed_at + INTERVAL '7 days'
                   GROUP BY f.ticker,f.company_name,f.form_type,f.filed_at
                   ORDER BY news_same_day DESC,f.filed_at DESC LIMIT 50""")
        window = st.slider("Date window (days either side of filing)", 1, 14, 7)
        if window != 7:
            cross=q(f"""SELECT f.ticker,f.company_name,f.form_type,f.filed_at,
                              COUNT(n.id) AS news_same_day
                       FROM sec_filings f
                       LEFT JOIN news_sentiment n
                         ON n.date_only BETWEEN f.filed_at - INTERVAL '{window} days'
                                            AND f.filed_at + INTERVAL '{window} days'
                       GROUP BY f.ticker,f.company_name,f.form_type,f.filed_at
                       ORDER BY news_same_day DESC,f.filed_at DESC LIMIT 50""")

        if not cross.empty and cross["news_same_day"].max()>0:
            cd=cross[cross["news_same_day"]>0]
            fig=px.scatter(cd,x="filed_at",y="ticker",size="news_same_day",color="form_type",
                           color_discrete_map={"8-K":"#EF4444","10-K":BLUE},
                           hover_data={"company_name":True,"news_same_day":True},size_max=35)
            fig_style(fig, 350, dark=DARK); fig.update_layout(title=f"Filing events with news coverage within ±{window} days (bubble = article count)")
            st.plotly_chart(fig,use_container_width=True)
            st.success(f"✓ Found {len(cd)} filings with nearby news coverage using a ±{window}-day window")
        else:
            st.info(f"No overlaps found with ±{window}-day window — try increasing the window above or re-running the pipeline with more keywords")
        _cross_disp = cross.rename(columns={"ticker":"Ticker","company_name":"Company","form_type":"Form","filed_at":"Filed","news_same_day":f"News within ±{window}d"})
        st.markdown(html_table(_cross_disp, dark=DARK), unsafe_allow_html=True)

    with tab2:
        ind=st.selectbox("Indicator to plot",["SP500","DFF","GS10","VIXCLS","CPIAUCSL","UNRATE"])
        sp=q("SELECT date,value FROM market_data WHERE series_code=%s ORDER BY date",params=(ind,))
        fd=q("SELECT DISTINCT filed_at date,ticker,form_type FROM sec_filings ORDER BY filed_at")
        if not sp.empty:
            sp["date"]=pd.to_datetime(sp["date"])
            fig2=go.Figure()
            fig2.add_trace(go.Scatter(x=sp["date"],y=sp["value"],name=ind,line=dict(color=BLUE,width=2),fill="tozeroy",fillcolor="rgba(58,134,255,0.05)"))
            if not fd.empty:
                fd["date"]=pd.to_datetime(fd["date"])
                mg=pd.merge_asof(fd.sort_values("date"),sp.sort_values("date"),on="date",direction="nearest")
                for ftype,color,sym in [("8-K","#EF4444","diamond"),("10-K","#FFD166","triangle-up")]:
                    sub=mg[mg["form_type"]==ftype]
                    if not sub.empty:
                        fig2.add_trace(go.Scatter(x=sub["date"],y=sub["value"],mode="markers",name=f"{ftype} filing",
                                                  marker=dict(color=color,size=10,symbol=sym),text=sub["ticker"],
                                                  hovertemplate="%{text} "+ftype+"<br>%{x}<extra></extra>"))
            fig_style(fig2, 400, dark=DARK); fig2.update_layout(legend=dict(orientation="h",y=1.05))
            st.plotly_chart(fig2,use_container_width=True)

    with tab3:
        st.markdown("Pearson correlation between FRED series (monthly resampled).")
        am=q("SELECT date,series_code,value FROM market_data WHERE date BETWEEN %s AND %s ORDER BY date",params=(sd,ed))
        if not am.empty:
            am["date"]=pd.to_datetime(am["date"])
            piv=am.pivot_table(index="date",columns="series_code",values="value").resample("ME").last().dropna(thresh=3)
            corr=piv.corr()
            fh=px.imshow(corr,color_continuous_scale=["#EF4444","white",BLUE],color_continuous_midpoint=0,text_auto=".2f",aspect="auto")
            fh.update_layout(margin=dict(l=0,r=0,t=20,b=0),height=400,paper_bgcolor="white",font_family="Inter")
            st.plotly_chart(fh,use_container_width=True)
            st.caption("Values near +1 = strong positive correlation, near −1 = strong negative.")

    with tab4:
        pick=st.selectbox("Company",tickers)
        ca,cb=st.columns(2)
        with ca:
            st.markdown(f'<p class="sec">{pick} — filing history</p>', unsafe_allow_html=True)
            cf=q("SELECT form_type,filed_at,period,is_material_event,document_url FROM sec_filings WHERE ticker=%s ORDER BY filed_at DESC",params=(pick,))
            if not cf.empty:
                st.metric("Total filings",len(cf)); st.metric("8-K events",int(cf["is_material_event"].sum()))
                cf["filed_at"]=pd.to_datetime(cf["filed_at"]).dt.strftime("%Y-%m-%d")
                st.markdown(html_table(cf[["form_type","filed_at","period"]].rename(columns={"form_type":"Form","filed_at":"Filed","period":"Period"}), dark=DARK), unsafe_allow_html=True)
        with cb:
            st.markdown(f'<p class="sec">{pick} — annual filing frequency</p>', unsafe_allow_html=True)
            if not cf.empty:
                cf2=cf.copy(); cf2["filed_at"]=pd.to_datetime(cf2["filed_at"]); cf2["year"]=cf2["filed_at"].dt.year
                yr=cf2.groupby(["year","form_type"]).size().reset_index(name="n")
                fy=px.bar(yr,x="year",y="n",color="form_type",barmode="group",color_discrete_map={"8-K":"#EF4444","10-K":BLUE})
                fig_style(fy, 260, dark=DARK); st.plotly_chart(fy,use_container_width=True)

    with tab5:
        st.markdown("### 🚨 8-K Material Event Alert Simulation")
        st.markdown("*Simulates what a real-time alert system would have triggered based on the 8-K filings in your database.*")

        # Alert settings
        a1,a2,a3 = st.columns(3)
        with a1:
            alert_tickers = st.multiselect("Watch these tickers", tickers, default=tickers[:5])
        with a2:
            alert_lookback = st.slider("Look back (days)", 7, 90, 30)
        with a3:
            alert_form = st.selectbox("Alert on", ["8-K only (material events)", "All filings"])

        form_cond = "AND form_type='8-K'" if "8-K only" in alert_form else ""
        if alert_tickers:
            aph = ",".join(["%s"]*len(alert_tickers))
            alerts = q(f"""
                SELECT ticker, company_name, form_type, filed_at, document_url
                FROM sec_filings
                WHERE ticker IN ({aph})
                  AND filed_at >= CURRENT_DATE - INTERVAL '{alert_lookback} days'
                  {form_cond}
                ORDER BY filed_at DESC
            """, params=tuple(alert_tickers))

            if not alerts.empty:
                st.success(f"✓ {len(alerts)} alert(s) would have fired in the last {alert_lookback} days")
                for _,row in alerts.iterrows():
                    col_ico = "🔴" if row["form_type"]=="8-K" else "🔵"
                    filed = str(row["filed_at"])[:10]
                    url = row.get("document_url") or "#"
                    st.markdown(f"""
                    <div style="background:white;border:1px solid #FCA5A5;border-left:4px solid #EF4444;
                                border-radius:10px;padding:12px 16px;margin-bottom:8px;
                                box-shadow:0 1px 3px rgba(239,68,68,.1)">
                      <div style="display:flex;align-items:center;justify-content:space-between">
                        <div>
                          <span style="font-family:JetBrains Mono,monospace;font-size:12px;background:#EFF6FF;
                                       color:#1D4ED8;padding:2px 8px;border-radius:5px;font-weight:600">{row["ticker"]}</span>
                          <span style="font-size:13px;font-weight:500;color:#080E1D;margin-left:10px">{row["company_name"]}</span>
                        </div>
                        <div style="text-align:right">
                          <span style="font-size:11px;color:#EF4444;font-weight:600">{col_ico} {row["form_type"]} FILED</span><br>
                          <span style="font-size:11px;color:#94A3B8">{filed}</span>
                        </div>
                      </div>
                      {"<a href='"+url+"' target='_blank' style='font-size:11px;color:#3A86FF;text-decoration:none;margin-top:4px;display:block'>View filing document →</a>" if url != "#" else ""}
                    </div>
                    """, unsafe_allow_html=True)

                # Alert frequency chart
                st.markdown("---")
                st.markdown('<p class="sec">Alert frequency over period</p>', unsafe_allow_html=True)
                alerts["filed_at"] = pd.to_datetime(alerts["filed_at"].astype(str))
                alerts["week"] = alerts["filed_at"].dt.strftime("%Y-W%V")
                weekly = alerts.groupby(["week","ticker"]).size().reset_index(name="alerts")
                fig_a = px.bar(weekly, x="week", y="alerts", color="ticker",
                               color_discrete_sequence=COLORS, title="Alerts by week")
                fig_style(fig_a, 240, dark=DARK)
                st.plotly_chart(fig_a, use_container_width=True)
            else:
                st.info(f"No {alert_form.split()[0]} filings found for selected tickers in the last {alert_lookback} days")


# ══════════════════════════════════════════════════════════════════════════════
# ABOUT
# ══════════════════════════════════════════════════════════════════════════════
elif page == "About":
    st.markdown('<p class="sec">Group 7 · Columbia University</p>', unsafe_allow_html=True)
    st.title("About the Platform")
    st.markdown("""
    A real-time ETL pipeline ingesting financial news, SEC filings, macroeconomic indicators,
    and live stock prices from **four free APIs** — transformed with distributed computing
    and served via this dashboard. Daily automation via GitHub Actions means data updates
    without any manual intervention.

    **Team:** Ce Zhang · Cai Gao · Yuchun Wu · Yanji Li
    """)

    st.markdown("---")

    # ── Architecture diagram ─────────────────────────────────────────────────
    st.markdown("### System Architecture")

    arch_svg = """
    <div style="width:100%;overflow-x:auto;margin:8px 0 24px">
    <svg viewBox="0 0 900 560" xmlns="http://www.w3.org/2000/svg" style="width:100%;max-width:900px;display:block;margin:auto">
      <defs>
        <marker id="arr" markerWidth="8" markerHeight="8" refX="6" refY="3" orient="auto">
          <path d="M0,0 L0,6 L8,3 z" fill="#475569"/>
        </marker>
        <marker id="arr-blue" markerWidth="8" markerHeight="8" refX="6" refY="3" orient="auto">
          <path d="M0,0 L0,6 L8,3 z" fill="#3A86FF"/>
        </marker>
      </defs>

      <!-- Background -->
      <rect width="900" height="560" fill="#0D1117" rx="14"/>

      <!-- ── ROW 1: Data Sources ───────────────────────────── -->
      <rect x="16" y="16" width="868" height="100" rx="8" fill="#0F1729" stroke="#1E3A5F" stroke-width="1"/>
      <text x="450" y="36" text-anchor="middle" font-family="Inter,sans-serif" font-size="9" fill="#475569" letter-spacing="2" font-weight="600">DATA SOURCES</text>

      <!-- NewsAPI -->
      <rect x="32" y="44" width="190" height="60" rx="6" fill="#0D2818" stroke="#3BFFA0" stroke-width="1"/>
      <text x="127" y="65" text-anchor="middle" font-family="Inter,sans-serif" font-size="12" fill="#3BFFA0" font-weight="600">NewsAPI</text>
      <text x="127" y="81" text-anchor="middle" font-family="Inter,sans-serif" font-size="10" fill="#475569">Headlines · JSON</text>
      <text x="127" y="96" text-anchor="middle" font-family="Inter,sans-serif" font-size="9" fill="#374151">~2,000 articles/run</text>

      <!-- SEC Edgar -->
      <rect x="240" y="44" width="190" height="60" rx="6" fill="#0D1829" stroke="#5BA4FF" stroke-width="1"/>
      <text x="335" y="65" text-anchor="middle" font-family="Inter,sans-serif" font-size="12" fill="#5BA4FF" font-weight="600">SEC Edgar</text>
      <text x="335" y="81" text-anchor="middle" font-family="Inter,sans-serif" font-size="10" fill="#475569">8-K · 10-K · REST</text>
      <text x="335" y="96" text-anchor="middle" font-family="Inter,sans-serif" font-size="9" fill="#374151">38 companies · on filing</text>

      <!-- FRED -->
      <rect x="448" y="44" width="190" height="60" rx="6" fill="#1A1500" stroke="#FFD166" stroke-width="1"/>
      <text x="543" y="65" text-anchor="middle" font-family="Inter,sans-serif" font-size="12" fill="#FFD166" font-weight="600">FRED API</text>
      <text x="543" y="81" text-anchor="middle" font-family="Inter,sans-serif" font-size="10" fill="#475569">12 macro series</text>
      <text x="543" y="96" text-anchor="middle" font-family="Inter,sans-serif" font-size="9" fill="#374151">Back to 2010 · daily</text>

      <!-- Alpaca -->
      <rect x="656" y="44" width="212" height="60" rx="6" fill="#1A0A0A" stroke="#FF6B6B" stroke-width="1"/>
      <text x="762" y="65" text-anchor="middle" font-family="Inter,sans-serif" font-size="12" fill="#FF6B6B" font-weight="600">Alpaca Markets</text>
      <text x="762" y="81" text-anchor="middle" font-family="Inter,sans-serif" font-size="10" fill="#475569">OHLCV · IEX feed</text>
      <text x="762" y="96" text-anchor="middle" font-family="Inter,sans-serif" font-size="9" fill="#374151">38 tickers · daily bars</text>

      <!-- Arrows down to Kafka -->
      <line x1="127" y1="116" x2="400" y2="154" stroke="#475569" stroke-width="1" stroke-dasharray="3,2" marker-end="url(#arr)"/>
      <line x1="335" y1="116" x2="420" y2="154" stroke="#475569" stroke-width="1" stroke-dasharray="3,2" marker-end="url(#arr)"/>
      <line x1="543" y1="116" x2="460" y2="154" stroke="#475569" stroke-width="1" stroke-dasharray="3,2" marker-end="url(#arr)"/>
      <line x1="762" y1="116" x2="500" y2="154" stroke="#475569" stroke-width="1" stroke-dasharray="3,2" marker-end="url(#arr)"/>

      <!-- ── ROW 2: Kafka ─────────────────────────────────── -->
      <rect x="16" y="156" width="868" height="72" rx="8" fill="#0F1729" stroke="#FF6B6B" stroke-width="1"/>
      <text x="450" y="175" text-anchor="middle" font-family="Inter,sans-serif" font-size="9" fill="#475569" letter-spacing="2" font-weight="600">STREAMING LAYER</text>
      <text x="450" y="197" text-anchor="middle" font-family="Inter,sans-serif" font-size="14" fill="#FF6B6B" font-weight="700">Apache Kafka</text>
      <text x="450" y="213" text-anchor="middle" font-family="Inter,sans-serif" font-size="10" fill="#475569">3 topics: news-articles · sec-filings · market-data · Decouples producers from consumers · durable log</text>

      <!-- Airflow badge -->
      <rect x="730" y="162" width="140" height="56" rx="6" fill="#140D26" stroke="#8338EC" stroke-width="1"/>
      <text x="800" y="183" text-anchor="middle" font-family="Inter,sans-serif" font-size="10" fill="#8338EC" font-weight="600">Apache Airflow</text>
      <text x="800" y="199" text-anchor="middle" font-family="Inter,sans-serif" font-size="9" fill="#475569">Orchestration · DAGs</text>
      <text x="800" y="212" text-anchor="middle" font-family="Inter,sans-serif" font-size="9" fill="#374151">schedules all tasks</text>

      <!-- Arrow down to Spark -->
      <line x1="450" y1="228" x2="450" y2="248" stroke="#475569" stroke-width="1.5" marker-end="url(#arr)"/>

      <!-- ── ROW 3: PySpark ───────────────────────────────── -->
      <rect x="16" y="250" width="700" height="72" rx="8" fill="#0F1729" stroke="#5BA4FF" stroke-width="1"/>
      <text x="350" y="269" text-anchor="middle" font-family="Inter,sans-serif" font-size="9" fill="#475569" letter-spacing="2" font-weight="600">PROCESSING LAYER</text>
      <text x="350" y="291" text-anchor="middle" font-family="Inter,sans-serif" font-size="14" fill="#5BA4FF" font-weight="700">Apache PySpark</text>
      <text x="350" y="307" text-anchor="middle" font-family="Inter,sans-serif" font-size="10" fill="#475569">Deduplicate → Standardise → Tag categories → Flag 8-K events → Cross-source join on date</text>

      <!-- GitHub Actions badge -->
      <rect x="730" y="250" width="140" height="72" rx="6" fill="#0A1F0A" stroke="#3BFFA0" stroke-width="1"/>
      <text x="800" y="272" text-anchor="middle" font-family="Inter,sans-serif" font-size="10" fill="#3BFFA0" font-weight="600">GitHub Actions</text>
      <text x="800" y="288" text-anchor="middle" font-family="Inter,sans-serif" font-size="9" fill="#475569">pipeline.py daily</text>
      <text x="800" y="302" text-anchor="middle" font-family="Inter,sans-serif" font-size="9" fill="#374151">06:00 UTC · automated</text>
      <text x="800" y="316" text-anchor="middle" font-family="Inter,sans-serif" font-size="9" fill="#374151">replaces Colab runs</text>

      <!-- Arrows down to storage -->
      <line x1="240" y1="322" x2="180" y2="370" stroke="#475569" stroke-width="1.5" marker-end="url(#arr)"/>
      <text x="170" y="352" font-family="Inter,sans-serif" font-size="8" fill="#475569">structured</text>
      <line x1="460" y1="322" x2="560" y2="370" stroke="#475569" stroke-width="1.5" marker-end="url(#arr)"/>
      <text x="510" y="352" font-family="Inter,sans-serif" font-size="8" fill="#475569">unstructured</text>

      <!-- ── ROW 4: Storage ───────────────────────────────── -->
      <!-- PostgreSQL -->
      <rect x="16" y="372" width="420" height="120" rx="8" fill="#0F1729" stroke="#3BFFA0" stroke-width="1"/>
      <text x="226" y="393" text-anchor="middle" font-family="Inter,sans-serif" font-size="11" fill="#3BFFA0" font-weight="700">PostgreSQL  ·  Supabase</text>
      <rect x="32" y="400" width="88" height="24" rx="4" fill="#0D2818" stroke="#3BFFA0" stroke-width="0.5"/>
      <text x="76" y="416" text-anchor="middle" font-family="Inter,sans-serif" font-size="9" fill="#3BFFA0">market_data</text>
      <rect x="130" y="400" width="88" height="24" rx="4" fill="#0D2818" stroke="#3BFFA0" stroke-width="0.5"/>
      <text x="174" y="416" text-anchor="middle" font-family="Inter,sans-serif" font-size="9" fill="#3BFFA0">sec_filings</text>
      <rect x="228" y="400" width="100" height="24" rx="4" fill="#0D2818" stroke="#3BFFA0" stroke-width="0.5"/>
      <text x="278" y="416" text-anchor="middle" font-family="Inter,sans-serif" font-size="9" fill="#3BFFA0">news_sentiment</text>
      <rect x="338" y="400" width="84" height="24" rx="4" fill="#0D2818" stroke="#3BFFA0" stroke-width="0.5"/>
      <text x="380" y="416" text-anchor="middle" font-family="Inter,sans-serif" font-size="9" fill="#3BFFA0">stock_prices</text>
      <text x="226" y="450" text-anchor="middle" font-family="Inter,sans-serif" font-size="9" fill="#475569">SQL queries · time-series · aggregations · indexed by date + ticker</text>
      <text x="226" y="465" text-anchor="middle" font-family="Inter,sans-serif" font-size="9" fill="#374151">4 tables · ~30,000+ rows and growing</text>
      <text x="226" y="480" text-anchor="middle" font-family="Inter,sans-serif" font-size="9" fill="#374151">Free tier: 500 MB · Session Pooler for IPv4</text>

      <!-- MongoDB -->
      <rect x="450" y="372" width="420" height="120" rx="8" fill="#0F1729" stroke="#FF6B6B" stroke-width="1"/>
      <text x="660" y="393" text-anchor="middle" font-family="Inter,sans-serif" font-size="11" fill="#FF6B6B" font-weight="700">MongoDB Atlas  ·  M0 free cluster</text>
      <rect x="466" y="400" width="168" height="24" rx="4" fill="#1A0A0A" stroke="#FF6B6B" stroke-width="0.5"/>
      <text x="550" y="416" text-anchor="middle" font-family="Inter,sans-serif" font-size="9" fill="#FF6B6B">news_articles</text>
      <rect x="648" y="400" width="204" height="24" rx="4" fill="#1A0A0A" stroke="#FF6B6B" stroke-width="0.5"/>
      <text x="750" y="416" text-anchor="middle" font-family="Inter,sans-serif" font-size="9" fill="#FF6B6B">sec_filing_documents</text>
      <text x="660" y="450" text-anchor="middle" font-family="Inter,sans-serif" font-size="9" fill="#475569">Flexible schema · full article text · JSON documents</text>
      <text x="660" y="465" text-anchor="middle" font-family="Inter,sans-serif" font-size="9" fill="#374151">No fixed columns · ideal for variable-length content</text>
      <text x="660" y="480" text-anchor="middle" font-family="Inter,sans-serif" font-size="9" fill="#374151">Free tier: 512 MB · 0.0.0.0/0 Network Access</text>

      <!-- Arrows down to Streamlit -->
      <line x1="226" y1="492" x2="380" y2="530" stroke="#3A86FF" stroke-width="1.5" marker-end="url(#arr-blue)"/>
      <line x1="660" y1="492" x2="520" y2="530" stroke="#3A86FF" stroke-width="1.5" marker-end="url(#arr-blue)"/>

      <!-- ── ROW 5: Streamlit ─────────────────────────────── -->
      <rect x="16" y="530" width="868" height="18" rx="6" fill="#0D1829" stroke="#3A86FF" stroke-width="1"/>
      <text x="450" y="543" text-anchor="middle" font-family="Inter,sans-serif" font-size="10" fill="#5BA4FF" font-weight="600">Streamlit Dashboard  ·  fin-intelligence-dashboard.streamlit.app  ·  7 pages  ·  dark/light mode</text>
    </svg>
    </div>
    """
    st.markdown(arch_svg, unsafe_allow_html=True)

    st.markdown("---")
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("### Data Sources")
        st.markdown("""| Source | Type | Volume | Frequency |
|---|---|---|---|
| **NewsAPI** | Headlines | ~2,000 articles/run | Every 15 min |
| **SEC Edgar** | 8-K & 10-K | ~380 filings, 38 cos | On filing |
| **FRED** | 12 macro series | ~15,000+ data points | Daily/Monthly |
| **Alpaca** | Stock OHLCV | 38 tickers × 365 days | Daily bars |""")

        st.markdown("### Technology Stack")
        st.markdown("""| Technology | Role | Hosted on |
|---|---|---|
| **Kafka** | Streaming buffer (3 topics) | Docker / local |
| **PySpark** | Distributed transform + join | Colab / local |
| **PostgreSQL** | Structured warehouse (4 tables) | Supabase free |
| **MongoDB** | Document store (2 collections) | Atlas M0 free |
| **Airflow** | Pipeline orchestration | Docker / local |
| **GitHub Actions** | Daily automation (06:00 UTC) | GitHub free |
| **Streamlit Cloud** | Web dashboard hosting | Streamlit free |""")

    with c2:
        st.markdown("### Scalability Assessment")
        st.markdown("""| Dimension | Current (demo) | Production path |
|---|---|---|
| **Tickers** | 38 | Add rows to config list |
| **FRED series** | 12 | Add keys to FRED_SERIES dict |
| **News keywords** | 20 | Up to 100 (free tier limit) |
| **History depth** | 2010–present | Back to 1947 for macro |
| **Update frequency** | Daily (GitHub Actions) | 3× daily within free limits |
| **DB size (est.)** | ~50–100 MB now | ~2 GB/year at current rate |
| **Kafka brokers** | 1 (demo) | Add brokers linearly |
| **Spark workers** | 1 node (demo) | Add workers, same code |""")

        st.markdown("### Cost Estimate")
        st.markdown("""| Component | Demo | Production |
|---|---|---|
| Kafka | $0 | $50–150/mo |
| PySpark | $0 | $200–500/mo |
| PostgreSQL | $0 (Supabase) | $15–50/mo |
| MongoDB | $0 (Atlas M0) | $57–200/mo |
| Airflow | $0 | $100–200/mo |
| Alpaca data | $0 (free tier) | $0 (stays free) |
| GitHub Actions | $0 (2,000 min/mo) | $0 |
| Streamlit Cloud | $0 (free) | $0 |
| **Total** | **$0** | **$422–1,100/mo** |""")

        st.markdown("### Data Quality")
        st.markdown("""| Dimension | Approach |
|---|---|
| **Completeness** | Deduplication by URL / accession number |
| **Consistency** | PySpark schema enforcement + type casting |
| **Timeliness** | Daily GitHub Actions + refresh button |
| **Accuracy** | Primary sources only (no aggregators) |
| **Licensing** | All free-tier / public domain sources |""")
