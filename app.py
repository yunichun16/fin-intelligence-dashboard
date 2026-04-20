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
st.markdown(f"""<script>
  var root = window.parent.document.querySelector('[data-testid="stAppViewContainer"]');
  if (root) root.setAttribute('data-theme', '{"dark" if "Dark" in _theme else "light"}');
</script>""", unsafe_allow_html=True)
if "Dark" in _theme:
    st.markdown("""<style>
      .main, [data-testid="stAppViewContainer"] { background: #0D1117 !important; }
      .news-card { background: #161B27 !important; }
      .news-title { color: #E2E8F0 !important; }
      .macro-card { background: #161B27 !important; border-color:#1E3A5F !important; }
      .macro-value { color: #E2E8F0 !important; }
      [data-testid="stMetric"] { background: #161B27 !important; border-radius:8px; padding:8px; }
      .stDataFrame { background: #161B27 !important; }
      p, span, div { color: #CBD5E1; }
      h1,h2,h3 { color: #F1F5F9 !important; }
    </style>""", unsafe_allow_html=True)

NAVY = "#080E1D"
BLUE = "#3A86FF"
COLORS = [BLUE, "#FF6B6B", "#FFD166", "#06D6A0", "#8338EC", "#FB5607", "#3A86FF", "#FFBE0B"]

def fig_style(fig, h=300):
    fig.update_layout(
        plot_bgcolor="white", paper_bgcolor="white", font_family="Inter",
        margin=dict(l=8,r=8,t=28,b=8), height=h,
        xaxis=dict(showgrid=False, showline=True, linecolor="#E2E8F0", title=""),
        yaxis=dict(showgrid=True, gridcolor="#F1F5F9", title=""),
        legend=dict(bgcolor="rgba(0,0,0,0)", borderwidth=0),
        hoverlabel=dict(bgcolor="white", font_size=12),
    )
    return fig


# ── Connections ────────────────────────────────────────────────────────────────
@st.cache_resource(show_spinner=False)
def pg():
    import psycopg2
    try:
        if hasattr(st,"secrets") and "postgres" in st.secrets:
            s = st.secrets["postgres"]
            h,port,db,u,pw = str(s["host"]).strip(),int(s.get("port",5432)),str(s["dbname"]).strip(),str(s["user"]).strip(),str(s["password"]).strip()
        else:
            from dotenv import load_dotenv; load_dotenv()
            h,port,db,u,pw = os.getenv("SUPABASE_HOST","").strip(),int(os.getenv("SUPABASE_PORT","5432")),os.getenv("SUPABASE_DB","postgres"),os.getenv("SUPABASE_USER","").strip(),os.getenv("SUPABASE_PASSWORD","").strip()
        if not h: st.error("SUPABASE_HOST empty"); return None
        return psycopg2.connect(host=h,port=port,dbname=db,user=u,password=pw,sslmode="require",connect_timeout=10)
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
        st.error("MONGO_URI not found in any location")
        return None

    try:
        c = MongoClient(uri, serverSelectionTimeoutMS=8000)
        c.admin.command("ping")
        return c
    except Exception as e:
        st.error(f"MongoDB connection failed: {e}")
        return None

@st.cache_data(ttl=300, show_spinner=False)
def q(sql, params=None):
    conn = pg()
    if conn is None: return pd.DataFrame()
    try:
        return pd.read_sql_query(sql, conn, params=params)
    except Exception as e:
        st.warning(f"Query: {e}"); return pd.DataFrame()


# ── Sidebar ────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
    <div style='padding:6px 0 14px'>
      <p style='font-family:Syne;font-size:15px;font-weight:700;color:#E2E8F0;margin:0'>📈 FinIntel</p>
      <p style='font-size:10px;color:#475569;margin:2px 0 0'>Group 7 · Columbia University</p>
    </div>""", unsafe_allow_html=True)

    page = st.radio("", ["🏠 Overview","📊 Market Data","📁 SEC Filings",
                         "📰 News Feed","🔗 Cross-Source","ℹ️ About"],
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
if page == "Overview":
    st.markdown('<p class="sec">Financial Intelligence Platform</p>', unsafe_allow_html=True)
    st.title("Market Overview")

    # Auto-refresh handler
    if auto_refresh:
        import time as _t
        _placeholder = st.empty()
        for _i in range(30, 0, -1):
            _placeholder.caption(f"🔴 Live · {today.strftime('%B %d, %Y')} · Supabase + MongoDB Atlas · refreshing in {_i}s")
            _t.sleep(1)
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
        (SELECT pg_size_pretty(pg_total_relation_size('market_data') +
                               pg_total_relation_size('sec_filings') +
                               pg_total_relation_size('news_sentiment'))) AS db_size
    """)
    if not vol.empty:
        r = vol.iloc[0]
        total = int(r["m"]) + int(r["f"]) + int(r["n"])
        st.markdown(f"""
        <div style="background:linear-gradient(90deg,#080E1D,#162040);border-radius:12px;
                    padding:14px 24px;margin:12px 0;display:flex;align-items:center;gap:32px;
                    border:1px solid #1E3A5F;color:white;">
          <div>
            <p style="font-size:9px;letter-spacing:.12em;opacity:.5;margin:0">TOTAL RECORDS IN DATABASE</p>
            <p style="font-family:Syne,sans-serif;font-size:28px;font-weight:700;margin:2px 0;letter-spacing:-.02em">
              {total:,} <span style="font-size:14px;opacity:.5;font-weight:400">records</span>
            </p>
          </div>
          <div style="width:1px;height:40px;background:rgba(255,255,255,.1)"></div>
          <div><p style="font-size:9px;opacity:.5;margin:0">MARKET DATA</p><p style="font-size:18px;font-weight:600;margin:0">{int(r["m"]):,}</p></div>
          <div><p style="font-size:9px;opacity:.5;margin:0">SEC FILINGS</p><p style="font-size:18px;font-weight:600;margin:0">{int(r["f"]):,}</p></div>
          <div><p style="font-size:9px;opacity:.5;margin:0">NEWS ARTICLES</p><p style="font-size:18px;font-weight:600;margin:0">{int(r["n"]):,}</p></div>
          <div style="width:1px;height:40px;background:rgba(255,255,255,.1)"></div>
          <div><p style="font-size:9px;opacity:.5;margin:0">DB SIZE</p><p style="font-size:18px;font-weight:600;margin:0">{r["db_size"]}</p></div>
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
            fig_style(fig,290); fig.update_xaxes(rangeslider_visible=True,rangeslider_thickness=0.05)
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
            fig_style(fig2,290); st.plotly_chart(fig2,use_container_width=True)

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
            st.dataframe(rf.rename(columns={"ticker":"Ticker","company_name":"Company","form_type":"Form","filed_at":"Filed"}),use_container_width=True,hide_index=True)

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
        fig_style(fig,380); fig.update_layout(title="Normalized to 100 at period start",yaxis_title="Index")
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
        fig_style(fig,400); fig.update_layout(title=sel_name)
        fig.update_xaxes(rangeslider_visible=True,rangeslider_thickness=0.05)
        st.plotly_chart(fig,use_container_width=True)

    st.markdown("---")
    a1,a2=st.columns(2)
    with a1:
        st.markdown('<p class="sec">Value distribution</p>', unsafe_allow_html=True)
        fh=px.histogram(data,x="value",nbins=50,color_discrete_sequence=[BLUE])
        fig_style(fh,200); st.plotly_chart(fh,use_container_width=True)
    with a2:
        st.markdown('<p class="sec">Period-over-period % change</p>', unsafe_allow_html=True)
        data["pct"]=data["value"].pct_change()*100
        fc=px.bar(data.dropna(),x="date",y="pct",color="pct",
                  color_continuous_scale=["#FF6B6B","#F8FAFF",BLUE],color_continuous_midpoint=0)
        fig_style(fc,200); fc.update_coloraxes(showscale=False)
        st.plotly_chart(fc,use_container_width=True)

    if st.checkbox("Show raw data + download"):
        st.dataframe(data.sort_values("date",ascending=False).head(500),use_container_width=True,hide_index=True)
        st.download_button("Download CSV",data.to_csv(index=False),f"{code}.csv","text/csv")


# ══════════════════════════════════════════════════════════════════════════════
# SEC FILINGS
# ══════════════════════════════════════════════════════════════════════════════
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
        fp.update_layout(margin=dict(l=0,r=0,t=10,b=0),height=200,paper_bgcolor="white")
        st.plotly_chart(fp,use_container_width=True)
    with ch2:
        st.markdown('<p class="sec">Monthly timeline</p>', unsafe_allow_html=True)
        tl=fil.copy(); tl["filed_at"]=pd.to_datetime(tl["filed_at"].astype(str)); tl["month"]=tl["filed_at"].dt.strftime("%Y-%m")
        ml=tl.groupby(["month","form_type"]).size().reset_index(name="n")
        ft=px.bar(ml,x="month",y="n",color="form_type",barmode="stack",color_discrete_map={"8-K":"#FF6B6B","10-K":BLUE})
        fig_style(ft,200); st.plotly_chart(ft,use_container_width=True)
    with ch3:
        st.markdown('<p class="sec">By ticker</p>', unsafe_allow_html=True)
        bt=fil.groupby("ticker").size().reset_index(name="n").sort_values("n",ascending=True)
        fb2=px.bar(bt,x="n",y="ticker",orientation="h",color_discrete_sequence=[BLUE])
        fig_style(fb2,200); st.plotly_chart(fb2,use_container_width=True)

    st.markdown("---")
    disp=fil[["ticker","company_name","form_type","filed_at","period","is_material_event"]].copy()
    disp.columns=["Ticker","Company","Form","Filed","Period","Material"]
    st.dataframe(disp,use_container_width=True,hide_index=True)
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
        fig_style(fc,140); fc.update_layout(showlegend=False,margin=dict(l=0,r=0,t=0,b=0))
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
            fig_style(fig,350); fig.update_layout(title=f"Filing events with news coverage within ±{window} days (bubble = article count)")
            st.plotly_chart(fig,use_container_width=True)
            st.success(f"✓ Found {len(cd)} filings with nearby news coverage using a ±{window}-day window")
        else:
            st.info(f"No overlaps found with ±{window}-day window — try increasing the window above or re-running the pipeline with more keywords")
        st.dataframe(cross.rename(columns={"ticker":"Ticker","company_name":"Company","form_type":"Form","filed_at":"Filed","news_same_day":f"News within ±{window}d"}),use_container_width=True,hide_index=True)

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
            fig_style(fig2,400); fig2.update_layout(legend=dict(orientation="h",y=1.05))
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
                st.dataframe(cf[["form_type","filed_at","period"]],use_container_width=True,hide_index=True)
        with cb:
            st.markdown(f'<p class="sec">{pick} — annual filing frequency</p>', unsafe_allow_html=True)
            if not cf.empty:
                cf2=cf.copy(); cf2["filed_at"]=pd.to_datetime(cf2["filed_at"]); cf2["year"]=cf2["filed_at"].dt.year
                yr=cf2.groupby(["year","form_type"]).size().reset_index(name="n")
                fy=px.bar(yr,x="year",y="n",color="form_type",barmode="group",color_discrete_map={"8-K":"#EF4444","10-K":BLUE})
                fig_style(fy,260); st.plotly_chart(fy,use_container_width=True)

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
                fig_style(fig_a, 240)
                st.plotly_chart(fig_a, use_container_width=True)
            else:
                st.info(f"No {alert_form.split()[0]} filings found for selected tickers in the last {alert_lookback} days")


# ══════════════════════════════════════════════════════════════════════════════
# ABOUT
# ══════════════════════════════════════════════════════════════════════════════
elif page == "About":
    st.markdown('<p class="sec">Group 7 · Columbia University</p>', unsafe_allow_html=True)
    st.title("About the Platform")
    st.markdown("A real-time ETL pipeline ingesting financial news, SEC filings, and macroeconomic data from three free APIs — transformed with distributed computing and served via this dashboard.\n\n**Team:** Ce Zhang · Cai Gao · Yuchun Wu · Yanji Li")
    st.markdown("---")
    c1,c2=st.columns(2)
    with c1:
        st.markdown("### Data Sources")
        st.markdown("""| Source | Type | Volume | Frequency |
|---|---|---|---|
| **NewsAPI** | Headlines | ~500/day | Every 15 min |
| **SEC Edgar** | 8-K & 10-K | ~50–200/day | On filing |
| **FRED** | 12 macro series | Back to 2010 | Daily/Monthly |""")
        st.markdown("### Stack")
        st.markdown("""| Technology | Role |
|---|---|
| **Kafka** | Streaming buffer |
| **PySpark** | Distributed transform + join |
| **PostgreSQL** | Structured warehouse |
| **MongoDB** | Document store |
| **Airflow** | Orchestration |""")
    with c2:
        st.markdown("### ETL Process")
        st.code("""Extract
├── NewsAPI    → Kafka: news-articles
├── SEC Edgar  → Kafka: sec-filings
└── FRED API   → Kafka: market-data

Transform (PySpark)
├── Deduplicate by URL / accession number
├── Standardise dates + parse types
├── Tag news categories
├── Flag 8-K material events
└── Cross-source join on timestamp

Load
├── PostgreSQL ← market_data, sec_filings, news_sentiment
└── MongoDB    ← news_articles, sec_filing_documents""", language="")
        st.markdown("### Cost Estimate")
        st.markdown("""| Component | Demo | Production |
|---|---|---|
| Kafka | $0 | $50–150/mo |
| PySpark | $0 | $200–500/mo |
| PostgreSQL | $0 | $15–50/mo |
| MongoDB | $0 | $57–200/mo |
| Airflow | $0 | $100–200/mo |
| **Total** | **$0** | **$422–1,100/mo** |""")
