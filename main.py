# ==== main.py =========================================================
"""
Market‑Explorer v2
------------------
1.  Reads the industry keyword from Sheet tab **Industry_Control!A2**.
2.  Builds / updates **Candidates** (GICS API + Google scrape).
3.  Pulls 10 yrs of financial metrics for *every* candidate ticker found.
    • Retries once if the API gives nothing.
    • Logs tickers the API doesn’t know.
4.  Appends a rough market‑size figure (CustomSearch) to **MarketSize**.
5.  *Optionally* calls GPT to write three‑bullet insights – you can toggle
    that with an env‑var:  `RUN_INSIGHTS=1`.
6.  Logs duration + status in **RunLog**.

Required env vars (workflow → secrets):
  FMP_KEY, SERP_KEY, OPENAI_KEY,
  CSE_ID,  CSE_KEY,
  GSHEET_ID,
  GOOGLE_SERVICE_JSON   ← service‑account JSON as *one line*
"""

import json, os, re, time, textwrap, datetime
import requests, openai, pandas as pd, gspread
from google.oauth2.service_account import Credentials

# ----- runtime flags ---------------------------------------------------
RUN_INSIGHTS = os.getenv("RUN_INSIGHTS", "0") == "1"   # default OFF

# ---------- 0.  auth helpers -------------------------------------------
SERVICE_INFO = json.loads(os.environ["GOOGLE_SERVICE_JSON"])
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
gc = gspread.authorize(Credentials.from_service_account_info(SERVICE_INFO, scopes=SCOPES))
sh = gc.open_by_key(os.environ["GSHEET_ID"])

openai.api_key = os.environ["OPENAI_KEY"]

FMP_KEY  = os.environ["FMP_KEY"]
SERP_KEY = os.environ["SERP_KEY"]
CSE_ID   = os.environ["CSE_ID"]
CSE_KEY  = os.environ["CSE_KEY"]

# ---------- 1.  read control cell --------------------------------------
industry = (sh.worksheet("Industry_Control").acell("A2").value or "").strip()
if not industry:
    raise SystemExit("⚠️  Industry_Control!A2 is empty")

print(f"▶️  Industry selected: {industry}")

# ---------- 2.  helpers ------------------------------------------------

def gics_screen(industry_kw: str, limit: int = 30):
    """Return list[dict] from FMP sector screen; empty list on error."""
    url = (
        "https://financialmodelingprep.com/api/v4/stock-screening"
        f"?sector={requests.utils.quote(industry_kw)}&limit={limit}&apikey={FMP_KEY}"
    )
    try:
        data = requests.get(url, timeout=20).json()
    except Exception as e:
        print(f"⚠️  FMP request error: {e}")
        return []

    if not isinstance(data, list):  # FMP returns dict on error
        print(f"ℹ️  FMP: no sector match for '{industry_kw}'.")
        return []

    return [
        {"Company": d["companyName"], "Ticker": d["symbol"], "Source": "GICS"}
        for d in data if isinstance(d, dict) and d.get("companyName")
    ]


def serpapi_screen(industry_kw: str, limit: int = 20):
    """Scrape Google 'top … public companies' via SerpAPI."""
    qs = f"top {industry_kw} public companies"
    url = (
        f"https://serpapi.com/search.json?engine=google&q={requests.utils.quote(qs)}"
        f"&api_key={SERP_KEY}"
    )
    hits = requests.get(url, timeout=20).json().get("organic_results", [])[:limit]
    out = []
    for h in hits:
        title = h.get("title", "")
        m = re.search(r"\((\w{1,5})\)", title)  # crude ticker within ()
        if m:
            out.append({
                "Company": title.split("(")[0].strip(),
                "Ticker": m.group(1).upper(),
                "Source": "Web"
            })
    return out


def merge_candidates(existing_df: pd.DataFrame, new_rows: list[dict]):
    if existing_df.empty or "Ticker" not in existing_df.columns:
        return pd.DataFrame(new_rows)

    existing = set(existing_df["Ticker"].astype(str).str.upper())
    fresh = [r for r in new_rows if r["Ticker"].upper() not in existing]
    return pd.concat([existing_df, pd.DataFrame(fresh)], ignore_index=True)


def write_df(ws_name: str, df: pd.DataFrame):
    ws = sh.worksheet(ws_name)
    ws.clear()
    ws.update([df.columns.values.tolist()] + df.values.tolist(), value_input_option="RAW")

# ---------- 3.  update Candidates tab ---------------------------------
cand_ws = sh.worksheet("Candidates")
try:
    cand_df = pd.DataFrame(cand_ws.get_all_records())
except gspread.exceptions.APIError:
    cand_df = pd.DataFrame()

if cand_df.empty:
    cand_df = pd.DataFrame(columns=["Industry", "Company", "Ticker", "Source"])

other_rows = cand_df[cand_df["Industry"] != industry]
current    = cand_df[cand_df["Industry"] == industry].copy()

new_list   = gics_screen(industry) + serpapi_screen(industry)
if new_list:
    current = merge_candidates(current, [{**r, "Industry": industry} for r in new_list])

cand_df = pd.concat([other_rows, current], ignore_index=True)
write_df("Candidates", cand_df)
print(f"ℹ️  Candidates list updated – {len(current)} rows for {industry}")

# ---------- 4.  pull metrics for ALL candidates -----------------------
approved = current  # no manual Keep? step now
if approved.empty:
    print("🟡 No valid companies found for this industry. Exiting.")
    exit(0)

metrics_ws = sh.worksheet("Metrics")
metrics_df = pd.DataFrame(metrics_ws.get_all_records())
metrics_df = metrics_df.reindex(columns=[
    "Industry", "Company", "Ticker", "FY", "Metric",
    "Value", "Source", "IsEstimate"
])


def fmp_financials(ticker: str, years: int = 10):
    url_i = f"https://financialmodelingprep.com/api/v3/income-statement/{ticker}?limit={years}&apikey={FMP_KEY}"
    url_c = f"https://financialmodelingprep.com/api/v3/cash-flow-statement/{ticker}?limit={years}&apikey={FMP_KEY}"
    inc = requests.get(url_i, timeout=20).json()
    cfs = requests.get(url_c, timeout=20).json()
    rows = []
    for rec in inc:
        fy = int(rec["calendarYear"])
        rows += [
            (fy, "Revenue",             rec["revenue"]),
            (fy, "Operating Expense",   rec["operatingExpenses"]),
            (fy, "EBITDA",              rec["ebitda"]),
            (fy, "Gross Margin",        rec["grossProfitRatio"]),
            (fy, "Net Margin",          rec["netProfitMargin"]),
            (fy, "R&D Expense",         rec.get("researchAndDevelopmentExpenses", 0)),
        ]
    for rec in cfs:
        fy = int(rec["calendarYear"])
        rows += [
            (fy, "CapEx",               rec["capitalExpenditure"]),
            (fy, "Free Cash Flow",      rec["freeCashFlow"])
        ]
    # profile endpoint for point‑in‑time metrics
    prof = requests.get(
        f"https://financialmodelingprep.com/api/v3/profile/{ticker}?apikey={FMP_KEY}", timeout=20
    ).json()
    if prof:
        this_year = datetime.datetime.now().year
        rows += [
            (this_year, "Employees",          prof[0].get("fullTimeEmployees", 0)),
            (this_year, "Cash & Equivalents", prof[0].get("cash", 0)),
            (this_year, "Total Debt",         prof[0].get("debt", 0)),
            (this_year, "Dividend per Share", prof[0].get("lastDiv", 0)),
        ]
    return rows


def safe_financials(tkr: str, tries: int = 2):
    for attempt in range(1, tries + 1):
        data = fmp_financials(tkr)
        if data:
            return data
        time.sleep(1)
    return []


def dedupe_insert(df: pd.DataFrame, new_rows, company: str, ticker: str):
    added = 0
    for (fy, metric, val) in new_rows:
        dup = df[(df["Industry"] == industry) & (df["Ticker"] == ticker) &
                 (df["FY"] == fy) & (df["Metric"] == metric)]
        if dup.empty:
            df.loc[len(df)] = [industry, company, ticker, fy, metric, val, "FMP-API", "N"]
            added += 1
    return df, added

added_total, missing = 0, []
start_ts = time.time()

for _, row in approved.iterrows():
    company, ticker = row["Company"], row["Ticker"]
    print(f"📊 {ticker:<6} …", end="", flush=True)
    try:
        rows = safe_financials(ticker)
        if rows:
            metrics_df, added = dedupe_insert(metrics_df, rows, company, ticker)
            added_total += added
            print(f" {added:>3} rows")
        else:
            missing.append(ticker)
            print(" no data")
    except Exception as e:
        missing.append(ticker)
        print(f" ⚠️  error → {e}")

write_df("Metrics", metrics_df)
print(f"✅ Metrics updated: +{added_total} rows")
if missing:
    print(f"⚠️  No FMP data for: {', '.join(missing)}")

# ---------- 5.  scrape market‑size ------------------------------------

def scrape_market_size(industry_kw: str):
    q = f"{industry_kw} market size revenue"
    url = (
        "https://customsearch.googleapis.com/customsearch/v1"
        f"?key={CSE_KEY}&cx={CSE_ID}&q={requests.utils.quote(q)}"
    )
    data = requests.get(url, timeout=20).json()
    snippet = data.get("items", [{"snippet": ""}])[0].get("snippet", "")
    m = re.search(r"\$?([0-9\.,]+)\s*(billion|million|trillion)", snippet, re.I)
    if m:
        num, scale = m.groups()
        figure = float(num.replace(",", ""))
        if scale.lower().startswith("million"):
            figure /= 1000
        elif scale.lower().startswith("trillion"):
            figure *= 1000
        return figure, "B", snippet[:120] + "…"
    return None, None, snippet[:120]

mkt_fig, mkt_unit, cite = scrape_market_size(industry)
if mkt_fig:
    sh.worksheet("MarketSize").append_row([
        industry, mkt_fig, mkt_unit, datetime.datetime.now().year, cite, "Y"
    ])
    print(f"🌍 Market size appended: {mkt_fig} {mkt_unit}")

# -------------- 6.  generate insights (optional) ----------------------
if RUN_INSIGHTS:
    def bullets_for(dfsub, target, level="Company"):
        prompt = textwrap.dedent(f"""
            Provide three concise, bullet‑point insights on the following financial data.
            Level: {level}, Target: {target}
            Respond with exactly three bullets.
            JSON:
            {dfsub.to_json()}
        """)
        resp = openai.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.2,
        )
        return resp.choices[0].message.content.strip()

    ins_ws = sh.worksheet("Insights")
    for _, row in approved.iterrows():
        tkr = row["Ticker"]
        df_ = metrics_df[(metrics_df["Ticker"] == tkr) & (metrics_df["Industry"] == industry)]
        ins_ws.append_row([
            industry, "Company", tkr, bullets_for(df_, tkr, "Company"),
            datetime.datetime.now(datetime.timezone.utc).isoformat()
        ])

    sector_df = metrics_df[metrics_df["Industry"] == industry]
    ins_ws.append_row([
        industry, "Sector", industry, bullets_for(sector_df, industry, "Sector"),
        datetime.datetime.now(datetime.timezone.utc).isoformat()
    ])
    print("💬 Insights section finished.")
else:
    print("💬 Insights generation skipped (RUN_INSIGHTS=0)")

# ---------- 7.  run‑log ------------------------------------------------
run_dur = round(time.time() - start_ts, 1)
sh.worksheet("RunLog").append_row([
    datetime.datetime.now(datetime.timezone.utc).isoformat(), industry,
    len(approved), run_dur, "ok"
])
print(f"🏁 Done in {run_dur}s")
# ======================================================================
