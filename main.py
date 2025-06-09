# ==== main.py  =========================================================
"""
Market-Explorer v2
  1. Reads the industry keyword from Industry_Control!A2.
  2. Builds / updates the Candidates tab   (FMP screener + Google/SerpAPI).
  3. Pulls financial metrics for every VALID candidate (no manual keep).
  4. Scrapes an approximate market-size figure with Custom Search.
  5. Writes three-bullet insights (per company + sector aggregate).
  6. Logs duration & status in RunLog.

Environment variables (GitHub Secrets):
  FMP_KEY, SERP_KEY, OPENAI_KEY,
  CSE_ID, CSE_KEY,
  GSHEET_ID,
  GOOGLE_SERVICE_JSON   (service-account JSON, single line)
"""

# ------------------ stdlib & 3rd-party -------------------------------
import os, json, re, time, datetime, textwrap
from urllib.parse import quote

import requests, openai, pandas as pd, gspread
from google.oauth2.service_account import Credentials

# ------------------ 0.  auth & globals --------------------------------
SERVICE_INFO = json.loads(os.environ["GOOGLE_SERVICE_JSON"])
SCOPES       = ["https://www.googleapis.com/auth/spreadsheets"]
gc           = gspread.authorize(Credentials.from_service_account_info(
                     SERVICE_INFO, scopes=SCOPES))
sh           = gc.open_by_key(os.environ["GSHEET_ID"])

openai.api_key = os.environ["OPENAI_KEY"]

FMP_KEY  = os.environ["FMP_KEY"]
SERP_KEY = os.environ["SERP_KEY"]
CSE_ID   = os.environ["CSE_ID"]
CSE_KEY  = os.environ["CSE_KEY"]

# ------------------ 1.  read control cell -----------------------------
industry = (sh.worksheet("Industry_Control").acell("A2").value or "").strip()
if not industry:
    raise SystemExit("⚠️  Industry_Control!A2 is empty")

print(f"▶️  Industry selected: {industry}")

# ------------------ 2.  helper functions ------------------------------
def fmp_screen(industry_kw: str, limit: int = 30):
    """
    Hit FMP screener twice:
      1) industry filter  2) sector filter (fallback)
    Returns list of {Company, Ticker, Source}.
    """
    urls = [
        (f"https://financialmodelingprep.com/api/v3/stock-screener"
         f"?industry={quote(industry_kw)}&limit={limit}&apikey={FMP_KEY}", "industry"),
        (f"https://financialmodelingprep.com/api/v3/stock-screener"
         f"?sector={quote(industry_kw)}&limit={limit}&apikey={FMP_KEY}", "sector"),
    ]
    for url, mode in urls:
        try:
            data = requests.get(url, timeout=20).json()
        except Exception as e:
            print(f"⚠️  FMP ({mode}) error: {e}")
            data = []
        if isinstance(data, list) and data:
            print(f"✅ FMP matched {len(data)} tickers via {mode} filter.")
            return [
                {"Company": d["companyName"], "Ticker": d["symbol"], "Source": "FMP"}
                for d in data if d.get("companyName") and d.get("symbol")
            ]
    print(f"ℹ️  FMP: no match for “{industry_kw}”.")
    return []

def is_valid_ticker(tkr: str) -> bool:
    """Cheap validation: 1-5 letters & FMP returns profile data."""
    if not (1 <= len(tkr) <= 5 and tkr.isalpha()):
        return False
    resp = requests.get(
        f"https://financialmodelingprep.com/api/v3/profile/{tkr}?apikey={FMP_KEY}",
        timeout=10,
    ).json()
    return bool(resp)

def serpapi_screen(industry_kw: str, limit: int = 20):
    qs  = f"top {industry_kw} public companies"
    url = f"https://serpapi.com/search.json?engine=google&q={quote(qs)}&api_key={SERP_KEY}"
    hits = requests.get(url, timeout=20).json().get("organic_results", [])[:limit]
    out  = []
    for h in hits:
        title = h.get("title", "")
        m = re.search(r"\((\w{1,5})\)", title)   # crude "(TICK)" scrape
        if m:
            tkr = m.group(1).upper()
            if is_valid_ticker(tkr):
                out.append({"Company": title.split("(")[0].strip(),
                            "Ticker":  tkr,
                            "Source":  "Web"})
    return out

def merge_candidates(existing_df: pd.DataFrame, new_rows: list[dict]):
    """Append only tickers we haven’t seen before."""
    if existing_df.empty or "Ticker" not in existing_df.columns:
        return pd.DataFrame(new_rows)
    seen = set(existing_df["Ticker"].astype(str).str.upper())
    fresh = [r for r in new_rows if r["Ticker"].upper() not in seen]
    return pd.concat([existing_df, pd.DataFrame(fresh)], ignore_index=True)

def write_df(ws_name: str, df: pd.DataFrame):
    ws = sh.worksheet(ws_name)
    ws.clear()
    ws.update([df.columns.values.tolist()] + df.values.tolist(), value_input_option="RAW")

# ------------------ 3.  update Candidates tab -------------------------
cand_ws = sh.worksheet("Candidates")
try:
    cand_df = pd.DataFrame(cand_ws.get_all_records())
except gspread.exceptions.APIError:
    cand_df = pd.DataFrame(columns=["Industry", "Company", "Ticker", "Source"])

cand_df = cand_df.reindex(columns=["Industry", "Company", "Ticker", "Source"])  # keep schema

other_rows = cand_df[cand_df["Industry"] != industry]
curr       = cand_df[cand_df["Industry"] == industry]

new_list   = fmp_screen(industry) + serpapi_screen(industry)
if new_list:
    curr = merge_candidates(curr,
            [dict(r, Industry=industry) for r in new_list])

cand_df = pd.concat([other_rows, curr], ignore_index=True)
write_df("Candidates", cand_df)

print(f"ℹ️  Candidates list updated – {len(curr)} rows for {industry}")

# ---------- 4.  pull metrics for *all* current candidates -------------
approved = curr  # no manual Keep? step

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
    """Return list of (FY, Metric, Value) tuples."""
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
            (fy, "CapEx",          rec["capitalExpenditure"]),
            (fy, "Free Cash Flow", rec["freeCashFlow"])
        ]
    prof = requests.get(
        f"https://financialmodelingprep.com/api/v3/profile/{ticker}?apikey={FMP_KEY}",
        timeout=20).json()
    if prof:
        this_yr = datetime.datetime.now().year
        p0 = prof[0]
        rows += [
            (this_yr, "Employees",          p0.get("fullTimeEmployees", 0)),
            (this_yr, "Cash & Equivalents", p0.get("cash", 0)),
            (this_yr, "Total Debt",         p0.get("debt", 0)),
            (this_yr, "Dividend per Share", p0.get("lastDiv", 0)),
        ]
    return rows

def dedupe_insert(df: pd.DataFrame, new_rows, company, ticker):
    added = 0
    for fy, metric, val in new_rows:
        dup = df[(df["Industry"] == industry) &
                 (df["Ticker"]   == ticker)   &
                 (df["FY"]       == fy)       &
                 (df["Metric"]   == metric)]
        if dup.empty:
            df.loc[len(df)] = [industry, company, ticker, fy,
                               metric, val, "FMP-API", "N"]
            added += 1
    return df, added

added_total = 0
start_ts = time.time()

for _, row in approved.iterrows():
    company, ticker = row["Company"], row["Ticker"]
    print(f"📊 Pulling {ticker}")
    try:
        rows = fmp_financials(ticker)
        metrics_df, a = dedupe_insert(metrics_df, rows, company, ticker)
        added_total += a
    except Exception as e:
        print(f"⚠️  {ticker} failed: {e}")

write_df("Metrics", metrics_df)
print(f"✅ Metrics updated: +{added_total} new rows")

# -------------- 5.  scrape market-size figure -------------------------
def scrape_market_size(industry_kw: str):
    q = f"{industry_kw} market size revenue"
    url = (f"https://customsearch.googleapis.com/customsearch/v1"
           f"?key={CSE_KEY}&cx={CSE_ID}&q={quote(q)}")
    data = requests.get(url, timeout=20).json()
    snippet = data.get("items", [{}])[0].get("snippet", "")
    m = re.search(r"\$?([\d\.,]+)\s*(billion|million|trillion)", snippet, re.I)
    if not m:
        return None, None, snippet[:120]
    num, scale = m.groups()
    fig = float(num.replace(",", ""))
    if scale.lower().startswith("million"):   fig /= 1_000
    if scale.lower().startswith("trillion"):  fig *= 1_000
    return fig, "B", snippet[:120] + "..."

fig, unit, cite = scrape_market_size(industry)
if fig:
    sh.worksheet("MarketSize").append_row(
        [industry, fig, unit, datetime.datetime.now().year, cite, "Y"])
    print(f"🌍 Market size appended: {fig} {unit}")

# -------------- 6.  generate GPT insights -----------------------------
def bullets_for(dfsub: pd.DataFrame, target: str, level: str):
    prompt = textwrap.dedent(f"""
        Provide three concise, bullet-point insights on the following
        financial data. Level: {level}, Target: {target}
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
    sub = metrics_df[(metrics_df["Ticker"] == tkr) &
                     (metrics_df["Industry"] == industry)]
    ins_ws.append_row([industry, "Company", tkr,
                       bullets_for(sub, tkr, "Company"),
                       datetime.datetime.utcnow().isoformat()])

sector_df = metrics_df[metrics_df["Industry"] == industry]
ins_ws.append_row([industry, "Sector", industry,
                   bullets_for(sector_df, industry, "Sector"),
                   datetime.datetime.utcnow().isoformat()])

# -------------- 7.  run-log -------------------------------------------
dur = round(time.time() - start_ts, 1)
sh.worksheet("RunLog").append_row(
    [datetime.datetime.utcnow().isoformat(), industry,
     len(approved), dur, "ok"])
print(f"🏁 Done in {dur}s")
# ======================================================================
