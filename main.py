# ==== main.py  =========================================================
"""
Market-Explorer v1 (AUTOMATED VERSION)
- Reads industry keyword from Industry_Control!A2.
- Overwrites Candidates tab for this industry, marks all Keep? as 'Y'.
- Pulls metrics for all valid companies.
- Scrapes market size.
- Writes insights.
- Logs run.

Only cell you edit is Industry_Control!A2.
"""

import json, os, re, time, textwrap, datetime
import requests, openai, pandas as pd, gspread
from google.oauth2.service_account import Credentials
from datetime import timezone

# --- 0. Auth ---
SERVICE_INFO = json.loads(os.environ["GOOGLE_SERVICE_JSON"])
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
gc = gspread.authorize(Credentials.from_service_account_info(SERVICE_INFO, scopes=SCOPES))
sh = gc.open_by_key(os.environ["GSHEET_ID"])

openai.api_key = os.environ["OPENAI_KEY"]

FMP_KEY  = os.environ["FMP_KEY"]
SERP_KEY = os.environ["SERP_KEY"]
CSE_ID   = os.environ["CSE_ID"]
CSE_KEY  = os.environ["CSE_KEY"]

# --- 1. Read control cell ---
industry = sh.worksheet("Industry_Control").acell("A2").value or ""
industry = industry.strip()
if not industry:
    raise SystemExit("⚠️  Industry_Control!A2 is empty")
print(f"▶️  Industry selected: {industry}")

# --- 2. Candidate screens ---
def gics_screen(industry_kw: str, limit=30):
    url = (
        "https://financialmodelingprep.com/api/v4/stock-screening"
        f"?sector={requests.utils.quote(industry_kw)}&limit={limit}&apikey={FMP_KEY}"
    )
    try:
        data = requests.get(url, timeout=20).json()
    except Exception as e:
        print(f"⚠️  FMP request error: {e}")
        return []
    if not isinstance(data, list):
        print(f"ℹ️  FMP: no sector match for '{industry_kw}'.")
        return []
    return [
        {"Company": d["companyName"], "Ticker": d["symbol"], "Source": "GICS"}
        for d in data
        if isinstance(d, dict) and "companyName" in d
    ]

def serpapi_screen(industry_kw: str, limit=20):
    qs = f"top {industry_kw} public companies"
    url = f"https://serpapi.com/search.json?engine=google&q={requests.utils.quote(qs)}&api_key={SERP_KEY}"
    hits = requests.get(url, timeout=20).json().get("organic_results", [])[:limit]
    out = []
    for h in hits:
        title = h.get("title", "")
        m = re.search(r"\((\w{1,5})\)", title)
        if m:
            out.append({"Company": title.split("(")[0].strip(), "Ticker": m.group(1), "Source": "Web"})
    return out

def is_valid_ticker(t):
    return bool(t) and t.isalnum() and not t.isdigit() and 1 <= len(t) <= 5

def write_df(ws_name, df):
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
    cand_df = pd.DataFrame(columns=["Industry","Company","Ticker","Source","Keep?"])

# keep only current industry rows
other_rows = cand_df[cand_df["Industry"] != industry]
curr      = cand_df[cand_df["Industry"] == industry]

# Try to get candidates from both sources
fmp_list = gics_screen(industry)
serp_list = serpapi_screen(industry)

if not fmp_list and not serp_list:
    print("🟡 No valid companies found for this industry. Exiting.")
    exit(0)

# Prefer FMP, but merge both if available
if fmp_list and serp_list:
    print(f"ℹ️  Candidates found from both FMP ({len(fmp_list)}) and SerpAPI ({len(serp_list)}).")
elif fmp_list:
    print(f"ℹ️  Candidates found from FMP only ({len(fmp_list)}).")
elif serp_list:
    print(f"ℹ️  Candidates found from SerpAPI only ({len(serp_list)}).")

new_list = fmp_list + serp_list

if new_list:
    curr = merge_candidates(curr, [dict(r, Industry=industry, **{"Keep?": ""}) for r in new_list])

cand_df = pd.concat([other_rows, curr], ignore_index=True)
write_df("Candidates", cand_df)

print(f"ℹ️  Candidates list updated – {len(curr)} rows for {industry}")

# --- 4. Pull metrics only for kept companies (all are kept) ---
if curr.empty:
    print("🟡 No valid companies found for this industry. Exiting.")
    exit(0)

metrics_ws = sh.worksheet("Metrics")
try:
    metrics_df = pd.DataFrame(metrics_ws.get_all_records())
except gspread.exceptions.APIError:
    metrics_df = pd.DataFrame()
if metrics_df.empty:
    metrics_df = pd.DataFrame(columns=["Industry","Company","Ticker","FY","Metric","Value","Source","IsEstimate"])

def fmp_financials(ticker: str, years=10):
    url_i = f"https://financialmodelingprep.com/api/v3/income-statement/{ticker}?limit={years}&apikey={FMP_KEY}"
    url_c = f"https://financialmodelingprep.com/api/v3/cash-flow-statement/{ticker}?limit={years}&apikey={FMP_KEY}"
    inc   = requests.get(url_i, timeout=20).json()
    cfs   = requests.get(url_c, timeout=20).json()
    rows  = []
    for rec in inc:
        fy = int(rec["calendarYear"])
        rows += [
            (fy,"Revenue",             rec["revenue"]),
            (fy,"Operating Expense",   rec["operatingExpenses"]),
            (fy,"EBITDA",              rec["ebitda"]),
            (fy,"Gross Margin",        rec["grossProfitRatio"]),
            (fy,"Net Margin",          rec["netProfitMargin"]),
            (fy,"R&D Expense",         rec.get("researchAndDevelopmentExpenses", 0)),
        ]
    for rec in cfs:
        fy = int(rec["calendarYear"])
        rows += [
            (fy,"CapEx",               rec["capitalExpenditure"]),
            (fy,"Free Cash Flow",      rec["freeCashFlow"])
        ]
    prof = requests.get(f"https://financialmodelingprep.com/api/v3/profile/{ticker}?apikey={FMP_KEY}", timeout=20).json()
    if prof:
        rows.append( (int(datetime.datetime.now().year),"Employees", prof[0].get("fullTimeEmployees",0)) )
        rows.append( (int(datetime.datetime.now().year),"Cash & Equivalents", prof[0].get("cash",0)) )
        rows.append( (int(datetime.datetime.now().year),"Total Debt", prof[0].get("debt",0)) )
        rows.append( (int(datetime.datetime.now().year),"Dividend per Share", prof[0].get("lastDiv",0)) )
    return rows

def dedupe_insert(df: pd.DataFrame, new_rows, company, ticker, industry):
    added, clashes = 0, 0
    for (fy,metric,val) in new_rows:
        key = (industry, company, ticker, fy, metric)
        dup = df[(df["Industry"]==industry)&(df["Ticker"]==ticker)&
                 (df["FY"]==fy)&(df["Metric"]==metric)]
        if dup.empty:
            df.loc[len(df)] = [industry,company,ticker,fy,metric,val,"FMP-API","N"]
            added +=1
        elif dup.iloc[0]["Value"] != val:
            pr = sh.worksheet("Pending_Review")
            pr.append_row([company, fy, metric, dup.iloc[0]["Value"], val])
            clashes +=1
    return df, added, clashes

added_total, clash_total = 0, 0
start = time.time()
for _, row in curr.iterrows():
    company, ticker = row["Company"], row["Ticker"]
    print(f"📊 Pulling {ticker}")
    try:
        new_rows = fmp_financials(ticker)
        metrics_df, a, c = dedupe_insert(metrics_df, new_rows, company, ticker, industry)
        added_total  += a
        clash_total  += c
    except Exception as e:
        print(f"⚠️  {ticker} failed: {e}")

write_df("Metrics", metrics_df)
print(f"✅ Metrics updated: +{added_total} new rows  |  {clash_total} clashes sent to Pending_Review")

# --- 5. Market size ---
def scrape_market_size(industry_kw: str):
    q  = f"{industry_kw} market size revenue"
    url = ("https://customsearch.googleapis.com/customsearch/v1"
           f"?key={CSE_KEY}&cx={CSE_ID}&q={requests.utils.quote(q)}")
    data = requests.get(url, timeout=20).json()
    snippet = data.get("items",[{}])[0].get("snippet","")
    m = re.search(r"\$?([0-9\.,]+)\s*(billion|million|trillion)", snippet, re.I)
    if m:
        num, scale = m.groups()
        figure = float(num.replace(",",""))
        if scale.lower().startswith("million"): figure /= 1000
        if scale.lower().startswith("trillion"): figure *= 1000
        return figure, "B", snippet[:120]+"..."
    return None, None, snippet[:120]

mkt_fig, mkt_unit, cite = scrape_market_size(industry)
if mkt_fig:
    ms_ws = sh.worksheet("MarketSize")
    ms_ws.append_row([industry, mkt_fig, mkt_unit, datetime.datetime.now().year, cite, "Y"])
    print(f"🌍 Market size appended: {mkt_fig} {mkt_unit}")

# --- 6. Insights ---
def bullets_for(dfsub, target, level="Company"):
    prompt = textwrap.dedent(f"""
        Provide three concise, bullet-point insights on the following financial data.
        Level: {level}, Target: {target}
        Respond with exactly three bullets.
        JSON:
        {dfsub.to_json()}
    """)
    resp = openai.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role":"user","content":prompt}],
        temperature=0.2,
    )
    return resp.choices[0].message.content.strip()

ins_ws = sh.worksheet("Insights")
for _, row in curr.iterrows():
    ticker = row["Ticker"]
    com_df = metrics_df[(metrics_df["Ticker"]==ticker)&(metrics_df["Industry"]==industry)]
    ins_ws.append_row([industry,"Company",ticker,bullets_for(com_df, ticker, "Company"),
                       datetime.datetime.now(timezone.utc).isoformat()])

sector_df = metrics_df[metrics_df["Industry"]==industry]
ins_ws.append_row([industry,"Sector",industry,bullets_for(sector_df, industry, "Sector"),
                   datetime.datetime.now(timezone.utc).isoformat()])

# --- 7. Run log ---
dur = round(time.time() - start, 1)
sh.worksheet("RunLog").append_row([datetime.datetime.now(timezone.utc).isoformat(),
                                   industry, len(curr), dur, "ok"])
print(f"🏁 Done in {dur}s")
# ======================================================================
