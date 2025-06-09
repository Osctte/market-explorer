# ==== main.py  =========================================================
"""
Market-Explorer v1.1
  1. Reads industry keyword from Industry_Control!A2
  2. Builds / updates the Candidates tab (GICS  +  Google scrape)
  3. Pulls 10 years of FMP metrics for every candidate            (no more Keep? flag)
  4. Writes clashes to Pending_Review when existing values differ
  5. Appends a simple run-log

ENV VARS (GitHub Secrets): FMP_KEY, SERP_KEY, OPENAI_KEY,
                           CSE_ID, CSE_KEY, GSHEET_ID, GOOGLE_SERVICE_JSON
"""
# ----------------------------------------------------------------------
import os, re, json, time, datetime, textwrap, requests, pandas as pd, openai, gspread
from google.oauth2.service_account import Credentials
# ----------------------------------------------------------------------
# 0  AUTH & SHEET HANDLES
SERVICE_INFO = json.loads(os.environ["GOOGLE_SERVICE_JSON"])
gc = gspread.authorize(
    Credentials.from_service_account_info(
        SERVICE_INFO,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
)
sh = gc.open_by_key(os.environ["GSHEET_ID"])
openai.api_key = os.environ["OPENAI_KEY"]

FMP_KEY  = os.environ["FMP_KEY"]
SERP_KEY = os.environ["SERP_KEY"]

# ----------------------------------------------------------------------
# 1  INDUSTRY CONTROL
industry = (sh.worksheet("Industry_Control").acell("A2").value or "").strip()
if not industry:
    raise SystemExit("⚠️  Industry_Control!A2 is empty")
print(f"▶️  Industry selected: {industry}")

# ----------------------------------------------------------------------
# 2  HELPERS
def gics_screen(industry_kw: str, limit=30):
    """Screen by sector from Financial Modeling Prep (can be empty)."""
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
        if isinstance(d, dict) and d.get("companyName")
    ]


def serpapi_screen(industry_kw: str, limit=25):
    """Google ‘top … public companies’."""
    qs  = f"top {industry_kw} public companies"
    url = f"https://serpapi.com/search.json?engine=google&q={requests.utils.quote(qs)}&api_key={SERP_KEY}"
    hits = requests.get(url, timeout=20).json().get("organic_results", [])[:limit]

    out = []
    for h in hits:
        title = h.get("title", "")
        # ticker = all-caps letters only (1-5) → avoids 2025, DJT, etc. if they include digits
        m = re.search(r"\(([A-Z]{1,5})\)", title)
        if m:
            out.append(
                {"Company": title.split("(")[0].strip(), "Ticker": m.group(1), "Source": "Web"}
            )
    return out


def merge_candidates(existing_df: pd.DataFrame, new_rows: list[dict]):
    if existing_df.empty or "Ticker" not in existing_df.columns:
        return pd.DataFrame(new_rows)

    existing = set(existing_df["Ticker"].astype(str).str.upper())
    fresh    = [r for r in new_rows if r["Ticker"].upper() not in existing]
    return pd.concat([existing_df, pd.DataFrame(fresh)], ignore_index=True)


def write_df(ws_name: str, df: pd.DataFrame):
    ws = sh.worksheet(ws_name)
    ws.clear()
    ws.update([df.columns.values.tolist()] + df.values.tolist(), value_input_option="RAW")

# ----------------------------------------------------------------------
# 3  CANDIDATES TAB
cand_ws = sh.worksheet("Candidates")
try:
    cand_df = pd.DataFrame(cand_ws.get_all_records())
except gspread.exceptions.APIError:
    cand_df = pd.DataFrame(columns=["Industry", "Company", "Ticker", "Source"])

other_rows = cand_df[cand_df["Industry"] != industry]
curr       = cand_df[cand_df["Industry"] == industry]

new_list   = gics_screen(industry) + serpapi_screen(industry)
if new_list:
    curr = merge_candidates(
        curr,
        [dict(r, Industry=industry) for r in new_list],
    )

cand_df = pd.concat([other_rows, curr], ignore_index=True)
write_df("Candidates", cand_df)
print(f"ℹ️  Candidates list updated – {len(curr)} rows for {industry}")

# ----------------------------------------------------------------------
# 4  FMP FINANCIAL PULL
metrics_ws = sh.worksheet("Metrics")
metrics_df = pd.DataFrame(metrics_ws.get_all_records())
if metrics_df.empty:
    metrics_df = pd.DataFrame(
        columns=["Industry", "Company", "Ticker", "FY", "Metric", "Value", "Source", "IsEstimate"]
    )


def fmp_financials(ticker: str, years=10):
    """Safely collect Income + Cash-Flow for *years* back."""
    inc_url = f"https://financialmodelingprep.com/api/v3/income-statement/{ticker}?limit={years}&apikey={FMP_KEY}"
    cfs_url = f"https://financialmodelingprep.com/api/v3/cash-flow-statement/{ticker}?limit={years}&apikey={FMP_KEY}"

    inc = requests.get(inc_url, timeout=20).json() or []
    cfs = requests.get(cfs_url, timeout=20).json() or []

    rows = []
    for rec in inc:
        try:
            fy = int(rec["calendarYear"])
        except (KeyError, ValueError):
            continue

        rows_map = [
            ("Revenue",           rec.get("revenue")),
            ("Operating Expense", rec.get("operatingExpenses")),
            ("EBITDA",            rec.get("ebitda")),
            ("Gross Margin",      rec.get("grossProfitRatio")),
            ("Net Margin",        rec.get("netProfitMargin") or rec.get("netIncomeRatio")),
            ("R&D Expense",       rec.get("researchAndDevelopmentExpenses")),
        ]
        rows.extend([(fy, m, v) for m, v in rows_map if v is not None])

    for rec in cfs:
        try:
            fy = int(rec["calendarYear"])
        except (KeyError, ValueError):
            continue

        rows.extend(
            [
                (fy, "CapEx",          rec.get("capitalExpenditure")),
                (fy, "Free Cash Flow", rec.get("freeCashFlow")),
            ]
        )

    # one-shot profile for balance-sheet-ish items
    prof   = requests.get(
        f"https://financialmodelingprep.com/api/v3/profile/{ticker}?apikey={FMP_KEY}",
        timeout=20,
    ).json()
    thisyr = datetime.datetime.now().year
    if prof:
        p = prof[0]
        rows.extend(
            [
                (thisyr, "Employees",           p.get("fullTimeEmployees")),
                (thisyr, "Cash & Equivalents",  p.get("cash")),
                (thisyr, "Total Debt",          p.get("debt")),
                (thisyr, "Dividend per Share",  p.get("lastDiv")),
            ]
        )
    return rows


def dedupe_insert(df: pd.DataFrame, industry: str, company: str, ticker: str, new_rows):
    added, clashes = 0, 0
    for fy, metric, val in new_rows:
        if val is None:
            continue
        dup = df[
            (df["Industry"] == industry)
            & (df["Ticker"] == ticker)
            & (df["FY"] == fy)
            & (df["Metric"] == metric)
        ]
        if dup.empty:
            df.loc[len(df)] = [
                industry,
                company,
                ticker,
                fy,
                metric,
                val,
                "FMP-API",
                "N",
            ]
            added += 1
        elif dup.iloc[0]["Value"] != val:
            sh.worksheet("Pending_Review").append_row(
                [company, fy, metric, dup.iloc[0]["Value"], val]
            )
            clashes += 1
    return df, added, clashes


start = time.time()
added_total, clash_total, missing = 0, 0, []

for _, row in curr.iterrows():
    company, ticker = row["Company"], row["Ticker"]
    print(f"📊 {ticker:<6}", end="")

    try:
        new_rows = fmp_financials(ticker)
        if not new_rows:
            print(" … no data")
            missing.append(ticker)
            continue

        metrics_df, a, c = dedupe_insert(metrics_df, industry, company, ticker, new_rows)
        added_total  += a
        clash_total  += c
        print(f" …  {a:>2} rows")
    except Exception as e:
        print(f" … ⚠️  error → {e}")
        missing.append(ticker)

# pretty print any totally missing tickers
if missing:
    print("⚠️  No FMP data for:", ", ".join(map(str, missing)))

write_df("Metrics", metrics_df)
print(f"✅ Metrics updated: +{added_total} rows   |   {clash_total} clashes sent to Pending_Review")

# ----------------------------------------------------------------------
# 5  RUN-LOG
dur = round(time.time() - start, 1)
sh.worksheet("RunLog").append_row(
    [datetime.datetime.utcnow().isoformat(), industry, len(curr), dur, "ok"]
)
print(f"🏁 Done in {dur}s")
# ======================================================================
