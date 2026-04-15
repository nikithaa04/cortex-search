"""
Snowflake Client Service
========================
Provides a unified interface for:
  - Executing SQL against Snowflake secure views
  - Calling Snowflake Cortex COMPLETE for LLM inference

MOCK MODE (default): Uses an in-memory SQLite database seeded with realistic
sales data. Mirrors the exact schema of the Snowflake secure views so the rest
of the app is unchanged when switching to a real Snowflake connection.

PRODUCTION MODE: Set USE_MOCK=False and populate the SNOWFLAKE_CONFIG dict
(or use environment variables) to connect to real Snowflake.
"""

import os
import json
import sqlite3
import random
import re
from datetime import date, timedelta
from typing import Any, Optional

import pandas as pd

# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────
USE_MOCK = True  # Set to False to use real Snowflake

SNOWFLAKE_CONFIG = {
    "account":   os.getenv("SNOWFLAKE_ACCOUNT", "your_account"),
    "user":      os.getenv("SNOWFLAKE_USER", "your_user"),
    "password":  os.getenv("SNOWFLAKE_PASSWORD", "your_password"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
    "database":  os.getenv("SNOWFLAKE_DATABASE", "CORTEX_DEMO"),
    "schema":    os.getenv("SNOWFLAKE_SCHEMA", "SALES"),
    "role":      os.getenv("SNOWFLAKE_ROLE", "CORTEX_ANALYST_ROLE"),
}

# Cortex model to use for LLM inference
CORTEX_MODEL = "snowflake-arctic"   # or "mistral-large", "llama3-70b", etc.

# Default tenant for demo purposes
DEFAULT_TENANT_ID = "tenant_acme"


# ─────────────────────────────────────────────
# MOCK DATABASE SETUP
# ─────────────────────────────────────────────
_MOCK_DB: Optional[sqlite3.Connection] = None


def _seed_mock_db() -> sqlite3.Connection:
    """Create and seed an in-memory SQLite database that mirrors Snowflake secure views."""
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    conn.row_factory = sqlite3.Row

    # ── SALES_VIEW_SECURE ──────────────────────────────────────────────────
    conn.execute("""
        CREATE TABLE SALES_VIEW_SECURE (
            tenant_id     TEXT,
            deal_id       TEXT PRIMARY KEY,
            customer_id   TEXT,
            customer_name TEXT,
            region        TEXT,
            segment       TEXT,
            revenue       REAL,
            deal_size     REAL,
            close_date    TEXT,
            product_line  TEXT,
            stage         TEXT,
            owner_id      TEXT
        )
    """)

    # ── CUSTOMER_VIEW_SECURE ───────────────────────────────────────────────
    conn.execute("""
        CREATE TABLE CUSTOMER_VIEW_SECURE (
            tenant_id              TEXT,
            customer_id            TEXT PRIMARY KEY,
            customer_name          TEXT,
            account_owner          TEXT,
            industry               TEXT,
            annual_contract_value  REAL,
            renewal_date           TEXT,
            health_score           INTEGER,
            churn_risk             TEXT
        )
    """)

    # ── SEED DATA ─────────────────────────────────────────────────────────
    random.seed(42)

    regions    = ["EMEA", "AMER", "APAC"]
    segments   = ["Enterprise", "Mid-Market", "SMB"]
    products   = ["Platform Pro", "Analytics Suite", "Data Connector", "Security Add-on"]
    industries = ["Financial Services", "Healthcare", "Technology", "Retail", "Manufacturing"]
    owners     = ["alice_smith", "bob_jones", "carol_wu", "david_kim", "eva_martinez"]
    risk_map   = {"High": (0, 40), "Medium": (40, 70), "Low": (70, 100)}

    customers = []
    for i in range(1, 51):
        cid = f"CUST{i:04d}"
        seg = random.choice(segments)
        acv = (
            random.uniform(100_000, 800_000) if seg == "Enterprise"
            else random.uniform(25_000, 100_000) if seg == "Mid-Market"
            else random.uniform(5_000, 25_000)
        )
        health = random.randint(0, 100)
        risk = "High" if health < 40 else ("Medium" if health < 70 else "Low")
        renewal = date.today() + timedelta(days=random.randint(30, 365))
        customers.append({
            "tenant_id":             DEFAULT_TENANT_ID,
            "customer_id":           cid,
            "customer_name":         f"{'Acme Corp' if i == 1 else f'Client {i:02d}'} {'Inc' if i % 3 == 0 else 'Ltd'}",
            "account_owner":         random.choice(owners),
            "industry":              random.choice(industries),
            "annual_contract_value": round(acv, 2),
            "renewal_date":          renewal.isoformat(),
            "health_score":          health,
            "churn_risk":            risk,
        })

    conn.executemany(
        "INSERT INTO CUSTOMER_VIEW_SECURE VALUES "
        "(:tenant_id,:customer_id,:customer_name,:account_owner,:industry,"
        ":annual_contract_value,:renewal_date,:health_score,:churn_risk)",
        customers,
    )

    # Generate ~300 sales deals spread across the last 18 months
    today = date.today()
    deals = []
    for idx in range(300):
        cust    = random.choice(customers)
        region  = random.choice(regions)
        seg     = cust["customer_id"]  # reuse customer segment via join logic
        seg_val = random.choice(segments)
        deal_sz = round(cust["annual_contract_value"] * random.uniform(0.8, 1.4), 2)
        rev     = round(deal_sz * random.uniform(0.9, 1.0), 2)
        days_ago = random.randint(0, 548)  # ~18 months
        close_dt = (today - timedelta(days=days_ago)).isoformat()
        deals.append({
            "tenant_id":    DEFAULT_TENANT_ID,
            "deal_id":      f"DEAL{idx+1:05d}",
            "customer_id":  cust["customer_id"],
            "customer_name": cust["customer_name"],
            "region":       region,
            "segment":      seg_val,
            "revenue":      rev,
            "deal_size":    deal_sz,
            "close_date":   close_dt,
            "product_line": random.choice(products),
            "stage":        "Closed Won",
            "owner_id":     random.choice(owners),
        })

    conn.executemany(
        "INSERT INTO SALES_VIEW_SECURE VALUES "
        "(:tenant_id,:deal_id,:customer_id,:customer_name,:region,:segment,"
        ":revenue,:deal_size,:close_date,:product_line,:stage,:owner_id)",
        deals,
    )

    conn.commit()
    return conn


def _get_mock_db() -> sqlite3.Connection:
    global _MOCK_DB
    if _MOCK_DB is None:
        _MOCK_DB = _seed_mock_db()
    return _MOCK_DB


# ─────────────────────────────────────────────
# SQL REWRITING FOR SQLITE COMPATIBILITY
# ─────────────────────────────────────────────
def _adapt_sql_for_sqlite(sql: str) -> str:
    """
    Translate Snowflake-specific SQL syntax to SQLite equivalents.
    Used in mock mode only. Production uses SQL as-is against Snowflake.
    """
    # Strip tenant_id placeholders
    sql = re.sub(r"tenant_id\s*=\s*'[^']*'", "1=1", sql, flags=re.IGNORECASE)
    sql = re.sub(r"AND\s+1=1", "", sql, flags=re.IGNORECASE)
    sql = re.sub(r"WHERE\s+1=1\s+AND", "WHERE", sql, flags=re.IGNORECASE)
    sql = re.sub(r"WHERE\s+1=1", "", sql, flags=re.IGNORECASE)

    # DATEADD / DATE_TRUNC → SQLite date()
    today = date.today()

    # Last quarter approximation
    sql = re.sub(
        r"DATEADD\(quarter,\s*-1,\s*DATE_TRUNC\('quarter',\s*CURRENT_DATE\(\)\)\)",
        f"'{(today - timedelta(days=90)).isoformat()}'",
        sql, flags=re.IGNORECASE,
    )
    sql = re.sub(
        r"DATE_TRUNC\('quarter',\s*CURRENT_DATE\(\)\)",
        f"'{today.replace(day=1).isoformat()}'",
        sql, flags=re.IGNORECASE,
    )

    # CURRENT_DATE()
    sql = re.sub(r"CURRENT_DATE\(\)", f"'{today.isoformat()}'", sql, flags=re.IGNORECASE)

    # YEAR(col) = X  → strftime('%Y', col) = 'X'
    sql = re.sub(
        r"YEAR\((\w+)\)\s*=\s*(\d{4})",
        r"strftime('%Y', \1) = '\2'",
        sql, flags=re.IGNORECASE,
    )

    # AVG → AVG (already valid in SQLite, just ensure AVERAGE → AVG)
    sql = re.sub(r"\bAVERAGE\b", "AVG", sql, flags=re.IGNORECASE)

    # ILIKE → LIKE
    sql = re.sub(r"\bILIKE\b", "LIKE", sql, flags=re.IGNORECASE)

    return sql.strip()


# ─────────────────────────────────────────────
# PUBLIC INTERFACE
# ─────────────────────────────────────────────
def execute_query(sql: str, tenant_id: str = DEFAULT_TENANT_ID) -> pd.DataFrame:
    """
    Execute a validated SQL query and return results as a DataFrame.
    In mock mode: runs against SQLite.
    In production: runs against Snowflake via snowflake-connector-python.
    """
    if USE_MOCK:
        db = _get_mock_db()
        adapted_sql = _adapt_sql_for_sqlite(sql)
        try:
            df = pd.read_sql_query(adapted_sql, db)
            return df
        except Exception as e:
            raise RuntimeError(f"Mock query execution failed: {e}\nAdapted SQL:\n{adapted_sql}")
    else:
        # Production path — requires: pip install snowflake-connector-python[pandas]
        import snowflake.connector  # noqa
        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        cur = conn.cursor()
        cur.execute(sql)
        df = cur.fetch_pandas_all()
        cur.close()
        conn.close()
        return df


def cortex_complete(prompt: str, model: str = CORTEX_MODEL) -> str:
    """
    Call Snowflake Cortex COMPLETE to get an LLM response.

    In mock mode: uses a lightweight local fallback (pattern-based SQL gen
    or simple text generation) to keep the demo self-contained.

    In production: executes  SELECT SNOWFLAKE.CORTEX.COMPLETE(model, prompt)
    against Snowflake.

    NOTE: For a real Cortex connection without full Snowflake data, you can
    also set USE_MOCK=True but provide SNOWFLAKE_CONFIG with Cortex access —
    the execute_query path will be mocked while Cortex will be real.
    """
    if USE_MOCK:
        return _mock_cortex_complete(prompt)
    else:
        sql = f"SELECT SNOWFLAKE.CORTEX.COMPLETE('{model}', $$ {prompt} $$) AS response"
        df = execute_query(sql)
        return str(df.iloc[0, 0])


# ─────────────────────────────────────────────
# MOCK CORTEX COMPLETE (self-contained LLM sim)
# ─────────────────────────────────────────────
def _mock_cortex_complete(prompt: str) -> str:
    """
    Lightweight mock of Snowflake Cortex COMPLETE.

    Detects which agent is calling based on prompt content, then returns
    a realistic response appropriate for demo purposes.

    In production this function is never called — the real Cortex endpoint
    is used instead.
    """
    prompt_lower = prompt.lower()

    # ── Answer Synthesis Agent (check first — most distinctive) ───────────
    if "business analyst assistant" in prompt_lower or "write your answer now:" in prompt_lower:
        return _mock_answer_synthesis(prompt)

    # ── SQL Generation Agent ───────────────────────────────────────────────
    # Identified by the opening line of SQL_GENERATION_PROMPT
    if "you are a sql generation agent" in prompt_lower or "only select statements are allowed" in prompt_lower:
        return _mock_sql_generation(prompt)

    # ── Query Understanding Agent ──────────────────────────────────────────
    # Identified by "extract a structured query intent" in QU prompt
    if "extract a structured query intent" in prompt_lower or "return only valid json" in prompt_lower:
        return _mock_query_understanding(prompt)

    # Fallback
    return "No response generated. Connect to Snowflake Cortex COMPLETE in production."


def _mock_query_understanding(prompt: str) -> str:
    """Parse the question from the prompt and return a plausible intent JSON."""
    import re, json

    # Extract the actual question — it appears after the last "Question:" line
    match = re.search(r"Question:\s*(.+?)(?:\n|$)", prompt, re.IGNORECASE)
    question = match.group(1).strip() if match else ""
    q = question.lower()

    intent: dict[str, Any] = {
        "metric": "revenue",
        "aggregation": "SUM",
        "dimensions": [],
        "filters": {},
        "time_filter": None,
        "limit": None,
        "primary_view": "SALES_VIEW_SECURE",
        "intent_summary": f"User wants to analyze: {question}",
    }

    # ── Metric detection ───────────────────────────────────────────────────
    if "deal size" in q or "deal_size" in q:
        intent["metric"] = "deal_size"
        intent["aggregation"] = "AVG"
    elif "churn" in q:
        intent["metric"] = "churn_risk"
        intent["aggregation"] = "COUNT"
        intent["primary_view"] = "CUSTOMER_VIEW_SECURE"
    elif "acv" in q or "annual contract" in q:
        intent["metric"] = "annual_contract_value"
        intent["aggregation"] = "SUM"
        intent["primary_view"] = "CUSTOMER_VIEW_SECURE"
    elif "how many" in q or "number of" in q or "count" in q:
        intent["metric"] = "deal_id"
        intent["aggregation"] = "COUNT"

    # ── Dimension detection ────────────────────────────────────────────────
    # "top N customers" / "by customer" / "show customers"
    if re.search(r"top\s+\d+\s+customer|top\s+customer|by customer|show.*customer|customer.*revenue", q):
        intent["dimensions"] = ["customer_name"]
        intent["metric"] = "revenue"
        intent["aggregation"] = "SUM"
        intent["primary_view"] = "SALES_VIEW_SECURE"
    if "by region" in q or "per region" in q or "region" in q and not intent["filters"].get("region"):
        if "by region" in q or "per region" in q:
            intent["dimensions"].append("region")
    if "by segment" in q or "per segment" in q:
        intent["dimensions"].append("segment")
    if "by product" in q or "per product" in q:
        intent["dimensions"].append("product_line")

    # ── Filter detection ───────────────────────────────────────────────────
    for region in ["EMEA", "AMER", "APAC"]:
        if region.lower() in q:
            intent["filters"]["region"] = region
    for seg in ["enterprise", "mid-market", "smb"]:
        if seg in q:
            intent["filters"]["segment"] = seg.title()
    for risk in ["high churn", "high risk"]:
        if risk in q:
            intent["filters"]["churn_risk"] = "High"
            intent["primary_view"] = "CUSTOMER_VIEW_SECURE"

    # ── Time filter ────────────────────────────────────────────────────────
    for tf in ["last quarter", "this quarter", "last month", "this year", "last year"]:
        if tf in q:
            intent["time_filter"] = tf
            break

    # ── Limit ──────────────────────────────────────────────────────────────
    top_match = re.search(r"top\s+(\d+)", q)
    if top_match:
        intent["limit"] = int(top_match.group(1))
    elif re.search(r"top\s+customer|show.*customer", q):
        intent["limit"] = 10

    return json.dumps(intent)


def _mock_sql_generation(prompt: str) -> str:
    """Extract intent JSON from prompt and generate appropriate SQL."""
    import re, json

    # Find the JSON block that follows "QUERY INTENT:" — use greedy match
    # to capture the full nested object (the last } in the section).
    # We locate the start of the JSON, then use a bracket counter to find the end.
    intent = None
    qi_match = re.search(r"QUERY INTENT:\s*\n?(\{)", prompt, re.DOTALL)
    if qi_match:
        start = qi_match.start(1)
        depth = 0
        end   = start
        for i, ch in enumerate(prompt[start:], start):
            if ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    end = i + 1
                    break
        try:
            intent = json.loads(prompt[start:end])
        except json.JSONDecodeError:
            intent = None

    if intent is None:
        return (
            "SELECT customer_name, SUM(revenue) AS total_revenue\n"
            "FROM SALES_VIEW_SECURE\n"
            "WHERE tenant_id = 'tenant_acme'\n"
            "GROUP BY customer_name\n"
            "ORDER BY total_revenue DESC\n"
            "LIMIT 10"
        )

    view      = intent.get("primary_view", "SALES_VIEW_SECURE")
    metric    = intent.get("metric", "revenue")
    agg       = intent.get("aggregation", "SUM")
    dims      = intent.get("dimensions", [])
    filters   = intent.get("filters", {})
    time_filt = intent.get("time_filter")
    limit     = intent.get("limit", 10)

    # Build SELECT
    alias = f"{agg.lower()}_{metric}"
    if dims:
        select_cols = ", ".join(dims) + f", {agg}({metric}) AS {alias}"
    else:
        select_cols = f"{agg}({metric}) AS {alias}"

    # Build WHERE
    where_clauses = ["tenant_id = 'tenant_acme'"]
    for col, val in filters.items():
        where_clauses.append(f"{col} = '{val}'")

    if time_filt:
        today = date.today()
        if time_filt == "last quarter":
            q_start = date(today.year, ((today.month - 1) // 3) * 3 + 1, 1)
            if q_start.month == 1:
                pq_start = date(q_start.year - 1, 10, 1)
            else:
                pq_start = date(q_start.year, q_start.month - 3, 1)
            pq_end = date(q_start.year, q_start.month, 1) - timedelta(days=1)
            where_clauses.append(f"close_date BETWEEN '{pq_start.isoformat()}' AND '{pq_end.isoformat()}'")
        elif time_filt == "this year":
            where_clauses.append(f"strftime('%Y', close_date) = '{today.year}'")
        elif time_filt == "last year":
            where_clauses.append(f"strftime('%Y', close_date) = '{today.year - 1}'")

    where_str = " AND ".join(where_clauses)

    # Build GROUP BY / ORDER BY
    group_str  = f"GROUP BY {', '.join(dims)}" if dims else ""
    order_str  = f"ORDER BY {alias} DESC"
    limit_str  = f"LIMIT {limit or 10}"

    sql_parts = [
        f"SELECT {select_cols}",
        f"FROM {view}",
        f"WHERE {where_str}",
    ]
    if group_str:
        sql_parts.append(group_str)
    sql_parts.append(order_str)
    sql_parts.append(limit_str)

    return "\n".join(sql_parts)


def _mock_answer_synthesis(prompt: str) -> str:
    """Generate a plain-English answer from results embedded in the prompt."""
    import re, json

    # Extract question
    q_match = re.search(r"User Question:\s*(.+)", prompt)
    question = q_match.group(1).strip() if q_match else "your question"

    # Extract results
    r_match = re.search(r"Query Results \(as JSON\):\s*(\[.+?\]|\{.+?\})", prompt, re.DOTALL)
    kb_match = re.search(r"Business Context.*?:\s*(.+?)(?:Write your answer|$)", prompt, re.DOTALL)
    kb_text  = kb_match.group(1).strip() if kb_match else ""

    if not r_match:
        return (
            f"No results could be retrieved for: \"{question}\". "
            "Try rephrasing the question or verify the filters are valid."
        )

    try:
        results = json.loads(r_match.group(1))
    except Exception:
        results = []

    if not results:
        return (
            f"No records matched your query for: \"{question}\". "
            "This could be due to the filters applied (e.g., date range, region, or segment). "
            "Try broadening your search criteria."
        )

    # Build a natural language answer based on result shape
    if isinstance(results, list) and len(results) > 0:
        first = results[0]
        keys  = list(first.keys())

        # Single aggregate result
        if len(keys) == 1:
            val = first[keys[0]]
            if isinstance(val, float):
                val_str = f"${val:,.0f}"
            else:
                val_str = str(val)
            answer = f"The result for your question — \"{question}\" — is **{val_str}**."

        # Two columns (dimension + metric)
        elif len(keys) == 2:
            dim_key = keys[0]
            met_key = keys[1]
            top_val = first[met_key]
            top_dim = first[dim_key]
            if isinstance(top_val, float):
                top_str = f"${top_val:,.0f}"
            else:
                top_str = str(top_val)
            answer = (
                f"The top result for \"{question}\" is **{top_dim}** with {met_key.replace('_', ' ')} "
                f"of **{top_str}**. There are {len(results)} record(s) in the result set."
            )

        else:
            answer = (
                f"Your query returned {len(results)} record(s). "
                f"The first result shows: "
                + ", ".join(f"{k} = {v}" for k, v in list(first.items())[:3]) + "."
            )
    else:
        answer = f"The query returned no results for: \"{question}\"."

    # Append KB context if available
    if kb_text and "no relevant" not in kb_text.lower() and len(kb_text) > 20:
        short_kb = kb_text[:200].rsplit(".", 1)[0] + "."
        answer += f"\n\n> **Definition used:** {short_kb}"

    return answer
