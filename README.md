# ❄️ Cortex Search
### Governed Natural Language to SQL on Snowflake

A governance-first AI data access product that lets business users query Snowflake in plain English — without writing SQL, without compromising security.

---

## Quick Start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Run the app
streamlit run app.py
```

The app launches in **mock mode** by default — no Snowflake credentials needed. It uses an in-memory SQLite database seeded with 300 realistic sales deals and 50 customer accounts.

---

## Architecture

```
User Question
     │
     ▼
Agent 1: Query Understanding    →  Extracts intent JSON (metric, filters, time range)
     │
     ▼
Agent 2: SQL Generation         →  Generates Snowflake SQL (Cortex COMPLETE)
     │
     ▼
Agent 3: SQL Validation         →  Governance guardrail (rule-based, sqlglot)
     │
     ▼  (blocked → returned to user with reason)
Snowflake Secure View           →  Executes on approved views only
     │
     ▼
Agent 4: KB Retrieval           →  Finds relevant metric definitions / policies
     │
     ▼
Agent 5: Answer Synthesis       →  Plain-English answer + business context
     │
     ▼
Query Log                       →  Audit trail for every interaction
```

---

## File Structure

```
cortex-search/
├── app.py                          # Streamlit frontend + pipeline orchestration
├── agents/
│   ├── query_understanding.py      # Agent 1: NL → structured intent
│   ├── sql_generator.py            # Agent 2: intent → SQL
│   ├── sql_validator.py            # Agent 3: governance guardrail
│   ├── kb_retriever.py             # Agent 4: knowledge base lookup
│   └── answer_synthesizer.py       # Agent 5: results → plain-English answer
├── services/
│   ├── snowflake_client.py         # Snowflake connector + Cortex COMPLETE wrapper
│   ├── kb_service.py               # Knowledge base retrieval
│   └── logging_service.py          # Query audit logging
├── config/
│   ├── schema_context.py           # Approved views, columns, business rules
│   └── prompts.py                  # LLM prompt templates
├── data/
│   └── kb_seed.json                # Knowledge base seed (10 entries)
└── requirements.txt
```

---

## Governance Controls

| Control | Implementation |
|---|---|
| Secure views only | Allowlist enforced before every query |
| No raw table access | Validator rejects any non-approved table reference |
| No DDL / DML | Keyword blocklist (INSERT, UPDATE, DROP, etc.) |
| No SELECT * | Explicit regex check |
| Row limit enforcement | Auto-appended if LIMIT missing |
| Tenant isolation | `tenant_id` filter injected into every query |
| Column masking | PII column blocklist (email, phone, ssn, etc.) |
| Full audit log | Every query logged with SQL, validation status, latency |

---

## Connecting to Real Snowflake

1. Open `services/snowflake_client.py`
2. Set `USE_MOCK = False`
3. Set environment variables (or edit `SNOWFLAKE_CONFIG` directly):

```bash
export SNOWFLAKE_ACCOUNT=your_account.snowflakecomputing.com
export SNOWFLAKE_USER=your_user
export SNOWFLAKE_PASSWORD=your_password
export SNOWFLAKE_WAREHOUSE=COMPUTE_WH
export SNOWFLAKE_DATABASE=CORTEX_DEMO
export SNOWFLAKE_SCHEMA=SALES
export SNOWFLAKE_ROLE=CORTEX_ANALYST_ROLE
```

4. Create the secure views in Snowflake using the schema defined in `config/schema_context.py`
5. Enable Snowflake Cortex on your account (Settings → Cortex)
6. Uncomment `snowflake-connector-python[pandas]` in `requirements.txt` and reinstall

---

## Example Queries to Try

- `Show me top 10 customers by revenue`
- `What was the average deal size for Enterprise customers in EMEA last quarter?`
- `How many deals closed in AMER this year?`
- `Which customers have high churn risk?`
- `Total revenue by region`
- `Average deal size by segment`

---

## Monetization

This product monetizes as a **Snowflake-native data access layer**:

- **Seat-based**: per business user / analyst seat
- **Usage-based**: per query or Cortex token consumed
- **Enterprise tier**: governance dashboard, admin controls, SSO, SLA

**Target buyers**: Data Platform teams at mid-market and enterprise companies already on Snowflake who want to extend self-service analytics to non-technical users without sacrificing governance.

---

*Built with Streamlit · Snowflake Cortex · sqlglot · Python*
