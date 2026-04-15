"""
Schema context for SQL generation agents.
Defines the approved secure views, allowed columns, and business rules.
In production, this would reflect actual Snowflake secure view definitions.
"""

# ─────────────────────────────────────────────
# APPROVED SECURE VIEWS (allowlist)
# ─────────────────────────────────────────────
ALLOWED_VIEWS = [
    "SALES_VIEW_SECURE",
    "CUSTOMER_VIEW_SECURE",
]

# ─────────────────────────────────────────────
# COLUMN DEFINITIONS PER VIEW
# ─────────────────────────────────────────────
VIEW_SCHEMAS = {
    "SALES_VIEW_SECURE": {
        "columns": [
            "tenant_id",
            "deal_id",
            "customer_id",
            "customer_name",
            "region",
            "segment",
            "revenue",
            "deal_size",
            "close_date",
            "product_line",
            "stage",
            "owner_id",
        ],
        "description": (
            "Secure view of sales transactions. Contains one row per closed deal. "
            "Revenue and deal_size are in USD. close_date is a DATE column. "
            "Regions: EMEA, AMER, APAC. Segments: Enterprise, Mid-Market, SMB."
        ),
    },
    "CUSTOMER_VIEW_SECURE": {
        "columns": [
            "tenant_id",
            "customer_id",
            "customer_name",
            "account_owner",
            "industry",
            "annual_contract_value",
            "renewal_date",
            "health_score",
            "churn_risk",
        ],
        "description": (
            "Secure view of customer accounts. One row per customer. "
            "annual_contract_value is in USD. renewal_date is a DATE column. "
            "churn_risk values: High, Medium, Low. health_score is 0–100."
        ),
    },
}

# ─────────────────────────────────────────────
# DATE HELPER EXPRESSIONS (for SQL generation context)
# ─────────────────────────────────────────────
DATE_HELPERS = {
    "last quarter": "close_date >= DATEADD(quarter, -1, DATE_TRUNC('quarter', CURRENT_DATE())) AND close_date < DATE_TRUNC('quarter', CURRENT_DATE())",
    "this quarter": "close_date >= DATE_TRUNC('quarter', CURRENT_DATE()) AND close_date < DATEADD(quarter, 1, DATE_TRUNC('quarter', CURRENT_DATE()))",
    "last month": "close_date >= DATEADD(month, -1, DATE_TRUNC('month', CURRENT_DATE())) AND close_date < DATE_TRUNC('month', CURRENT_DATE())",
    "this year": "YEAR(close_date) = YEAR(CURRENT_DATE())",
    "last year": "YEAR(close_date) = YEAR(CURRENT_DATE()) - 1",
}

# ─────────────────────────────────────────────
# METRIC DEFINITIONS (business rules for SQL gen)
# ─────────────────────────────────────────────
METRIC_DEFINITIONS = {
    "revenue": "SUM(revenue)",
    "total revenue": "SUM(revenue)",
    "average deal size": "AVG(deal_size)",
    "avg deal size": "AVG(deal_size)",
    "deal count": "COUNT(deal_id)",
    "number of deals": "COUNT(deal_id)",
    "top customers": "SUM(revenue) AS total_revenue ORDER BY total_revenue DESC",
    "churn risk": "churn_risk",
    "annual contract value": "SUM(annual_contract_value)",
    "acv": "SUM(annual_contract_value)",
}

# ─────────────────────────────────────────────
# FORBIDDEN PATTERNS (for SQL validator reference)
# ─────────────────────────────────────────────
FORBIDDEN_SQL_KEYWORDS = [
    "INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER",
    "TRUNCATE", "MERGE", "COPY", "GRANT", "REVOKE", "EXEC",
    "EXECUTE", "CALL", "BEGIN", "COMMIT", "ROLLBACK",
]

DEFAULT_ROW_LIMIT = 100
