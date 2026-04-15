"""
Agent 3: SQL Validation Agent
================================
Rule-based (not LLM-based) SQL safety and governance guardrail.

Input  : raw SQL string
Output : ValidationResult (valid: bool, reason: str, safe_sql: str | None)

Checks enforced:
  1. Query must begin with SELECT
  2. Only approved secure views may be referenced
  3. No DDL / DML keywords (INSERT, UPDATE, DELETE, DROP, CREATE, ALTER, ...)
  4. No SELECT * (wildcard selects)
  5. Must include a LIMIT clause
  6. No disallowed column references (e.g. raw email / PII columns)
  7. Optional: no raw base table access

Uses sqlglot for robust SQL parsing when available; falls back to regex.
"""

import re
from dataclasses import dataclass
from typing import Optional

from config.schema_context import (
    ALLOWED_VIEWS,
    FORBIDDEN_SQL_KEYWORDS,
    DEFAULT_ROW_LIMIT,
)

# Columns that must never appear in query results (PII / masked fields)
MASKED_COLUMNS = {"email", "phone", "ssn", "tax_id", "password", "credit_card"}

# Try to import sqlglot for accurate parsing
try:
    import sqlglot
    import sqlglot.expressions as exp
    _HAS_SQLGLOT = True
except ImportError:
    _HAS_SQLGLOT = False


@dataclass
class ValidationResult:
    valid:    bool
    reason:   str
    safe_sql: Optional[str] = None  # cleaned / limit-enforced SQL if valid


def validate_sql(sql: str) -> ValidationResult:
    """
    Validate a SQL string against all governance rules.
    Returns a ValidationResult indicating whether the query is safe to execute.
    """
    if not sql or not sql.strip():
        return ValidationResult(valid=False, reason="Empty SQL query")

    sql = sql.strip()

    # ── Rule 1: Must start with SELECT ────────────────────────────────────
    first_token = sql.split()[0].upper() if sql.split() else ""
    if first_token != "SELECT":
        return ValidationResult(
            valid=False,
            reason=f"Query must begin with SELECT. Got: '{first_token}'. Only read queries are permitted.",
        )

    # ── Rule 2: No forbidden DDL/DML keywords ─────────────────────────────
    sql_upper = sql.upper()
    for kw in FORBIDDEN_SQL_KEYWORDS:
        # Use word-boundary check to avoid false positives
        if re.search(rf"\b{kw}\b", sql_upper):
            return ValidationResult(
                valid=False,
                reason=f"Forbidden keyword detected: '{kw}'. Data modification statements are not permitted.",
            )

    # ── Rule 3: No SELECT * ───────────────────────────────────────────────
    if re.search(r"SELECT\s+\*", sql_upper):
        return ValidationResult(
            valid=False,
            reason="SELECT * is not permitted. All queries must explicitly list columns.",
        )

    # ── Rule 4: Only allowed secure views ─────────────────────────────────
    referenced_tables = _extract_table_names(sql)
    disallowed = [t for t in referenced_tables if t.upper() not in [v.upper() for v in ALLOWED_VIEWS]]
    if disallowed:
        return ValidationResult(
            valid=False,
            reason=(
                f"Query references disallowed table(s): {disallowed}. "
                f"Only the following secure views are permitted: {ALLOWED_VIEWS}."
            ),
        )

    # ── Rule 5: No masked / PII column references ─────────────────────────
    col_refs = _extract_column_references(sql)
    pii_found = [c for c in col_refs if c.lower() in MASKED_COLUMNS]
    if pii_found:
        return ValidationResult(
            valid=False,
            reason=f"Query references masked/PII column(s): {pii_found}. These fields are not accessible.",
        )

    # ── Rule 6: Enforce LIMIT ─────────────────────────────────────────────
    safe_sql = sql
    if not re.search(r"\bLIMIT\s+\d+", sql_upper):
        # Append a default limit
        safe_sql = sql.rstrip(";").rstrip() + f"\nLIMIT {DEFAULT_ROW_LIMIT}"

    # ── All checks passed ─────────────────────────────────────────────────
    return ValidationResult(
        valid=True,
        reason="All governance checks passed.",
        safe_sql=safe_sql,
    )


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────
def _extract_table_names(sql: str) -> list[str]:
    """Extract referenced table/view names from SQL."""
    if _HAS_SQLGLOT:
        try:
            parsed = sqlglot.parse_one(sql, read="snowflake")
            tables = [
                t.name.upper()
                for t in parsed.find_all(exp.Table)
            ]
            return tables
        except Exception:
            pass  # Fall through to regex

    # Regex fallback: look for FROM and JOIN clauses
    pattern = r"(?:FROM|JOIN)\s+([a-zA-Z_][a-zA-Z0-9_]*)"
    matches = re.findall(pattern, sql, re.IGNORECASE)
    return [m.upper() for m in matches]


def _extract_column_references(sql: str) -> list[str]:
    """Extract all identifiers from SELECT clause for PII check."""
    # Find everything between SELECT and FROM
    match = re.search(r"SELECT\s+(.+?)\s+FROM\b", sql, re.IGNORECASE | re.DOTALL)
    if not match:
        return []
    select_str = match.group(1)
    # Split by comma, strip aliases and functions, extract bare names
    cols = []
    for part in select_str.split(","):
        # Remove AS alias
        part = re.sub(r"\bAS\s+\w+", "", part, flags=re.IGNORECASE)
        # Extract word tokens (potential column names)
        tokens = re.findall(r"\b[a-zA-Z_]\w*\b", part)
        cols.extend(tokens)
    return cols


def format_validation_badge(result: ValidationResult) -> str:
    """Return a short badge string for display in the UI."""
    if result.valid:
        return "Validation passed"
    return f"Validation blocked: {result.reason}"
