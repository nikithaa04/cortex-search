"""
Query Logging Service
=====================
Logs every query interaction for audit, analytics, and debugging.

Writes to a local SQLite file (query_log.db) in mock / dev mode.
In production this would insert to a Snowflake QUERY_LOG table.

Log schema:
  - log_id       : unique identifier
  - timestamp    : UTC ISO timestamp
  - tenant_id    : tenant context
  - question     : original user question
  - intent_json  : structured intent extracted by Agent 1
  - generated_sql: SQL produced by Agent 2
  - validation   : pass/fail + reason from Agent 3
  - row_count    : number of rows returned
  - latency_ms   : end-to-end latency
  - kb_sources   : comma-separated KB titles used
  - status       : success / error
  - error_msg    : error message if status = error
"""

import sqlite3
import json
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Union

# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────
LOG_DB_PATH = Path(__file__).parent.parent / "query_log.db"


# ─────────────────────────────────────────────
# DB SETUP
# ─────────────────────────────────────────────
def _get_log_db() -> sqlite3.Connection:
    conn = sqlite3.connect(str(LOG_DB_PATH), check_same_thread=False)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS query_log (
            log_id        TEXT PRIMARY KEY,
            timestamp     TEXT NOT NULL,
            tenant_id     TEXT,
            question      TEXT,
            intent_json   TEXT,
            generated_sql TEXT,
            validation    TEXT,
            row_count     INTEGER,
            latency_ms    REAL,
            kb_sources    TEXT,
            status        TEXT,
            error_msg     TEXT
        )
    """)
    conn.commit()
    return conn


# ─────────────────────────────────────────────
# PUBLIC INTERFACE
# ─────────────────────────────────────────────
class QueryLog:
    """Context manager / builder for a single query log entry."""

    def __init__(self, tenant_id: str = "tenant_acme", question: str = ""):
        self.log_id       = str(uuid.uuid4())
        self.tenant_id    = tenant_id
        self.question     = question
        self.intent_json  = None
        self.generated_sql = None
        self.validation   = None
        self.row_count    = None
        self.kb_sources   = None
        self.status       = "success"
        self.error_msg    = None
        self._start       = time.time()

    def set_intent(self, intent: "Union[dict, str]") -> None:
        self.intent_json = json.dumps(intent) if isinstance(intent, dict) else intent

    def set_sql(self, sql: str) -> None:
        self.generated_sql = sql

    def set_validation(self, valid: bool, reason: str = "") -> None:
        self.validation = f"{'PASS' if valid else 'FAIL'}: {reason}" if reason else ("PASS" if valid else "FAIL")

    def set_results(self, row_count: int) -> None:
        self.row_count = row_count

    def set_kb_sources(self, sources: list[str]) -> None:
        self.kb_sources = ", ".join(sources)

    def set_error(self, msg: str) -> None:
        self.status    = "error"
        self.error_msg = msg

    def save(self) -> str:
        """Persist the log entry and return the log_id."""
        latency_ms = round((time.time() - self._start) * 1000, 2)
        timestamp  = datetime.now(timezone.utc).isoformat()

        db = _get_log_db()
        db.execute(
            """INSERT OR REPLACE INTO query_log
               (log_id, timestamp, tenant_id, question, intent_json,
                generated_sql, validation, row_count, latency_ms,
                kb_sources, status, error_msg)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                self.log_id, timestamp, self.tenant_id, self.question,
                self.intent_json, self.generated_sql, self.validation,
                self.row_count, latency_ms, self.kb_sources,
                self.status, self.error_msg,
            ),
        )
        db.commit()
        return self.log_id


def get_recent_logs(limit: int = 20) -> list[dict]:
    """Retrieve the most recent query log entries for display in the audit panel."""
    db = _get_log_db()
    cur = db.execute(
        """SELECT log_id, timestamp, question, validation, row_count,
                  latency_ms, status, generated_sql, kb_sources
           FROM query_log
           ORDER BY timestamp DESC
           LIMIT ?""",
        (limit,),
    )
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def get_log_stats() -> dict:
    """Return aggregate statistics across all logged queries."""
    db = _get_log_db()
    cur = db.execute("""
        SELECT
            COUNT(*)                                        AS total_queries,
            SUM(CASE WHEN status = 'success' THEN 1 END)   AS successful,
            SUM(CASE WHEN status = 'error'   THEN 1 END)   AS errors,
            SUM(CASE WHEN validation LIKE 'FAIL%' THEN 1 END) AS blocked,
            ROUND(AVG(latency_ms), 1)                       AS avg_latency_ms,
            ROUND(AVG(row_count), 1)                        AS avg_rows_returned
        FROM query_log
    """)
    cols = [d[0] for d in cur.description]
    row  = cur.fetchone()
    return dict(zip(cols, row)) if row else {}
