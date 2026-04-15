"""
Agent 1: Query Understanding Agent
====================================
Classifies user intent and extracts structured metadata from a natural language question.

Input  : raw user question (string)
Output : QueryIntent dataclass containing metric, dimensions, filters, time range, etc.

Uses Snowflake Cortex COMPLETE (or mock equivalent) to do the extraction.
"""

import json
import re
from dataclasses import dataclass, field, asdict
from typing import Optional

from services.snowflake_client import cortex_complete
from config.prompts import QUERY_UNDERSTANDING_PROMPT


@dataclass
class QueryIntent:
    metric:          str                 = "revenue"
    aggregation:     Optional[str]       = "SUM"
    dimensions:      list[str]           = field(default_factory=list)
    filters:         dict[str, str]      = field(default_factory=dict)
    time_filter:     Optional[str]       = None
    limit:           Optional[int]       = None
    primary_view:    str                 = "SALES_VIEW_SECURE"
    intent_summary:  str                 = ""

    def to_dict(self) -> dict:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2)


def understand_query(question: str) -> QueryIntent:
    """
    Run the Query Understanding Agent on a user question.

    Calls Cortex COMPLETE with the structured extraction prompt,
    then parses the returned JSON into a QueryIntent.

    Falls back to a default intent if the LLM response cannot be parsed.
    """
    prompt = QUERY_UNDERSTANDING_PROMPT.replace("{question}", question)

    raw_response = cortex_complete(prompt)

    # Strip markdown code fences if present
    cleaned = re.sub(r"```(?:json)?", "", raw_response).strip().rstrip("```").strip() """Remove markdown junk"""

    # Extract first JSON object from the response
    json_match = re.search(r"\{.*\}", cleaned, re.DOTALL) """Extract only the JSON part"""
    if not json_match:
        return _fallback_intent(question)

    try:
        data = json.loads(json_match.group())
        return QueryIntent(
            metric         = data.get("metric", "revenue"),
            aggregation    = data.get("aggregation", "SUM"),
            dimensions     = data.get("dimensions", []),
            filters        = data.get("filters", {}),
            time_filter    = data.get("time_filter"),
            limit          = data.get("limit"),
            primary_view   = data.get("primary_view", "SALES_VIEW_SECURE"),
            intent_summary = data.get("intent_summary", question),
        )
    except (json.JSONDecodeError, KeyError):
        return _fallback_intent(question)


def _fallback_intent(question: str) -> QueryIntent:
    """Return a safe default intent when parsing fails."""
    return QueryIntent(
        metric         = "revenue",
        aggregation    = "SUM",
        dimensions     = ["customer_name"],
        filters        = {},
        time_filter    = None,
        limit          = 10,
        primary_view   = "SALES_VIEW_SECURE",
        intent_summary = f"Fallback intent for: {question}",
    )
