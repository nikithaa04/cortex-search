"""
Agent 5: Answer Synthesis Agent
=================================
Combines query results, business context, and the original question
into a clear, plain-English response suitable for business users.

Input  : question, DataFrame results, KB context string
Output : AnswerResult (answer_text, confidence_note, sources_used)
"""

import json
from dataclasses import dataclass, field
from typing import Optional, Union

import pandas as pd

from services.snowflake_client import cortex_complete
from config.prompts import ANSWER_SYNTHESIS_PROMPT


@dataclass
class AnswerResult:
    answer:       str
    confidence:   str                = "High"
    sources:      list[str]          = field(default_factory=list)
    row_count:    int                = 0
    empty_result: bool               = False


def synthesize_answer(
    question:   str,
    df:         pd.DataFrame,
    kb_context: str,
    kb_sources: list[str] = None,
) -> AnswerResult:
    """
    Run the Answer Synthesis Agent.

    Converts a DataFrame of query results + KB context into
    a human-readable business answer using Cortex COMPLETE.

    Args:
        question   : Original user question.
        df         : Query result DataFrame.
        kb_context : Formatted knowledge base context string.
        kb_sources : List of KB source titles used.

    Returns:
        AnswerResult with the synthesized answer and metadata.
    """
    if kb_sources is None:
        kb_sources = []

    row_count = len(df)

    # Truncate results for prompt (max 20 rows to keep prompt size manageable)
    results_for_prompt: Union[list, str]
    if df.empty:
        results_for_prompt = []
        empty_result = True
    else:
        results_for_prompt = df.head(20).to_dict(orient="records")
        # Round floats for cleaner display
        results_for_prompt = _round_floats(results_for_prompt)
        empty_result = False

    results_json = json.dumps(results_for_prompt, default=str, indent=2)

    prompt = ANSWER_SYNTHESIS_PROMPT.format(
        question   = question,
        results    = results_json,
        kb_context = kb_context if kb_context else "No relevant business context found.",
    )

    raw_answer = cortex_complete(prompt)
    answer = raw_answer.strip()

    # Determine confidence level
    confidence = _assess_confidence(df, kb_sources)

    return AnswerResult(
        answer       = answer,
        confidence   = confidence,
        sources      = kb_sources,
        row_count    = row_count,
        empty_result = empty_result,
    )


def _round_floats(records: list[dict]) -> list[dict]:
    """Round float values in result records to 2 decimal places."""
    cleaned = []
    for row in records:
        new_row = {}
        for k, v in row.items():
            if isinstance(v, float):
                new_row[k] = round(v, 2)
            else:
                new_row[k] = v
        cleaned.append(new_row)
    return cleaned


def _assess_confidence(df: pd.DataFrame, kb_sources: list[str]) -> str:
    """
    Simple heuristic confidence assessment.
    High   = results + KB context found
    Medium = results but no KB context
    Low    = empty results
    """
    if df.empty:
        return "Low"
    if kb_sources:
        return "High"
    return "Medium"
