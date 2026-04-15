"""
Knowledge Base Service
======================
Retrieves relevant policy snippets, metric definitions, and business context
to enrich query answers.

Uses a local JSON file (data/kb_seed.json) in mock / dev mode.
In production this would query a Snowflake table or vector store.
"""

import json
import os
import re
from pathlib import Path
from typing import Optional


# ─────────────────────────────────────────────
# KB LOADING
# ─────────────────────────────────────────────
def _load_kb() -> list[dict]:
    """Load knowledge base entries from the seed JSON file."""
    kb_path = Path(__file__).parent.parent / "data" / "kb_seed.json"
    if not kb_path.exists():
        return []
    with open(kb_path, "r") as f:
        return json.load(f)


_KB_ENTRIES: list[dict] = _load_kb()


# ─────────────────────────────────────────────
# RETRIEVAL
# ─────────────────────────────────────────────
def retrieve_context(question: str, results_summary: str = "", top_k: int = 2) -> list[dict]:
    """
    Retrieve the most relevant KB entries for a given question.

    Uses simple keyword matching against tags and content.
    In production this would use embeddings / vector similarity search.

    Returns a list of matching KB entries (dicts with title, content, category).
    """
    if not _KB_ENTRIES:
        return []

    q_tokens = set(re.findall(r"\w+", (question + " " + results_summary).lower()))

    scored: list[tuple[float, dict]] = []
    for entry in _KB_ENTRIES:
        score = 0.0
        tags    = [t.lower() for t in entry.get("tags", [])]
        content = entry.get("content", "").lower()
        title   = entry.get("title", "").lower()

        for token in q_tokens:
            if token in tags:
                score += 3.0          # tag match = high signal
            if token in title:
                score += 2.0          # title match = medium signal
            if token in content:
                score += 0.5          # content match = low signal (many words)

        if score > 0:
            scored.append((score, entry))

    # Sort by score descending, return top_k
    scored.sort(key=lambda x: x[0], reverse=True)
    return [e for _, e in scored[:top_k]]


def format_context_for_prompt(entries: list[dict]) -> str:
    """Format retrieved KB entries into a compact string for LLM prompts."""
    if not entries:
        return "No relevant business context found."
    parts = []
    for e in entries:
        parts.append(f"[{e.get('title', 'Context')}] {e.get('content', '')}")
    return "\n\n".join(parts)


def get_context_snippet(question: str, results_summary: str = "") -> tuple[str, list[str]]:
    """
    High-level helper: retrieve context and return a formatted string + source titles.

    Returns:
        (formatted_context_str, [list of source titles used])
    """
    entries = retrieve_context(question, results_summary)
    formatted = format_context_for_prompt(entries)
    titles = [e.get("title", "Unknown") for e in entries]
    return formatted, titles
