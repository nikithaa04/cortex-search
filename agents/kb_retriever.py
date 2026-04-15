"""
Agent 4: Knowledge Context Agent
==================================
Retrieves relevant business definitions, policies, and glossary entries
to enrich query answers with enterprise context.

Input  : original user question + optional results summary
Output : (context_string, list_of_source_titles)

Delegates to services/kb_service.py for actual retrieval logic.
In production, this would use Snowflake's VECTOR_COSINE_SIMILARITY or
a Cortex Search Service for semantic retrieval from a KB table.
"""

from services.kb_service import get_context_snippet


def retrieve_kb_context(
    question: str,
    results_summary: str = "",
) -> tuple[str, list[str]]:
    """
    Retrieve knowledge base context relevant to the user's question.

    Args:
        question        : The original user question.
        results_summary : Optional string summarizing query results
                          (used to improve retrieval relevance).

    Returns:
        (formatted_context, list_of_source_titles)
    """
    context, sources = get_context_snippet(question, results_summary)
    return context, sources
