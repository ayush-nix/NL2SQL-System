"""
Query Planner — Agent 1: The Brain

Breaks down vague natural language questions into a structured logical
execution plan BEFORE schema linking and SQL generation.

Example:
  Input:  "Who were our best soldiers last quarter?"
  Output: {
    "steps": [
      "Filter soldiers by date_of_joining in the last 3 months",
      "Aggregate performance metrics",
      "Sort by performance descending",
      "Limit to top results"
    ],
    "intent": "RANKING",
    "needs_aggregation": true,
    "needs_temporal_filter": true,
    "ambiguous_terms": ["best"] → resolved to "highest rank or most postings"
  }

This structured plan gives the SQL Generator precise instructions
instead of a vague natural language blob.
"""
import json
import re
import logging
from config import Config
from models.llm_manager import llm_manager

logger = logging.getLogger("nl2sql.planner")


class QueryPlanner:
    """
    Agent 1: Decomposes vague NL queries into step-by-step logical plans.
    
    WHY this is needed:
    - Users ask vague questions ("show me the best soldiers")
    - Without a plan, the SQL model guesses what "best" means
    - The Planner resolves ambiguity BEFORE SQL generation
    - Result: More accurate SQL on the first attempt
    """

    PLANNER_PROMPT = """You are a database query planner. Given a user's natural language question and database schema, break it down into a clear logical execution plan.

DATABASE TABLES:
{tables}

USER QUESTION: "{question}"

Analyze the question and output ONLY valid JSON (no markdown, no backticks):
{{
  "rewritten_question": "Rewrite the question to be precise and unambiguous using database terminology",
  "steps": [
    "Step 1: description of what to do first",
    "Step 2: description of what to do next"
  ],
  "intent": "one of: LOOKUP, AGGREGATION, RANKING, COMPARISON, TEMPORAL, LISTING",
  "needs_join": true/false,
  "needs_aggregation": true/false,
  "needs_temporal_filter": true/false,
  "needs_sorting": true/false,
  "ambiguous_terms": {{"vague_term": "resolved_meaning"}},
  "suggested_limit": null or number
}}"""

    async def plan(self, question: str, table_summaries: list[str]) -> dict:
        """
        Generate a structured execution plan from a vague NL query.
        Returns plan dict or a safe fallback if planning fails.
        """
        tables_text = "\n".join(table_summaries) if table_summaries else "No schema loaded."

        prompt = self.PLANNER_PROMPT.format(
            tables=tables_text,
            question=question,
        )

        try:
            raw = await llm_manager.generate(
                prompt=prompt,
                model=Config.FAST_MODEL,
                temperature=0.0,
                num_ctx=Config.FAST_NUM_CTX,
            )
            return self._parse_plan(raw, question)

        except Exception as e:
            logger.warning(f"Planner error (non-critical): {e}")
            return self._fallback_plan(question)

    def _parse_plan(self, raw: str, question: str) -> dict:
        """Parse the LLM's JSON plan, with fallback."""
        raw = raw.strip()

        # Remove markdown fences
        if "```" in raw:
            match = re.search(r"```(?:json)?\s*\n?(.*?)```", raw, re.DOTALL)
            if match:
                raw = match.group(1).strip()

        # Find JSON object
        json_match = re.search(r'\{.*\}', raw, re.DOTALL)
        if json_match:
            raw = json_match.group(0)

        try:
            plan = json.loads(raw)
            # Ensure required fields
            plan.setdefault("rewritten_question", question)
            plan.setdefault("steps", [])
            plan.setdefault("intent", "LISTING")
            plan.setdefault("needs_join", False)
            plan.setdefault("needs_aggregation", False)
            plan.setdefault("needs_temporal_filter", False)
            plan.setdefault("needs_sorting", False)
            plan.setdefault("ambiguous_terms", {})
            plan.setdefault("suggested_limit", None)

            logger.info(
                f"Plan: intent={plan['intent']}, "
                f"steps={len(plan['steps'])}, "
                f"join={plan['needs_join']}, agg={plan['needs_aggregation']}"
            )
            return plan

        except json.JSONDecodeError:
            return self._fallback_plan(question)

    def _fallback_plan(self, question: str) -> dict:
        """Safe fallback when planning fails."""
        return {
            "rewritten_question": question,
            "steps": ["Execute the query as stated"],
            "intent": "LISTING",
            "needs_join": False,
            "needs_aggregation": False,
            "needs_temporal_filter": False,
            "needs_sorting": False,
            "ambiguous_terms": {},
            "suggested_limit": None,
        }

    def build_plan_context(self, plan: dict) -> str:
        """
        Convert the plan into a text block that gets injected
        into the SQL generator's prompt.
        """
        lines = []
        if plan.get("steps"):
            lines.append("### Execution Plan")
            for i, step in enumerate(plan["steps"], 1):
                lines.append(f"{i}. {step}")

        if plan.get("ambiguous_terms"):
            lines.append("\n### Resolved Ambiguities")
            for term, meaning in plan["ambiguous_terms"].items():
                lines.append(f'- "{term}" means: {meaning}')

        hints = []
        if plan.get("needs_aggregation"):
            hints.append("Use GROUP BY with aggregate functions")
        if plan.get("needs_temporal_filter"):
            hints.append("Use date filtering with strftime() for SQLite")
        if plan.get("needs_sorting"):
            hints.append("Use ORDER BY for sorting")
        if plan.get("suggested_limit"):
            hints.append(f"Use LIMIT {plan['suggested_limit']}")
        if plan.get("needs_join"):
            hints.append("Use explicit JOIN syntax with ON conditions")

        if hints:
            lines.append("\n### Query Hints")
            for h in hints:
                lines.append(f"- {h}")

        return "\n".join(lines)
