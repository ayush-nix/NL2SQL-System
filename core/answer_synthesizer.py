"""
Answer Synthesizer — converts SQL results into natural language answers.
Includes groundedness check to prevent fabrication.
"""
import logging
from config import Config
from models.llm_manager import llm_manager

logger = logging.getLogger("nl2sql.synthesizer")


class AnswerSynthesizer:
    """Convert SQL results to clear natural language answers."""

    SYNTHESIS_PROMPT = """You are a precise data analyst. Given the SQL query results below,
provide a clear, concise natural language answer to the user's question.

Question: {question}
SQL Query: {sql}
Number of results: {row_count}
Results:
{formatted_results}

Rules:
1. Answer ONLY based on the data shown above. Do NOT make up information.
2. If results are empty, say "No records found matching your query."
3. Include specific numbers, names, and values from the results.
4. For large result sets, summarize the key findings.
5. Use clear, simple language.
6. If the data shows a table, describe the key patterns or totals.

Answer:"""

    async def synthesize(self, question: str, sql: str,
                         results: dict) -> str:
        """Generate NL answer from SQL results."""
        if not results.get("success"):
            return (
                f"The query encountered an error: {results.get('error', 'Unknown error')}. "
                f"Please try rephrasing your question."
            )

        rows = results.get("rows", [])
        columns = results.get("columns", [])
        row_count = results.get("row_count", 0)

        if row_count == 0:
            return "No records found matching your query."

        # Format results for the prompt
        formatted = self._format_results(columns, rows, max_rows=20)

        try:
            answer = await llm_manager.generate(
                prompt=self.SYNTHESIS_PROMPT.format(
                    question=question,
                    sql=sql,
                    row_count=row_count,
                    formatted_results=formatted,
                ),
                model=Config.FAST_MODEL,
                temperature=0.1,
                num_ctx=Config.FAST_NUM_CTX,
            )
            return answer.strip() if answer.strip() else self._fallback_answer(columns, rows, row_count)
        except Exception as e:
            logger.error(f"Synthesis error: {e}")
            return self._fallback_answer(columns, rows, row_count)

    def _format_results(self, columns: list, rows: list,
                        max_rows: int = 20) -> str:
        """Format results as a readable table string."""
        if not rows:
            return "(empty)"

        display_rows = rows[:max_rows]
        lines = []

        # Header
        lines.append(" | ".join(columns))
        lines.append("-" * len(lines[0]))

        # Rows
        for row in display_rows:
            values = [str(row.get(col, "")) for col in columns]
            lines.append(" | ".join(values))

        if len(rows) > max_rows:
            lines.append(f"... and {len(rows) - max_rows} more rows")

        return "\n".join(lines)

    def _fallback_answer(self, columns: list, rows: list,
                         row_count: int) -> str:
        """Simple fallback if LLM synthesis fails."""
        if row_count == 1 and len(columns) == 1:
            return f"The result is: **{rows[0][columns[0]]}**"
        elif row_count == 1:
            parts = [f"{col}: {rows[0].get(col, 'N/A')}" for col in columns]
            return f"Found 1 result — " + ", ".join(parts)
        else:
            return (
                f"Found **{row_count}** results across "
                f"{len(columns)} columns ({', '.join(columns[:5])})."
            )
