"""
SQL Generator — the core NL-to-SQL engine.
Combines: Schema-aware prompting, Chain-of-Thought, RAG few-shot,
self-correction loop, and CodeLlama fallback.

SECURITY: Generates ONLY SELECT statements. All output is validated
by sql_validator.py before execution.
"""
import logging
import time
from config import Config
from core.sql_validator import validate_sql, extract_clean_sql, ValidationResult
from models.llm_manager import llm_manager

logger = logging.getLogger("nl2sql.generator")


class SQLGenerator:
    """
    Agentic SQL generator with Chain-of-Thought reasoning,
    self-correction loop, and model fallback.
    """

    MASTER_PROMPT = """### Task
Generate a SQL query to answer the following question:
"{question}"

### Database Schema
{schema}

### Table Relationships (Foreign Keys)
{relationships}

### Column Value Examples
{sample_values}

{business_hints}

{plan_context}

### JOIN Path Hints
{join_hints}

### Rules — STRICTLY ENFORCED
1. Generate ONLY a SELECT statement. NEVER use DELETE, UPDATE, INSERT, DROP, ALTER, TRUNCATE.
2. Use table aliases for readability.
3. Always qualify column names with table alias to avoid ambiguity.
4. For multi-table queries, use explicit JOIN syntax (INNER JOIN, LEFT JOIN).
5. Use the EXACT column names and table names from the schema above.
6. Do NOT invent columns or tables not listed in the schema.
7. Return ONLY the SQL query — no explanations, no markdown fences.
8. Use SQLite-compatible syntax:
   - Use strftime('%Y', col) NOT EXTRACT(YEAR FROM col)
   - Use || for string concatenation NOT CONCAT()
   - Use LIMIT N NOT TOP N
   - Use substr() NOT SUBSTRING()
   - CAST(col AS INTEGER) for type conversion

### Think Step-by-Step
Before writing your SQL:
1. Which tables contain the data needed to answer this question?
2. How do these tables connect? (What are the JOIN conditions?)
3. What columns should appear in the SELECT clause?
4. What filtering conditions go in WHERE?
5. Is GROUP BY, ORDER BY, or LIMIT needed?

{few_shot_section}

### SQL Query:
"""

    CORRECTION_PROMPT = """The following SQL query has an error:

{failed_sql}

Error: {error}

Original question: "{question}"
Available tables and columns:
{schema}

Fix the SQL query. Think step by step about what went wrong.
Return ONLY the corrected SQL query, nothing else.
"""

    CLASSIFIER_PROMPT = """Classify this question into ONE category.

Question: "{question}"

Categories:
- SINGLE_TABLE: involves only one table
- MULTI_JOIN: requires joining 2+ tables
- AGGREGATION: needs COUNT/SUM/AVG/GROUP BY
- SUBQUERY: needs nested queries or CTEs
- TEMPORAL: date/time-based filtering

Reply with ONLY the category name:"""

    def __init__(self):
        self.few_shot_examples: list[dict] = []

    def add_few_shot(self, question: str, sql: str):
        """Add a Q→SQL example for in-context learning."""
        self.few_shot_examples.append({
            "question": question,
            "sql": sql
        })

    async def generate(self, question: str, schema_text: str,
                       relationships_text: str, sample_values: str,
                       join_hints: str, business_hints: list[str],
                       schema_metadata=None, plan_context: str = "") -> dict:
        """
        Full generation pipeline:
        1. Classify query type
        2. Build enriched prompt
        3. Generate SQL (SQLCoder2)
        4. Validate → self-correct up to N retries
        5. Fallback to CodeLlama if needed
        """
        start_time = time.time()

        # Build few-shot section
        few_shot_section = ""
        if self.few_shot_examples:
            examples = []
            for ex in self.few_shot_examples[-5:]:  # Last 5 examples
                examples.append(
                    f"Question: {ex['question']}\nSQL: {ex['sql']}"
                )
            few_shot_section = (
                "### Similar Examples\n" + "\n\n".join(examples)
            )

        # Build business hints
        hints_text = ""
        if business_hints:
            hints_text = "### Business Context\n" + "\n".join(
                f"- {h}" for h in business_hints
            )

        # Build master prompt
        prompt = self.MASTER_PROMPT.format(
            question=question,
            schema=schema_text,
            relationships=relationships_text,
            sample_values=sample_values,
            join_hints=join_hints or "No specific JOIN hints.",
            business_hints=hints_text,
            plan_context=plan_context,
            few_shot_section=few_shot_section,
        )

        # ── Generate SQL ─────────────────────────────────────
        sql = ""
        attempts = 0
        model_used = Config.SQL_MODEL
        validation: ValidationResult = ValidationResult(passed=False)

        for attempt in range(Config.MAX_RETRIES + 1):
            attempts = attempt + 1

            if attempt == 0:
                # Primary model
                raw = await llm_manager.generate(
                    prompt=prompt,
                    model=Config.SQL_MODEL,
                    temperature=0.0,
                    num_ctx=Config.SQL_NUM_CTX,
                )
            elif attempt < Config.MAX_RETRIES:
                # Self-correction with error feedback
                correction = self.CORRECTION_PROMPT.format(
                    failed_sql=sql,
                    error=validation.error,
                    question=question,
                    schema=schema_text,
                )
                raw = await llm_manager.generate(
                    prompt=correction,
                    model=Config.SQL_MODEL,
                    temperature=0.1,
                    num_ctx=Config.SQL_NUM_CTX,
                )
                model_used = Config.SQL_MODEL + " (retry)"
            else:
                # Fallback model
                raw = await llm_manager.generate(
                    prompt=prompt,
                    model=Config.FAST_MODEL,
                    temperature=0.1,
                    num_ctx=Config.FAST_NUM_CTX,
                )
                model_used = Config.FAST_MODEL + " (fallback)"

            sql = extract_clean_sql(raw)
            logger.info(f"Attempt {attempts}: {sql[:100]}...")

            # Validate
            validation = validate_sql(sql, schema_metadata)
            if validation.passed:
                break

            logger.warning(
                f"Attempt {attempts} failed pass {validation.pass_number}: "
                f"{validation.error}"
            )

        elapsed = time.time() - start_time

        return {
            "sql": sql,
            "valid": validation.passed,
            "validation_error": validation.error if not validation.passed else "",
            "attempts": attempts,
            "model_used": model_used,
            "generation_time_ms": int(elapsed * 1000),
            "confidence": validation.confidence if validation.passed else 0.3,
        }

    async def classify_query(self, question: str) -> str:
        """Classify query type using fast model."""
        try:
            result = await llm_manager.generate(
                prompt=self.CLASSIFIER_PROMPT.format(question=question),
                model=Config.FAST_MODEL,
                temperature=0.0,
                num_ctx=Config.FAST_NUM_CTX,
            )
            category = result.strip().upper().replace(" ", "_")
            valid = {"SINGLE_TABLE", "MULTI_JOIN", "AGGREGATION",
                     "SUBQUERY", "TEMPORAL"}
            return category if category in valid else "MULTI_JOIN"
        except Exception:
            return "MULTI_JOIN"  # Safe default
