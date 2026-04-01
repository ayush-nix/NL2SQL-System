"""
SQL Generator v3 — Multi-candidate generation with ranking.

Architecture:
1. Generate k=2 SQL candidates at different temperatures
2. Validate all candidates (column check, syntax)
3. Return all valid candidates for ranking

Key design: SQL model only generates SQL. Everything else is code.
"""
import logging
import time
import asyncio
from config import Config
from core.sql_validator import validate_sql, extract_clean_sql, ValidationResult
from models.llm_manager import llm_manager

logger = logging.getLogger("nl2sql.generator")


class SQLGenerator:
    """Multi-candidate SQL generator with self-correction."""

    MASTER_PROMPT = """### Task
Generate a SQL query to answer the following question:
"{question}"

### Database Schema
{schema}

{value_grounding}

{query_hints}

{business_hints}

### Reference Examples
{dynamic_examples}

### Rules
1. Generate ONLY a SELECT statement.
2. Use EXACT column names from the CREATE TABLE above. Do NOT invent column names.
3. Use SQLite syntax: LIMIT (not TOP), no ILIKE (use LIKE).
4. Return ONLY the SQL query — no explanation, no markdown fences.

### SQL Query:
"""

    CORRECTION_PROMPT = """The SQL query below failed validation:

{failed_sql}

Error: {error}

Original question: "{question}"

{schema}

IMPORTANT: Use ONLY the exact column names from the CREATE TABLE above.
Fix the error and return ONLY the corrected SQL query:
"""

    def __init__(self):
        self.few_shot_examples: list[dict] = []

    def add_few_shot(self, question: str, sql: str):
        self.few_shot_examples.append({"question": question, "sql": sql})

    async def generate_candidates(self, question: str, schema_text: str,
                                   value_grounding: str = "",
                                   query_hints: str = "",
                                   business_hints: list[str] = None,
                                   schema_metadata=None,
                                   dynamic_examples: str = "",
                                   k: int = 2) -> dict:
        """
        Generate k SQL candidates and validate each.
        Returns all candidates with validation results.
        """
        start_time = time.time()

        hints_text = ""
        if business_hints:
            hints_text = "### Hints\n" + "\n".join(
                f"- {h}" for h in business_hints[:3]
            )

        prompt = self.MASTER_PROMPT.format(
            question=question,
            schema=schema_text,
            value_grounding=value_grounding,
            query_hints=query_hints,
            business_hints=hints_text,
            dynamic_examples=dynamic_examples or "(no similar examples found)",
        )

        candidates = []
        temperatures = [0.0, 0.3][:k]  # First precise, second creative

        for i, temp in enumerate(temperatures):
            try:
                raw = await llm_manager.generate(
                    prompt=prompt,
                    model=Config.SQL_MODEL,
                    temperature=temp,
                    num_ctx=Config.SQL_NUM_CTX,
                )
                sql = extract_clean_sql(raw)
                validation = validate_sql(sql, schema_metadata)

                candidates.append({
                    "sql": sql,
                    "valid": validation.passed,
                    "error": validation.error if not validation.passed else "",
                    "temperature": temp,
                    "attempt": i + 1,
                })

                logger.info(
                    f"Candidate {i+1} (t={temp}): "
                    f"{'VALID' if validation.passed else 'INVALID'} — "
                    f"{sql[:100]}..."
                )

                # If first candidate is valid, try correction for invalid ones
                if not validation.passed and i == 0:
                    # Self-correct
                    correction = self.CORRECTION_PROMPT.format(
                        failed_sql=sql,
                        error=validation.error,
                        question=question,
                        schema=schema_text,
                    )
                    raw_fix = await llm_manager.generate(
                        prompt=correction,
                        model=Config.SQL_MODEL,
                        temperature=0.1,
                        num_ctx=Config.SQL_NUM_CTX,
                    )
                    fixed_sql = extract_clean_sql(raw_fix)
                    fix_validation = validate_sql(fixed_sql, schema_metadata)
                    candidates.append({
                        "sql": fixed_sql,
                        "valid": fix_validation.passed,
                        "error": fix_validation.error if not fix_validation.passed else "",
                        "temperature": 0.1,
                        "attempt": i + 1,
                        "is_correction": True,
                    })
                    logger.info(
                        f"Correction: {'VALID' if fix_validation.passed else 'INVALID'} — "
                        f"{fixed_sql[:100]}..."
                    )

            except Exception as e:
                logger.error(f"Candidate {i+1} generation failed: {e}")

        elapsed = time.time() - start_time
        valid_count = sum(1 for c in candidates if c["valid"])

        return {
            "candidates": candidates,
            "valid_candidates": [c for c in candidates if c["valid"]],
            "total_generated": len(candidates),
            "valid_count": valid_count,
            "generation_time_ms": int(elapsed * 1000),
            "model_used": Config.SQL_MODEL,
        }

    async def generate(self, question: str, schema_text: str,
                       relationships_text: str = "", sample_values: str = "",
                       join_hints: str = "", business_hints: list[str] = None,
                       schema_metadata=None, plan_context: str = "",
                       value_grounding: str = "",
                       query_hints: str = "",
                       dynamic_examples: str = "") -> dict:
        """
        Backward-compatible single-candidate generation.
        Generates k=2 candidates internally, returns best.
        """
        result = await self.generate_candidates(
            question=question,
            schema_text=schema_text,
            value_grounding=value_grounding,
            query_hints=query_hints,
            business_hints=business_hints,
            schema_metadata=schema_metadata,
            dynamic_examples=dynamic_examples,
            k=2,
        )

        # Pick first valid candidate
        valid = result["valid_candidates"]
        if valid:
            best = valid[0]
            return {
                "sql": best["sql"],
                "valid": True,
                "validation_error": "",
                "attempts": result["total_generated"],
                "model_used": result["model_used"],
                "generation_time_ms": result["generation_time_ms"],
                "confidence": 0.8,
                "all_candidates": result["candidates"],
            }
        else:
            # All failed — return first candidate's error
            first = result["candidates"][0] if result["candidates"] else {"sql": "", "error": "No SQL generated"}
            return {
                "sql": first.get("sql", ""),
                "valid": False,
                "validation_error": first.get("error", "Unknown"),
                "attempts": result["total_generated"],
                "model_used": result["model_used"],
                "generation_time_ms": result["generation_time_ms"],
                "confidence": 0.2,
                "all_candidates": result.get("candidates", []),
            }
