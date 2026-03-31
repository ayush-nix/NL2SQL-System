"""
Evaluator — LLM-as-Judge + Golden Pair Comparator (Layer 7)

Two evaluation mechanisms:
1. LLM-as-Judge: Scores every query on Faithfulness (1-5) + Helpfulness (1-5)
2. Golden Pair Comparator: Compares model SQL results vs manually-verified SQL results

Runs ASYNCHRONOUSLY — does NOT block the user response.
"""
import json
import logging
import time
from pathlib import Path
from config import Config
from models.llm_manager import llm_manager

logger = logging.getLogger("nl2sql.evaluator")


class LLMJudge:
    """
    Scores every NL2SQL output on two dimensions:
    - Faithfulness: Does the answer only contain info from the DB results?
    - Helpfulness: Is the answer clear and useful to the user?
    """

    JUDGE_PROMPT = """You are a strict evaluation judge for a Natural Language to SQL system.
Score the system's output on two dimensions.

USER QUESTION: "{question}"

GENERATED SQL: {sql}

SQL EXECUTION RESULTS ({row_count} rows):
{results_preview}

GENERATED ANSWER: "{answer}"

Score each dimension from 1 to 5:

FAITHFULNESS — Does the answer ONLY contain information from the SQL results above?
- 5: Every claim in the answer is directly supported by the results data
- 4: Mostly accurate, very minor liberties taken
- 3: Generally correct but some unsupported claims
- 2: Multiple claims not supported by the data
- 1: Contains hallucinated or fabricated information

HELPFULNESS — Is the answer clear, well-formatted, and useful?
- 5: Concise, well-organized, directly addresses the question
- 4: Good answer, minor formatting improvements possible
- 3: Answers the question but could be clearer or more organized
- 2: Partially addresses the question, confusing structure
- 1: Does not address the question or is incomprehensible

Output ONLY valid JSON:
{{"faithfulness": N, "helpfulness": N, "reasoning": "brief explanation"}}"""

    async def judge(self, question: str, sql: str, results: dict,
                    answer: str) -> dict:
        """Score a single query-answer pair."""
        rows = results.get("rows", [])
        columns = results.get("columns", [])
        row_count = results.get("row_count", 0)

        # Format preview of results
        preview_lines = []
        if columns:
            preview_lines.append(" | ".join(columns))
            preview_lines.append("-" * 40)
        for row in rows[:10]:
            vals = [str(row.get(c, "")) for c in columns]
            preview_lines.append(" | ".join(vals))
        if row_count > 10:
            preview_lines.append(f"... ({row_count - 10} more rows)")

        results_preview = "\n".join(preview_lines) if preview_lines else "(empty)"

        prompt = self.JUDGE_PROMPT.format(
            question=question,
            sql=sql,
            row_count=row_count,
            results_preview=results_preview,
            answer=answer,
        )

        try:
            raw = await llm_manager.generate(
                prompt=prompt,
                model=Config.FAST_MODEL,
                temperature=0.0,
                num_ctx=Config.FAST_NUM_CTX,
            )
            return self._parse_scores(raw)
        except Exception as e:
            logger.error(f"Judge error: {e}")
            return {"faithfulness": 0, "helpfulness": 0, "reasoning": f"Error: {e}"}

    def _parse_scores(self, raw: str) -> dict:
        """Parse judge JSON response."""
        raw = raw.strip()
        import re
        json_match = re.search(r'\{.*\}', raw, re.DOTALL)
        if json_match:
            try:
                result = json.loads(json_match.group(0))
                f = max(1, min(5, int(result.get("faithfulness", 3))))
                h = max(1, min(5, int(result.get("helpfulness", 3))))
                return {
                    "faithfulness": f,
                    "helpfulness": h,
                    "reasoning": result.get("reasoning", ""),
                }
            except (json.JSONDecodeError, ValueError):
                pass
        return {"faithfulness": 3, "helpfulness": 3, "reasoning": "Could not parse scores"}


class PartialRewardScorer:
    """
    VLDB 2025 Reasoning-SQL inspired partial reward signals.
    
    Instead of a single binary pass/fail, decomposes SQL quality into 4 signals:
    1. Syntax Reward (0/1): Is the SQL valid?
    2. Schema Reward (0-1): Jaccard similarity of used columns vs available
    3. Execution Reward (0/1): Did it execute without error?
    4. Structure Reward (0-1): N-gram similarity vs golden SQL
    
    Combined with LLM-Judge, this provides granular feedback for the correction loop.
    """

    def score(self, generated_sql: str, golden_sql: str = None,
              available_columns: list = None, execution_success: bool = None) -> dict:
        """Compute all partial rewards."""
        syntax = self._syntax_reward(generated_sql)
        schema = self._schema_reward(generated_sql, available_columns) if available_columns else 0.5
        execution = 1.0 if execution_success else 0.0 if execution_success is not None else 0.5
        structure = self._structure_reward(generated_sql, golden_sql) if golden_sql else 0.5

        # Weighted composite (syntax most important, then execution)
        composite = (
            syntax * 0.25 +
            schema * 0.20 +
            execution * 0.30 +
            structure * 0.25
        )

        return {
            "syntax_reward": syntax,
            "schema_reward": round(schema, 3),
            "execution_reward": execution,
            "structure_reward": round(structure, 3),
            "composite_score": round(composite, 3),
        }

    def _syntax_reward(self, sql: str) -> float:
        """Check if SQL parses without errors."""
        try:
            import sqlglot
            parsed = sqlglot.parse(sql, dialect="sqlite")
            return 1.0 if parsed and parsed[0] is not None else 0.0
        except Exception:
            return 0.0

    def _schema_reward(self, sql: str, available_columns: list) -> float:
        """Jaccard similarity of columns used in SQL vs available columns."""
        import re
        sql_lower = sql.lower()
        available_set = set(c.lower() for c in available_columns)
        # Extract tokens that match column names
        sql_tokens = set(re.findall(r'[a-z_][a-z0-9_]*', sql_lower))
        used_cols = sql_tokens & available_set
        if not available_set:
            return 0.5
        # Precision: what fraction of referenced columns are valid?
        return len(used_cols) / max(1, len(sql_tokens & available_set | (sql_tokens - available_set - {"select","from","where","and","or","order","by","group","having","limit","as","on","join","inner","left","right","outer","asc","desc","count","avg","sum","max","min","distinct","between","like","in","not","null","is","case","when","then","else","end","cast","substr","round"})))

    def _structure_reward(self, generated: str, golden: str) -> float:
        """N-gram (bigram) Jaccard similarity for structural comparison."""
        def bigrams(sql):
            tokens = sql.lower().split()
            return set(zip(tokens, tokens[1:])) if len(tokens) > 1 else set()
        
        gen_bi = bigrams(generated)
        gold_bi = bigrams(golden)
        if not gen_bi and not gold_bi:
            return 1.0
        if not gen_bi or not gold_bi:
            return 0.0
        intersection = len(gen_bi & gold_bi)
        union = len(gen_bi | gold_bi)
        return intersection / union if union > 0 else 0.0


class GoldenPairTester:
    """
    Compares model-generated SQL against manually-verified golden SQL pairs.
    
    Each golden pair has:
    - question: The NL question
    - sql: The correct SQL query  
    - expected_answer_contains: Key terms that must appear in the answer
    """

    def __init__(self, golden_pairs_path: str = None):
        self.pairs: list[dict] = []
        if golden_pairs_path:
            self.load(golden_pairs_path)

    def load(self, path: str):
        """Load golden pairs from JSON file."""
        try:
            with open(path, "r") as f:
                self.pairs = json.load(f)
            logger.info(f"Loaded {len(self.pairs)} golden pairs")
        except FileNotFoundError:
            logger.warning(f"Golden pairs file not found: {path}")
            self.pairs = []
        except json.JSONDecodeError as e:
            logger.error(f"Golden pairs JSON error: {e}")
            self.pairs = []

    def compare(self, model_sql: str, model_results: dict,
                golden_sql: str, db_executor) -> dict:
        """
        Compare model output against golden pair.
        Runs both SQLs and compares results.
        """
        # Execute golden SQL
        golden_results = db_executor.execute(golden_sql)

        if not golden_results.get("success"):
            return {
                "exact_match": False,
                "semantic_match": False,
                "error": f"Golden SQL failed: {golden_results.get('error')}",
            }

        if not model_results.get("success"):
            return {
                "exact_match": False,
                "semantic_match": False,
                "error": f"Model SQL failed: {model_results.get('error')}",
            }

        golden_rows = golden_results.get("rows", [])
        model_rows = model_results.get("rows", [])

        # Exact match: same row count and same data
        exact_match = self._exact_compare(golden_rows, model_rows)

        # Semantic match: same row count and key values overlap
        semantic_match = self._semantic_compare(golden_rows, model_rows)

        return {
            "exact_match": exact_match,
            "semantic_match": semantic_match,
            "golden_row_count": len(golden_rows),
            "model_row_count": len(model_rows),
            "golden_columns": golden_results.get("columns", []),
            "model_columns": model_results.get("columns", []),
        }

    def _exact_compare(self, golden_rows: list, model_rows: list) -> bool:
        """Check if both result sets are identical."""
        if len(golden_rows) != len(model_rows):
            return False
        # Sort both for order-independent comparison
        try:
            golden_sorted = sorted([json.dumps(r, sort_keys=True) for r in golden_rows])
            model_sorted = sorted([json.dumps(r, sort_keys=True) for r in model_rows])
            return golden_sorted == model_sorted
        except Exception:
            return False

    def _semantic_compare(self, golden_rows: list, model_rows: list) -> bool:
        """
        Looser comparison: same row count and key values overlap.
        Useful when column names differ but data is the same.
        """
        if not golden_rows or not model_rows:
            return len(golden_rows) == len(model_rows)

        # Same row count is a good signal
        if len(golden_rows) != len(model_rows):
            return False

        # Extract all values from both sets
        golden_values = set()
        for row in golden_rows:
            for v in row.values():
                if v is not None:
                    golden_values.add(str(v).lower())

        model_values = set()
        for row in model_rows:
            for v in row.values():
                if v is not None:
                    model_values.add(str(v).lower())

        # Check value overlap (>80% = semantic match)
        if not golden_values:
            return True
        overlap = len(golden_values & model_values) / len(golden_values)
        return overlap >= 0.8

    async def run_all(self, query_fn, db_executor) -> dict:
        """
        Run all golden pairs as a regression test.
        query_fn: async function(question) → {sql, results, answer}
        """
        if not self.pairs:
            return {"total": 0, "message": "No golden pairs loaded"}

        results = {
            "total": len(self.pairs),
            "exact_matches": 0,
            "semantic_matches": 0,
            "failures": 0,
            "details": [],
        }

        for pair in self.pairs:
            try:
                model_output = await query_fn(pair["question"])
                comparison = self.compare(
                    model_sql=model_output.get("sql", ""),
                    model_results=model_output.get("results", {}),
                    golden_sql=pair["sql"],
                    db_executor=db_executor,
                )

                if comparison["exact_match"]:
                    results["exact_matches"] += 1
                elif comparison["semantic_match"]:
                    results["semantic_matches"] += 1
                else:
                    results["failures"] += 1

                results["details"].append({
                    "id": pair.get("id"),
                    "question": pair["question"],
                    "golden_sql": pair["sql"],
                    "model_sql": model_output.get("sql", ""),
                    **comparison,
                })

            except Exception as e:
                results["failures"] += 1
                results["details"].append({
                    "id": pair.get("id"),
                    "question": pair["question"],
                    "error": str(e),
                })

        results["accuracy"] = round(
            (results["exact_matches"] + results["semantic_matches"]) / results["total"] * 100, 1
        ) if results["total"] > 0 else 0

        return results
