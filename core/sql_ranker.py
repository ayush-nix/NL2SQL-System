"""
SQL Ranker — Scores and ranks multiple SQL candidates.

Instead of trusting a single SQL generation, generate k candidates
and pick the BEST one based on deterministic scoring.

Scoring criteria:
1. Executes without error (+3)
2. Returns non-empty results (+2)
3. All columns exist in schema (+3)
4. Matches query intent patterns (+2)
5. Reasonable result count (+1)

LATENCY: ~5ms per candidate (deterministic code, no LLM)
"""
import logging
import re
from core.sql_validator import validate_sql, extract_clean_sql

logger = logging.getLogger("nl2sql.ranker")


class SQLRanker:
    """Ranks SQL candidates by execution success and quality signals."""

    def __init__(self):
        self.connection = None

    def set_connection(self, connection):
        self.connection = connection

    def rank(self, candidates: list[str], question: str,
             schema_metadata=None) -> dict:
        """
        Score each SQL candidate and return the best one.
        
        Args:
            candidates: List of SQL strings
            question: Original user question
            schema_metadata: Schema info for validation
            
        Returns:
            dict with best_sql, score, all_scores, confidence
        """
        if not candidates:
            return {"best_sql": "", "score": 0, "confidence": 0.0, "all_scores": []}

        scored = []
        for sql in candidates:
            score = self._score(sql, question, schema_metadata)
            scored.append({"sql": sql, "score": score["total"], "details": score})

        # Sort by score descending
        scored.sort(key=lambda x: x["score"], reverse=True)
        best = scored[0]

        confidence = min(best["score"] / 11.0, 1.0)  # Max possible = 11

        logger.info(
            f"Ranked {len(candidates)} candidates. "
            f"Best score: {best['score']}/11 "
            f"(confidence={confidence:.2f})"
        )

        return {
            "best_sql": best["sql"],
            "score": best["score"],
            "confidence": confidence,
            "all_scores": [s["score"] for s in scored],
        }

    def _score(self, sql: str, question: str, schema_metadata) -> dict:
        """Score a single SQL candidate."""
        scores = {
            "executes": 0,       # +3
            "has_results": 0,    # +2
            "valid_columns": 0,  # +3
            "intent_match": 0,   # +2
            "reasonable": 0,     # +1
            "total": 0,
        }

        # 1. Validate columns (no hallucination)
        if schema_metadata:
            validation = validate_sql(sql, schema_metadata)
            if validation.passed:
                scores["valid_columns"] = 3

        # 2. Try execution
        if self.connection and scores["valid_columns"] > 0:
            try:
                cursor = self.connection.execute(sql)
                rows = cursor.fetchall()

                scores["executes"] = 3

                # 3. Has results
                if len(rows) > 0:
                    scores["has_results"] = 2

                # 5. Reasonable result count
                if 0 < len(rows) <= 1000:
                    scores["reasonable"] = 1

            except Exception:
                pass

        # 4. Intent matching (pattern-based)
        q = question.lower()
        sql_upper = sql.upper()

        intent_score = 0
        if any(w in q for w in ["top", "highest", "most"]):
            if "ORDER BY" in sql_upper and "DESC" in sql_upper:
                intent_score += 1
            if "LIMIT" in sql_upper:
                intent_score += 1
        elif any(w in q for w in ["lowest", "least", "minimum"]):
            if "ORDER BY" in sql_upper and "ASC" in sql_upper:
                intent_score += 1
        if any(w in q for w in ["average", "avg", "mean"]):
            if "AVG" in sql_upper:
                intent_score += 1
        if any(w in q for w in ["count", "how many"]):
            if "COUNT" in sql_upper:
                intent_score += 1
        if any(w in q for w in ["each", "per", "group"]):
            if "GROUP BY" in sql_upper:
                intent_score += 1

        scores["intent_match"] = min(intent_score, 2)

        scores["total"] = sum(v for k, v in scores.items() if k != "total")
        return scores


# Singleton
sql_ranker = SQLRanker()
