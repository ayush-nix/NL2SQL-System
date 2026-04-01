"""
Query Logger — Logs every query for learning and debugging.

Stores: question, sql, success/fail, error, execution time, timestamp
Used for: offline analysis, pattern extraction, feedback loop training.
"""
import json
import logging
import os
from datetime import datetime
from pathlib import Path

logger = logging.getLogger("nl2sql.query_logger")

LOG_DIR = Path(__file__).parent.parent / "data" / "query_logs"


class QueryLogger:
    """Logs all queries for offline analysis and learning."""

    def __init__(self):
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        self.log_file = LOG_DIR / "queries.jsonl"
        self.success_count = 0
        self.fail_count = 0

    def log(self, question: str, sql: str, success: bool,
            error: str = "", execution_time_ms: int = 0,
            row_count: int = 0, model_used: str = "",
            confidence: float = 0.0):
        """Log a query result."""
        entry = {
            "timestamp": datetime.now().isoformat(),
            "question": question,
            "sql": sql,
            "success": success,
            "error": error,
            "execution_time_ms": execution_time_ms,
            "row_count": row_count,
            "model_used": model_used,
            "confidence": confidence,
        }

        if success:
            self.success_count += 1
        else:
            self.fail_count += 1

        try:
            with open(self.log_file, "a", encoding="utf-8") as f:
                f.write(json.dumps(entry) + "\n")
        except Exception as e:
            logger.warning(f"Failed to log query: {e}")

    def get_stats(self) -> dict:
        """Get query success/fail stats."""
        total = self.success_count + self.fail_count
        rate = (self.success_count / total * 100) if total > 0 else 0
        return {
            "total_queries": total,
            "successful": self.success_count,
            "failed": self.fail_count,
            "success_rate": round(rate, 1),
        }

    def get_successful_pairs(self, limit: int = 50) -> list:
        """Extract successful Q→SQL pairs for few-shot learning."""
        pairs = []
        try:
            if not self.log_file.exists():
                return pairs
            with open(self.log_file, "r", encoding="utf-8") as f:
                for line in f:
                    entry = json.loads(line.strip())
                    if entry.get("success") and entry.get("sql"):
                        pairs.append({
                            "question": entry["question"],
                            "sql": entry["sql"],
                        })
            return pairs[-limit:]  # Most recent
        except Exception:
            return pairs


# Singleton
query_logger = QueryLogger()
