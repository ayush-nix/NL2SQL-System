"""
Automated Feedback Loop — Continuous Learning System

High-scoring queries (Judge ≥ 4/5) are automatically added to few-shot examples.
Low-scoring queries are flagged for human review.
The system gets better with every query.
"""
import json
import logging
import time
from pathlib import Path
from config import Config

logger = logging.getLogger("nl2sql.feedback")

FEEDBACK_DIR = Path(__file__).parent.parent / "data"
FEW_SHOT_FILE = FEEDBACK_DIR / "learned_examples.json"
FLAGGED_FILE = FEEDBACK_DIR / "flagged_for_review.json"


class FeedbackLoop:
    """
    Manages the automated feedback cycle:
    1. After each query, LLM-as-Judge scores Faithfulness + Helpfulness
    2. Score ≥ 4 on BOTH → auto-add to few-shot examples
    3. Score < 4 on EITHER → flag for human review
    4. Few-shot examples grow → RAG retriever gets smarter → accuracy improves
    """

    def __init__(self):
        FEEDBACK_DIR.mkdir(parents=True, exist_ok=True)
        self.learned_examples: list[dict] = self._load_json(FEW_SHOT_FILE)
        self.flagged: list[dict] = self._load_json(FLAGGED_FILE)
        self.stats = {
            "total_processed": 0,
            "auto_approved": 0,
            "flagged_for_review": 0,
        }

    def process_feedback(self, question: str, sql: str, answer: str,
                         results: dict, judge_scores: dict) -> dict:
        """
        Process a completed query through the feedback loop.
        Returns action taken.
        """
        self.stats["total_processed"] += 1

        faithfulness = judge_scores.get("faithfulness", 0)
        helpfulness = judge_scores.get("helpfulness", 0)
        reasoning = judge_scores.get("reasoning", "")

        entry = {
            "question": question,
            "sql": sql,
            "answer": answer[:500],
            "row_count": results.get("row_count", 0),
            "faithfulness": faithfulness,
            "helpfulness": helpfulness,
            "reasoning": reasoning,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        }

        if faithfulness >= 4 and helpfulness >= 4:
            # Auto-approve: add to few-shot examples
            self.learned_examples.append({
                "question": question,
                "sql": sql,
            })
            self._save_json(FEW_SHOT_FILE, self.learned_examples)
            self.stats["auto_approved"] += 1

            logger.info(
                f"✅ Auto-approved: F={faithfulness} H={helpfulness} — "
                f"Added to few-shot ({len(self.learned_examples)} total)"
            )
            return {"action": "approved", "added_to_few_shot": True, **entry}

        else:
            # Flag for review
            self.flagged.append(entry)
            self._save_json(FLAGGED_FILE, self.flagged)
            self.stats["flagged_for_review"] += 1

            logger.info(
                f"⚠️ Flagged: F={faithfulness} H={helpfulness} — "
                f"Needs review ({len(self.flagged)} total)"
            )
            return {"action": "flagged", "added_to_few_shot": False, **entry}

    def get_learned_examples(self) -> list[dict]:
        """Get all auto-learned few-shot examples."""
        return self.learned_examples

    def approve_flagged(self, index: int, corrected_sql: str = None) -> dict:
        """
        Manually approve a flagged query (with optional corrected SQL).
        Moves it from flagged → few-shot examples.
        """
        if index < 0 or index >= len(self.flagged):
            return {"error": "Invalid index"}

        item = self.flagged.pop(index)
        approved = {
            "question": item["question"],
            "sql": corrected_sql or item["sql"],
        }
        self.learned_examples.append(approved)

        self._save_json(FEW_SHOT_FILE, self.learned_examples)
        self._save_json(FLAGGED_FILE, self.flagged)

        return {"action": "manually_approved", "example": approved}

    def get_stats(self) -> dict:
        """Get feedback loop statistics."""
        return {
            **self.stats,
            "learned_examples_count": len(self.learned_examples),
            "flagged_count": len(self.flagged),
        }

    def _load_json(self, path: Path) -> list:
        """Load JSON list from file."""
        try:
            if path.exists():
                with open(path, "r") as f:
                    return json.load(f)
        except (json.JSONDecodeError, Exception) as e:
            logger.warning(f"Could not load {path}: {e}")
        return []

    def _save_json(self, path: Path, data: list):
        """Save JSON list to file."""
        try:
            with open(path, "w") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logger.error(f"Could not save {path}: {e}")
