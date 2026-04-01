"""
Dynamic Few-Shot Retriever — TF-IDF based golden pair matching.

Instead of hardcoding few-shots (which creates bias), this module:
1. Loads all golden Q→SQL pairs
2. For each user query, finds the 2 MOST SIMILAR golden pairs via TF-IDF
3. Injects ONLY those as few-shots in the prompt

This teaches the model correct column names WITHOUT bias from irrelevant examples.

ALSO: If similarity > 0.85, returns the golden SQL directly (template cache).

LATENCY: ~1ms (TF-IDF is just matrix multiplication)
NO EXTERNAL DEPENDENCIES (uses sklearn-like manual implementation)
"""
import json
import logging
import math
import re
from collections import Counter
from pathlib import Path

logger = logging.getLogger("nl2sql.fewshot")

GOLDEN_PAIRS_PATH = Path(__file__).parent.parent / "data" / "golden_pairs.json"


class FewShotRetriever:
    """TF-IDF based few-shot example retriever."""

    def __init__(self):
        self.pairs = []
        self.tfidf_matrix = []  # list of {term: tfidf_score} dicts
        self.idf = {}
        self._loaded = False

    def load(self):
        """Load golden pairs and build TF-IDF index."""
        try:
            if not GOLDEN_PAIRS_PATH.exists():
                logger.warning("No golden_pairs.json found")
                return

            with open(GOLDEN_PAIRS_PATH, "r", encoding="utf-8") as f:
                self.pairs = json.load(f)

            if not self.pairs:
                return

            # Build TF-IDF
            docs = [self._tokenize(p["question"]) for p in self.pairs]
            self.idf = self._compute_idf(docs)
            self.tfidf_matrix = [self._compute_tfidf(doc) for doc in docs]
            self._loaded = True

            logger.info(f"Loaded {len(self.pairs)} golden pairs for few-shot retrieval")

        except Exception as e:
            logger.warning(f"Failed to load golden pairs: {e}")

    def retrieve(self, question: str, k: int = 2) -> list:
        """
        Find the k most similar golden pairs to the question.
        Returns list of {"question": ..., "sql": ..., "similarity": ...}
        """
        if not self._loaded or not self.pairs:
            return []

        query_tokens = self._tokenize(question)
        query_tfidf = self._compute_tfidf(query_tokens)

        # Compute cosine similarity with each golden pair
        scored = []
        for i, doc_tfidf in enumerate(self.tfidf_matrix):
            sim = self._cosine_sim(query_tfidf, doc_tfidf)
            scored.append((i, sim))

        # Sort by similarity descending
        scored.sort(key=lambda x: x[1], reverse=True)

        results = []
        for idx, sim in scored[:k]:
            results.append({
                "question": self.pairs[idx]["question"],
                "sql": self.pairs[idx]["sql"],
                "similarity": round(sim, 3),
            })

        return results

    def get_template_match(self, question: str, threshold: float = 0.85) -> str:
        """
        If a golden pair matches with very high similarity, return its SQL directly.
        This is the template cache — instant response, 0 LLM calls.
        """
        matches = self.retrieve(question, k=1)
        if matches and matches[0]["similarity"] >= threshold:
            logger.info(
                f"Template cache HIT (sim={matches[0]['similarity']}): "
                f"'{matches[0]['question'][:50]}'"
            )
            return matches[0]["sql"]
        return ""

    def build_few_shot_text(self, question: str, k: int = 2) -> str:
        """Build few-shot examples text for the prompt."""
        matches = self.retrieve(question, k)
        if not matches:
            return ""

        lines = []
        for m in matches:
            # Only include if similarity is reasonable (> 0.2)
            if m["similarity"] > 0.2:
                lines.append(f"Question: {m['question']}")
                lines.append(f"SQL: {m['sql']}")
                lines.append("")

        return "\n".join(lines) if lines else ""

    def _tokenize(self, text: str) -> list:
        """Simple tokenizer: lowercase, split on non-alphanumeric."""
        return re.findall(r'[a-z0-9_]+', text.lower())

    def _compute_idf(self, docs: list) -> dict:
        """Compute inverse document frequency."""
        n = len(docs)
        df = Counter()
        for doc in docs:
            for term in set(doc):
                df[term] += 1
        return {term: math.log(n / (1 + count)) for term, count in df.items()}

    def _compute_tfidf(self, tokens: list) -> dict:
        """Compute TF-IDF vector for a document."""
        tf = Counter(tokens)
        total = len(tokens) if tokens else 1
        tfidf = {}
        for term, count in tf.items():
            tf_score = count / total
            idf_score = self.idf.get(term, math.log(len(self.pairs) + 1))
            tfidf[term] = tf_score * idf_score
        return tfidf

    def _cosine_sim(self, a: dict, b: dict) -> float:
        """Cosine similarity between two TF-IDF vectors."""
        # Dot product
        dot = sum(a.get(k, 0) * b.get(k, 0) for k in set(a) | set(b))
        # Magnitudes
        mag_a = math.sqrt(sum(v*v for v in a.values())) if a else 0
        mag_b = math.sqrt(sum(v*v for v in b.values())) if b else 0
        if mag_a == 0 or mag_b == 0:
            return 0.0
        return dot / (mag_a * mag_b)


# Singleton
few_shot_retriever = FewShotRetriever()
