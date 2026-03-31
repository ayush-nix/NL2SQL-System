"""
Query Preprocessor — cleans, normalizes, and expands NL queries.
Handles military abbreviations, basic spell correction, date normalization.
All offline, no internet required.
"""
import re
from utils.domain_dictionary import ABBREVIATIONS, BUSINESS_TERM_HINTS


class QueryPreprocessor:
    """Clean and normalize user NL queries before LLM processing."""

    def __init__(self):
        self.abbreviations = ABBREVIATIONS
        self.business_hints = BUSINESS_TERM_HINTS

    def preprocess(self, query: str) -> str:
        """Full preprocessing pipeline."""
        query = query.strip()
        if not query:
            return query

        query = self._expand_abbreviations(query)
        query = self._normalize_whitespace(query)
        return query

    def _expand_abbreviations(self, query: str) -> str:
        """Expand military abbreviations to full forms."""
        words = query.split()
        expanded = []
        for word in words:
            # Preserve punctuation
            prefix = ""
            suffix = ""
            clean = word
            while clean and not clean[0].isalnum():
                prefix += clean[0]
                clean = clean[1:]
            while clean and not clean[-1].isalnum():
                suffix = clean[-1] + suffix
                clean = clean[:-1]

            lower = clean.lower()
            if lower in self.abbreviations:
                expanded.append(prefix + self.abbreviations[lower] + suffix)
            else:
                expanded.append(word)

        return " ".join(expanded)

    def _normalize_whitespace(self, query: str) -> str:
        """Collapse multiple spaces into one."""
        return re.sub(r"\s+", " ", query).strip()

    def get_business_hints(self, query: str) -> list[str]:
        """Extract business term hints relevant to the query."""
        hints = []
        query_lower = query.lower()
        for term, hint in self.business_hints.items():
            if term in query_lower:
                hints.append(f"'{term}': {hint}")
        return hints
