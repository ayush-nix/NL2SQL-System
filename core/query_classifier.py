"""
Query Classifier — Rule-based query type detection (NO LLM, instant).

Detects: FILTER, AGGREGATION, RANKING, COMPARISON, TEMPORAL, SUBQUERY
Uses keyword patterns to guide SQL generation strategy.
"""
import re
import logging

logger = logging.getLogger("nl2sql.classifier")

# Pattern → query type + SQL hints
PATTERNS = {
    "RANKING": {
        "keywords": ["top", "highest", "lowest", "best", "worst", "most", "least", "rank"],
        "sql_hint": "Use ORDER BY ... DESC/ASC LIMIT N",
    },
    "AGGREGATION": {
        "keywords": ["average", "avg", "mean", "total", "sum", "count", "how many",
                     "minimum", "maximum", "min", "max"],
        "sql_hint": "Use aggregate functions: AVG(), SUM(), COUNT(), MIN(), MAX()",
    },
    "GROUPBY": {
        "keywords": ["each", "per", "by", "group", "for every", "breakdown", "distribution"],
        "sql_hint": "Use GROUP BY clause",
    },
    "COMPARISON": {
        "keywords": ["higher than average", "above average", "below average",
                     "more than", "less than", "compared to", "greater than average",
                     "higher than the overall"],
        "sql_hint": "Use subquery: WHERE col > (SELECT AVG(col) FROM table)",
    },
    "TEMPORAL": {
        "keywords": ["date", "when", "recent", "latest", "today", "this week",
                     "last month", "trend", "over time"],
        "sql_hint": "Use prediction_date column with ORDER BY or strftime()",
    },
    "FILTER": {
        "keywords": ["where", "find", "show", "get", "which", "list", "display"],
        "sql_hint": "Use WHERE clause with conditions",
    },
}

# Special aggregation keyword → SQL function mapping
AGG_RULES = {
    "top": "ORDER BY {col} DESC LIMIT {n}",
    "bottom": "ORDER BY {col} ASC LIMIT {n}",
    "highest": "ORDER BY {col} DESC LIMIT 1",
    "lowest": "ORDER BY {col} ASC LIMIT 1",
    "average": "AVG({col})",
    "total": "SUM({col})",
    "count": "COUNT(*)",
    "how many": "COUNT(*)",
}

# Correlated query keywords → SQL patterns
CORRELATED_RULES = {
    "higher than": "> (SELECT AVG({col}) FROM {table})",
    "above average": "> (SELECT AVG({col}) FROM {table})",
    "below average": "< (SELECT AVG({col}) FROM {table})",
    "lower than average": "< (SELECT AVG({col}) FROM {table})",
    "greater than average": "> (SELECT AVG({col}) FROM {table})",
    "more than average": "> (SELECT AVG({col}) FROM {table})",
    "higher than the overall": "> (SELECT AVG({col}) FROM {table})",
}


def classify_query(question: str) -> dict:
    """
    Classify the query type and return SQL generation hints.
    Returns: {"types": [...], "hints": [...], "agg_rules": [...], "correlated": [...]}
    """
    q_lower = question.lower()
    
    detected_types = []
    hints = []
    
    for qtype, info in PATTERNS.items():
        for kw in info["keywords"]:
            if kw in q_lower:
                if qtype not in detected_types:
                    detected_types.append(qtype)
                    hints.append(info["sql_hint"])
                break
    
    # Detect aggregation rules
    agg_rules = []
    for kw, pattern in AGG_RULES.items():
        if kw in q_lower:
            agg_rules.append({"keyword": kw, "pattern": pattern})
    
    # Detect correlated query patterns
    correlated = []
    for kw, pattern in CORRELATED_RULES.items():
        if kw in q_lower:
            correlated.append({"keyword": kw, "pattern": pattern})
    
    # Extract numbers (for "top 5", "top 10")
    numbers = re.findall(r'\b(\d+)\b', question)
    limit_n = int(numbers[0]) if numbers else None
    
    if not detected_types:
        detected_types = ["FILTER"]
        hints = ["Use WHERE clause"]
    
    result = {
        "types": detected_types,
        "hints": hints,
        "agg_rules": agg_rules,
        "correlated": correlated,
        "limit_n": limit_n,
        "primary_type": detected_types[0],
    }
    
    logger.info(f"Query classified: {detected_types}, limit={limit_n}")
    return result
