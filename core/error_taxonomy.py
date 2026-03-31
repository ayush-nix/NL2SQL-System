"""
Taxonomy-Guided Error Correction — SQL-of-Thought Style

Instead of generic "fix this SQL" retries, this module classifies
WHY a SQL query failed into a structured taxonomy, then provides
targeted diagnostic instructions to the Coder Model for precise fixes.

Reference: SQL-of-Thought (NeurIPS 2024) — uses 9 main error categories
and 31 subcategories for guided rectification.

SECURITY: This module only analyzes errors. No destructive operations.
"""
import re
import logging

logger = logging.getLogger("nl2sql.error_taxonomy")


# ── Error Taxonomy ───────────────────────────────────────────
# Each category has: pattern matchers, diagnostic message, fix hints
ERROR_TAXONOMY = {
    "MISSING_TABLE": {
        "patterns": [
            r"no such table[:\s]+(\w+)",
            r"table[:\s]+['\"]?(\w+)['\"]?\s+does not exist",
            r"Table '(\w+)' does not exist",
        ],
        "category": "Schema Error",
        "subcategory": "Missing Table Reference",
        "diagnostic": (
            "The SQL references a table that does not exist in the database. "
            "This is likely a hallucinated table name."
        ),
        "fix_hints": [
            "Check the available tables listed in the schema",
            "Use only tables that exist in the CREATE TABLE statements",
            "The table name may be misspelled — check for similar names",
        ],
    },
    "MISSING_COLUMN": {
        "patterns": [
            r"no such column[:\s]+(\w+\.?\w*)",
            r"column[:\s]+['\"]?(\w+)['\"]?\s+does not exist",
            r"has no column named (\w+)",
        ],
        "category": "Schema Error",
        "subcategory": "Missing Column Reference",
        "diagnostic": (
            "The SQL references a column that does not exist. "
            "This is the #1 cause of NL2SQL failures."
        ),
        "fix_hints": [
            "Check the exact column names from the schema",
            "Column names are case-sensitive in some databases",
            "The user's term may map to a differently-named column",
            "Use table.column qualification to avoid ambiguity",
        ],
    },
    "AMBIGUOUS_COLUMN": {
        "patterns": [
            r"ambiguous column name[:\s]+(\w+)",
            r"column reference ['\"]?(\w+)['\"]? is ambiguous",
        ],
        "category": "Join Error",
        "subcategory": "Ambiguous Column in JOIN",
        "diagnostic": (
            "A column name appears in multiple tables and the query "
            "doesn't specify which table it belongs to."
        ),
        "fix_hints": [
            "Use table_alias.column_name to qualify every column",
            "Add table aliases (e.g., s for soldiers, p for postings)",
            "Every column in SELECT, WHERE, JOIN ON must be qualified",
        ],
    },
    "JOIN_ERROR": {
        "patterns": [
            r"cannot join",
            r"ON clause.*mismatch",
            r"join.*condition",
        ],
        "category": "Join Error",
        "subcategory": "Invalid JOIN Condition",
        "diagnostic": (
            "The JOIN condition is invalid — columns may not match, "
            "or a bridge table is missing from the query."
        ),
        "fix_hints": [
            "Check the foreign key relationships in the schema",
            "Use the exact FK relationships provided",
            "You may need an intermediate bridge table for indirect joins",
            "Ensure JOIN ON conditions use matching column types",
        ],
    },
    "SYNTAX_ERROR": {
        "patterns": [
            r"syntax error",
            r"near ['\"](\w+)['\"]",
            r"unexpected token",
            r"incomplete input",
        ],
        "category": "Syntax Error",
        "subcategory": "SQL Syntax Violation",
        "diagnostic": (
            "The SQL has a syntax error. Common causes: missing commas, "
            "unclosed parentheses, wrong keyword order."
        ),
        "fix_hints": [
            "Check for missing commas between SELECT columns",
            "Ensure all parentheses are properly closed",
            "Verify keyword order: SELECT → FROM → JOIN → WHERE → GROUP BY → ORDER BY → LIMIT",
            "Use SQLite syntax: strftime() not EXTRACT(), || not CONCAT()",
        ],
    },
    "AGGREGATION_ERROR": {
        "patterns": [
            r"not an aggregate",
            r"must appear in the GROUP BY",
            r"misuse of aggregate",
        ],
        "category": "Logic Error",
        "subcategory": "Aggregation Misuse",
        "diagnostic": (
            "A non-aggregated column is used without GROUP BY, "
            "or an aggregate function is used incorrectly."
        ),
        "fix_hints": [
            "Every non-aggregated column in SELECT must appear in GROUP BY",
            "Use COUNT(*), SUM(), AVG(), MAX(), MIN() for aggregations",
            "If using GROUP BY, don't select raw columns not in the group",
        ],
    },
    "TYPE_MISMATCH": {
        "patterns": [
            r"datatype mismatch",
            r"type mismatch",
            r"cannot compare",
        ],
        "category": "Data Error",
        "subcategory": "Type Mismatch in Comparison",
        "diagnostic": (
            "A comparison is being made between incompatible types "
            "(e.g., comparing a TEXT column with an INTEGER)."
        ),
        "fix_hints": [
            "Use CAST(column AS INTEGER) or CAST(column AS TEXT) for conversion",
            "Check if date columns are stored as TEXT in SQLite",
            "String comparisons need single quotes: WHERE status = 'Active'",
        ],
    },
    "EMPTY_RESULT": {
        "patterns": [
            r"no results",
            r"empty result",
            r"0 rows",
        ],
        "category": "Logic Error",
        "subcategory": "Over-Restrictive Filters",
        "diagnostic": (
            "The query executed successfully but returned zero rows. "
            "The WHERE conditions may be too restrictive or filter values incorrect."
        ),
        "fix_hints": [
            "Check if filter values match the actual data (case-sensitive!)",
            "Review sample values for the filtered columns",
            "Try using LIKE with wildcards instead of exact match",
            "Consider using LOWER() for case-insensitive matching",
        ],
    },
    "TIMEOUT": {
        "patterns": [
            r"timeout",
            r"took too long",
            r"execution.*exceeded",
        ],
        "category": "Performance Error",
        "subcategory": "Query Timeout",
        "diagnostic": (
            "The query took too long. Likely caused by a Cartesian product "
            "(missing JOIN condition) or scanning too many rows."
        ),
        "fix_hints": [
            "Ensure every JOIN has an ON condition — missing ON creates Cartesian products",
            "Add LIMIT to prevent scanning entire tables",
            "Use WHERE clauses to filter early",
        ],
    },
}

# Fallback for unrecognized errors
UNKNOWN_ERROR = {
    "category": "Unknown Error",
    "subcategory": "Unclassified",
    "diagnostic": "The error does not match any known pattern.",
    "fix_hints": [
        "Re-read the schema carefully",
        "Use only SELECT statements",
        "Ensure all table and column names exist in the schema",
    ],
}


def classify_error(error_message: str) -> dict:
    """
    Classify a SQL error into the taxonomy.
    Returns the error category, diagnostic, and targeted fix hints.
    """
    error_lower = error_message.lower()

    for error_type, config in ERROR_TAXONOMY.items():
        for pattern in config["patterns"]:
            if re.search(pattern, error_lower, re.IGNORECASE):
                logger.info(
                    f"Error classified: {error_type} "
                    f"({config['category']} → {config['subcategory']})"
                )
                return {
                    "error_type": error_type,
                    "category": config["category"],
                    "subcategory": config["subcategory"],
                    "diagnostic": config["diagnostic"],
                    "fix_hints": config["fix_hints"],
                    "original_error": error_message,
                }

    logger.info(f"Error unclassified: {error_message[:100]}")
    return {
        "error_type": "UNKNOWN",
        **UNKNOWN_ERROR,
        "original_error": error_message,
    }


def build_correction_prompt(
    failed_sql: str,
    error_info: dict,
    question: str,
    schema_text: str,
) -> str:
    """
    Build a taxonomy-guided correction prompt.
    Instead of "fix this SQL", we tell the model EXACTLY what went wrong
    and HOW to fix it, based on the error classification.
    """
    hints = "\n".join(f"  {i+1}. {h}" for i, h in enumerate(error_info["fix_hints"]))

    return f"""### SQL Error Correction Task

**Error Classification:** {error_info['category']} → {error_info['subcategory']}

**Diagnostic:** {error_info['diagnostic']}

**Original Error:** {error_info['original_error']}

**Failed SQL:**
{failed_sql}

**Original Question:** "{question}"

**Available Schema:**
{schema_text}

### Targeted Fix Instructions:
{hints}

### Rules:
1. Fix ONLY the identified issue — do not rewrite the entire query unnecessarily.
2. Use ONLY tables and columns from the schema above.
3. Return ONLY the corrected SQL query, nothing else.
4. Use SQLite-compatible syntax.

### Corrected SQL:
"""
