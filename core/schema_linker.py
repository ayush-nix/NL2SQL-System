"""
Schema Linker — Entity-to-Column Mapper (Layer 2)

Maps user-mentioned entities to exact database table/column names
BEFORE the query reaches the SQL generator. This prevents hallucinated
column names — the #1 source of SQL generation errors.

Uses the base generative model (Mistral/Llama) for semantic understanding.
"""
import json
import logging
import re
from config import Config
from models.llm_manager import llm_manager

logger = logging.getLogger("nl2sql.linker")


class SchemaLinker:
    """
    Layer 2: Resolves user entities to exact schema elements.
    
    Input:  preprocessed NL query + full schema metadata
    Output: linked query with exact column/table references + target tables
    """

    LINKING_PROMPT = """You are a database schema linker. Given the database schema below and a user's question, map every entity the user mentions to the exact table and column names in the schema.

DATABASE SCHEMA:
{schema}

FOREIGN KEY RELATIONSHIPS:
{relationships}

SAMPLE VALUES:
{samples}

USER QUESTION: "{question}"

Your task:
1. Identify which tables are needed to answer this question
2. Map each user-referenced entity to the exact column name from the schema
3. Rewrite the question replacing vague terms with exact column references

Output ONLY valid JSON (no markdown, no backticks):
{{
  "target_tables": ["table1", "table2"],
  "column_mappings": [
    {{"user_term": "the term user used", "maps_to": "table.column", "filter_value": "exact value if applicable or null"}}
  ],
  "resolved_question": "Rewrite of the user question using exact table.column names",
  "join_needed": true or false
}}"""

    async def link(self, question: str, schema_metadata) -> dict:
        """
        Link user entities to schema elements.
        Returns structured mapping + resolved question.
        """
        schema_text = self._build_schema_text(schema_metadata)
        relationships_text = self._build_relationships_text(schema_metadata)
        samples_text = self._build_samples_text(schema_metadata)

        prompt = self.LINKING_PROMPT.format(
            schema=schema_text,
            relationships=relationships_text,
            samples=samples_text,
            question=question,
        )

        try:
            raw = await llm_manager.generate(
                prompt=prompt,
                model=Config.FAST_MODEL,
                temperature=0.0,
                num_ctx=Config.FAST_NUM_CTX,
            )

            result = self._parse_response(raw, schema_metadata)
            logger.info(f"Schema linked: {len(result['target_tables'])} tables, "
                       f"{len(result['column_mappings'])} mappings")
            return result

        except Exception as e:
            logger.warning(f"Schema linking error: {e}. Using fallback.")
            return self._fallback_linking(question, schema_metadata)

    def _parse_response(self, raw: str, schema_metadata) -> dict:
        """Parse LLM JSON response, with fallback for malformed output."""
        raw = raw.strip()

        # Remove markdown fences if present
        if "```" in raw:
            match = re.search(r"```(?:json)?\s*\n?(.*?)```", raw, re.DOTALL)
            if match:
                raw = match.group(1).strip()

        # Try to find JSON object
        json_match = re.search(r'\{.*\}', raw, re.DOTALL)
        if json_match:
            raw = json_match.group(0)

        try:
            result = json.loads(raw)
        except json.JSONDecodeError:
            return self._fallback_linking("", schema_metadata)

        # Validate target_tables exist
        valid_tables = set(t.lower() for t in schema_metadata.tables)
        target_tables = [
            t for t in result.get("target_tables", [])
            if t.lower() in valid_tables
        ]

        if not target_tables:
            target_tables = list(schema_metadata.tables)[:5]

        return {
            "target_tables": target_tables,
            "column_mappings": result.get("column_mappings", []),
            "resolved_question": result.get("resolved_question", ""),
            "join_needed": result.get("join_needed", len(target_tables) > 1),
        }

    def _fallback_linking(self, question: str, schema_metadata) -> dict:
        """Fallback: use all tables if linking fails."""
        return {
            "target_tables": list(schema_metadata.tables),
            "column_mappings": [],
            "resolved_question": question,
            "join_needed": len(schema_metadata.tables) > 1,
        }

    def _build_schema_text(self, metadata) -> str:
        """Build concise schema DDL for the prompt."""
        lines = []
        for table in metadata.tables:
            cols = metadata.columns.get(table, [])
            col_defs = []
            for c in cols:
                parts = [f"{c.name} {c.dtype}"]
                if c.is_pk:
                    parts.append("PRIMARY KEY")
                if c.fk_ref:
                    parts.append(f"REFERENCES {c.fk_ref}")
                col_defs.append(" ".join(parts))
            lines.append(f"CREATE TABLE {table} (\n  " + ",\n  ".join(col_defs) + "\n);")
            lines.append(f"-- {metadata.row_counts.get(table, '?')} rows")
            lines.append("")
        return "\n".join(lines)

    def _build_relationships_text(self, metadata) -> str:
        """Build FK relationship descriptions."""
        if not metadata.relationships:
            return "No foreign key relationships detected."
        lines = []
        for r in metadata.relationships:
            lines.append(f"{r.from_table}.{r.from_column} → {r.to_table}.{r.to_column}")
        return "\n".join(lines)

    def _build_samples_text(self, metadata) -> str:
        """Build sample values text (truncated)."""
        lines = []
        for key, vals in list(metadata.samples.items())[:25]:
            lines.append(f"{key}: {vals[:3]}")
        return "\n".join(lines) if lines else "No samples available."
