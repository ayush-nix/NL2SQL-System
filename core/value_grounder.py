"""
Value Grounder — Queries actual database for column values BEFORE SQL generation.

This is the #1 accuracy booster: instead of letting the model GUESS values,
we TELL it exactly what values exist in each column.

Example: "high risk" → model might guess WHERE prediction = 'HIGH'
After grounding: model sees [0, 1] → generates WHERE prediction = 1

LATENCY: ~1ms (SQLite queries are instant)
"""
import logging
from config import Config

logger = logging.getLogger("nl2sql.grounder")


class ValueGrounder:
    """Probes the database for actual values in relevant columns."""

    def __init__(self):
        self.connection = None
        self._cache = {}  # column_name → distinct values

    def set_connection(self, connection):
        """Set database connection and clear cache."""
        self.connection = connection
        self._cache.clear()

    def ground_values(self, column_names: list, table_name: str = "avalanche_data") -> dict:
        """Fetch distinct values (categorical) or min/max (numeric) for columns."""
        if not self.connection:
            return {}

        grounding = {}
        try:
            cursor = self.connection.cursor()
            for col in column_names:
                if col in self._cache:
                    grounding[col] = self._cache[col]
                    continue
                try:
                    cursor.execute(f"SELECT COUNT(DISTINCT [{col}]) FROM [{table_name}]")
                    distinct_count = cursor.fetchone()[0]

                    if distinct_count <= 20:
                        cursor.execute(
                            f"SELECT DISTINCT [{col}] FROM [{table_name}] "
                            f"WHERE [{col}] IS NOT NULL ORDER BY [{col}] LIMIT 20"
                        )
                        values = [str(row[0]) for row in cursor.fetchall()]
                        info = f"Exact values: [{', '.join(values)}]"
                    else:
                        cursor.execute(
                            f"SELECT MIN([{col}]), MAX([{col}]), "
                            f"ROUND(AVG([{col}]), 2) FROM [{table_name}]"
                        )
                        mn, mx, avg = cursor.fetchone()
                        info = f"Range: [{mn} to {mx}], avg={avg}"

                    self._cache[col] = info
                    grounding[col] = info
                except Exception as col_err:
                    logger.debug(f"Could not ground {col}: {col_err}")
        except Exception as e:
            logger.warning(f"Value grounding failed: {e}")

        return grounding

    def build_grounding_text(self, grounding: dict) -> str:
        """Format grounding info for the prompt."""
        if not grounding:
            return ""
        lines = ["### Actual Database Values"]
        for col, info in grounding.items():
            lines.append(f"-- {col}: {info}")
        return "\n".join(lines)


# Singleton
value_grounder = ValueGrounder()
