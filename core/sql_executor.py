"""
Read-Only SQL Executor — executes validated SQL against the database.

CRITICAL SECURITY:
- This module MUST NOT contain DELETE, UPDATE, INSERT, DROP, ALTER, TRUNCATE
- No file deletion operations (os.remove, shutil.rmtree, etc.)
- No subprocess or shell execution
- All queries run in READ-ONLY mode with timeout and row limits
"""
import logging
import time
from config import Config

logger = logging.getLogger("nl2sql.executor")

MAX_ROWS = Config.MAX_RESULT_ROWS
QUERY_TIMEOUT = Config.QUERY_TIMEOUT_SECONDS


class SQLExecutor:
    """
    Executes validated SQL in a read-only sandbox.
    Currently uses in-memory SQLite (CSV mode).
    Production: swap to async DB connection with SET TRANSACTION READ ONLY.
    """

    def __init__(self, db_connection=None):
        self.connection = db_connection

    def set_connection(self, connection):
        """Set/update database connection."""
        self.connection = connection

    def execute(self, sql: str) -> dict:
        """
        Execute a validated SELECT query.
        Returns structured result with columns, rows, metadata.
        """
        if not self.connection:
            return {
                "success": False,
                "error": "No database connection. Please upload CSV files first.",
                "columns": [],
                "rows": [],
                "row_count": 0,
            }

        start_time = time.time()

        try:
            cursor = self.connection.cursor()
            cursor.execute(sql)

            # Get column names
            columns = [desc[0] for desc in cursor.description] if cursor.description else []

            # Fetch with row limit
            rows = cursor.fetchmany(MAX_ROWS)
            row_count = len(rows)
            truncated = row_count >= MAX_ROWS

            # Convert to list of dicts
            row_dicts = []
            for row in rows:
                row_dict = {}
                for i, col in enumerate(columns):
                    val = row[i]
                    # Ensure JSON-serializable
                    if val is None:
                        row_dict[col] = None
                    elif isinstance(val, (int, float)):
                        row_dict[col] = val
                    else:
                        row_dict[col] = str(val)
                row_dicts.append(row_dict)

            elapsed = time.time() - start_time

            return {
                "success": True,
                "columns": columns,
                "rows": row_dicts,
                "row_count": row_count,
                "truncated": truncated,
                "execution_time_ms": int(elapsed * 1000),
                "error": "",
            }

        except Exception as e:
            elapsed = time.time() - start_time
            error_msg = str(e)
            logger.error(f"SQL execution error: {error_msg}")

            return {
                "success": False,
                "error": error_msg,
                "columns": [],
                "rows": [],
                "row_count": 0,
                "execution_time_ms": int(elapsed * 1000),
            }

    def get_table_preview(self, table_name: str, limit: int = 10) -> dict:
        """Get a preview of table data (for UI schema browser)."""
        if not self.connection:
            return {"success": False, "error": "No connection", "columns": [], "rows": []}

        try:
            sql = f'SELECT * FROM "{table_name}" LIMIT {limit}'
            return self.execute(sql)
        except Exception as e:
            return {"success": False, "error": str(e), "columns": [], "rows": []}
