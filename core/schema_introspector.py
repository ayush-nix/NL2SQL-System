"""
Schema Introspector — discovers database schema from CSV uploads or live DB.
Builds rich metadata store with tables, columns, types, PKs, FKs, samples.

For testing: loads CSV files into in-memory SQLite.
For production: connects to the real DB via API endpoint.
"""
import csv
import io
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path

from utils.graph import TableGraph


@dataclass
class ColumnInfo:
    name: str
    dtype: str
    nullable: bool = True
    is_pk: bool = False
    fk_ref: str = ""  # "other_table.column" if FK
    sample_values: list = field(default_factory=list)
    description: str = ""


@dataclass
class Relationship:
    from_table: str
    from_column: str
    to_table: str
    to_column: str
    rel_type: str = "many-to-one"


@dataclass
class SchemaMetadata:
    tables: list[str] = field(default_factory=list)
    columns: dict[str, list[ColumnInfo]] = field(default_factory=dict)
    relationships: list[Relationship] = field(default_factory=list)
    samples: dict[str, list] = field(default_factory=dict)
    table_graph: TableGraph = field(default_factory=TableGraph)
    row_counts: dict[str, int] = field(default_factory=dict)
    db_connection: object = None  # sqlite3 connection for CSV mode


class SchemaIntrospector:
    """
    Introspects database schema. Two modes:
    1. CSV mode (testing): loads CSVs into in-memory SQLite
    2. DB mode (production): connects via connection string/API
    """

    def __init__(self):
        self.metadata = SchemaMetadata()

    def load_from_csvs(self, csv_files: dict[str, bytes]) -> SchemaMetadata:
        """
        Load multiple CSV files into in-memory SQLite.
        csv_files: {filename: file_bytes}
        Each CSV becomes a table named after the file (without .csv).
        """
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row

        tables = []
        columns = {}
        row_counts = {}

        for filename, file_bytes in csv_files.items():
            table_name = Path(filename).stem.lower().replace(" ", "_").replace("-", "_")
            tables.append(table_name)

            # Parse CSV
            text = file_bytes.decode("utf-8-sig", errors="replace")
            reader = csv.reader(io.StringIO(text))
            headers = next(reader)

            # Clean header names
            clean_headers = []
            for h in headers:
                clean = h.strip().lower().replace(" ", "_").replace("-", "_")
                clean = "".join(c for c in clean if c.isalnum() or c == "_")
                if not clean:
                    clean = f"col_{len(clean_headers)}"
                clean_headers.append(clean)

            # Collect all rows
            rows = list(reader)
            row_counts[table_name] = len(rows)

            if not rows:
                continue

            # Infer column types from data
            col_types = self._infer_types(clean_headers, rows)

            # Create table
            col_defs = ", ".join(
                f'"{h}" {col_types[h]}' for h in clean_headers
            )
            conn.execute(f'CREATE TABLE "{table_name}" ({col_defs})')

            # Insert data
            placeholders = ", ".join(["?"] * len(clean_headers))
            for row in rows:
                # Pad or trim row to match headers
                padded = row[:len(clean_headers)]
                while len(padded) < len(clean_headers):
                    padded.append("")
                conn.execute(
                    f'INSERT INTO "{table_name}" VALUES ({placeholders})',
                    padded
                )

            conn.commit()

            # Build column info with samples
            col_infos = []
            for h in clean_headers:
                # Get sample values (up to 5 distinct)
                try:
                    cursor = conn.execute(
                        f'SELECT DISTINCT "{h}" FROM "{table_name}" '
                        f'WHERE "{h}" IS NOT NULL AND "{h}" != "" LIMIT 5'
                    )
                    sample_vals = [str(r[0]) for r in cursor.fetchall()]
                except Exception:
                    sample_vals = []

                col_infos.append(ColumnInfo(
                    name=h,
                    dtype=col_types[h],
                    sample_values=sample_vals,
                ))

            columns[table_name] = col_infos

        # Auto-detect relationships based on column name patterns + value overlap
        relationships = self._detect_relationships(tables, columns, conn)

        # Build graph
        graph = TableGraph()
        for rel in relationships:
            graph.add_relationship(
                rel.from_table, rel.from_column,
                rel.to_table, rel.to_column
            )

        # Build samples dict
        samples = {}
        for table, cols in columns.items():
            for col in cols:
                if col.sample_values:
                    samples[f"{table}.{col.name}"] = col.sample_values

        self.metadata = SchemaMetadata(
            tables=tables,
            columns=columns,
            relationships=relationships,
            samples=samples,
            table_graph=graph,
            row_counts=row_counts,
            db_connection=conn,
        )

        return self.metadata

    def _infer_types(self, headers: list[str], rows: list[list[str]]) -> dict:
        """Infer SQL types from CSV data."""
        col_types = {}
        for i, h in enumerate(headers):
            values = [row[i] for row in rows[:100] if i < len(row) and row[i].strip()]
            if not values:
                col_types[h] = "TEXT"
                continue

            # Check if all numeric
            all_int = True
            all_float = True
            for v in values:
                v_clean = v.strip().replace(",", "")
                try:
                    int(v_clean)
                except (ValueError, OverflowError):
                    all_int = False
                try:
                    float(v_clean)
                except (ValueError, OverflowError):
                    all_float = False

            if all_int:
                col_types[h] = "INTEGER"
            elif all_float:
                col_types[h] = "REAL"
            else:
                col_types[h] = "TEXT"

        return col_types

    def _detect_relationships(self, tables: list[str],
                               columns: dict[str, list[ColumnInfo]],
                               conn=None) -> list[Relationship]:
        """
        Auto-detect FK relationships using multiple strategies:
        
        1. Naming convention: <other_table>_id → <other_table>.id
        2. Shared column names: if two tables share an identically-named
           column that looks like an ID/key, check value overlap
        3. Common patterns: code, number, no suffixes
        
        When conn is provided, uses value-overlap analysis to validate
        that the FK candidate actually has matching values in the
        referenced table (prevents false positives).
        """
        relationships = []
        table_set = set(tables)
        seen = set()  # (from_table, from_col, to_table, to_col)

        # ── Strategy 1: <table>_id naming convention ──────────
        for table in tables:
            for col in columns[table]:
                name = col.name.lower()

                if name.endswith("_id") and name != "id":
                    ref_table = name[:-3]  # Remove "_id"

                    # Direct match
                    if ref_table in table_set:
                        target_cols = [c.name for c in columns[ref_table]]
                        if "id" in target_cols:
                            key = (table, col.name, ref_table, "id")
                            if key not in seen:
                                relationships.append(Relationship(
                                    from_table=table,
                                    from_column=col.name,
                                    to_table=ref_table,
                                    to_column="id",
                                ))
                                col.fk_ref = f"{ref_table}.id"
                                seen.add(key)
                            continue

                    # Plural match (soldier_id → soldiers)
                    for suffix in ["s", "es"]:
                        plural = ref_table + suffix
                        if plural in table_set:
                            target_cols = [c.name for c in columns[plural]]
                            if "id" in target_cols:
                                key = (table, col.name, plural, "id")
                                if key not in seen:
                                    relationships.append(Relationship(
                                        from_table=table,
                                        from_column=col.name,
                                        to_table=plural,
                                        to_column="id",
                                    ))
                                    col.fk_ref = f"{plural}.id"
                                    seen.add(key)
                                break

                # Mark 'id' columns as PKs
                if name == "id":
                    col.is_pk = True

        # ── Strategy 2: Shared column names across tables ─────
        # If two tables share a column name that looks like an identifier
        # (contains 'id', 'code', 'no', 'number', 'key'), check value overlap
        id_indicators = {"id", "code", "no", "num", "number", "key", "ref"}

        for i, table_a in enumerate(tables):
            cols_a = {c.name: c for c in columns[table_a]}
            for table_b in tables[i+1:]:
                cols_b = {c.name: c for c in columns[table_b]}

                shared_cols = set(cols_a.keys()) & set(cols_b.keys())
                for shared in shared_cols:
                    # Skip generic 'id' columns — they're PKs, not FKs
                    if shared.lower() == "id":
                        continue

                    # Skip if already detected via Strategy 1
                    if (table_a, shared, table_b, shared) in seen:
                        continue
                    if (table_b, shared, table_a, shared) in seen:
                        continue
                    # Skip if these tables already have a relationship
                    tables_linked = any(
                        (t1 == table_a and t3 == table_b) or
                        (t1 == table_b and t3 == table_a)
                        for t1, _, t3, _ in seen
                    )
                    if tables_linked:
                        continue

                    # Check if it looks like a key column
                    name_lower = shared.lower()
                    is_key_like = any(ind in name_lower for ind in id_indicators)

                    if not is_key_like:
                        continue

                    # Validate with value overlap if connection available
                    if conn:
                        overlap = self._check_value_overlap(
                            conn, table_a, shared, table_b, shared
                        )
                        if overlap < 0.3:  # Less than 30% overlap → not a FK
                            continue

                    # Determine direction: table with fewer distinct values
                    # is likely the "parent" (referenced) table
                    key = (table_a, shared, table_b, shared)
                    relationships.append(Relationship(
                        from_table=table_a,
                        from_column=shared,
                        to_table=table_b,
                        to_column=shared,
                    ))
                    seen.add(key)

                    # Mark FK references
                    cols_a[shared].fk_ref = f"{table_b}.{shared}"

        # ── Strategy 3: Detect PKs beyond just 'id' ──────────
        for table in tables:
            for col in columns[table]:
                name = col.name.lower()
                if name == f"{table}_id" or name == f"{table}id":
                    col.is_pk = True
                # Common PK patterns: serial_no, emp_code, etc.
                if any(name.endswith(s) for s in ["_no", "_code", "_number"]):
                    if not col.fk_ref:  # Only if not already a FK
                        col.is_pk = True

        return relationships

    def _check_value_overlap(self, conn, table_a: str, col_a: str,
                              table_b: str, col_b: str) -> float:
        """
        Check what percentage of values in col_a exist in col_b.
        Returns overlap ratio (0.0 to 1.0).
        """
        try:
            cursor = conn.execute(f'''
                SELECT COUNT(DISTINCT a."{col_a}") as overlap_count
                FROM "{table_a}" a
                INNER JOIN "{table_b}" b ON a."{col_a}" = b."{col_b}"
            ''')
            overlap = cursor.fetchone()[0]

            cursor = conn.execute(
                f'SELECT COUNT(DISTINCT "{col_a}") FROM "{table_a}"'
            )
            total_a = cursor.fetchone()[0]

            if total_a == 0:
                return 0.0
            return overlap / total_a
        except Exception:
            return 0.0

    def get_schema_text(self, table_names: list[str] = None) -> str:
        """Generate a human-readable schema description for prompts."""
        tables = table_names or self.metadata.tables
        lines = []

        for table in tables:
            if table not in self.metadata.columns:
                continue
            cols = self.metadata.columns[table]
            col_strs = []
            for c in cols:
                parts = [f"{c.name} {c.dtype}"]
                if c.is_pk:
                    parts.append("PRIMARY KEY")
                if c.fk_ref:
                    parts.append(f"REFERENCES {c.fk_ref}")
                col_strs.append(", ".join(parts))

            lines.append(f"Table: {table}")
            lines.append(f"  Columns: {'; '.join(col_strs)}")

            # Add samples
            for c in cols:
                if c.sample_values:
                    lines.append(
                        f"  Sample {c.name}: {c.sample_values[:3]}"
                    )

            row_count = self.metadata.row_counts.get(table, "?")
            lines.append(f"  Row count: {row_count}")
            lines.append("")

        return "\n".join(lines)

    def get_relationships_text(self, table_names: list[str] = None) -> str:
        """Generate human-readable relationship descriptions."""
        rels = self.metadata.relationships
        if table_names:
            table_set = set(table_names)
            rels = [r for r in rels
                    if r.from_table in table_set or r.to_table in table_set]

        if not rels:
            return "No foreign key relationships detected."

        lines = []
        for r in rels:
            lines.append(
                f"{r.from_table}.{r.from_column} → {r.to_table}.{r.to_column} "
                f"({r.rel_type})"
            )
        return "\n".join(lines)
