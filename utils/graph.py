"""
Table relationship graph for FK-based JOIN path discovery.
Uses BFS to find shortest JOIN path between any two tables.
"""
from collections import defaultdict, deque


class TableGraph:
    """
    Adjacency-list graph of table relationships derived from foreign keys.
    Enables automatic JOIN path discovery via BFS.
    """

    def __init__(self):
        # graph[table_a] = [(table_b, "table_a.col = table_b.col"), ...]
        self.adjacency: dict[str, list[tuple[str, str]]] = defaultdict(list)

    def add_relationship(self, from_table: str, from_col: str,
                         to_table: str, to_col: str):
        """Add a bidirectional FK relationship."""
        join_cond = f"{from_table}.{from_col} = {to_table}.{to_col}"
        reverse_cond = f"{to_table}.{to_col} = {from_table}.{from_col}"

        # Avoid duplicates
        if (to_table, join_cond) not in self.adjacency[from_table]:
            self.adjacency[from_table].append((to_table, join_cond))
        if (from_table, reverse_cond) not in self.adjacency[to_table]:
            self.adjacency[to_table].append((from_table, reverse_cond))

    def get_neighbors(self, table: str) -> list[tuple[str, str]]:
        """Get all directly connected tables and their JOIN conditions."""
        return self.adjacency.get(table, [])

    def find_join_path(self, start: str, end: str) -> list[tuple[str, str]]:
        """
        BFS to find the shortest path of JOINs between two tables.
        Returns list of (table, join_condition) tuples forming the path.
        Returns empty list if no path exists.
        """
        if start == end:
            return []

        visited = {start}
        queue = deque([(start, [])])

        while queue:
            current, path = queue.popleft()

            for neighbor, join_cond in self.adjacency.get(current, []):
                if neighbor == end:
                    return path + [(neighbor, join_cond)]

                if neighbor not in visited:
                    visited.add(neighbor)
                    queue.append((neighbor, path + [(neighbor, join_cond)]))

        return []  # No path found

    def get_augmented_tables(self, tables: list[str],
                             hops: int = 1) -> set[str]:
        """
        FK Augmentation: expand a set of tables by N hops along FK edges.
        This catches intermediate/bridge tables that RAG might miss.
        """
        augmented = set(tables)

        for _ in range(hops):
            new_tables = set()
            for table in augmented:
                for neighbor, _ in self.adjacency.get(table, []):
                    new_tables.add(neighbor)
            augmented.update(new_tables)

        return augmented

    def get_join_hints(self, tables: list[str]) -> list[str]:
        """
        Generate JOIN path hints for a set of tables.
        Returns human-readable JOIN conditions.
        """
        hints = []
        seen_pairs = set()

        for table in tables:
            for neighbor, join_cond in self.adjacency.get(table, []):
                if neighbor in tables:
                    pair = tuple(sorted([table, neighbor]))
                    if pair not in seen_pairs:
                        seen_pairs.add(pair)
                        hints.append(f"JOIN {neighbor} ON {join_cond}")

        return hints
