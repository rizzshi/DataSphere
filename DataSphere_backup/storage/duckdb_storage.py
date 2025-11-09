import duckdb
from typing import Any, List

class DuckDBStorage:
    def __init__(self, db_path: str = 'agent_data.duckdb'):
        self.db_path = db_path
        self.conn = duckdb.connect(database=db_path)

    def create_table(self, table_name: str, schema: str):
        self.conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({schema})")

    def insert(self, table_name: str, values: List[Any]):
        placeholders = ','.join(['?'] * len(values))
        self.conn.execute(f"INSERT INTO {table_name} VALUES ({placeholders})", values)

    def query(self, query: str):
        return self.conn.execute(query).fetchall()

    def close(self):
        self.conn.close()
