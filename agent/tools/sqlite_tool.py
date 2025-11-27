import sqlite3
import os
from typing import List, Dict, Any, Optional

class SQLiteTool:
    def __init__(self, db_path: str = "data/northwind.sqlite"):
        self.db_path = db_path

    def _get_connection(self):
        return sqlite3.connect(self.db_path)

    def execute_query(self, query: str) -> Dict[str, Any]:
        """
        Executes a SQL query and returns the results or error.
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(query)
            columns = [description[0] for description in cursor.description] if cursor.description else []
            rows = cursor.fetchall()
            conn.close()
            return {
                "columns": columns,
                "rows": rows,
                "error": None
            }
        except Exception as e:
            return {
                "columns": [],
                "rows": [],
                "error": str(e)
            }

    def get_schema(self, table_names: Optional[List[str]] = None) -> str:
        """
        Returns the schema for the specified tables or all tables if None.
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        if table_names:
            tables_query = f"SELECT name FROM sqlite_master WHERE type='table' AND name IN ({','.join(['?']*len(table_names))})"
            cursor.execute(tables_query, table_names)
        else:
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            
        tables = [row[0] for row in cursor.fetchall()]
        schema_str = ""
        
        for table in tables:
            cursor.execute(f"PRAGMA table_info('{table}')")
            columns = cursor.fetchall()
            schema_str += f"Table: {table}\n"
            for col in columns:
                # cid, name, type, notnull, dflt_value, pk
                schema_str += f"  - {col[1]} ({col[2]})\n"
            schema_str += "\n"
            
        conn.close()
        return schema_str

    def get_all_tables(self) -> List[str]:
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        conn.close()
        return tables
