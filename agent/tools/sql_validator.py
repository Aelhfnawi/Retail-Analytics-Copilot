import re
from typing import List, Set, Dict, Any
from agent.tools.sqlite_tool import SQLiteTool

class SQLValidator:
    def __init__(self, db_path: str = "data/northwind.sqlite"):
        self.db = SQLiteTool(db_path)
        self.valid_tables = set()
        self.valid_columns = {}  # table -> set of columns
        self._load_schema()
    
    def _load_schema(self):
        """Load all valid tables and columns from the database."""
        tables = self.db.get_all_tables()
        for table in tables:
            self.valid_tables.add(table.lower())
            # Get columns for this table
            schema_str = self.db.get_schema([table])
            columns = self._extract_columns_from_schema(schema_str)
            self.valid_columns[table.lower()] = set(c.lower() for c in columns)
    
    def _extract_columns_from_schema(self, schema_str: str) -> List[str]:
        """Extract column names from schema string."""
        columns = []
        for line in schema_str.split('\n'):
            if line.strip().startswith('- '):
                # Format: "  - ColumnName (TYPE)"
                match = re.match(r'\s*-\s*(\w+)\s*\(', line)
                if match:
                    columns.append(match.group(1))
        return columns
    
    def validate_sql(self, sql_query: str) -> Dict[str, Any]:
        """
        Validate that SQL query only uses tables/columns from the schema.
        Returns: {"valid": bool, "errors": List[str]}
        """
        if not sql_query or sql_query.strip() == "":
            return {"valid": True, "errors": []}
        
        errors = []
        
        # Extract CTE names (Common Table Expressions) from WITH clauses
        # Simple approach: find all "name AS (" patterns that appear before the main query
        cte_names = set()
        if re.search(r'\bWITH\b', sql_query, re.IGNORECASE):
            # Find all potential CTE names (word followed by AS ()
            # This will catch: WITH cte1 AS (...), cte2 AS (...)
            all_cte_matches = re.findall(r'\b(\w+)\s+AS\s*\(', sql_query, re.IGNORECASE)
            # Add all found names as potential CTEs
            cte_names = set(all_cte_matches)
        
        
        
        # Extract table names from SQL (improved regex to handle quotes and spaces)
        # Look for FROM/JOIN patterns with optional quotes
        table_pattern = r'\b(?:FROM|JOIN)\s+(?:[`"\[]([^`"\]]+)[`"\]]|(\w+))'
        matches = re.findall(table_pattern, sql_query, re.IGNORECASE)
        
        # Flatten the tuple results (regex returns groups)
        found_tables = [m[0] if m[0] else m[1] for m in matches]
        
        for table in found_tables:
            # Clean the table name
            clean_table = table.strip()
            
            # Skip if this is a CTE
            if clean_table.lower() in {c.lower() for c in cte_names}:
                continue
            
            # Try multiple matching strategies
            table_lower = clean_table.lower()
            
            # Convert CamelCase to space-separated (e.g., OrderDetails -> order details)
            spaced_table = re.sub(r'([a-z])([A-Z])', r'\1 \2', clean_table).lower()
            
            # Also try matching by removing all spaces (e.g., orderdetails -> order details)
            # Check if the table (without spaces) matches any valid table (without spaces)
            table_no_space = table_lower.replace(' ', '')
            found_match = False
            
            for valid_table in self.valid_tables:
                valid_no_space = valid_table.replace(' ', '')
                if table_lower == valid_table or spaced_table == valid_table or table_no_space == valid_no_space:
                    found_match = True
                    break
            
            if not found_match:
                errors.append(f"Table '{table}' does not exist in schema. Valid tables: {', '.join(sorted(self.valid_tables))}")
        
        # Extract column references (simplified - looks for word.word patterns)
        # This is a heuristic and may have false positives/negatives
        column_pattern = r'\b(\w+)\.(\w+)\b'
        found_columns = re.findall(column_pattern, sql_query)
        
        for table_alias, column in found_columns:
            # Try to match alias to actual table (this is imperfect)
            # For now, just check if column exists in ANY table
            column_lower = column.lower()
            found_in_any_table = False
            for table_cols in self.valid_columns.values():
                if column_lower in table_cols:
                    found_in_any_table = True
                    break
            
            if not found_in_any_table:
                # Find similar column names (fuzzy matching)
                suggestions = self._find_similar_columns(column_lower)
                if suggestions:
                    errors.append(f"Column '{column}' not found in any table schema. Did you mean: {', '.join(suggestions)}?")
                else:
                    errors.append(f"Column '{column}' not found in any table schema")
        
        return {
            "valid": len(errors) == 0,
            "errors": errors
        }
    
    def _find_similar_columns(self, column: str, max_suggestions: int = 3) -> List[str]:
        """Find similar column names using simple string matching."""
        all_columns = set()
        for cols in self.valid_columns.values():
            all_columns.update(cols)
        
        # Simple similarity: check if column is a substring or vice versa
        suggestions = []
        for valid_col in all_columns:
            if column in valid_col or valid_col in column:
                suggestions.append(valid_col)
        
        return suggestions[:max_suggestions]
        
        return {
            "valid": len(errors) == 0,
            "errors": errors
        }
    
    def get_schema_summary(self) -> str:
        """Return a summary of valid tables and columns."""
        summary = "Valid Tables:\n"
        for table in sorted(self.valid_tables):
            summary += f"  - {table}\n"
            if table in self.valid_columns:
                summary += f"    Columns: {', '.join(sorted(self.valid_columns[table]))}\n"
        return summary
