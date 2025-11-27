import dspy
from typing import Literal

# --- Signatures ---

class Router(dspy.Signature):
    """
    Classify the user question to decide the best strategy:
    - 'sql': for questions requiring aggregation, counting, or specific data from the database (e.g., revenue, quantities, top customers).
    - 'rag': for questions about policies, marketing calendars, or static definitions (e.g., return policy, dates).
    - 'hybrid': for questions needing both (e.g., revenue during a specific named campaign).
    """
    question = dspy.InputField(desc="The user's question about retail analytics.")
    strategy = dspy.OutputField(desc="The best strategy: 'sql', 'rag', or 'hybrid'.")

class GenerateSQL(dspy.Signature):
    """
    You are generating SQL for a SQLite database. Follow these STRICT REQUIREMENTS:

    1. NEVER invent, guess, or hallucinate database columns, tables, or relationships.
    2. ONLY use columns and tables that exist in the provided schema. If a needed field does not exist, adjust the logic instead of creating new fields.
    3. SQL MUST be syntactically correct. No trailing commas, no missing aliases, no incomplete AS statements.
    4. When using JOINs, always reference the correct primary/foreign keys from the schema.
    5. If the question requires data NOT in the schema, return an empty SQL query: "".
    
    SQLite Syntax Rules:
    - Use `LIMIT N` instead of `TOP N`.
    - Use `strftime('%Y', date_col)` instead of `YEAR(date_col)`.
    - Use `strftime('%m', date_col)` instead of `MONTH(date_col)`.
    - Use `||` for string concatenation, not `CONCAT()`.
    - Always use table aliases for ALL columns to avoid 'ambiguous column name' errors (e.g. p.ProductName, not ProductName).

    Recommended tables/views: 'Orders', 'OrderDetails', 'Products', 'Customers', 'Categories'.

    Examples:
    Question: "Total revenue in 1997"
    SQL: SELECT SUM(od.UnitPrice * od.Quantity * (1 - od.Discount)) FROM OrderDetails od JOIN Orders o ON od.OrderID = o.OrderID WHERE strftime('%Y', o.OrderDate) = '1997'

    Question: "Top 3 products by revenue"
    SQL: SELECT p.ProductName, SUM(od.UnitPrice * od.Quantity) as revenue FROM Products p JOIN OrderDetails od ON p.ProductID = od.ProductID GROUP BY p.ProductID ORDER BY revenue DESC LIMIT 3
    
    IMPORTANT COLUMN NAMES:
    - Customers table uses 'CompanyName', NOT 'CustomerName'
    - Use proper aliases for all columns (e.g., c.CompanyName, p.ProductName)
    - Always define aliases before using them (e.g., FROM Orders o, not FROM Orders then use o.OrderID)
    
    CTEs (WITH clauses) are allowed but keep them simple. Prefer JOINs over complex CTEs when possible.
    """
    question = dspy.InputField(desc="The user's question.")
    database_schema = dspy.InputField(desc="The database schema.")
    sql_query = dspy.OutputField(desc="The SQLite query. Start with SELECT.")

class SynthesizeAnswer(dspy.Signature):
    """
    Synthesize a final answer based on the question, SQL results, and retrieved context.
    Ensure the answer matches the requested format hint.
    IMPORTANT: Double check your JSON keys. You must output 'explanation', not 'explanqation'.
    """
    question = dspy.InputField()
    context = dspy.InputField(desc="Retrieved documents or definitions.")
    sql_query = dspy.InputField(desc="The executed SQL query.")
    sql_result = dspy.InputField(desc="The result of the SQL query.")
    format_hint = dspy.InputField(desc="The expected format of the answer (e.g., int, float, list).")
    
    final_answer = dspy.OutputField(desc="The precise answer matching the format hint.")
    short_explanation = dspy.OutputField(desc="A brief explanation (max 2 sentences).")

# --- Modules ---

class CoT_Router(dspy.Module):
    def __init__(self):
        super().__init__()
        self.prog = dspy.ChainOfThought(Router)
    
    def forward(self, question):
        return self.prog(question=question)

class CoT_SQL(dspy.Module):
    def __init__(self):
        super().__init__()
        self.prog = dspy.ChainOfThought(GenerateSQL)
    
    def forward(self, question, schema):
        return self.prog(question=question, database_schema=schema)

class CoT_Synthesizer(dspy.Module):
    def __init__(self):
        super().__init__()
        self.prog = dspy.ChainOfThought(SynthesizeAnswer)
    
    def forward(self, question, context, sql_query, sql_result, format_hint):
        return self.prog(
            question=question,
            context=context,
            sql_query=sql_query,
            sql_result=sql_result,
            format_hint=format_hint
        )
