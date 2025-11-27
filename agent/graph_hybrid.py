import os
import dspy
from typing import TypedDict, List, Dict, Any, Annotated
from langgraph.graph import StateGraph, END
from agent.dspy_signatures import CoT_Router, CoT_SQL, CoT_Synthesizer
from agent.rag.retrieval import Retriever
from agent.tools.sqlite_tool import SQLiteTool

# --- State ---
class AgentState(TypedDict):
    question: str
    format_hint: str
    strategy: str
    context: List[Dict]
    schema: str
    sql_query: str
    sql_result: Dict
    final_answer: Any
    explanation: str
    citations: List[str]
    errors: List[str]
    repair_count: int

# --- Nodes ---

def router_node(state: AgentState):
    print(f"--- Router Node ---")
    question = state["question"]
    router = CoT_Router()
    pred = router(question=question)
    strategy = pred.strategy.lower().strip()
    if "sql" in strategy and "rag" in strategy:
        strategy = "hybrid"
    elif "sql" in strategy:
        strategy = "sql"
    else:
        strategy = "rag"
    
    print(f"Strategy: {strategy}")
    return {"strategy": strategy}

def retriever_node(state: AgentState):
    print(f"--- Retriever Node ---")
    question = state["question"]
    retriever = Retriever()
    results = retriever.search(question, top_k=3)
    return {"context": results}

def planner_node(state: AgentState):
    print(f"--- Planner Node ---")
    # Simple pass-through for now, could extract dates/entities
    return {}

def sql_generator_node(state: AgentState):
    print(f"--- SQL Generator Node ---")
    question = state["question"]
    db = SQLiteTool()
    schema = db.get_schema() # Get full schema or filter if needed
    
    generator = CoT_SQL()
    pred = generator(question=question, schema=schema)
    sql_query = pred.sql_query.replace("```sql", "").replace("```", "").strip()
    
    # Deterministic SQL Cleaning
    import re
    # Replace YEAR(x) -> strftime('%Y', x)
    sql_query = re.sub(r"YEAR\(([^)]+)\)", r"strftime('%Y', \1)", sql_query, flags=re.IGNORECASE)
    # Replace MONTH(x) -> strftime('%m', x)
    sql_query = re.sub(r"MONTH\(([^)]+)\)", r"strftime('%m', \1)", sql_query, flags=re.IGNORECASE)
    
    # Validate SQL against schema
    from agent.tools.sql_validator import SQLValidator
    validator = SQLValidator()
    validation_result = validator.validate_sql(sql_query)
    
    if not validation_result["valid"]:
        # Log validation errors
        error_msg = "; ".join(validation_result["errors"])
        print(f"SQL Validation Failed: {error_msg}")
        return {
            "sql_query": sql_query, 
            "schema": schema,
            "errors": validation_result["errors"]
        }
    
    return {"sql_query": sql_query, "schema": schema}

def executor_node(state: AgentState):
    print(f"--- Executor Node ---")
    sql_query = state["sql_query"]
    db = SQLiteTool()
    result = db.execute_query(sql_query)
    
    if result["error"]:
        print(f"SQL Error: {result['error']}")
        return {"sql_result": result, "errors": [result["error"]]}
    
    return {"sql_result": result}

def synthesizer_node(state: AgentState):
    print(f"--- Synthesizer Node ---")
    question = state["question"]
    context = state.get("context", [])
    sql_query = state.get("sql_query", "")
    sql_result = state.get("sql_result", {})
    format_hint = state["format_hint"]
    
    # Format context for prompt
    context_str = "\n".join([f"[{c['id']}] {c['content']}" for c in context])
    
    synthesizer = CoT_Synthesizer()
    pred = synthesizer(
        question=question,
        context=context_str,
        sql_query=sql_query,
        sql_result=str(sql_result),
        format_hint=format_hint
    )
    
    # Extract citations
    citations = []
    if sql_query:
        # Simple heuristic for table citations
        tables = ["Orders", "Order Details", "Products", "Customers"]
        for t in tables:
            if t.lower() in sql_query.lower() or t.replace(" ", "").lower() in sql_query.lower().replace(" ", ""):
                citations.append(t)
    
    for c in context:
        citations.append(c["id"])
        
    return {
        "final_answer": pred.final_answer,
        "explanation": pred.short_explanation,
        "citations": citations
    }

def repair_node(state: AgentState):
    print(f"--- Repair Node ---")
    repair_count = state.get("repair_count", 0) + 1
    
    # If validation errors exist, provide schema hints
    errors = state.get("errors", [])
    if errors and any("does not exist" in e or "not found" in e for e in errors):
        from agent.tools.sql_validator import SQLValidator
        validator = SQLValidator()
        schema_summary = validator.get_schema_summary()
        print(f"Providing schema hints for repair:\n{schema_summary}")
    
    return {"repair_count": repair_count, "errors": []} # Clear errors for retry

# --- Edges ---

def route_strategy(state: AgentState):
    return state["strategy"]

def check_execution(state: AgentState):
    errors = state.get("errors", [])
    repair_count = state.get("repair_count", 0)
    
    if errors and repair_count < 2:
        return "repair"
    return "synthesize"

# --- Graph Construction ---

def build_graph():
    workflow = StateGraph(AgentState)
    
    workflow.add_node("router", router_node)
    workflow.add_node("retriever", retriever_node)
    workflow.add_node("planner", planner_node)
    workflow.add_node("sql_generator", sql_generator_node)
    workflow.add_node("executor", executor_node)
    workflow.add_node("synthesizer", synthesizer_node)
    workflow.add_node("repair", repair_node)
    
    workflow.set_entry_point("router")
    
    workflow.add_conditional_edges(
        "router",
        route_strategy,
        {
            "rag": "retriever",
            "sql": "planner",
            "hybrid": "retriever" # Hybrid goes to retriever first, then planner
        }
    )
    
    def route_retriever(state):
        if state["strategy"] == "rag":
            return "synthesizer"
        return "planner"

    workflow.add_conditional_edges(
        "retriever",
        route_retriever,
        {
            "synthesizer": "synthesizer",
            "planner": "planner"
        }
    )
    
    workflow.add_edge("planner", "sql_generator")
    workflow.add_edge("sql_generator", "executor")
    
    workflow.add_conditional_edges(
        "executor",
        check_execution,
        {
            "repair": "repair",
            "synthesize": "synthesizer"
        }
    )
    
    workflow.add_edge("repair", "sql_generator") # Retry SQL generation
    workflow.add_edge("synthesizer", END)
    
    return workflow.compile()
