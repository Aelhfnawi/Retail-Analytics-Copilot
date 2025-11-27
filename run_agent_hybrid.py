import dspy
import json
import argparse
import os
from typing import List, Dict
from agent.graph_hybrid import build_graph

# Configure DSPy with Ollama
lm = dspy.LM(model='ollama/phi3.5:3.8b-mini-instruct-q4_K_M', max_tokens=1000)
dspy.configure(lm=lm)

def process_questions(input_file: str, output_file: str):
    print(f"Loading questions from {input_file}...")
    with open(input_file, "r") as f:
        questions = [json.loads(line) for line in f]
    
    app = build_graph()
    results = []
    
    for q in questions:
        print(f"\nProcessing: {q['id']}")
        initial_state = {
            "question": q["question"],
            "format_hint": q["format_hint"],
            "strategy": "",
            "context": [],
            "schema": "",
            "sql_query": "",
            "sql_result": {},
            "final_answer": None,
            "explanation": "",
            "citations": [],
            "errors": [],
            "repair_count": 0
        }
        
        try:
            final_state = app.invoke(initial_state)
            
            output = {
                "id": q["id"],
                "final_answer": final_state.get("final_answer"),
                "sql": final_state.get("sql_query", ""),
                "confidence": 0.8 if not final_state.get("errors") else 0.4, # Simple heuristic
                "explanation": final_state.get("explanation", ""),
                "citations": final_state.get("citations", [])
            }
        except Exception as e:
            print(f"Error processing {q['id']}: {e}")
            output = {
                "id": q["id"],
                "final_answer": "Error processing request.",
                "sql": "",
                "confidence": 0.0,
                "explanation": str(e),
                "citations": []
            }
        results.append(output)
    
    print(f"Writing results to {output_file}...")
    with open(output_file, "w") as f:
        for r in results:
            f.write(json.dumps(r) + "\n")
    print("Done.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch", required=True, help="Path to input jsonl file")
    parser.add_argument("--out", required=True, help="Path to output jsonl file")
    args = parser.parse_args()
    
    process_questions(args.batch, args.out)
