# Retail Analytics Copilot

A local, free AI agent that answers retail analytics questions using RAG (over local docs) and SQL (over a local SQLite DB). Built with LangGraph and DSPy, running locally with Phi-3.5 via Ollama.

## Architecture
- **Router**: Classifies questions as RAG, SQL, or Hybrid using DSPy.
- **RAG**: Retrieves relevant chunks from `docs/` using TF-IDF.
- **SQL**: Generates and executes SQLite queries against `data/northwind.sqlite`.
- **Hybrid**: Combines both strategies for complex questions.
- **Repair Loop**: Automatically retries SQL generation or synthesis if errors occur (up to 2 times).

## Setup
1. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```
2. **Download Data**:
   The `data/northwind.sqlite` database is downloaded automatically (or via `curl`).
3. **Ollama**:
   Ensure Ollama is running and the model is pulled:
   ```bash
   ollama pull phi3.5:3.8b-mini-instruct-q4_K_M
   ```

## Usage
Run the agent on the sample questions:
```bash
python run_agent_hybrid.py --batch sample_questions_hybrid_eval.jsonl --out outputs_hybrid.jsonl
```

## DSPy Optimization
The `agent/dspy_signatures.py` module uses `dspy.ChainOfThought` for the Router, SQL Generator, and Synthesizer.
- **Metric**: Success rate of SQL execution and format adherence.
- **Optimization**: The current implementation uses zero-shot CoT. Future improvements can use `BootstrapFewShot` to compile the modules with examples.

## Assumptions
- **CostOfGoods**: Approximated as 0.7 * UnitPrice if not available in the database.
- **Model**: Relies on `phi3.5:3.8b-mini-instruct-q4_K_M` for all inference.
