# DataSphere

Autonomous Data Mesh Orchestrator using AI agents

## Concept
A decentralized data mesh where AI agents auto-orchestrate pipelines, allocate compute, and validate data quality — no humans required. Each “agent” manages a dataset and negotiates data contracts with others through APIs.

## Tech Stack
- LangChain (agent intelligence)
- Airflow (orchestration)
- DuckDB (local analytical storage)
- Redis (coordination)
- Ray (distributed compute)
- FastAPI (API layer)

## Project Structure
- `agents/` — AI agent logic (LangChain)
- `orchestration/` — Airflow DAGs and orchestration logic
- `storage/` — DuckDB integration
- `coordination/` — Redis coordination and messaging
- `compute/` — Ray distributed compute logic
- `api/` — FastAPI services

## Setup
1. Install dependencies: `pip install -r requirements.txt`
2. Run the FastAPI agent service: `uvicorn api.agent_service:app --reload`

## Impact
Imagine ChatGPT but for pipelines — it runs, scales, and heals itself.