

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from DataSphere.storage.duckdb_storage import DuckDBStorage
from DataSphere.coordination.redis_coordination import RedisCoordinator
from langchain_community.llms import HuggingFacePipeline
from transformers import pipeline

app = FastAPI()
class ChatPipelineRequest(BaseModel):
    prompt: str

@app.post("/chat_pipeline")
def chat_pipeline(req: ChatPipelineRequest):
    # Use a local HuggingFace LLM for demo (distilgpt2)
    hf_pipeline = pipeline("text-generation", model="distilgpt2")
    llm = HuggingFacePipeline(pipeline=hf_pipeline)
    plan = llm.invoke(req.prompt)
    # Simple keyword-based parsing for demo
    steps = []
    actions = []
    plan_lower = plan.lower() if isinstance(plan, str) else str(plan).lower()
    if "ingest" in plan_lower:
        steps.append("ingest")
        actions.append("[Simulated] Ingesting data...")
    if "clean" in plan_lower:
        steps.append("clean")
        actions.append("[Simulated] Cleaning data...")
    if "summar" in plan_lower:
        steps.append("summarize")
        actions.append("[Simulated] Running summary pipeline...")
    # Simulate orchestration (replace with real triggers in production)
    return {
        "pipeline_plan": plan,
        "parsed_steps": steps,
        "actions": actions
    }

# Initialize storage and coordination (in production, use dependency injection)
duckdb = DuckDBStorage()
redis_coord = RedisCoordinator()

class ContractRequest(BaseModel):
    dataset: str
    contract_terms: dict

class PipelineTriggerRequest(BaseModel):
    pipeline_name: str
    params: dict = {}

@app.get("/health")
def health_check():
    return {"status": "ok"}

@app.post("/negotiate_contract")
def negotiate_contract(req: ContractRequest):
    # Example: store contract in Redis
    key = f"contract:{req.dataset}"
    redis_coord.set_state(key, str(req.contract_terms))
    return {"message": "Contract negotiation received", "dataset": req.dataset}

@app.post("/trigger_pipeline")
def trigger_pipeline(req: PipelineTriggerRequest):
    # Example: publish pipeline trigger to Redis
    redis_coord.publish("pipeline_triggers", f"{req.pipeline_name}:{req.params}")
    return {"message": "Pipeline trigger sent", "pipeline": req.pipeline_name}
