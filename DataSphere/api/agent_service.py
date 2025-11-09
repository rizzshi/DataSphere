

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from DataSphere.storage.duckdb_storage import DuckDBStorage
from DataSphere.coordination.redis_coordination import RedisCoordinator

from langchain_community.llms import HuggingFacePipeline
from transformers import pipeline
import subprocess
import ray

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
        # Trigger Airflow DAG via CLI (replace with Airflow REST API if needed)
        try:
            result = subprocess.run([
                "airflow", "dags", "trigger", "test_agent_dag"
            ], capture_output=True, text=True)
            actions.append(f"[Airflow] Ingest DAG triggered: {result.stdout.strip()}")
        except Exception as e:
            actions.append(f"[Airflow] Error triggering DAG: {e}")
    if "clean" in plan_lower:
        steps.append("clean")
        # Run Ray task for cleaning
        try:
            ray.init(ignore_reinit_error=True)
            @ray.remote
            def clean_task():
                return "[Ray] Data cleaned successfully."
            result = ray.get(clean_task.remote())
            actions.append(result)
            ray.shutdown()
        except Exception as e:
            actions.append(f"[Ray] Error running clean task: {e}")
    if "summar" in plan_lower:
        steps.append("summarize")
        # Log summary (could trigger another DAG or Ray task)
        actions.append("[Summary] Daily summary pipeline would be triggered here.")
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
