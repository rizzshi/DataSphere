

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from DataSphere.storage.duckdb_storage import DuckDBStorage
from DataSphere.coordination.redis_coordination import RedisCoordinator

from langchain_community.llms import HuggingFacePipeline
from transformers import pipeline
import subprocess
import ray
import json

app = FastAPI()
class ChatPipelineRequest(BaseModel):
    prompt: str

@app.post("/chat_pipeline")
def chat_pipeline(req: ChatPipelineRequest):
    # Use a prompt template to request structured JSON output
    prompt_template = (
        "You are an AI pipeline orchestrator. "
        "Given a user request, break it down into a list of steps with actions and parameters. "
        "Return the result as a JSON list of steps, each with 'step', 'action', and 'params'.\n"
        "User request: {user_prompt}\n"
        "Example output: [{{\"step\": \"ingest\", \"action\": \"trigger_airflow\", \"params\": {{\"dag\": \"test_agent_dag\"}}}}, {{\"step\": \"clean\", \"action\": \"run_ray_task\", \"params\": {{}}}}]"
    )
    full_prompt = prompt_template.format(user_prompt=req.prompt)
    hf_pipeline = pipeline("text-generation", model="distilgpt2")
    llm = HuggingFacePipeline(pipeline=hf_pipeline)
    plan = llm.invoke(full_prompt)
    # Try to parse JSON from the LLM output
    steps = []
    actions = []
    try:
        json_start = plan.find('[')
        json_end = plan.rfind(']') + 1
        plan_json = plan[json_start:json_end]
        steps = json.loads(plan_json)
    except Exception as e:
        actions.append(f"[Parser] Could not parse plan as JSON: {e}")
        steps = []
    # Execute orchestration for each step
    for step in steps:
        step_name = step.get("step", "").lower()
        action_type = step.get("action", "")
        params = step.get("params", {})
        if action_type == "trigger_airflow":
            dag = params.get("dag", "test_agent_dag")
            try:
                result = subprocess.run([
                    "airflow", "dags", "trigger", dag
                ], capture_output=True, text=True)
                actions.append(f"[Airflow] {dag} triggered: {result.stdout.strip()}")
            except Exception as e:
                actions.append(f"[Airflow] Error triggering {dag}: {e}")
        elif action_type == "run_ray_task":
            try:
                ray.init(ignore_reinit_error=True)
                @ray.remote
                def clean_task():
                    return f"[Ray] {step_name.capitalize()} task completed."
                result = ray.get(clean_task.remote())
                actions.append(result)
                ray.shutdown()
            except Exception as e:
                actions.append(f"[Ray] Error running {step_name} task: {e}")
        else:
            actions.append(f"[Simulated] {step_name.capitalize()} step: {action_type} with params {params}")
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
