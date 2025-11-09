

from fastapi import FastAPI, HTTPException, UploadFile, File, Form, Request
from pydantic import BaseModel
from DataSphere.storage.duckdb_storage import DuckDBStorage
from DataSphere.coordination.redis_coordination import RedisCoordinator

from langchain_community.llms import HuggingFacePipeline
from transformers import pipeline
import subprocess
import ray
import json

app = FastAPI()

# Accept both JSON and multipart/form-data
from typing import Optional

def get_form_value(form, key, default=None):
    v = form.get(key)
    return v if v is not None else default


import os

@app.post("/chat_pipeline")
async def chat_pipeline(
    request: Request,
    prompt: Optional[str] = Form(None),
    data_source: Optional[str] = Form(None),
    db_conn: Optional[str] = Form(None),
    s3_path: Optional[str] = Form(None),
    file: Optional[UploadFile] = File(None)
):
    # Support both JSON and multipart
    if request.headers.get("content-type", "").startswith("application/json"):
        body = await request.json()
        prompt = body.get("prompt")
        data_source = body.get("data_source")
        db_conn = body.get("db_conn")
        s3_path = body.get("s3_path")
        file = None
    # else: use form/file params

    prompt_template = (
        "You are an AI pipeline orchestrator. "
        "Given a user request, break it down into a list of steps with actions and parameters. "
        "Return the result as a JSON list of steps, each with 'step', 'action', and 'params'.\n"
        "User request: {user_prompt}\n"
        "Example output: [{{\"step\": \"ingest\", \"action\": \"trigger_airflow\", \"params\": {{\"dag\": \"test_agent_dag\"}}}}, {{\"step\": \"clean\", \"action\": \"run_ray_task\", \"params\": {{}}}}]"
    )
    full_prompt = prompt_template.format(user_prompt=prompt)
    hf_pipeline = pipeline("text-generation", model="distilgpt2")
    llm = HuggingFacePipeline(pipeline=hf_pipeline)
    plan = llm.invoke(full_prompt)
    # Try to parse JSON from the LLM output
    steps = []
    actions = []
    try:
        import re
        match = re.search(r'\[.*?\]', plan, re.DOTALL)
        if match:
            plan_json = match.group(0)
            steps = json.loads(plan_json)
        else:
            raise ValueError("No JSON array found in LLM output.")
    except Exception as e:
        actions.append(f"[Parser] Could not parse plan as JSON: {e}")
        steps = []

    # Save uploaded file if present
    uploaded_file_path = None
    if file is not None:
        upload_dir = "uploaded_data"
        os.makedirs(upload_dir, exist_ok=True)
        uploaded_file_path = os.path.join(upload_dir, file.filename)
        with open(uploaded_file_path, "wb") as f_out:
            f_out.write(await file.read())
        actions.append(f"[Upload] File saved to {uploaded_file_path}")

    # Execute orchestration for each step
    for step in steps:
        step_name = step.get("step", "").lower()
        action_type = step.get("action", "")
        params = step.get("params", {})
        # Attach user data info to params for ingestion
        if step_name == "ingest":
            if data_source == "Upload File" and uploaded_file_path:
                params["file_path"] = uploaded_file_path
            elif data_source == "Database" and db_conn:
                params["db_conn"] = db_conn
            elif data_source == "S3" and s3_path:
                params["s3_path"] = s3_path
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
                def ray_task():
                    return f"[Ray] {step_name.capitalize()} task completed."
                result = ray.get(ray_task.remote())
                actions.append(result)
                ray.shutdown()
            except Exception as e:
                actions.append(f"[Ray] Error running {step_name} task: {e}")
        elif action_type == "duckdb_query":
            try:
                from DataSphere.storage.duckdb_storage import DuckDBStorage
                db = DuckDBStorage()
                query = params.get("query", "SELECT 1")
                result = db.query(query)
                actions.append(f"[DuckDB] Query result: {result}")
                db.close()
            except Exception as e:
                actions.append(f"[DuckDB] Error: {e}")
        elif action_type == "redis_publish":
            try:
                from DataSphere.coordination.redis_coordination import RedisCoordinator
                rc = RedisCoordinator()
                channel = params.get("channel", "default")
                message = params.get("message", "test")
                rc.publish(channel, message)
                actions.append(f"[Redis] Published to {channel}: {message}")
            except Exception as e:
                actions.append(f"[Redis] Error: {e}")
        elif action_type == "export_s3":
            bucket = params.get("bucket", "my-bucket")
            actions.append(f"[S3] Data exported to bucket: {bucket}")
        elif action_type == "notify":
            recipient = params.get("recipient", "team")
            actions.append(f"[Notify] Notification sent to: {recipient}")
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
