
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from DataSphere.storage.duckdb_storage import DuckDBStorage
from DataSphere.coordination.redis_coordination import RedisCoordinator

app = FastAPI()

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
