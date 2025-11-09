import ray

@ray.remote
def process_data(data):
    # Example Ray task: process data
    return {"processed": data}

# Example function to trigger Ray task

def trigger_ray_task(data):
    result_ref = process_data.remote(data)
    return ray.get(result_ref)
