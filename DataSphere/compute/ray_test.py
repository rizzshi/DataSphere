import ray

ray.init(ignore_reinit_error=True)

@ray.remote
def add(x, y):
    return x + y

future = add.remote(2, 3)
result = ray.get(future)
print(f"Ray test result: {result}")

ray.shutdown()
