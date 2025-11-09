import redis

# Connect to Redis
client = redis.Redis(host='localhost', port=6379, db=0)

# Test set and get
client.set('test_key', 'test_value')
value = client.get('test_key')
print(f"Redis test value: {value.decode()}")
