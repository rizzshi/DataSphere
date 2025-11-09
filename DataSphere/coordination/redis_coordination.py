## Redis-based coordination module for distributed systems.
import redis
from typing import Any

class RedisCoordinator:
    def __init__(self, host: str = 'localhost', port: int = 6379, db: int = 0):
        self.client = redis.Redis(host=host, port=port, db=db)

    def publish(self, channel: str, message: str):
        self.client.publish(channel, message)

    def subscribe(self, channel: str):
        pubsub = self.client.pubsub()
        pubsub.subscribe(channel)
        return pubsub

    def set_state(self, key: str, value: Any):
        self.client.set(key, value)

    def get_state(self, key: str):
        return self.client.get(key)
