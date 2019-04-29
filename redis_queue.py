import json

from typing import Any, Dict

from os import getenv
from redis import Redis


class RedisQueue:
    redis: Redis
    name: str

    def __init__(self, queue_name: str, host: str = 'localhost', port: int = 6379, db: int = 0) -> None:
        """Create a redis queue."""

        self.name = queue_name
        self.redis = Redis(host=host, port=port, db=db)

    def enqueue(self, values: Dict[str, Any]) -> None:
        """Enqueues a value in the queue as a JSON string."""
        list_values: Dict[str, Any] = values
        json_str: str = json.dumps(list_values)

        self.redis.rpush(self.name, json_str)

    def priority_enqueue(self, values: Dict[str, Any]) -> None:
        """Next dequeue will pop these values."""

        list_values: Dict[str, Any] = values
        json_str: str = json.dumps(list_values)

        self.redis.lpush(self.name, json_str)

    def dequeue(self) -> Dict[str, Any]:
        """Dequeues a JSON string into a dict."""
        return json.loads(self.redis.lpop(self.name))

    def peek(self) -> Dict[str, Any]:
        """Returns the element at the head of the queue."""
        head_element: Dict[str, Any] = json.loads(self.redis.lpop(self.name))
        json_str: str = json.dumps(head_element)

        self.redis.lpush(self.name, json_str)

        return head_element

    def __len__(self) -> Any:
        """Returns the current size of the queue."""

        return self.redis.llen(self.name)


def initialize_redis_queue(name: str, db: int = 0) -> RedisQueue:
    """Wrapper function to get redis connection parameters from environment variables."""
    redis_host = getenv('REDIS_HOST', 'localhost')
    redis_port = getenv('REDIS_PORT', 6379)

    queue = RedisQueue(queue_name=name, host=redis_host, port=redis_port, db=db)

    return queue
