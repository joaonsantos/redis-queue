import json
from typing import Any, Dict

import redis as redis_py


class RedisQueue:
    redis: redis_py.client.Redis
    name: str

    def __init__(self, queue_name: str, host: str = 'localhost', port: int = 6379) -> None:
        self.name = queue_name
        self.redis = redis_py.Redis(host=host, port=port)


    def enqueue(self, key: str, values: Dict[str, Any]) -> None:
        list_name = self.name
        list_values: Dict[str, Any] = {key: values}
        json_str: str = json.dumps(list_values)

        self.redis.rpush(list_name, json_str)

    def dequeue(self) -> Dict[str, Any]:
        list_name = self.name
        response: str = self.redis.lpop(list_name)

        return json.loads(response)

    def size(self) -> Any:
        list_name = self.name
        return self.redis.llen(list_name)
