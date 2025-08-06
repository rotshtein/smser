import redis
import logging
from typing import Any, Optional

class RedisClient:
    def __init__(self, host='localhost', port=6379, decode_responses=True, socket_keepalive=True):
        """
        Initialize the RedisClient instance and connect to the Redis server.
        :param host: Redis server host
        :param port: Redis server port
        :param decode_responses: Whether to decode responses to strings
        :param socket_keepalive: Whether to enable TCP keepalive
        """
        self._logger = logging.getLogger(self.__class__.__name__)
        self._client = redis.Redis(
            host=host,
            port=port,
            decode_responses=decode_responses,
            socket_keepalive=socket_keepalive
        )

    def rpush(self, queue: str, value: Any) -> int:
        """
        Push a value onto the right end of a Redis list (queue).
        :param queue: Name of the Redis list (queue)
        :param value: Value to push onto the queue
        :return: The length of the list after the push operation
        """
        try:
            self._logger.debug(f"RPUSH to {queue}: {value}")
            return self._client.rpush(queue, value)
        except Exception as e:
            self._logger.exception(f"rpush error {e}")

    def blpop(self, queue: str, timeout: int = 0) -> Optional[Any]:
        """
        Block until a value is popped from the left end of a Redis list (queue), or until timeout.
        :param queue: Name of the Redis list (queue)
        :param timeout: Timeout in seconds (0 means block indefinitely)
        :return: Tuple of (queue name, value) if successful, None otherwise
        """
        try:
            self._logger.debug(f"BLPOP from {queue} with timeout {timeout}")
            return self._client.blpop(queue, timeout=timeout)
        except Exception as e:
            self._logger.error(f"blpop error {e}" )

    def pipeline(self):
        """
        Create a Redis pipeline for batch operations.
        :return: Redis pipeline object
        """
        self._logger.debug("Creating Redis pipeline")
        return self._client.pipeline()

# Example usage:
# if __name__ == "__main__":
#     logging.basicConfig(level=logging.INFO)
#     r = RedisClient(host='localhost', port=6379)
#     r.rpush('test_queue', 'hello')
#     print(r.blpop('test_queue', timeout=1)) 