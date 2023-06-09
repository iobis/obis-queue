from abc import ABC, abstractmethod


class Task():
     
    def __init__(self, queue: str, priority: int, payload: dict, id: int = None):
        self.id = id
        self.queue = queue
        self.priority = priority
        self.payload = payload
        self.created_at = None
        self.client = None
        self.locked_at = None
        self.expires_at = None
        self.release_at = None
        self.completed_at = None
        self.released_at = None


class Queue(ABC):
 
    @abstractmethod
    def publish(self, Task) -> int:
        pass

    @abstractmethod
    def consume(self, queue, client, release_minutes) -> Task:
        pass

    @abstractmethod
    def complete(self, id: int) -> None:
        pass
