from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime


@dataclass
class Task():
    id: int = None
    queue: str = None
    priority: int = None
    payload: dict = None
    created_at: datetime = None
    client: str = None
    locked_at: datetime = None
    expires_at: datetime = None
    release_at: datetime = None
    completed_at: datetime = None
    released_at: datetime = None


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
