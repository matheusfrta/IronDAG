import asyncio
import fnmatch
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable, Any, Dict, List, Coroutine


@dataclass
class Event:
    topic: str
    payload: Any
    timestamp: str = field(default_factory=lambda: datetime.utcnow().isoformat())


class EventBus:
    def __init__(self, history_limit: int = 1000):
        self._subscribers: Dict[str, List[Callable[[Event], Coroutine[Any, Any, None]]]] = defaultdict(list)
        self._history: List[Event] = []
        self._history_limit = history_limit
        self._lock = asyncio.Lock()

    async def subscribe(self, pattern: str, callback: Callable[[Event], Coroutine[Any, Any, None]]):
        async with self._lock:
            self._subscribers[pattern].append(callback)

    async def publish(self, topic: str, payload: Any):
        event = Event(topic=topic, payload=payload)
        
        async with self._lock:
            self._history.append(event)
            if len(self._history) > self._history_limit:
                self._history.pop(0)

        tasks = []
        for pattern, callbacks in self._subscribers.items():
            if fnmatch.fnmatch(topic, pattern):
                for callback in callbacks:
                    if asyncio.iscoroutinefunction(callback):
                        tasks.append(asyncio.create_task(callback(event)))
                    else:
                        loop = asyncio.get_running_loop()
                        tasks.append(loop.run_in_executor(None, callback, event))

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    def get_history(self) -> List[Event]:
        return list(self._history)