import time
from collections import defaultdict
from typing import Callable, Any, Dict, List
from irondag.core.task import Task, TaskResult


class Middleware:
    async def before_task(self, task: Task, context: Dict[str, Any]):
        pass

    async def after_task(self, task: Task, result: TaskResult, context: Dict[str, Any]):
        pass


class LoggingMiddleware(Middleware):
    async def before_task(self, task: Task, context: Dict[str, Any]):
        print(f"[Middleware] Starting task: {task.name}")

    async def after_task(self, task: Task, result: TaskResult, context: Dict[str, Any]):
        print(f"[Middleware] Finished task: {task.name} with state {result.state.value} in {result.execution_time_ms:.2f}ms")


class MetricsMiddleware(Middleware):
    def __init__(self):
        self.metrics: Dict[str, List[float]] = defaultdict(list)

    async def after_task(self, task: Task, result: TaskResult, context: Dict[str, Any]):
        self.metrics[task.name].append(result.execution_time_ms)


class MiddlewarePipeline:
    def __init__(self):
        self._middlewares: List[Middleware] = []

    def use(self, middleware: Middleware) -> 'MiddlewarePipeline':
        self._middlewares.append(middleware)
        return self

    async def execute_before(self, task: Task, context: Dict[str, Any]):
        for mw in self._middlewares:
            await mw.before_task(task, context)

    async def execute_after(self, task: Task, result: TaskResult, context: Dict[str, Any]):
        for mw in self._middlewares:
            await mw.after_task(task, result, context)