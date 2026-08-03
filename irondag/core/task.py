import asyncio
import time
import inspect
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, Any, Optional, Dict, List


class TaskState(Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    RETRYING = "RETRYING"
    SKIPPED = "SKIPPED"
    CANCELED = "CANCELED"


class CircuitState(Enum):
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"


class CircuitBreakerOpenException(Exception):
    pass


class CircuitBreaker:
    def __init__(self, failure_threshold: int = 3, recovery_timeout: float = 30.0):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failure_count = 0
        self.state = CircuitState.CLOSED
        self.last_state_change = time.time()

    def record_success(self):
        self.failure_count = 0
        if self.state != CircuitState.CLOSED:
            self.state = CircuitState.CLOSED
            self.last_state_change = time.time()

    def record_failure(self):
        self.failure_count += 1
        if self.failure_count >= self.failure_threshold:
            self.state = CircuitState.OPEN
            self.last_state_change = time.time()

    def allow_execution(self) -> bool:
        if self.state == CircuitState.CLOSED:
            return True
        if self.state == CircuitState.OPEN:
            if time.time() - self.last_state_change >= self.recovery_timeout:
                self.state = CircuitState.HALF_OPEN
                self.last_state_change = time.time()
                return True
            return False
        return True


@dataclass
class TaskResult:
    task_name: str
    state: TaskState
    output: Any = None
    error: Optional[str] = None
    execution_time_ms: float = 0.0
    attempts: int = 1


@dataclass
class Task:
    name: str
    func: Callable[..., Any]
    dependencies: List[str] = field(default_factory=list)
    retries: int = 3
    retry_delay: float = 1.0
    backoff_factor: float = 2.0
    timeout: Optional[float] = None
    state: TaskState = TaskState.PENDING
    result: Optional[TaskResult] = None
    circuit_breaker: Optional[CircuitBreaker] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    async def execute(self, context: Dict[str, Any]) -> TaskResult:
        if self.circuit_breaker and not self.circuit_breaker.allow_execution():
            self.state = TaskState.SKIPPED
            error_msg = f"Circuit breaker for task '{self.name}' is OPEN"
            self.result = TaskResult(
                task_name=self.name,
                state=TaskState.SKIPPED,
                error=error_msg,
                execution_time_ms=0.0,
                attempts=0
            )
            return self.result

        start_time = time.time()
        current_delay = self.retry_delay
        last_exception: Optional[Exception] = None

        for attempt in range(1, self.retries + 2):
            try:
                self.state = TaskState.RUNNING if attempt == 1 else TaskState.RETRYING
                
                sig = inspect.signature(self.func)
                kwargs = {}
                for param in sig.parameters.values():
                    if param.name in context:
                        kwargs[param.name] = context[param.name]

                if inspect.iscoroutinefunction(self.func):
                    if self.timeout:
                        output = await asyncio.wait_for(self.func(**kwargs), timeout=self.timeout)
                    else:
                        output = await self.func(**kwargs)
                else:
                    loop = asyncio.get_running_loop()
                    if self.timeout:
                        output = await asyncio.wait_for(
                            loop.run_in_executor(None, lambda: self.func(**kwargs)),
                            timeout=self.timeout
                        )
                    else:
                        output = await loop.run_in_executor(None, lambda: self.func(**kwargs))

                elapsed = (time.time() - start_time) * 1000.0
                self.state = TaskState.COMPLETED
                if self.circuit_breaker:
                    self.circuit_breaker.record_success()

                self.result = TaskResult(
                    task_name=self.name,
                    state=TaskState.COMPLETED,
                    output=output,
                    execution_time_ms=elapsed,
                    attempts=attempt
                )
                return self.result

            except Exception as exc:
                last_exception = exc
                if self.circuit_breaker:
                    self.circuit_breaker.record_failure()

                if attempt <= self.retries:
                    await asyncio.sleep(current_delay)
                    current_delay *= self.backoff_factor

        elapsed = (time.time() - start_time) * 1000.0
        self.state = TaskState.FAILED
        self.result = TaskResult(
            task_name=self.name,
            state=TaskState.FAILED,
            error=str(last_exception),
            execution_time_ms=elapsed,
            attempts=self.retries + 1
        )
        return self.result