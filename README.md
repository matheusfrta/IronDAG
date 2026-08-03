# IronDAG

A zero-dependency, resilient async workflow orchestrator and event-driven pipeline engine for Python.

IronDAG is a lightweight, high-performance execution engine designed to manage Directed Acyclic Graph (DAG) task dependencies, event streams, and fault-tolerance patterns. Built entirely on top of the Python standard library, it eliminates external dependencies such as Redis, RabbitMQ, or external database drivers, making it suitable for embedded systems, microservices, and lightweight data pipelines.

---

## Technical Overview

Modern Python applications often require structured task execution and fault isolation, but full-scale orchestration platforms like Apache Airflow or Celery introduce significant deployment and operational overhead. IronDAG bridges this gap by providing an enterprise-grade execution framework with zero third-party dependencies.

### Core Architecture

- **Directed Acyclic Graph (DAG) Execution:** Topological sorting and layer-based parallel task execution with strict cycle detection.
- **Resilience Engineering:** Exponential backoff retries, execution timeouts, and state-aware Circuit Breakers to prevent cascading failures.
- **Async Event Bus:** High-throughput in-memory pub/sub message router supporting wildcard topic pattern matching.
- **Embedded State Persistence:** Zero-configuration SQLite storage engine for workflow runs, task state checkpoints, and recovery logs.
- **Middleware Pipeline:** Interceptor architecture allowing custom hooks for logging, execution metrics, and rate limiting.
- **Embedded Web Dashboard:** Native HTTP monitoring interface serving real-time system metrics and workflow states.
- **Command Line Interface:** Integrated CLI for pipeline execution and dashboard management.

---

## Installation

Install the package directly using standard Python tooling:

```bash
pip install .
```

*IronDAG requires Python 3.10 or newer.*

---

## Usage Examples

### 1. Defining and Executing a DAG

IronDAG automatically resolves dependencies, verifies graph validity, and executes independent tasks concurrently.

```python
import asyncio
from irondag import DAG, Task, IronEngine

async def extract_data():
    return {"records": 500, "status": "ok"}

async def transform_data(extract_data):
    count = extract_data["records"]
    return f"Transformed {count} records."

async def load_data(transform_data):
    return f"Database load status: SUCCESS ({transform_data})"

# Initialize DAG and register tasks
dag = DAG("data_ingestion_pipeline")
dag.add_task(Task(name="extract_data", func=extract_data))
dag.add_task(Task(name="transform_data", func=transform_data, dependencies=["extract_data"]))
dag.add_task(Task(name="load_data", func=load_data, dependencies=["transform_data"]))

# Execute workflow
engine = IronEngine()
results = asyncio.run(engine.run_dag(dag))

for task_name, result in results.items():
    print(f"Task: {task_name} | State: {result.state.value} | Execution Time: {result.execution_time_ms:.2f}ms")
```

---

### 2. Resilience and Circuit Breakers

Protect external APIs or database connections using configurable retry policies and Circuit Breakers.

```python
from irondag import Task, CircuitBreaker

# Circuit breaker opens after 3 consecutive failures and enters half-open state after 15 seconds
circuit_breaker = CircuitBreaker(failure_threshold=3, recovery_timeout=15.0)

task = Task(
    name="external_api_call",
    func=fetch_remote_resource,
    retries=3,
    retry_delay=1.0,
    backoff_factor=2.0,
    timeout=5.0,
    circuit_breaker=circuit_breaker
)
```

---

### 3. Event Bus Subscriptions

IronDAG includes an asynchronous event bus that publishes lifecycle events during workflow execution.

```python
import asyncio
from irondag import EventBus

async def log_event(event):
    print(f"[Event Log] Topic: {event.topic} | Timestamp: {event.timestamp}")

async def main():
    bus = EventBus()
    # Subscribe to all task-related events using wildcards
    await bus.subscribe("task.*", log_event)
    
    await bus.publish("task.extract_data.started", {"run_id": "run-001"})
    await bus.publish("task.extract_data.completed", {"run_id": "run-001"})

asyncio.run(main())
```

---

### 4. Running the Dashboard

Launch the embedded monitoring interface using the CLI:

```bash
irondag dashboard --port 8088
```

Access the dashboard at `http://localhost:8088` to inspect engine status, active checkpoints, and execution history.

---

## Repository Structure

```
irondag/
├── cli.py                  # CLI entry point
├── core/
│   ├── dag.py              # Graph structure and topological sorting
│   ├── engine.py           # Async execution engine and worker pool
│   └── task.py             # Task definition, retries, and circuit breaker
├── dashboard/
│   └── server.py           # Native HTTP monitoring dashboard
├── event/
│   └── bus.py              # Async pub/sub event bus
├── middleware/
│   └── pipeline.py         # Task execution interceptors
└── storage/
    └── persistence.py      # SQLite persistence and checkpointing
```

---

## Testing

Run the test suite using Python's native test discovery:

```bash
python -m unittest discover tests
```

---

## License

This project is licensed under the MIT License.
