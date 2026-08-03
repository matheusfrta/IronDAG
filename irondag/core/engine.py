import asyncio
import uuid
from typing import Dict, Any, Optional
from irondag.core.dag import DAG
from irondag.core.task import TaskState, TaskResult
from irondag.event.bus import EventBus
from irondag.storage.persistence import SQLiteStore
from irondag.middleware.pipeline import MiddlewarePipeline


class IronEngine:
    def __init__(
        self,
        event_bus: Optional[EventBus] = None,
        store: Optional[SQLiteStore] = None,
        pipeline: Optional[MiddlewarePipeline] = None,
        max_concurrency: int = 10
    ):
        self.event_bus = event_bus or EventBus()
        self.store = store or SQLiteStore()
        self.pipeline = pipeline or MiddlewarePipeline()
        self.semaphore = asyncio.Semaphore(max_concurrency)

    async def run_dag(self, dag: DAG, initial_context: Optional[Dict[str, Any]] = None) -> Dict[str, TaskResult]:
        run_id = str(uuid.uuid4())[:8]
        context = dict(initial_context or {})
        results: Dict[str, TaskResult] = {}

        self.store.save_workflow_run(run_id, dag.name, "RUNNING", context)
        await self.event_bus.publish(f"workflow.{dag.name}.started", {"run_id": run_id})

        layers = dag.get_execution_layers()

        for layer in layers:
            async def run_single_task(task):
                async with self.semaphore:
                    await self.pipeline.execute_before(task, context)
                    await self.event_bus.publish(f"task.{task.name}.started", {"run_id": run_id})

                    res = await task.execute(context)

                    if res.state == TaskState.COMPLETED and res.output is not None:
                        context[task.name] = res.output

                    self.store.save_task_checkpoint(
                        run_id=run_id,
                        task_name=task.name,
                        state=res.state.value,
                        output=res.output,
                        error=res.error,
                        attempts=res.attempts,
                        execution_time_ms=res.execution_time_ms
                    )

                    await self.pipeline.execute_after(task, res, context)
                    await self.event_bus.publish(f"task.{task.name}.{res.state.value.lower()}", {
                        "run_id": run_id,
                        "result": res
                    })

                    return task.name, res

            task_coros = [run_single_task(task) for task in layer]
            layer_results = await asyncio.gather(*task_coros, return_exceptions=True)

            for item in layer_results:
                if isinstance(item, tuple):
                    name, res = item
                    results[name] = res

        failed_tasks = [name for name, res in results.items() if res.state == TaskState.FAILED]
        final_state = "FAILED" if failed_tasks else "COMPLETED"

        self.store.save_workflow_run(run_id, dag.name, final_state, context)
        await self.event_bus.publish(f"workflow.{dag.name}.{final_state.lower()}", {
            "run_id": run_id,
            "results": results
        })

        return results