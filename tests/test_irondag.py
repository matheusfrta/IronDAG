import unittest
import asyncio
from irondag.core.task import Task, TaskState, CircuitBreaker
from irondag.core.dag import DAG, DAGCycleException
from irondag.core.engine import IronEngine
from irondag.event.bus import EventBus
from irondag.storage.persistence import SQLiteStore


class TestIronDAG(unittest.TestCase):
    def test_dag_topological_sort(self):
        dag = DAG("test_dag")
        dag.add_task(Task(name="a", func=lambda: "a"))
        dag.add_task(Task(name="b", func=lambda: "b", dependencies=["a"]))
        layers = dag.get_execution_layers()
        self.assertEqual(len(layers), 2)
        self.assertEqual(layers[0][0].name, "a")
        self.assertEqual(layers[1][0].name, "b")

    def test_dag_cycle_detection(self):
        dag = DAG("cycle_dag")
        dag.add_task(Task(name="a", func=lambda: "a"))
        dag.add_task(Task(name="b", func=lambda: "b"))
        dag.add_dependency("a", "b")
        with self.assertRaises(DAGCycleException):
            dag.add_dependency("b", "a")

    def test_task_retries_and_failure(self):
        calls = 0

        def failing_func():
            nonlocal calls
            calls += 1
            raise ValueError("Failure")

        task = Task(name="fail_task", func=failing_func, retries=2, retry_delay=0.01)
        res = asyncio.run(task.execute({}))
        self.assertEqual(res.state, TaskState.FAILED)
        self.assertEqual(calls, 3)

    def test_engine_execution(self):
        dag = DAG("exec_dag")
        dag.add_task(Task(name="t1", func=lambda: 10))
        dag.add_task(Task(name="t2", func=lambda t1: t1 * 2, dependencies=["t1"]))

        engine = IronEngine()
        results = asyncio.run(engine.run_dag(dag))
        self.assertEqual(results["t1"].output, 10)
        self.assertEqual(results["t2"].output, 20)


if __name__ == "__main__":
    unittest.main()