import argparse
import asyncio
from irondag.core.dag import DAG
from irondag.core.task import Task
from irondag.core.engine import IronEngine
from irondag.dashboard.server import DashboardServer


def sample_workflow():
    async def step_a():
        return "Data loaded"

    async def step_b(step_a):
        return f"Processed: {step_a}"

    dag = DAG("sample_pipeline")
    dag.add_task(Task(name="step_a", func=step_a))
    dag.add_task(Task(name="step_b", func=step_b, dependencies=["step_a"]))

    engine = IronEngine()
    results = asyncio.run(engine.run_dag(dag))
    print(f"Workflow finished with results: {results}")


def main():
    parser = argparse.ArgumentParser(description="IronDAG Orchestrator CLI")
    subparsers = parser.add_subparsers(dest="command")

    run_parser = subparsers.add_parser("run", help="Run a sample pipeline")
    dash_parser = subparsers.add_parser("dashboard", help="Start the zero-dependency web dashboard")
    dash_parser.add_argument("--port", type=int, default=8088, help="Port to listen on")

    args = parser.parse_args()

    if args.command == "run":
        sample_workflow()
    elif args.command == "dashboard":
        server = DashboardServer(port=args.port)
        server.start()
        try:
            while True:
                pass
        except KeyboardInterrupt:
            server.stop()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()