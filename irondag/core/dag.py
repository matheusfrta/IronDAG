from collections import defaultdict, deque
from typing import Dict, List, Set
from irondag.core.task import Task


class DAGCycleException(Exception):
    pass


class DAG:
    def __init__(self, name: str = "default_dag"):
        self.name = name
        self.tasks: Dict[str, Task] = {}
        self.graph: Dict[str, List[str]] = defaultdict(list)
        self.in_degree: Dict[str, int] = defaultdict(int)

    def add_task(self, task: Task) -> 'DAG':
        if task.name in self.tasks:
            raise ValueError(f"Task '{task.name}' already exists in DAG '{self.name}'")
        self.tasks[task.name] = task
        if task.name not in self.in_degree:
            self.in_degree[task.name] = 0
        for dep in task.dependencies:
            self.add_dependency(parent=dep, child=task.name)
        return self

    def add_dependency(self, parent: str, child: str):
        if parent not in self.tasks:
            self.in_degree[parent] = self.in_degree.get(parent, 0)
        self.graph[parent].append(child)
        self.in_degree[child] = self.in_degree.get(child, 0) + 1
        self.validate()

    def validate(self):
        in_degree_copy = dict(self.in_degree)
        for node in list(self.tasks.keys()):
            if node not in in_degree_copy:
                in_degree_copy[node] = 0

        queue = deque([node for node, deg in in_degree_copy.items() if deg == 0])
        visited_count = 0

        while queue:
            curr = queue.popleft()
            visited_count += 1
            for neighbor in self.graph[curr]:
                in_degree_copy[neighbor] -= 1
                if in_degree_copy[neighbor] == 0:
                    queue.append(neighbor)

        if visited_count < len(in_degree_copy):
            raise DAGCycleException(f"Cycle detected in DAG '{self.name}'")

    def get_execution_layers(self) -> List[List[Task]]:
        self.validate()
        in_degree_copy = {node: self.in_degree[node] for node in self.tasks}
        queue = deque([node for node, deg in in_degree_copy.items() if deg == 0])
        layers = []

        while queue:
            layer_size = len(queue)
            current_layer = []
            for _ in range(layer_size):
                curr = queue.popleft()
                current_layer.append(self.tasks[curr])
                for neighbor in self.graph[curr]:
                    if neighbor in in_degree_copy:
                        in_degree_copy[neighbor] -= 1
                        if in_degree_copy[neighbor] == 0:
                            queue.append(neighbor)
            layers.append(current_layer)

        return layers