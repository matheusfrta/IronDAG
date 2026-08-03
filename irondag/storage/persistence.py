import sqlite3
import json
import os
from pathlib import Path
from typing import Dict, Any, List, Optional


class SQLiteStore:
    def __init__(self, db_path: str = "irondag.db"):
        self.db_path = db_path
        self._init_db()

    def _get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self):
        with self._get_connection() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS workflow_runs (
                    run_id TEXT PRIMARY KEY,
                    dag_name TEXT NOT NULL,
                    state TEXT NOT NULL,
                    context TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS task_checkpoints (
                    run_id TEXT NOT NULL,
                    task_name TEXT NOT NULL,
                    state TEXT NOT NULL,
                    output TEXT,
                    error TEXT,
                    attempts INTEGER DEFAULT 1,
                    execution_time_ms REAL DEFAULT 0.0,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (run_id, task_name)
                )
            """)
            conn.commit()

    def save_workflow_run(self, run_id: str, dag_name: str, state: str, context: Dict[str, Any]):
        with self._get_connection() as conn:
            conn.execute("""
                INSERT INTO workflow_runs (run_id, dag_name, state, context)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(run_id) DO UPDATE SET
                    state=excluded.state,
                    context=excluded.context,
                    updated_at=CURRENT_TIMESTAMP
            """, (run_id, dag_name, state, json.dumps(context, default=str)))
            conn.commit()

    def save_task_checkpoint(
        self,
        run_id: str,
        task_name: str,
        state: str,
        output: Any = None,
        error: Optional[str] = None,
        attempts: int = 1,
        execution_time_ms: float = 0.0
    ):
        with self._get_connection() as conn:
            conn.execute("""
                INSERT INTO task_checkpoints (run_id, task_name, state, output, error, attempts, execution_time_ms)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(run_id, task_name) DO UPDATE SET
                    state=excluded.state,
                    output=excluded.output,
                    error=excluded.error,
                    attempts=excluded.attempts,
                    execution_time_ms=excluded.execution_time_ms,
                    updated_at=CURRENT_TIMESTAMP
            """, (
                run_id,
                task_name,
                state,
                json.dumps(output, default=str) if output is not None else None,
                error,
                attempts,
                execution_time_ms
            ))
            conn.commit()

    def get_workflow_run(self, run_id: str) -> Optional[Dict[str, Any]]:
        with self._get_connection() as conn:
            cursor = conn.execute("SELECT * FROM workflow_runs WHERE run_id = ?", (run_id,))
            row = cursor.fetchone()
            if not row:
                return None
            return {
                "run_id": row["run_id"],
                "dag_name": row["dag_name"],
                "state": row["state"],
                "context": json.loads(row["context"]),
                "created_at": row["created_at"],
                "updated_at": row["updated_at"]
            }

    def list_task_checkpoints(self, run_id: str) -> List[Dict[str, Any]]:
        with self._get_connection() as conn:
            cursor = conn.execute("SELECT * FROM task_checkpoints WHERE run_id = ?", (run_id,))
            rows = cursor.fetchall()
            return [
                {
                    "run_id": r["run_id"],
                    "task_name": r["task_name"],
                    "state": r["state"],
                    "output": json.loads(r["output"]) if r["output"] else None,
                    "error": r["error"],
                    "attempts": r["attempts"],
                    "execution_time_ms": r["execution_time_ms"],
                    "updated_at": r["updated_at"]
                }
                for r in rows
            ]