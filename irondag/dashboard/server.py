import http.server
import socketserver
import json
import threading
from typing import Optional
from irondag.storage.persistence import SQLiteStore


class DashboardHTTPHandler(http.server.SimpleHTTPRequestHandler):
    store: Optional[SQLiteStore] = None

    def do_GET(self):
        if self.path == "/" or self.path == "/index.html":
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            html = """
            <!DOCTYPE html>
            <html>
            <head>
                <title>IronDAG Monitoring Dashboard</title>
                <style>
                    body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; margin: 0; padding: 20px; background: #0f172a; color: #f8fafc; }
                    h1 { color: #38bdf8; border-bottom: 2px solid #1e293b; padding-bottom: 10px; }
                    .card { background: #1e293b; border-radius: 8px; padding: 20px; margin-bottom: 20px; box-shadow: 0 4px 6px rgba(0,0,0,0.3); }
                    table { width: 100%; border-collapse: collapse; margin-top: 10px; }
                    th, td { text-align: left; padding: 12px; border-bottom: 1px solid #334155; }
                    th { color: #94a3b8; }
                    .badge { padding: 4px 8px; border-radius: 4px; font-weight: bold; font-size: 12px; }
                    .badge-COMPLETED { background: #059669; color: white; }
                    .badge-FAILED { background: #dc2626; color: white; }
                    .badge-RUNNING { background: #d97706; color: white; }
                </style>
            </head>
            <body>
                <h1>⚡ IronDAG Orchestrator Dashboard</h1>
                <div class="card">
                    <h2>Engine Status</h2>
                    <p>Status: <span class="badge badge-COMPLETED">ACTIVE</span></p>
                    <p>Database: <code>irondag.db</code></p>
                </div>
                <div class="card">
                    <h2>Live Activity</h2>
                    <p>Monitoring workflows, event bus, and checkpoints in real-time.</p>
                </div>
            </body>
            </html>
            """
            self.wfile.write(html.encode("utf-8"))
            return

        if self.path == "/api/status":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            data = {"status": "ok", "service": "IronDAG Engine"}
            self.wfile.write(json.dumps(data).encode("utf-8"))
            return

        self.send_error(404, "Not Found")


class DashboardServer:
    def __init__(self, port: int = 8088, store: Optional[SQLiteStore] = None):
        self.port = port
        self.store = store or SQLiteStore()
        self.server: Optional[socketserver.TCPServer] = None
        self.thread: Optional[threading.Thread] = None

    def start(self):
        handler = lambda *args, **kwargs: DashboardHTTPHandler(*args, store=self.store, **kwargs)
        socketserver.TCPServer.allow_reuse_address = True
        self.server = socketserver.TCPServer(("0.0.0.0", self.port), handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        print(f"IronDAG Dashboard server active on http://localhost:{self.port}")

    def stop(self):
        if self.server:
            self.server.shutdown()
            self.server.server_close()