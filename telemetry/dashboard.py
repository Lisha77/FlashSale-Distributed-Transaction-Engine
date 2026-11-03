"""
telemetry/dashboard.py
Tiny stdlib-only web server: streams bus events to the browser (Server-Sent Events)
and lets the page start a flash sale. No pip installs needed.
"""
import json
import os
import queue
import threading
import webbrowser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from .engine_events import BUS

HERE = os.path.dirname(os.path.abspath(__file__))


def _enc(o):
    return o.item() if hasattr(o, "item") else str(o)   # numpy -> python


class Hub:
    def __init__(self):
        self.history, self.clients, self.seq = [], [], 0
        self.lock = threading.Lock()
        BUS.subscribe(self.publish)

    def publish(self, ev):
        with self.lock:
            self.seq += 1
            ev = dict(ev, seq=self.seq)
            self.history.append(ev)
            del self.history[:-30000]
            for q in self.clients:
                q.put(ev)

    def reset(self):
        with self.lock:
            self.history.clear()
        BUS.emit({"event": "RESET"})

    def attach(self):
        q = queue.Queue()
        with self.lock:
            for ev in self.history:
                q.put(ev)
            self.clients.append(q)
        return q

    def detach(self, q):
        with self.lock:
            if q in self.clients:
                self.clients.remove(q)


def serve(runner, port=8000, open_browser=True):
    """runner(cfg: dict) runs one simulation. cfg comes from the page, e.g. {"tick": 0.3}."""
    hub, state = Hub(), {"running": False}

    def job(cfg):
        try:
            runner(cfg)
        finally:
            state["running"] = False

    class H(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def do_GET(self):
            if self.path == "/events":
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream")
                self.send_header("Cache-Control", "no-cache")
                self.end_headers()
                q = hub.attach()
                try:
                    while True:
                        try:
                            ev = q.get(timeout=15)
                            self.wfile.write(f"data: {json.dumps(ev, default=_enc)}\n\n".encode())
                        except queue.Empty:
                            self.wfile.write(b": ping\n\n")
                        self.wfile.flush()
                except (BrokenPipeError, ConnectionResetError, OSError):
                    pass
                finally:
                    hub.detach(q)
            else:
                body = open(os.path.join(HERE, "dashboard.html"), "rb").read()
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.end_headers()
                self.wfile.write(body)

        def do_POST(self):
            n = int(self.headers.get("Content-Length", 0))
            cfg = json.loads(self.rfile.read(n) or b"{}")
            if self.path == "/api/start" and not state["running"]:
                state["running"] = True
                hub.reset()
                threading.Thread(target=job, daemon=True,
                                 args=(cfg,)).start()
                self.send_response(202)
            else:
                self.send_response(409)
            self.end_headers()

    srv = ThreadingHTTPServer(("127.0.0.1", port), H)
    srv.daemon_threads = True
    url = f"http://127.0.0.1:{port}"
    print(f"Live dashboard: {url}  (Ctrl+C to stop)")
    if open_browser:
        webbrowser.open(url)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass