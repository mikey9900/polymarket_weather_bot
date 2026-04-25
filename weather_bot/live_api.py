"""Simple built-in ingress API and dashboard."""

from __future__ import annotations

import json
import threading
from functools import lru_cache
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


_DASHBOARD_PATH = Path(__file__).with_name("live_api_dashboard.html")


@lru_cache(maxsize=1)
def render_dashboard_html() -> str:
    return _DASHBOARD_PATH.read_text(encoding="utf-8")


class LiveApiServer:
    def __init__(self, dashboard_state, host: str = "0.0.0.0", port: int = 8099):
        self.dashboard_state = dashboard_state
        self.host = host
        self.port = int(port)
        self._server: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None

    def start_threaded(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        handler = self._handler_type()
        self._server = ThreadingHTTPServer((self.host, self.port), handler)
        self._thread = threading.Thread(target=self._server.serve_forever, name="weather-live-api", daemon=True)
        self._thread.start()

    def stop_threaded(self) -> None:
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
        if self._thread is not None:
            self._thread.join(timeout=5.0)
        self._server = None
        self._thread = None

    def _handler_type(self):
        dashboard_state = self.dashboard_state

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, format: str, *args) -> None:  # noqa: A003
                return

            def do_GET(self) -> None:  # noqa: N802
                if self.path in {"/", "/index.html"}:
                    self._send_text(render_dashboard_html(), content_type="text/html; charset=utf-8")
                    return
                if self.path == "/health":
                    self._send_json({"status": "ok"})
                    return
                if self.path == "/api/state":
                    self._send_json(dashboard_state.get_state_threadsafe())
                    return
                if self.path == "/api/history":
                    self._send_json(dashboard_state.get_history_threadsafe())
                    return
                self._send_json({"error": "not_found"}, status=HTTPStatus.NOT_FOUND)

            def do_POST(self) -> None:  # noqa: N802
                if self.path != "/api/control":
                    self._send_json({"error": "not_found"}, status=HTTPStatus.NOT_FOUND)
                    return
                length = int(self.headers.get("Content-Length", "0") or 0)
                body = self.rfile.read(length) if length else b"{}"
                try:
                    payload = json.loads(body.decode("utf-8"))
                except json.JSONDecodeError:
                    self._send_json({"ok": False, "message": "Invalid JSON body."}, status=HTTPStatus.BAD_REQUEST)
                    return
                response = dashboard_state.apply_control_threadsafe(payload)
                self._send_json(response, status=HTTPStatus(int(response.get("status", 200))))

            def _send_text(self, text: str, *, content_type: str) -> None:
                payload = text.encode("utf-8")
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def _send_json(self, payload: dict | list, *, status: HTTPStatus = HTTPStatus.OK) -> None:
                data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

        return Handler
