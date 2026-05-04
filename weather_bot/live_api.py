"""Simple built-in ingress API and dashboard."""

from __future__ import annotations

import json
import shutil
import threading
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlsplit


_DASHBOARD_PATH = Path(__file__).with_name("live_api_dashboard.html")
_LATEST_EXPORTS = {
    "bundle": (
        "latest_analysis_bundle_path",
        "application/zip",
    ),
    "zip": (
        "latest_analysis_bundle_path",
        "application/zip",
    ),
    "report": (
        "latest_analysis_report_path",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    ),
    "xlsx": (
        "latest_analysis_report_path",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    ),
    "excel": (
        "latest_analysis_report_path",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    ),
    "index": (
        "latest_analysis_index_path",
        "application/json; charset=utf-8",
    ),
    "json": (
        "latest_analysis_index_path",
        "application/json; charset=utf-8",
    ),
}


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
                route = urlsplit(self.path).path
                if route in {"/", "/index.html"}:
                    self._send_text(render_dashboard_html(), content_type="text/html; charset=utf-8")
                    return
                if route == "/health":
                    self._send_json({"status": "ok"})
                    return
                if route == "/api/state":
                    self._send_json(dashboard_state.get_state_threadsafe())
                    return
                if route == "/api/history":
                    self._send_json(dashboard_state.get_history_threadsafe())
                    return
                if route == "/api/export/latest":
                    self._send_latest_export("report")
                    return
                if route.startswith("/api/export/latest/"):
                    self._send_latest_export(route.rsplit("/", 1)[-1])
                    return
                self._send_json({"error": "not_found"}, status=HTTPStatus.NOT_FOUND)

            def do_POST(self) -> None:  # noqa: N802
                parts = urlsplit(self.path)
                route = parts.path
                if route != "/api/control" and not route.startswith("/api/control/"):
                    self._send_json({"error": "not_found"}, status=HTTPStatus.NOT_FOUND)
                    return
                length = int(self.headers.get("Content-Length", "0") or 0)
                body = self.rfile.read(length) if length else b"{}"
                try:
                    payload = json.loads(body.decode("utf-8"))
                except json.JSONDecodeError:
                    self._send_json({"ok": False, "message": "Invalid JSON body."}, status=HTTPStatus.BAD_REQUEST)
                    return
                if not isinstance(payload, dict):
                    payload = {}
                for key, values in parse_qs(parts.query, keep_blank_values=True).items():
                    if not values or key in payload:
                        continue
                    payload[key] = values[-1]
                path_action = _control_action_from_path(route)
                if path_action and not payload.get("action"):
                    payload["action"] = path_action
                try:
                    response = dashboard_state.apply_control_threadsafe(payload)
                except Exception as exc:
                    self._send_json(
                        {
                            "ok": False,
                            "status": 500,
                            "message": f"Control handler crashed: {type(exc).__name__}: {exc}",
                        },
                        status=HTTPStatus.INTERNAL_SERVER_ERROR,
                    )
                    return
                self._send_json(response, status=HTTPStatus(int(response.get("status", 200))))

            def _send_text(self, text: str, *, content_type: str) -> None:
                payload = text.encode("utf-8")
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Length", str(len(payload)))
                self._send_cache_headers()
                self.end_headers()
                self.wfile.write(payload)

            def _send_json(self, payload: dict | list, *, status: HTTPStatus = HTTPStatus.OK) -> None:
                data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(data)))
                self._send_cache_headers()
                self.end_headers()
                self.wfile.write(data)

            def _send_latest_export(self, kind: str) -> None:
                normalized = unquote(str(kind or "report")).strip().lower()
                spec = _LATEST_EXPORTS.get(normalized)
                if spec is None:
                    self._send_json(
                        {
                            "error": "unknown_export",
                            "message": "Unknown export type. Use report, bundle, or index.",
                        },
                        status=HTTPStatus.BAD_REQUEST,
                    )
                    return
                export_key, content_type = spec
                exports = _current_export_status(dashboard_state)
                path_value = exports.get(export_key)
                if not path_value:
                    self._send_json(
                        {
                            "error": "export_missing",
                            "message": f"No latest {normalized} export path is available yet. Run Export XLSX first.",
                        },
                        status=HTTPStatus.NOT_FOUND,
                    )
                    return
                path = Path(str(path_value))
                if not path.exists() or not path.is_file():
                    self._send_json(
                        {
                            "error": "export_missing",
                            "message": f"Latest {normalized} export file is missing on disk. Run Export XLSX again.",
                            "path": str(path),
                        },
                        status=HTTPStatus.NOT_FOUND,
                    )
                    return
                self._send_file(path, content_type=content_type)

            def _send_file(self, path: Path, *, content_type: str) -> None:
                filename = _attachment_filename(path.name)
                size = path.stat().st_size
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Length", str(size))
                self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
                self._send_cache_headers()
                self.end_headers()
                with path.open("rb") as handle:
                    shutil.copyfileobj(handle, self.wfile)

            def _send_cache_headers(self) -> None:
                self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
                self.send_header("Pragma", "no-cache")
                self.send_header("Expires", "0")

        return Handler


def _control_action_from_path(path: str) -> str:
    prefix = "/api/control/"
    if not path.startswith(prefix):
        return ""
    return unquote(path[len(prefix):]).strip().lower()


def _current_export_status(dashboard_state) -> dict:
    state = dashboard_state.get_state_threadsafe()
    exports = dict((state or {}).get("exports") or {})
    if exports:
        return exports
    analysis_exporter = getattr(dashboard_state, "analysis_exporter", None)
    if analysis_exporter is not None:
        return dict(analysis_exporter.status())
    return {}


def _attachment_filename(filename: str) -> str:
    return "".join(ch for ch in str(filename or "export") if ch.isalnum() or ch in {" ", ".", "_", "-"}).strip() or "export"
