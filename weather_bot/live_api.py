"""Simple built-in ingress API and dashboard."""

from __future__ import annotations

import json
import threading
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


def render_dashboard_html() -> str:
    return """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Weather Bot Control Room</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;700&family=IBM+Plex+Mono:wght@400;500&display=swap" rel="stylesheet">
  <style>
    :root {
      --bg: #071019;
      --bg-soft: #0d2330;
      --panel: rgba(10, 29, 39, 0.86);
      --line: rgba(143, 217, 255, 0.16);
      --line-strong: rgba(143, 217, 255, 0.28);
      --accent: #8fd9ff;
      --accent-2: #99ffc7;
      --warn: #ffd98a;
      --danger: #ff9c8b;
      --ink: #eef8ff;
      --ink-soft: #98b8cc;
      --pill: rgba(255, 255, 255, 0.05);
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      min-height: 100vh;
      color: var(--ink);
      font-family: "Space Grotesk", "Segoe UI", sans-serif;
      background:
        radial-gradient(circle at top left, rgba(66, 195, 255, 0.20), transparent 32%),
        radial-gradient(circle at 85% 15%, rgba(127, 228, 176, 0.15), transparent 24%),
        linear-gradient(160deg, #071018 0%, #0a1822 44%, #102938 100%);
    }
    .shell { max-width: 1380px; margin: 0 auto; padding: 24px; }
    .hero {
      padding: 28px;
      border: 1px solid var(--line);
      border-radius: 28px;
      background: linear-gradient(140deg, rgba(10, 24, 33, 0.95), rgba(12, 32, 44, 0.88));
      box-shadow: 0 24px 80px rgba(0, 0, 0, 0.30);
    }
    .eyebrow {
      color: var(--accent);
      letter-spacing: 0.18em;
      text-transform: uppercase;
      font-size: 12px;
      margin-bottom: 10px;
    }
    h1 {
      margin: 0 0 10px;
      font-size: clamp(34px, 6vw, 60px);
      line-height: 0.94;
    }
    .sub {
      margin: 0;
      max-width: 760px;
      color: var(--ink-soft);
      font-size: 16px;
      line-height: 1.45;
    }
    .statusbar {
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      margin-top: 18px;
    }
    .pill {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      padding: 10px 14px;
      border-radius: 999px;
      border: 1px solid var(--line);
      background: var(--pill);
      color: var(--ink-soft);
      font-size: 13px;
    }
    .pill strong { color: var(--ink); }
    .grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
      gap: 18px;
      margin-top: 20px;
    }
    .two-up {
      display: grid;
      grid-template-columns: minmax(0, 1.15fr) minmax(0, 0.85fr);
      gap: 18px;
      margin-top: 18px;
    }
    .three-up {
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 18px;
      margin-top: 18px;
    }
    .panel {
      padding: 20px;
      border-radius: 22px;
      border: 1px solid var(--line);
      background: var(--panel);
      backdrop-filter: blur(10px);
    }
    .panel h2 {
      margin: 0 0 14px;
      font-size: 13px;
      letter-spacing: 0.18em;
      text-transform: uppercase;
      color: var(--accent);
    }
    .value {
      margin: 0 0 6px;
      font-size: 34px;
      line-height: 1;
      font-weight: 700;
    }
    .muted { color: var(--ink-soft); }
    .mono { font-family: "IBM Plex Mono", monospace; }
    .row {
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      align-items: center;
    }
    .stack { display: grid; gap: 10px; }
    .control-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap: 12px;
    }
    .toggle {
      padding: 14px;
      border-radius: 18px;
      border: 1px solid var(--line);
      background: rgba(255, 255, 255, 0.03);
    }
    .toggle strong { display: block; margin-bottom: 6px; }
    .toggle .row { margin-top: 10px; }
    button, input {
      font: inherit;
    }
    button {
      border: 1px solid var(--line);
      background: rgba(66, 195, 255, 0.08);
      color: var(--ink);
      border-radius: 999px;
      padding: 11px 14px;
      cursor: pointer;
      transition: 120ms ease;
    }
    button:hover {
      border-color: var(--line-strong);
      background: rgba(66, 195, 255, 0.14);
    }
    button.warn {
      background: rgba(255, 217, 138, 0.08);
      border-color: rgba(255, 217, 138, 0.18);
    }
    button.danger {
      background: rgba(255, 156, 139, 0.08);
      border-color: rgba(255, 156, 139, 0.20);
    }
    button.success {
      background: rgba(153, 255, 199, 0.08);
      border-color: rgba(153, 255, 199, 0.20);
    }
    input[type="number"] {
      width: 100%;
      padding: 12px 14px;
      border-radius: 14px;
      border: 1px solid var(--line);
      background: rgba(255, 255, 255, 0.04);
      color: var(--ink);
      outline: none;
    }
    .list { display: grid; gap: 10px; }
    .item {
      border-radius: 16px;
      border: 1px solid rgba(143, 217, 255, 0.10);
      background: rgba(255, 255, 255, 0.03);
      padding: 12px 14px;
    }
    .item strong { display: block; margin-bottom: 5px; }
    .item .meta {
      color: var(--ink-soft);
      font-size: 13px;
      line-height: 1.45;
    }
    .sparkline {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(36px, 1fr));
      gap: 6px;
      align-items: end;
      min-height: 140px;
      margin-top: 12px;
    }
    .bar {
      border-radius: 10px 10px 4px 4px;
      background: linear-gradient(180deg, rgba(143, 217, 255, 0.94), rgba(143, 217, 255, 0.22));
      min-height: 8px;
      position: relative;
    }
    .bar span {
      position: absolute;
      left: 50%;
      bottom: -20px;
      transform: translateX(-50%);
      color: var(--ink-soft);
      font-size: 11px;
      font-family: "IBM Plex Mono", monospace;
    }
    .footer {
      margin-top: 18px;
      color: var(--ink-soft);
      font-size: 13px;
      font-family: "IBM Plex Mono", monospace;
    }
    @media (max-width: 1024px) {
      .two-up, .three-up { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <div class="shell">
    <section class="hero">
      <div class="eyebrow">Home Assistant Ingress</div>
      <h1>Weather Bot Control Room</h1>
      <p class="sub">Home Assistant is the primary operator surface now. Runtime controls, paper capital, scanner toggles, codex jobs, and settlement history all flow through the same control plane that Telegram uses as backup.</p>
      <div class="statusbar" id="statusbar"></div>
    </section>

    <div class="grid" id="cards"></div>

    <div class="two-up">
      <section class="panel">
        <h2>Runtime Controls</h2>
        <div class="control-grid">
          <div class="toggle">
            <strong>Automation</strong>
            <div class="muted">Pause or resume the shared runtime.</div>
            <div class="row">
              <button class="success" onclick="act('start')">Resume</button>
              <button class="warn" onclick="act('stop')">Pause</button>
            </div>
          </div>
          <div class="toggle">
            <strong>Manual Scans</strong>
            <div class="muted">Run the live adapters without waiting for the scheduler.</div>
            <div class="row">
              <button onclick="act('scan_temperature')">Temperature</button>
              <button onclick="act('scan_precipitation')">Precipitation</button>
            </div>
          </div>
          <div class="toggle">
            <strong>Temperature Strategy</strong>
            <div class="muted" id="temp-toggle-state">Waiting for state...</div>
            <div class="row">
              <button class="success" onclick="act('toggle_temperature', true)">Enable</button>
              <button class="danger" onclick="act('toggle_temperature', false)">Disable</button>
            </div>
          </div>
          <div class="toggle">
            <strong>Precipitation Strategy</strong>
            <div class="muted" id="precip-toggle-state">Waiting for state...</div>
            <div class="row">
              <button class="success" onclick="act('toggle_precipitation', true)">Enable</button>
              <button class="danger" onclick="act('toggle_precipitation', false)">Disable</button>
            </div>
          </div>
          <div class="toggle">
            <strong>Paper Auto Trade</strong>
            <div class="muted" id="paper-auto-state">Waiting for state...</div>
            <div class="row">
              <button class="success" onclick="act('toggle_paper_auto_trade', true)">Enable</button>
              <button class="danger" onclick="act('toggle_paper_auto_trade', false)">Disable</button>
            </div>
          </div>
          <div class="toggle">
            <strong>Paper Capital</strong>
            <div class="muted">Reset the available bankroll for learning runs.</div>
            <div class="stack">
              <input id="paper-capital-input" type="number" min="1" step="1" placeholder="5000" />
              <button onclick="setPaperCapital()">Apply Capital</button>
            </div>
          </div>
        </div>
      </section>

      <section class="panel">
        <h2>Codex Sidecar</h2>
        <div class="stack">
          <div class="row">
            <button onclick="act('research_run_now')">Research Refresh</button>
            <button onclick="act('tuner_run_now')">Build Candidate</button>
          </div>
          <div class="row">
            <button class="success" onclick="act('tuner_promote_latest')">Promote Candidate</button>
            <button class="danger" onclick="act('tuner_reject_latest')">Reject Candidate</button>
          </div>
          <div class="list" id="codex-status"></div>
        </div>
      </section>
    </div>

    <div class="three-up">
      <section class="panel">
        <h2>Recent Signals</h2>
        <div id="signals" class="list"></div>
      </section>
      <section class="panel">
        <h2>Recent Trades</h2>
        <div id="trades" class="list"></div>
      </section>
      <section class="panel">
        <h2>Recent Resolutions</h2>
        <div id="resolutions" class="list"></div>
      </section>
    </div>

    <div class="two-up">
      <section class="panel">
        <h2>Control History</h2>
        <div id="actions" class="list"></div>
      </section>
      <section class="panel">
        <h2>Paper PnL Trend</h2>
        <div class="muted">Recent dashboard snapshots from the shared runtime state.</div>
        <div id="history-bars" class="sparkline"></div>
      </section>
    </div>

    <div class="footer" id="footer"></div>
  </div>

  <script>
    function apiUrl(path) {
      return new URL(path, window.location.href).toString();
    }

    async function loadState() {
      const [stateRes, historyRes] = await Promise.all([
        fetch(apiUrl('./api/state')),
        fetch(apiUrl('./api/history'))
      ]);
      const state = await stateRes.json();
      const history = await historyRes.json();
      render(state, history);
    }

    async function act(action, value = null) {
      const res = await fetch(apiUrl('./api/control'), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ action, value })
      });
      const payload = await res.json();
      const historyRes = await fetch(apiUrl('./api/history'));
      const history = await historyRes.json();
      render(payload.state || {}, history);
    }

    function setPaperCapital() {
      const input = document.getElementById('paper-capital-input');
      const value = Number(input.value || 0);
      if (!value) {
        input.focus();
        return;
      }
      act('set_paper_capital', value);
    }

    function card(title, value, meta) {
      return `<section class="panel"><h2>${title}</h2><div class="value">${value}</div><div class="muted">${meta}</div></section>`;
    }

    function renderList(targetId, items, emptyMessage, renderer) {
      const root = document.getElementById(targetId);
      if (!items || !items.length) {
        root.innerHTML = `<div class="muted">${emptyMessage}</div>`;
        return;
      }
      root.innerHTML = items.map(renderer).join('');
    }

    function formatDate(value) {
      if (!value) return 'never';
      const date = new Date(value);
      if (Number.isNaN(date.getTime())) return value;
      return date.toLocaleString();
    }

    function boolText(value) {
      return value ? 'Enabled' : 'Disabled';
    }

    function render(state, history) {
      const summary = state.summary || {};
      const paper = summary.paper || {};
      const controls = state.controls || {};
      const runtime = state.runtime || {};
      const codex = state.codex || {};
      const tuner = state.tuner || {};
      const signalSummary = state.signal_summary_24h || {};
      const totalSignals = Object.values(signalSummary).reduce((acc, value) => acc + Number(value || 0), 0);

      document.getElementById('statusbar').innerHTML = [
        `<div class="pill"><strong>${controls.state || 'unknown'}</strong><span>runtime state</span></div>`,
        `<div class="pill"><strong>${boolText(controls.temperature_enabled)}</strong><span>temperature</span></div>`,
        `<div class="pill"><strong>${boolText(controls.precipitation_enabled)}</strong><span>precipitation</span></div>`,
        `<div class="pill"><strong>${boolText(controls.paper_auto_trade)}</strong><span>paper auto trade</span></div>`,
        `<div class="pill"><strong>${codex.healthy ? 'healthy' : 'waiting'}</strong><span>codex runner</span></div>`,
        `<div class="pill"><strong>${controls.last_message || 'Operator link ready.'}</strong><span>last control</span></div>`
      ].join('');

      document.getElementById('cards').innerHTML = [
        card('Paper Balance', `$${Number(paper.balance || 0).toFixed(2)}`, `Equity $${Number(paper.equity || 0).toFixed(2)} | PnL $${Number(paper.pnl || 0).toFixed(2)}`),
        card('Open Positions', Number(paper.open_positions || 0), `Wins ${paper.wins || 0} | Losses ${paper.losses || 0} | Win rate ${Number(paper.win_rate || 0).toFixed(1)}%`),
        card('Signals / 24h', totalSignals, JSON.stringify(signalSummary || {})),
        card('Last Temp Scan', formatDate(runtime.last_temperature_scan_at), runtime.last_temperature_signal_count ? `${runtime.last_temperature_signal_count} signals flagged` : 'No run yet'),
        card('Last Precip Scan', formatDate(runtime.last_precipitation_scan_at), runtime.last_precipitation_signal_count ? `${runtime.last_precipitation_signal_count} signals flagged` : 'No run yet'),
        card('Codex Queue', Number(codex.queue_depth || 0), codex.last_run ? `${codex.last_run.job_type || 'run'} at ${formatDate(codex.last_run.finished_at)}` : 'No completed sidecar runs yet')
      ].join('');

      document.getElementById('temp-toggle-state').textContent = controls.temperature_enabled ? 'Currently enabled.' : 'Currently disabled.';
      document.getElementById('precip-toggle-state').textContent = controls.precipitation_enabled ? 'Currently enabled.' : 'Currently disabled.';
      document.getElementById('paper-auto-state').textContent = controls.paper_auto_trade ? 'Automatic entries are enabled.' : 'Automatic entries are disabled.';

      renderList(
        'codex-status',
        [
          { label: 'Runner', value: codex.healthy ? 'Healthy' : 'Not reporting', meta: codex.last_heartbeat_at ? `Heartbeat ${formatDate(codex.last_heartbeat_at)}` : 'No heartbeat yet' },
          { label: 'Queue depth', value: Number(codex.queue_depth || 0), meta: codex.active_run ? `Active ${codex.active_run.job_type || 'run'}` : 'No active sidecar job' },
          { label: 'Latest candidate', value: tuner.candidate_status || 'none', meta: (tuner.latest_candidate || {}).candidate_id || 'No candidate proposal yet' }
        ],
        'Codex sidecar unavailable.',
        item => `<div class="item"><strong>${item.label}: ${item.value}</strong><div class="meta mono">${item.meta}</div></div>`
      );

      renderList(
        'signals',
        state.recent_signals || [],
        'No recent signals yet.',
        signal => `<div class="item"><strong>${signal.event_title || signal.market_slug}</strong><div class="meta mono">${signal.market_type} | ${signal.direction} | score ${Number(signal.score || 0).toFixed(2)} | edge ${(Number(signal.edge || 0) * 100).toFixed(1)}%</div></div>`
      );

      renderList(
        'trades',
        state.recent_trades || [],
        'No paper trades yet.',
        trade => `<div class="item"><strong>${trade.market_slug}</strong><div class="meta mono">${trade.direction} | ${trade.status} | cost $${Number(trade.cost || 0).toFixed(2)} | pnl ${trade.realized_pnl == null ? 'open' : '$' + Number(trade.realized_pnl).toFixed(2)}</div></div>`
      );

      renderList(
        'resolutions',
        state.recent_resolutions || [],
        'No settled markets yet.',
        item => `<div class="item"><strong>${item.market_slug}</strong><div class="meta mono">${item.resolution} | positions ${item.resolved_positions} | payout $${Number(item.total_payout || 0).toFixed(2)} | pnl $${Number(item.total_realized_pnl || 0).toFixed(2)}</div></div>`
      );

      renderList(
        'actions',
        state.recent_operator_actions || [],
        'No operator actions recorded yet.',
        item => `<div class="item"><strong>${item.action}</strong><div class="meta mono">${formatDate(item.created_at)} | ${(item.payload || {}).message || 'No message'}</div></div>`
      );

      const bars = (history || []).slice(-18);
      const maxAbs = Math.max(1, ...bars.map(item => Math.abs(Number(item.paper_pnl || 0))));
      document.getElementById('history-bars').innerHTML = bars.length ? bars.map((item, index) => {
        const pnl = Number(item.paper_pnl || 0);
        const height = Math.max(8, Math.round((Math.abs(pnl) / maxAbs) * 110));
        return `<div class="bar" style="height:${height}px; opacity:${0.45 + (index / Math.max(bars.length, 1)) * 0.5}"><span>${pnl.toFixed(0)}</span></div>`;
      }).join('') : '<div class="muted">History will appear after a few refresh cycles.</div>';

      document.getElementById('footer').textContent = `Updated ${formatDate(state.timestamp_utc)} | Last control: ${controls.last_action || 'none'} | Dashboard port mirrors HA ingress`;
    }

    loadState();
    setInterval(loadState, 5000);
  </script>
</body>
</html>"""


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
