from __future__ import annotations

import os
import socket
import time
from threading import Event, Lock, Thread

from mitmproxy import http


def _log(message: str) -> None:
    print(f"[ProxyFlow] {message}", flush=True)


class ProxyLoggerAddon:
    def __init__(self) -> None:
        self._idle_lock = Lock()
        self._flow_lock = Lock()
        self._last_activity = time.monotonic()
        self._last_idle_report = 0.0
        self._stop_event = Event()
        self._watcher: Thread | None = None
        self._upload_bytes = 0
        self._download_bytes = 0
        self._live_flows: dict[str, object] = {}

    def load(self, loader) -> None:
        _log("日志插件已加载")
        self._watcher = Thread(target=self._watch_idle_timeout, daemon=True)
        self._watcher.start()

    def running(self) -> None:
        _log("代理运行中")

    def _mark_activity(self) -> None:
        with self._idle_lock:
            self._last_activity = time.monotonic()
            self._last_idle_report = 0.0

    def _track_flow(self, flow) -> None:
        flow_id = getattr(flow, "id", "")
        if not flow_id:
            return
        with self._flow_lock:
            self._live_flows[flow_id] = flow

    def _cleanup_flows(self) -> None:
        with self._flow_lock:
            self._live_flows = {
                flow_id: flow
                for flow_id, flow in self._live_flows.items()
                if getattr(flow, "live", False)
            }

    def _kill_active_flows(self) -> None:
        with self._flow_lock:
            flows = list(self._live_flows.values())
        killed = 0
        for flow in flows:
            if not getattr(flow, "killable", False):
                continue
            try:
                flow.kill()
                killed += 1
            except Exception:
                continue
        self._cleanup_flows()
        _log(f"kill active flows: {killed}")

    def _watch_idle_timeout(self) -> None:
        while not self._stop_event.wait(0.5):
            with self._idle_lock:
                now = time.monotonic()
                if now - self._last_activity < 5.0:
                    continue
                if now - self._last_idle_report < 60.0:
                    continue
                self._last_idle_report = now
            self._kill_active_flows()
            self._report_control_event("IDLE")
            _log("超过 60 秒没有数据流出")

    def _get_selected_access_token(self) -> str:
        port_text = os.environ.get("AUTOLOAD_CONTROL_PORT", "").strip()
        if not port_text:
            return ""
        try:
            port = int(port_text)
        except ValueError:
            return ""
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2) as conn:
                conn.settimeout(0.2)
                data = conn.recv(4096)
        except OSError:
            return ""
        return data.decode("utf-8", errors="ignore").strip()

    def _rewrite_bearer_headers(self, flow: http.HTTPFlow, access_token: str) -> None:
        if not access_token:
            return
        headers = flow.request.headers
        for key, value in list(headers.items()):
            if not isinstance(value, str) or not value.startswith("Bearer "):
                continue
            headers[key] = f"Bearer {access_token}"

    def _extract_bearer_token(self, flow: http.HTTPFlow) -> str:
        for value in flow.request.headers.values():
            if not isinstance(value, str) or not value.startswith("Bearer "):
                continue
            return value.removeprefix("Bearer ").strip()
        return ""

    def _estimate_http_bytes(self, headers, body) -> int:
        total = 0
        for key, value in headers.items():
            total += len(str(key).encode("utf-8", errors="ignore"))
            total += len(str(value).encode("utf-8", errors="ignore"))
            total += 4
        if body:
            total += len(body)
        return total

    def _report_traffic(self) -> None:
        self._report_control_event(f"TRAFFIC {self._upload_bytes} {self._download_bytes}")

    def _report_access_token_used(self, access_token: str) -> None:
        port_text = os.environ.get("AUTOLOAD_CONTROL_PORT", "").strip()
        if not port_text:
            return
        try:
            port = int(port_text)
        except ValueError:
            return
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2) as conn:
                conn.sendall(f"USED {access_token}\n".encode("utf-8"))
        except OSError:
            return

    def _report_control_event(self, event_name: str) -> None:
        port_text = os.environ.get("AUTOLOAD_CONTROL_PORT", "").strip()
        if not port_text:
            return
        try:
            port = int(port_text)
        except ValueError:
            return
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2) as conn:
                conn.sendall(f"{event_name}\n".encode("utf-8"))
        except OSError:
            return

    def client_connected(self, *args, **kwargs) -> None:
        self._mark_activity()
        return None

    def client_disconnected(self, *args, **kwargs) -> None:
        self._mark_activity()
        return None

    def server_connect(self, *args, **kwargs) -> None:
        self._mark_activity()
        return None

    def server_connected(self, *args, **kwargs) -> None:
        self._mark_activity()
        return None

    def tls_established_client(self, *args, **kwargs) -> None:
        self._mark_activity()
        return None

    def tls_established_server(self, *args, **kwargs) -> None:
        self._mark_activity()
        return None

    def tcp_start(self, flow) -> None:
        self._mark_activity()
        self._track_flow(flow)
        return None

    def tcp_end(self, flow) -> None:
        self._mark_activity()
        self._cleanup_flows()
        return None

    def request(self, flow: http.HTTPFlow) -> None:
        self._mark_activity()
        self._track_flow(flow)
        self._cleanup_flows()
        original_token = self._extract_bearer_token(flow)
        selected_token = self._get_selected_access_token()
        if selected_token:
            self._rewrite_bearer_headers(flow, selected_token)
        usage_token = selected_token or original_token
        if usage_token:
            self._report_access_token_used(usage_token)
        self._upload_bytes += self._estimate_http_bytes(flow.request.headers, flow.request.raw_content)
        self._report_traffic()

    def response(self, flow: http.HTTPFlow) -> None:
        self._mark_activity()
        self._track_flow(flow)
        self._cleanup_flows()
        resp = flow.response
        if resp is None:
            return
        self._download_bytes += self._estimate_http_bytes(resp.headers, resp.raw_content)
        self._report_traffic()

    def error(self, flow: http.HTTPFlow) -> None:
        self._mark_activity()
        self._track_flow(flow)
        self._cleanup_flows()
        _log(f"错误: {flow.error}")

    def websocket_end(self, flow: http.HTTPFlow) -> None:
        self._mark_activity()
        self._cleanup_flows()

    def done(self) -> None:
        self._stop_event.set()
        _log("代理插件退出")

addons = [ProxyLoggerAddon()]
