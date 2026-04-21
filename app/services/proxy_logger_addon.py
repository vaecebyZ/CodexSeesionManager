from __future__ import annotations

import logging
import os
import socket
import time
from threading import Event, Lock, Thread

from mitmproxy import http


_RESELECT_EVENT = "RESELECT"
_IDLE_TIMEOUT_EVENT = "IDLE_TIMEOUT"


class _PingPongLogHandler(logging.Handler):
    def __init__(self, addon: "ProxyLoggerAddon") -> None:
        super().__init__(level=logging.INFO)
        self._addon = addon

    def emit(self, record: logging.LogRecord) -> None:
        try:
            message = record.getMessage()
        except Exception:
            return
        if "Received WebSocket ping from" not in message and "Received WebSocket pong from" not in message:
            return
        self._addon.handle_ping_pong_log()


def _log(message: str) -> None:
    print(f"[ProxyFlow] {message}", flush=True)


class ProxyLoggerAddon:
    def __init__(self) -> None:
        self._idle_lock = Lock()
        self._flow_lock = Lock()
        self._last_activity = time.monotonic()
        self._reselect_sent = False
        self._idle_kill_sent = False
        self._stop_event = Event()
        self._watcher: Thread | None = None
        self._idle_watcher: Thread | None = None
        self._log_handler: _PingPongLogHandler | None = None
        self._websocket_logger: logging.Logger | None = None
        self._websocket_logger_level: int | None = None
        self._upload_bytes = 0
        self._download_bytes = 0
        self._live_flows: dict[str, object] = {}

    def load(self, loader) -> None:
        _log("日志插件已加载")
        self._log_handler = _PingPongLogHandler(self)
        # WebSocket ping/pong 的实际日志由 proxy.server 输出。
        self._websocket_logger = logging.getLogger("mitmproxy.proxy.server")
        self._websocket_logger_level = self._websocket_logger.level
        if self._websocket_logger.getEffectiveLevel() > logging.INFO:
            self._websocket_logger.setLevel(logging.INFO)
        self._websocket_logger.addHandler(self._log_handler)
        self._idle_watcher = Thread(target=self._watch_idle_reselect, daemon=True)
        self._idle_watcher.start()
        self._watcher = Thread(target=self._watch_idle_timeout, daemon=True)
        self._watcher.start()

    def running(self) -> None:
        _log("代理运行中")

    def _mark_activity(self) -> None:
        with self._idle_lock:
            self._last_activity = time.monotonic()
            self._reselect_sent = False
            self._idle_kill_sent = False

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

    def _kill_active_flows(self) -> int:
        with self._flow_lock:
            flows = list(self._live_flows.values())
        killed = 0
        killable = 0
        for flow in flows:
            if not getattr(flow, "killable", False):
                continue
            killable += 1
            try:
                flow.kill()
                killed += 1
            except Exception as exc:
                _log(f"断开代理连接失败: id={getattr(flow, 'id', '')} error={exc}")
                continue
        self._cleanup_flows()
        _log(f"断开代理连接检查: tracked={len(flows)} killable={killable} killed={killed}")
        return killed

    def _watch_idle_reselect(self) -> None:
        while not self._stop_event.wait(0.5):
            with self._idle_lock:
                now = time.monotonic()
                if now - self._last_activity < 5.0:
                    continue
                if self._reselect_sent:
                    continue
                self._reselect_sent = True
            self._report_control_event(_RESELECT_EVENT)
            _log("超过 5 秒没有数据输出，重新选举负载")

    def _watch_idle_timeout(self) -> None:
        while not self._stop_event.wait(0.5):
            with self._idle_lock:
                now = time.monotonic()
                if now - self._last_activity < 60.0:
                    continue
                if self._idle_kill_sent:
                    continue
                self._idle_kill_sent = True
            killed = self._kill_active_flows()
            self._report_control_event(_IDLE_TIMEOUT_EVENT)
            if killed > 0:
                _log(f"kill active flows: {killed}")
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
        self._send_control_message(event_name)

    def _send_control_message(self, message: str, read_response: bool = False) -> str:
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
                conn.sendall(f"{message}\n".encode("utf-8"))
                if not read_response:
                    return ""
                try:
                    data = conn.recv(256)
                except OSError:
                    return ""
        except OSError:
            return ""
        return data.decode("utf-8", errors="ignore").strip()

    def handle_ping_pong_log(self) -> None:
        if self._should_disconnect_on_pingpong():
            killed = self._kill_active_flows()
            self._report_control_event(f"KILL_RESULT {killed}")
            if killed > 0:
                _log(f"ping/pong 命中，已断开 {killed} 个代理连接")
            else:
                _log("ping/pong 命中，但未找到可断开的代理连接")

    def _should_disconnect_on_pingpong(self) -> bool:
        return self._send_control_message("PINGPONG", read_response=True) == "1"

    def client_connected(self, *args, **kwargs) -> None:
        self._mark_activity()
        return None

    def client_disconnected(self, *args, **kwargs) -> None:
        self._cleanup_flows()
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

    def websocket_message(self, flow: http.HTTPFlow) -> None:
        self._mark_activity()
        self._track_flow(flow)
        return None

    def error(self, flow: http.HTTPFlow) -> None:
        self._track_flow(flow)
        self._cleanup_flows()
        _log(f"错误: {flow.error}")

    def websocket_end(self, flow: http.HTTPFlow) -> None:
        self._cleanup_flows()

    def done(self) -> None:
        self._stop_event.set()
        if self._websocket_logger is not None and self._log_handler is not None:
            try:
                self._websocket_logger.removeHandler(self._log_handler)
            except Exception:
                pass
            if self._websocket_logger_level is not None:
                self._websocket_logger.setLevel(self._websocket_logger_level)
            self._websocket_logger = None
            self._websocket_logger_level = None
        if self._log_handler is not None:
            self._log_handler = None
        _log("代理插件退出")

addons = [ProxyLoggerAddon()]
