from __future__ import annotations

import os
from datetime import datetime
import re
import socket
import subprocess
import shutil
import tempfile
import time
import tkinter as tk
from dataclasses import dataclass
from queue import Empty, Queue
from pathlib import Path
from threading import Event, Lock, Thread
from typing import Callable

from app.services.auth_sync_service import AuthFileRow, AuthSyncService
from app.services.app_config_service import AppConfig, AppConfigService
from app.services.auth_usage_service import AuthUsageService
from tkinter import messagebox, ttk

from app.services.proxy_service import ProxyConfig, ProxyService


@dataclass
class CodexInstallRow:
    name: str
    path: str
    display_path: str
    size: str
    version: str


class ProxyWindow:
    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        self.config_service = AppConfigService()
        loaded_config = self.config_service.load()
        self.service = ProxyService()
        if loaded_config is not None:
            self.service.config.port = loaded_config.port
            self.service.config.upstream_proxy = loaded_config.upstream_proxy
            self.service.config.use_upstream_proxy = loaded_config.use_upstream_proxy
        self.auth_sync_service = AuthSyncService()
        self.auth_usage_service = AuthUsageService(self.auth_sync_service)
        self.root.title("Codex 账户管理工具")
        self.root.minsize(960, 660)
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)

        self.port_var = tk.StringVar(value=str(self.service.config.port))
        self.upstream_proxy_var = tk.StringVar(value=self.service.config.upstream_proxy)
        self.use_upstream_proxy_var = tk.BooleanVar(value=self.service.config.use_upstream_proxy)
        self._rows_by_item: dict[str, CodexInstallRow] = {}
        self._auth_rows_by_item: dict[str, AuthFileRow] = {}
        self._tooltip: tk.Toplevel | None = None
        self._tooltip_label: ttk.Label | None = None
        self._auth_menu: tk.Menu | None = None
        self._auto_load_lock = Lock()
        self._proxy_lock = Lock()
        auto_load_enabled = loaded_config.auto_load if loaded_config is not None else True
        self.auto_load_var = tk.BooleanVar(value=auto_load_enabled)
        self._auto_load_enabled = auto_load_enabled
        self._use_upstream_proxy = self.service.config.use_upstream_proxy
        self._upstream_proxy = self.service.config.upstream_proxy
        self._auto_load_target_refresh_token = ""
        self._auto_load_target_access_token = ""
        self._auto_load_control_stop = Event()
        self._auto_load_control_socket: socket.socket | None = None
        self._auto_load_control_port = self._start_auto_load_control_server()
        self._proxy_restart_lock = Lock()
        self._proxy_restart_pending = False
        self._port_entry: ttk.Entry | None = None
        self._upstream_entry: ttk.Entry | None = None
        self._traffic_status_var = tk.StringVar(value="上行: 0  下行: 0")
        self._traffic_refresh_pending = False
        self._quota_refresh_pending = False
        self._ui_queue: Queue[Callable[[], None]] = Queue()

        self._build_ui()
        self.root.after(50, self._drain_ui_queue)
        self.auth_sync_service.set_change_callback(lambda: self._post_ui(self._schedule_refresh_auth_files))
        self.auth_usage_service.set_change_callback(lambda: self._post_ui(self._schedule_refresh_auth_files))
        self.auth_usage_service.set_quota_change_callback(
            lambda: (
                self._post_ui(self._schedule_quota_refresh_auth_files),
                self._post_ui(self._recompute_auto_load_target),
            )
        )
        self.auth_usage_service.set_proxy_provider(self._get_proxy_for_usage_request)
        self.service.set_idle_timeout_callback(self._on_proxy_idle_timeout)
        self.service.set_traffic_callback(self._on_proxy_traffic_update)
        self.auth_sync_service.start()
        self.auth_usage_service.start()
        self._center_window(960, 660)
        self._load_or_probe_initial_config(loaded_config is not None)
        self._sync_proxy_config_cache()
        self._set_config_editable(True)
        self.refresh_all()
        self._recompute_auto_load_target()
        self.root.after(500, self._auto_start_with_certificate_check)

    def _build_ui(self) -> None:
        main = ttk.Frame(self.root, padding=14)
        main.pack(fill="both", expand=True)

        top = ttk.Frame(main)
        top.pack(fill="x")

        port_row = ttk.Frame(top)
        port_row.pack(side="left", anchor="w")
        ttk.Label(port_row, text="监听端口").pack(side="left")
        self._port_entry = ttk.Entry(port_row, textvariable=self.port_var, width=10)
        self._port_entry.pack(side="left", padx=(8, 0))

        upstream_row = ttk.Frame(main)
        upstream_row.pack(fill="x", pady=(8, 0))
        ttk.Checkbutton(
            upstream_row,
            text="二级代理",
            variable=self.use_upstream_proxy_var,
            command=self._on_use_upstream_proxy_toggled,
        ).pack(side="left")
        self._upstream_entry = ttk.Entry(upstream_row, textvariable=self.upstream_proxy_var, width=28)
        self._upstream_entry.pack(side="left", padx=(8, 0))

        actions = ttk.Frame(top)
        actions.pack(side="right", anchor="e")

        ttk.Button(actions, text="一键安装证书", command=self.install_certificate).pack(side="right")
        self.toggle_button = ttk.Button(actions, text="一键启动服务器", command=self.toggle_server)
        self.toggle_button.pack(side="right", padx=(0, 8))

        tables = ttk.Frame(main)
        tables.pack(fill="both", expand=True, pady=(12, 0))

        table_frame = ttk.LabelFrame(tables, text="安装项", padding=8)
        table_frame.pack(fill="x")

        install_actions = ttk.Frame(table_frame)
        install_actions.pack(fill="x")
        ttk.Button(install_actions, text="刷新", command=self.refresh_all).pack(side="right")

        tree_wrap = ttk.Frame(table_frame)
        tree_wrap.pack(fill="x", pady=(6, 0))

        columns = ("name", "path", "size", "version", "action")
        self.tree = ttk.Treeview(tree_wrap, columns=columns, show="headings", selectmode="browse", height=4)
        self.tree.heading("name", text="名称")
        self.tree.heading("path", text="路径")
        self.tree.heading("size", text="大小")
        self.tree.heading("version", text="版本")
        self.tree.heading("action", text="操作")
        self.tree.column("name", width=100, anchor="w", stretch=False)
        self.tree.column("path", width=420, anchor="w", stretch=True)
        self.tree.column("size", width=90, anchor="center", stretch=False)
        self.tree.column("version", width=100, anchor="center", stretch=False)
        self.tree.column("action", width=70, anchor="center", stretch=False)
        self.tree.bind("<Button-1>", self._on_tree_click)
        self.tree.bind("<Motion>", self._on_tree_motion)
        self.tree.bind("<Leave>", lambda _event: self._hide_tooltip())

        y_scroll = ttk.Scrollbar(tree_wrap, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=y_scroll.set)
        self.tree.grid(row=0, column=0, sticky="nsew")
        y_scroll.grid(row=0, column=1, sticky="ns")
        tree_wrap.rowconfigure(0, weight=1)
        tree_wrap.columnconfigure(0, weight=1)

        auth_frame = ttk.LabelFrame(tables, text="授权文件 - 双击切换", padding=8)
        auth_frame.pack(fill="both", expand=True, pady=(10, 0))

        auth_options = ttk.Frame(auth_frame)
        auth_options.pack(fill="x", pady=(0, 6))
        ttk.Checkbutton(auth_options, text="自动负载", variable=self.auto_load_var, command=self._on_auto_load_toggled).pack(side="right")

        auth_wrap = ttk.Frame(auth_frame)
        auth_wrap.pack(fill="both", expand=True)

        auth_columns = (
            "currentMark",
            "loadMark",
            "accountId",
            "tokenRefreshTime",
            "quotaRefreshTime",
            "quota",
            "planType",
            "traffic",
        )
        self.auth_tree = ttk.Treeview(auth_wrap, columns=auth_columns, show="headings", selectmode="browse", height=5)
        self.auth_tree.heading("currentMark", text="当前")
        self.auth_tree.heading("loadMark", text="负载")
        self.auth_tree.heading("accountId", text="账户id")
        self.auth_tree.heading("tokenRefreshTime", text="令牌刷新时间")
        self.auth_tree.heading("quotaRefreshTime", text="额度刷新时间")
        self.auth_tree.heading("quota", text="额度(5h/7d)")
        self.auth_tree.heading("planType", text="类型")
        self.auth_tree.heading("traffic", text="流量")
        self.auth_tree.column("currentMark", width=46, anchor="center", stretch=False)
        self.auth_tree.column("loadMark", width=46, anchor="center", stretch=False)
        self.auth_tree.column("accountId", width=260, anchor="w", stretch=True)
        self.auth_tree.column("tokenRefreshTime", width=128, anchor="center", stretch=False)
        self.auth_tree.column("quotaRefreshTime", width=128, anchor="center", stretch=False)
        self.auth_tree.column("quota", width=112, anchor="center", stretch=False)
        self.auth_tree.column("planType", width=76, anchor="center", stretch=False)
        self.auth_tree.column("traffic", width=104, anchor="center", stretch=False)
        self.auth_tree.bind("<Double-1>", self._on_auth_tree_double_click)
        self.auth_tree.bind("<Button-3>", self._on_auth_tree_right_click)
        self.auth_tree.bind("<Motion>", self._on_auth_tree_motion)
        self.auth_tree.bind("<Leave>", lambda _event: self._hide_tooltip())
        self._auth_menu = tk.Menu(self.root, tearoff=0)
        self._auth_menu.add_command(label="切换", command=self._activate_selected_auth_row)
        self._auth_menu.add_command(label="删除", command=self._delete_selected_auth_row)

        auth_scroll = ttk.Scrollbar(auth_wrap, orient="vertical", command=self.auth_tree.yview)
        self.auth_tree.configure(yscrollcommand=auth_scroll.set)
        self.auth_tree.grid(row=0, column=0, sticky="nsew")
        auth_scroll.grid(row=0, column=1, sticky="ns")
        auth_wrap.rowconfigure(0, weight=1)
        auth_wrap.columnconfigure(0, weight=1)

        traffic_frame = ttk.Frame(main)
        traffic_frame.pack(fill="x", pady=(8, 0))
        ttk.Label(traffic_frame, textvariable=self._traffic_status_var).pack(side="right")

    def _center_window(self, width: int, height: int) -> None:
        self.root.update_idletasks()
        screen_width = self.root.winfo_screenwidth()
        screen_height = self.root.winfo_screenheight()
        x = (screen_width - width) // 2
        y = (screen_height - height) // 2
        self.root.geometry(f"{width}x{height}+{x}+{y}")

    def _post_ui(self, callback: Callable[[], None]) -> None:
        self._ui_queue.put(callback)

    def _drain_ui_queue(self) -> None:
        try:
            while True:
                callback = self._ui_queue.get_nowait()
                try:
                    callback()
                except Exception as exc:
                    print(f"[ProxyWindow] UI 回调异常: {exc}", flush=True)
        except Empty:
            pass
        try:
            self.root.after(50, self._drain_ui_queue)
        except tk.TclError:
            pass

    def _refresh_config(self) -> bool:
        try:
            port = int(self.port_var.get().strip())
        except ValueError:
            messagebox.showerror("端口错误", "请输入有效的端口号。")
            return False
        use_upstream_proxy = self.use_upstream_proxy_var.get()
        upstream_proxy = self.upstream_proxy_var.get().strip()
        if use_upstream_proxy and not upstream_proxy:
            messagebox.showerror("代理错误", "请输入二级代理地址。")
            return False
        self.service.config = ProxyConfig(
            port=port,
            upstream_proxy=upstream_proxy,
            use_upstream_proxy=use_upstream_proxy,
        )
        self._sync_proxy_config_cache()
        self._persist_config()
        return True

    def _prime_default_port(self) -> None:
        try:
            port = self.service.find_available_port()
        except RuntimeError:
            return
        self.port_var.set(str(port))

    def _load_or_probe_initial_config(self, has_config: bool) -> None:
        if has_config:
            return
        self._prime_default_port()
        self._persist_config()

    def _auto_start(self) -> None:
        print("启动中...")
        self._set_busy(True)
        Thread(target=self._auto_start_worker, daemon=True).start()

    def _auto_start_worker(self) -> None:
        self._post_ui(self.start_server)

    def _auto_start_with_certificate_check(self) -> None:
        Thread(target=self._auto_start_with_certificate_check_worker, daemon=True).start()

    def _auto_start_with_certificate_check_worker(self) -> None:
        try:
            cert_path = self.service.certificate_files()["cer"]
            if cert_path.exists() and not self.service.is_certificate_installed():
                ok, message = self.service.install_certificate()
                if ok:
                    print(f"[ProxyWindow] 启动时自动安装证书成功: {cert_path}", flush=True)
                else:
                    print(f"[ProxyWindow] 启动时自动安装证书失败: {message}", flush=True)
            elif cert_path.exists():
                print(f"[ProxyWindow] 证书已安装: {cert_path}", flush=True)
            else:
                print(f"[ProxyWindow] 未找到证书文件，跳过自动安装: {cert_path}", flush=True)
        except Exception as exc:
            print(f"[ProxyWindow] 自动检查证书异常: {exc}", flush=True)
        finally:
            self._post_ui(self._auto_start)

    def _auto_start_failed(self, message: str) -> None:
        print("代理未启动")
        self._set_busy(False)
        messagebox.showerror("端口错误", message)

    def _schedule_refresh_auth_files(self) -> None:
        try:
            self._apply_pending_quota_items()
            self._post_ui(self.refresh_auth_files)
        except tk.TclError:
            pass

    def _schedule_quota_refresh_auth_files(self) -> None:
        try:
            if not self._apply_pending_quota_items():
                return
            if self._quota_refresh_pending:
                return
            self._quota_refresh_pending = True
            self._post_ui(self._refresh_quota_table)
        except tk.TclError:
            self._quota_refresh_pending = False

    def _refresh_quota_table(self) -> None:
        self._quota_refresh_pending = False
        try:
            self.refresh_auth_files(update_status=False)
        except tk.TclError:
            pass

    def _apply_pending_quota_items(self) -> bool:
        quota_items = self.auth_usage_service.pop_pending_quota_items()
        if not quota_items:
            return False
        self.auth_sync_service.update_usage_cache(
            self.auth_usage_service.quota_snapshot(),
            self.auth_usage_service.plan_type_snapshot(),
            self.auth_usage_service.user_id_snapshot(),
            self.auth_usage_service.email_snapshot(),
            self.auth_usage_service.quota_refresh_time_5h_snapshot(),
            self.auth_usage_service.quota_refresh_time_7d_snapshot(),
        )
        return True

    def _on_access_token_hit(self, access_token: str) -> None:
        if not self.auth_sync_service.increment_traffic_by_access_token(access_token):
            return
        self._schedule_refresh_traffic()

    def _on_auto_load_access_token_used(self, access_token: str) -> None:
        if not self.auth_sync_service.increment_traffic_by_access_token(access_token):
            return
        self._schedule_refresh_traffic()

    def _on_proxy_idle_timeout(self) -> None:
        with self._auto_load_lock:
            enabled = self._auto_load_enabled
        if not enabled:
            return
        try:
            self._post_ui(self._recompute_auto_load_target)
        except tk.TclError:
            pass

    def _on_proxy_traffic_update(self, up_bytes: int, down_bytes: int) -> None:
        try:
            self._post_ui(lambda: self._update_traffic_status(up_bytes, down_bytes))
        except tk.TclError:
            pass

    def _update_traffic_status(self, up_bytes: int, down_bytes: int) -> None:
        self._traffic_status_var.set(
            f"上行: {self._format_traffic_bytes(up_bytes)}  下行: {self._format_traffic_bytes(down_bytes)}"
        )

    def _on_auto_load_toggled(self) -> None:
        enabled = self.auto_load_var.get()
        with self._auto_load_lock:
            self._auto_load_enabled = enabled
        self._persist_config()
        if enabled:
            self._recompute_auto_load_target()
            return
        self._set_auto_load_target("", "")
        self.refresh_auth_files(update_status=False)

    def _recompute_auto_load_target(self) -> None:
        with self._auto_load_lock:
            if not self._auto_load_enabled:
                return
        rows = self.auth_sync_service.list_auth_rows()
        if not rows:
            self._set_auto_load_target("", "")
            print("[AutoLoad] 当前没有可选授权文件", flush=True)
            return
        row = max(rows, key=lambda item: (self._quota_priority(item.quota), item.refresh_token))
        self._set_auto_load_target(row.refresh_token, row.access_token)
        print(
            f"[AutoLoad] 负载目标: account_id={row.account_id} refresh_token={row.refresh_token} quota={row.quota}",
            flush=True,
        )
        self.refresh_auth_files(update_status=False)

    def _restart_proxy_server_async(self) -> None:
        with self._proxy_restart_lock:
            if self._proxy_restart_pending:
                return
            if not (self.service.process and self.service.process.poll() is None):
                return
            self._proxy_restart_pending = True
        Thread(target=self._restart_proxy_server_worker, daemon=True).start()

    def _restart_proxy_server_worker(self) -> None:
        try:
            self.service.stop()
            time.sleep(0.3)
            ok, message = self.service.run()
            if not ok:
                print(f"[ProxyWindow] 代理重启失败: {message}", flush=True)
        except Exception as exc:
            print(f"[ProxyWindow] 代理重启异常: {exc}", flush=True)
        finally:
            with self._proxy_restart_lock:
                self._proxy_restart_pending = False
            try:
                self._post_ui(self._update_toggle_button)
            except tk.TclError:
                pass

    def _set_auto_load_target(self, refresh_token: str, access_token: str) -> None:
        with self._auto_load_lock:
            self._auto_load_target_refresh_token = refresh_token
            self._auto_load_target_access_token = access_token

    def _persist_config(self) -> None:
        try:
            port = int(self.port_var.get().strip())
        except ValueError:
            return
        upstream_proxy = self.upstream_proxy_var.get().strip()
        if self.use_upstream_proxy_var.get() and not upstream_proxy:
            return
        self.config_service.save(
            AppConfig(
                port=port,
                upstream_proxy=upstream_proxy,
                use_upstream_proxy=self.use_upstream_proxy_var.get(),
                auto_load=self.auto_load_var.get(),
            )
        )

    def _get_proxy_for_usage_request(self) -> str:
        with self._proxy_lock:
            if not self._use_upstream_proxy:
                return ""
            return self._upstream_proxy

    def _sync_proxy_config_cache(self) -> None:
        with self._proxy_lock:
            self._use_upstream_proxy = self.use_upstream_proxy_var.get()
            self._upstream_proxy = self.upstream_proxy_var.get().strip()

    def _on_use_upstream_proxy_toggled(self) -> None:
        self._sync_proxy_config_cache()
        self._persist_config()

    def _get_auto_load_target_refresh_token(self) -> str:
        with self._auto_load_lock:
            return self._auto_load_target_refresh_token if self._auto_load_enabled else ""

    def _get_auto_load_target_access_token(self) -> str:
        with self._auto_load_lock:
            return self._auto_load_target_access_token if self._auto_load_enabled else ""

    def _start_auto_load_control_server(self) -> int:
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(("127.0.0.1", 0))
        server.listen(5)
        server.settimeout(0.5)
        self._auto_load_control_socket = server
        port = server.getsockname()[1]
        Thread(target=self._run_auto_load_control_server, args=(server,), daemon=True).start()
        return port

    def _run_auto_load_control_server(self, server: socket.socket) -> None:
        while not self._auto_load_control_stop.is_set():
            try:
                conn, _addr = server.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            with conn:
                try:
                    conn.settimeout(0.1)
                    try:
                        payload = conn.recv(4096).decode("utf-8", errors="ignore").strip()
                    except socket.timeout:
                        payload = ""
                    if payload.startswith("USED "):
                        token = payload.removeprefix("USED ").strip()
                        if token:
                            self._post_ui(lambda value=token: self._on_auto_load_access_token_used(value))
                        continue
                    if payload.startswith("TRAFFIC "):
                        parts = payload.split()
                        if len(parts) == 3:
                            try:
                                up_bytes = int(parts[1])
                                down_bytes = int(parts[2])
                            except ValueError:
                                continue
                            self._post_ui(lambda up=up_bytes, down=down_bytes: self._on_proxy_traffic_update(up, down))
                        continue
                    if payload == "IDLE":
                        self._post_ui(self._recompute_auto_load_target)
                        continue
                    token = self._get_auto_load_target_access_token()
                    conn.sendall((token + "\n").encode("utf-8"))
                except OSError:
                    continue

    def _schedule_refresh_traffic(self) -> None:
        if self._traffic_refresh_pending:
            return
        self._traffic_refresh_pending = True
        try:
            self._post_ui(self._refresh_traffic_table)
        except tk.TclError:
            self._traffic_refresh_pending = False

    def _refresh_traffic_table(self) -> None:
        self._traffic_refresh_pending = False
        try:
            self.refresh_auth_files(update_status=False)
        except tk.TclError:
            pass

    def _set_busy(self, busy: bool) -> None:
        self.toggle_button.config(state="disabled" if busy else "normal")
        if busy:
            self.toggle_button.config(text="启动中...")
        self._set_config_editable(not busy)

    def _set_config_editable(self, enabled: bool) -> None:
        state = "normal" if enabled else "disabled"
        if self._port_entry is not None:
            self._port_entry.config(state=state)
        if self._upstream_entry is not None:
            self._upstream_entry.config(state=state)

    def _update_toggle_button(self) -> None:
        if self.service.process and self.service.process.poll() is None:
            self.toggle_button.config(text="停止服务器")
        else:
            self.toggle_button.config(text="一键启动服务器")

    def toggle_server(self) -> None:
        if self.service.process and self.service.process.poll() is None:
            self.stop_server()
            return
        self.start_server()

    def start_server(self) -> None:
        print("启动中...")
        if not self._refresh_config():
            print("代理未启动")
            self._set_busy(False)
            return
        if self.service.process and self.service.process.poll() is None:
            print(f"代理正在运行: {self.service.config.host}:{self.service.config.port}")
            self._update_toggle_button()
            self._set_busy(False)
            self._post_ui(self.refresh_installs)
            return
        self._set_busy(True)
        Thread(target=self._start_server_worker, daemon=True).start()

    def _start_server_worker(self) -> None:
        try:
            os.environ["AUTOLOAD_CONTROL_PORT"] = str(self._auto_load_control_port)
            ok, message = self.service.run()
        except Exception as exc:
            self._post_ui(lambda: self._start_failed(str(exc)))
            return
        if not ok:
            self._post_ui(lambda: self._start_failed(message))
            return
        self._post_ui(self._start_succeeded)

    def _start_failed(self, message: str) -> None:
        print("代理未启动")
        self._update_toggle_button()
        self._set_busy(False)
        messagebox.showerror("启动失败", message)

    def _start_succeeded(self) -> None:
        print(f"代理已启动: {self.service.config.host}:{self.service.config.port}")
        self._update_toggle_button()
        self._set_busy(False)
        self._set_config_editable(False)
        self.refresh_installs()

    def stop_server(self) -> None:
        self.service.stop()
        print("代理已停止")
        self._update_toggle_button()
        self._set_busy(False)
        self._set_config_editable(True)

    def install_certificate(self) -> None:
        ok, message = self.service.install_certificate()
        if ok:
            messagebox.showinfo("安装结果", message)
        else:
            messagebox.showerror("安装失败", message)

    def refresh_all(self) -> None:
        install_count = self.refresh_installs(update_status=False)
        auth_count = self.refresh_auth_files(update_status=False)
        print(f"已刷新 {install_count} 项 / {auth_count} 个文件")

    def refresh_installs(self, update_status: bool = True) -> int:
        for item in self.tree.get_children():
            self.tree.delete(item)
        self._rows_by_item.clear()
        rows = self._scan_codex_installs()
        for row in rows:
            item = self.tree.insert("", "end", values=(row.name, row.display_path, row.size, row.version, "启动"))
            self._rows_by_item[item] = row
        if update_status:
            print(f"已刷新 {len(rows)} 项")
        return len(rows)

    def refresh_auth_files(self, update_status: bool = True) -> int:
        for item in self.auth_tree.get_children():
            self.auth_tree.delete(item)
        self._auth_rows_by_item.clear()
        rows = self.auth_sync_service.list_auth_rows()
        load_refresh_token = self._get_auto_load_target_refresh_token()
        for row in rows:
            item = self.auth_tree.insert(
                "",
                "end",
                values=(
                    "★" if row.current else "",
                    "●" if row.refresh_token == load_refresh_token else "",
                    self._shorten_middle(row.account_id, 16, 10),
                    self._format_last_refresh(row.last_refresh),
                    self._format_last_refresh(row.quota_refresh_time_5h),
                    row.quota,
                    row.plan_type or "",
                    row.traffic,
                ),
            )
            self._auth_rows_by_item[item] = row
        if update_status:
            print(f"已刷新 {len(rows)} 个文件")
        return len(rows)

    def _scan_codex_installs(self) -> list[CodexInstallRow]:
        rows: list[CodexInstallRow] = []
        rows.extend(self._scan_windowsapps_codex())
        rows.extend(self._scan_node_codex())
        return rows

    def _scan_windowsapps_codex(self) -> list[CodexInstallRow]:
        base_dir = Path(os.environ.get("ProgramFiles", r"C:\Program Files")) / "WindowsApps"
        if not base_dir.exists():
            return []

        rows: list[CodexInstallRow] = []
        for app_dir in sorted(base_dir.iterdir(), key=lambda p: p.name):
            if not app_dir.is_dir() or not app_dir.name.startswith("OpenAI.Codex"):
                continue
            for exe_path in self._iter_codex_executables(app_dir):
                rows.append(
                    CodexInstallRow(
                        name=self._shorten_name(exe_path.name),
                        path=str(exe_path),
                        display_path=self._shorten_path(str(exe_path)),
                        size=self._format_size(exe_path.stat().st_size),
                        version=self._extract_version(app_dir.name),
                    )
                )
        return rows

    def _scan_node_codex(self) -> list[CodexInstallRow]:
        node_exe = self._locate_node_exe()
        if not node_exe:
            return []

        codex_root = node_exe.parent / "node_modules" / "@openai" / "codex"
        if not codex_root.exists():
            return []

        rows: list[CodexInstallRow] = []
        for exe_path in self._iter_code_executables(codex_root):
            rows.append(
                CodexInstallRow(
                    name=self._shorten_name(exe_path.name),
                    path=str(exe_path),
                    display_path=self._shorten_path(str(exe_path)),
                    size=self._format_size(exe_path.stat().st_size),
                    version="",
                )
            )
        return rows

    def _locate_node_exe(self) -> Path | None:
        located = shutil.which("node.exe") or shutil.which("node")
        if located:
            return Path(located)
        path_value = os.environ.get("PATH", "")
        for raw_dir in path_value.split(os.pathsep):
            if not raw_dir:
                continue
            candidate = Path(raw_dir) / "node.exe"
            if candidate.exists():
                return candidate
        return None

    def _shorten_path(self, path: str, max_length: int = 72) -> str:
        if len(path) <= max_length:
            return path
        parts = Path(path).parts
        if len(parts) >= 4:
            tail = Path(*parts[-4:])
            short = f"...\\{tail}"
            if len(short) <= max_length:
                return short
        if len(parts) >= 3:
            tail = Path(*parts[-3:])
            short = f"...\\{tail}"
            if len(short) <= max_length:
                return short
        keep = max_length - 3
        return f"{path[:keep]}..."

    def _shorten_name(self, name: str, max_length: int = 10) -> str:
        if len(name) <= max_length:
            return name
        stem = Path(name).stem
        suffix = Path(name).suffix
        if len(stem) > 6:
            stem = f"{stem[:4]}..."
        return f"{stem}{suffix}"

    def _shorten_middle(self, text: str, keep_prefix: int = 14, keep_suffix: int = 10) -> str:
        if len(text) <= keep_prefix + keep_suffix + 3:
            return text
        return f"{text[:keep_prefix]}...{text[-keep_suffix:]}"

    def _quota_priority(self, quota: str) -> float:
        if not quota:
            return -1.0
        match = re.search(r"(\d+(?:\.\d+)?)%", quota)
        if not match:
            return -1.0
        try:
            return float(match.group(1))
        except ValueError:
            return -1.0

    def _format_last_refresh(self, text: str) -> str:
        if not text:
            return ""
        try:
            value = datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone()
        except ValueError:
            return text
        return value.strftime("%Y-%m-%d %H:%M")

    def _redact_middle(self, text: str, keep_prefix: int = 12, keep_suffix: int = 8) -> str:
        if not text:
            return "-"
        if len(text) <= keep_prefix + keep_suffix + 3:
            return text
        return f"{text[:keep_prefix]}...{text[-keep_suffix:]}"

    def _build_auth_tooltip(self, row: AuthFileRow) -> str:
        traffic = row.traffic
        quota = row.quota or ""
        lines = [
            f"账户ID: {row.account_id or '-'}",
            f"用户ID: {row.user_id or ''}",
            f"邮箱: {row.email or ''}",
            f"刷新令牌: {row.refresh_token or '-'}",
            f"访问令牌: {row.access_token or ''}",
            f"令牌刷新时间: {row.last_refresh or ''}",
            f"额度刷新时间(5小时): {row.quota_refresh_time_5h or ''}",
            f"额度刷新时间(7天): {row.quota_refresh_time_7d or ''}",
            f"额度(5h/7d): {quota}",
            f"类型: {row.plan_type or ''}",
            f"流量: {traffic}",
        ]
        return "\n".join(lines)

    def _on_tree_motion(self, event: tk.Event) -> None:
        row_id = self.tree.identify_row(event.y)
        column = self.tree.identify_column(event.x)
        if not row_id:
            self.tree.configure(cursor="")
            self._hide_tooltip()
            return
        row = self._rows_by_item.get(row_id)
        if not row:
            self.tree.configure(cursor="")
            self._hide_tooltip()
            return
        if column == "#5":
            self.tree.configure(cursor="hand2")
            self._show_tooltip(event.x_root, event.y_root, row.path)
            return
        if column in {"#1", "#2", "#3", "#4"}:
            self.tree.configure(cursor="")
            self._show_tooltip(event.x_root, event.y_root, row.path if column == "#2" else row.name)
            return
        self.tree.configure(cursor="")
        self._hide_tooltip()

    def _on_auth_tree_motion(self, event: tk.Event) -> None:
        row_id = self.auth_tree.identify_row(event.y)
        if not row_id:
            self.auth_tree.configure(cursor="")
            self._hide_tooltip()
            return
        row = self._auth_rows_by_item.get(row_id)
        if not row:
            self.auth_tree.configure(cursor="")
            self._hide_tooltip()
            return
        self.auth_tree.configure(cursor="")
        self._show_tooltip(event.x_root, event.y_root, self._build_auth_tooltip(row))

    def _on_auth_tree_double_click(self, event: tk.Event) -> None:
        row_id = self.auth_tree.identify_row(event.y)
        if not row_id:
            return
        row = self._auth_rows_by_item.get(row_id)
        if not row:
            return
        self._activate_auth_row(row)

    def _on_auth_tree_right_click(self, event: tk.Event) -> None:
        row_id = self.auth_tree.identify_row(event.y)
        if not row_id:
            return
        row = self._auth_rows_by_item.get(row_id)
        if not row or self._auth_menu is None:
            return
        self.auth_tree.selection_set(row_id)
        self.auth_tree.focus(row_id)
        try:
            self._auth_menu.tk_popup(event.x_root, event.y_root)
        finally:
            self._auth_menu.grab_release()

    def _activate_selected_auth_row(self) -> None:
        row = self._get_selected_auth_row()
        if row is not None:
            self._activate_auth_row(row)

    def _delete_selected_auth_row(self) -> None:
        row = self._get_selected_auth_row()
        if row is not None:
            self._delete_auth_row(row)

    def _get_selected_auth_row(self) -> AuthFileRow | None:
        selection = self.auth_tree.selection()
        if not selection:
            return None
        return self._auth_rows_by_item.get(selection[0])

    def _activate_auth_row(self, row: AuthFileRow) -> None:
        if not messagebox.askyesno("切换授权文件", f"确认切换到该授权文件吗？\n\n账户ID: {row.account_id}\n刷新令牌: {row.refresh_token}"):
            return
        ok, message = self.auth_sync_service.activate_auth_file(row.refresh_token)
        if not ok:
            messagebox.showerror("切换失败", message)
            return
        self.refresh_auth_files()

    def _delete_auth_row(self, row: AuthFileRow) -> None:
        if not messagebox.askyesno("删除授权文件", f"确认删除该授权文件吗？\n\n账户ID: {row.account_id}\n刷新令牌: {row.refresh_token}"):
            return
        ok, message = self.auth_sync_service.delete_auth_file(row.refresh_token)
        if not ok:
            messagebox.showerror("删除失败", message)
            return
        if row.refresh_token == self._get_auto_load_target_refresh_token():
            self._set_auto_load_target("", "")
            if self.auto_load_var.get():
                self._recompute_auto_load_target()
        self.refresh_auth_files()

    def _show_tooltip(self, x: int, y: int, text: str) -> None:
        if self._tooltip is None:
            self._tooltip = tk.Toplevel(self.root)
            self._tooltip.overrideredirect(True)
            self._tooltip.attributes("-topmost", True)
            self._tooltip_label = ttk.Label(
                self._tooltip,
                text=text,
                padding=(8, 4),
                relief="solid",
                justify="left",
                anchor="w",
            )
            self._tooltip_label.pack()
        else:
            assert self._tooltip_label is not None
            self._tooltip_label.config(text=text)
        self._tooltip.geometry(f"+{x + 12}+{y + 12}")
        self._tooltip.deiconify()

    def _hide_tooltip(self) -> None:
        if self._tooltip is not None:
            self._tooltip.withdraw()

    def _on_tree_click(self, event: tk.Event) -> None:
        region = self.tree.identify_region(event.x, event.y)
        if region != "cell":
            return
        row_id = self.tree.identify_row(event.y)
        column = self.tree.identify_column(event.x)
        if not row_id or column != "#5":
            return
        row = self._rows_by_item.get(row_id)
        if not row:
            return
        self._launch_codex(row)

    def _launch_codex(self, row: CodexInstallRow) -> None:
        exe = Path(row.path)
        if not exe.exists():
            messagebox.showerror("启动失败", f"找不到文件 {row.path}")
            return
        ok, message = self.service.ensure_launch_permissions(exe)
        if not ok:
            messagebox.showerror("启动失败", message)
            return
        proxy_port = self.service.config.port
        script = (
            "@echo off\r\n"
            'if "%~1"=="" exit /b 1\r\n'
            'cd /d "%~1"\r\n'
            f"set HTTP_PROXY=http://127.0.0.1:{proxy_port}\r\n"
            f"set HTTPS_PROXY=http://127.0.0.1:{proxy_port}\r\n"
            f"set ALL_PROXY=http://127.0.0.1:{proxy_port}\r\n"
            "set NO_PROXY=localhost,127.0.0.1\r\n"
            f'"{exe}"\r\n'
        )
        current_dir = str(exe.parent)
        with tempfile.NamedTemporaryFile("w", suffix=".bat", delete=False, encoding="utf-8", newline="\r\n") as batch_file:
            batch_file.write(script)
            batch_path = batch_file.name
        print(f"[ProxyWindow] 启动外部程序: {exe.name}", flush=True)
        subprocess.Popen(
            ["cmd.exe", "/k", batch_path, current_dir],
            creationflags=subprocess.CREATE_NEW_CONSOLE,
        )

    def _iter_codex_executables(self, root: Path) -> list[Path]:
        matches: list[Path] = []
        for path in root.rglob("*"):
            if path.is_file() and path.name in {"codex.exe", "Codex.exe", "Code"}:
                matches.append(path)
        return matches

    def _iter_code_executables(self, root: Path) -> list[Path]:
        matches: list[Path] = []
        for path in root.rglob("*"):
            if path.is_file() and path.name in {"code.exe", "codex.exe", "Code", "Codex.exe"}:
                matches.append(path)
        return matches

    def _extract_version(self, folder_name: str) -> str:
        match = re.match(r"^OpenAI\.Codex_(\d+(?:\.\d+)*)_", folder_name)
        if match:
            return match.group(1)
        return ""

    def _format_size(self, size_bytes: int) -> str:
        if size_bytes < 1024:
            return f"{size_bytes} B"
        if size_bytes < 1024 * 1024:
            return f"{size_bytes / 1024:.1f} KB"
        return f"{size_bytes / (1024 * 1024):.1f} MB"

    def _format_traffic_bytes(self, size_bytes: int) -> str:
        if size_bytes < 1024:
            return f"{size_bytes}B"
        if size_bytes < 1024 * 1024:
            return f"{size_bytes / 1024:.1f}K"
        if size_bytes < 1024 * 1024 * 1024:
            return f"{size_bytes / (1024 * 1024):.1f}M"
        return f"{size_bytes / (1024 * 1024 * 1024):.1f}G"

    def _on_close(self) -> None:
        self._persist_config()
        self.auth_usage_service.stop()
        self.auth_sync_service.stop()
        self.service.stop()
        self._auto_load_control_stop.set()
        if self._auto_load_control_socket is not None:
            try:
                self._auto_load_control_socket.close()
            except OSError:
                pass
        self.root.destroy()
