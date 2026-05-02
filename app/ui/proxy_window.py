from __future__ import annotations

import os
from datetime import datetime
import re
import socket
import subprocess
import shutil
import tempfile
import sys
import time
import tkinter as tk
import webbrowser
from dataclasses import dataclass
from queue import Empty, Queue
from pathlib import Path
from threading import Event, Lock, Thread
from typing import Callable

import pystray
import psutil
from PIL import Image

from app.services.auth_sync_service import AuthFileRow, AuthSyncService
from app.services.app_config_service import AppConfig, AppConfigService
from app.services.auth_usage_service import AuthQuotaItem, AuthUsageService
from app.services.low_price_account_service import LowPriceAccount, LowPriceAccountService
from tkinter import messagebox, ttk

from app.services.proxy_service import ProxyConfig, ProxyService


_AUTO_LOAD_MISMATCH_WINDOW_SECONDS = 60.0
_QUOTA_DROP_EPSILON = 0.01
_QUOTA_DROP_GRACE_SECONDS = 10.0
_TRAY_ICON_TIP = "Codex 账户管理"
_TRAY_ICON_MAX_ROWS = 4
_TRAY_ICON_MAX_TIP_LENGTH = 120


@dataclass
class CodexInstallRow:
    name: str
    path: str
    display_path: str
    size: str
    version: str


@dataclass
class CodexProcessRow:
    pid: int
    name: str
    children: list["CodexProcessRow"]


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
        self.low_price_account_service = LowPriceAccountService()
        self.root.title("Codex 账户管理工具")
        self.root.minsize(960, 660)
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)

        self.port_var = tk.StringVar(value=str(self.service.config.port))
        self.upstream_proxy_var = tk.StringVar(value=self.service.config.upstream_proxy)
        self.use_upstream_proxy_var = tk.BooleanVar(value=self.service.config.use_upstream_proxy)
        self._rows_by_item: dict[str, CodexInstallRow] = {}
        self._launch_buttons: dict[str, ttk.Button] = {}
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
        self._last_used_access_token = ""
        self._proxy_kill_pending = False
        self._proxy_kill_pending_access_token = ""
        self._proxy_kill_pending_used_access_token = ""
        self._proxy_kill_pending_reason = ""
        self._proxy_kill_next_allowed_at = 0.0
        self._proxy_kill_attempt_in_flight = False
        self._manual_proxy_kill_pending = False
        self._correct_traffic_button: ttk.Button | None = None
        self._correct_traffic_refreshing = False
        self._clean_auth_button: ttk.Button | None = None
        self._clean_auth_refreshing = False
        self._low_price_window: tk.Toplevel | None = None
        self._low_price_tree: ttk.Treeview | None = None
        self._low_price_refresh_button: ttk.Button | None = None
        self._low_price_refreshing = False
        self._low_price_items_by_product_id: dict[str, LowPriceAccount] = {}
        self._low_price_product_id_by_item: dict[str, str] = {}
        self._low_price_titles_by_item: dict[str, str] = {}
        self._correct_traffic_after_id: str | None = None
        self._auto_load_target_selected_at = 0.0
        self._quota_priority_by_refresh_token: dict[str, float] = {}
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
        self._closing = False
        self._last_install_rows_signature: tuple[tuple[str, str, str, str], ...] | None = None
        self._last_auth_rows_signature: tuple[tuple[bool, bool, bool, str, str, str, str, str, int], ...] | None = None
        self._tray_icon_visible = False
        self._tray_icon: pystray.Icon | None = None

        self._build_ui()
        self.root.bind("<Unmap>", self._on_root_unmap, add="+")
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
        ttk.Button(install_actions, text="结束进程", command=self.kill_codex_processes).pack(side="right", padx=(0, 8))

        tree_wrap = ttk.Frame(table_frame)
        tree_wrap.pack(fill="x", pady=(6, 0))

        columns = ("name", "path", "size", "version", "action")
        style = ttk.Style()
        style.configure("Install.Treeview", rowheight=32)
        self.tree = ttk.Treeview(
            tree_wrap,
            columns=columns,
            show="headings",
            selectmode="none",
            height=4,
            style="Install.Treeview",
        )
        self.tree.heading("name", text="名称")
        self.tree.heading("path", text="路径")
        self.tree.heading("size", text="大小")
        self.tree.heading("version", text="版本")
        self.tree.heading("action", text="操作")
        self.tree.column("name", width=100, anchor="w", stretch=False)
        self.tree.column("path", width=420, anchor="w", stretch=True)
        self.tree.column("size", width=90, anchor="center", stretch=False)
        self.tree.column("version", width=100, anchor="center", stretch=False)
        self.tree.column("action", width=82, anchor="center", stretch=False)
        self.tree.bind("<Button-1>", self._on_tree_click)
        self.tree.bind("<Motion>", self._on_tree_motion)
        self.tree.bind("<Leave>", lambda _event: self._hide_tooltip())
        self.tree.bind("<Configure>", lambda _event: self._refresh_launch_buttons())

        y_scroll = ttk.Scrollbar(
            tree_wrap,
            orient="vertical",
            command=lambda *args: (self.tree.yview(*args), self.root.after_idle(self._refresh_launch_buttons)),
        )
        self.tree.configure(
            yscrollcommand=lambda first, last: (
                y_scroll.set(first, last),
                self.root.after_idle(self._refresh_launch_buttons),
            )
        )
        self.tree.grid(row=0, column=0, sticky="nsew")
        y_scroll.grid(row=0, column=1, sticky="ns")
        tree_wrap.rowconfigure(0, weight=1)
        tree_wrap.columnconfigure(0, weight=1)

        auth_frame = ttk.LabelFrame(tables, text="授权文件 - 双击切换", padding=8)
        auth_frame.pack(fill="both", expand=True, pady=(10, 0))

        auth_options = ttk.Frame(auth_frame)
        auth_options.pack(fill="x", pady=(0, 6))
        ttk.Checkbutton(auth_options, text="自动负载", variable=self.auto_load_var, command=self._on_auto_load_toggled).pack(side="left")
        self._correct_traffic_button = ttk.Button(auth_options, text="矫正流量", command=self.correct_traffic)
        self._correct_traffic_button.pack(side="right")
        self._clean_auth_button = ttk.Button(auth_options, text="清理授权", command=self.clean_auth_files)
        self._clean_auth_button.pack(side="right", padx=(0, 8))
        update_auth_button = ttk.Button(auth_options, text="更新授权", command=self.update_auth)
        update_auth_button.pack(side="right", padx=(0, 8))
        low_price_button = ttk.Button(auth_options, text="低价购号", command=self.open_low_price_window)
        low_price_button.pack(side="right", padx=(0, 8))
        self._bind_widget_tooltip(
            low_price_button,
            "查看低价账号",
        )
        self._bind_widget_tooltip(
            update_auth_button,
            "更新授权文件",
        )
        self._bind_widget_tooltip(
            self._clean_auth_button,
            "清理同账户重复授权",
        )

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
        self.auth_tree.bind("<Delete>", self._on_auth_tree_delete_key)
        self.auth_tree.bind("<KP_Delete>", self._on_auth_tree_delete_key)
        self.auth_tree.bind("<Motion>", self._on_auth_tree_motion)
        self.auth_tree.bind("<Leave>", lambda _event: self._hide_tooltip())
        self._auth_menu = tk.Menu(self.root, tearoff=0)
        self._auth_menu.add_command(label="切换", command=self._activate_selected_auth_row)
        self._auth_menu.add_command(label="禁用", command=self._toggle_selected_auth_row_disabled)
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

    def _center_child_window(self, window: tk.Toplevel, width: int, height: int) -> None:
        self.root.update_idletasks()
        window.update_idletasks()
        root_x = self.root.winfo_rootx()
        root_y = self.root.winfo_rooty()
        root_width = self.root.winfo_width()
        root_height = self.root.winfo_height()
        x = root_x + max((root_width - width) // 2, 0)
        y = root_y + max((root_height - height) // 2, 0)
        window.geometry(f"{width}x{height}+{x}+{y}")

    def _bind_dialog_close_keys(self, dialog: tk.Toplevel) -> None:
        def close_dialog(_event: tk.Event | None = None) -> str:
            if dialog.winfo_exists():
                dialog.destroy()
            return "break"

        dialog.bind("<Escape>", close_dialog)
        dialog.bind_all("<Escape>", close_dialog)
        dialog.bind("<Destroy>", lambda _event: dialog.unbind_all("<Escape>"), add="+")

    def _post_ui(self, callback: Callable[[], None]) -> None:
        if self._closing:
            return
        self._ui_queue.put(callback)

    def _drain_ui_queue(self) -> None:
        if self._closing:
            return
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
            if not self._closing:
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
        quota_snapshot = self.auth_usage_service.quota_snapshot()
        self._update_quota_drop_state(quota_snapshot)
        self.auth_sync_service.update_usage_cache(
            quota_snapshot,
            self.auth_usage_service.plan_type_snapshot(),
            self.auth_usage_service.user_id_snapshot(),
            self.auth_usage_service.email_snapshot(),
            self.auth_usage_service.quota_refresh_time_5h_snapshot(),
            self.auth_usage_service.quota_refresh_time_7d_snapshot(),
        )
        return True

    def _update_quota_drop_state(self, quota_snapshot: dict[str, str]) -> None:
        current_priorities: dict[str, float] = {}
        dropped_items: list[tuple[str, float, float, str]] = []
        for refresh_token, quota in quota_snapshot.items():
            priority = self._quota_priority(quota)
            if priority < 0:
                continue
            current_priorities[refresh_token] = priority
            previous = self._quota_priority_by_refresh_token.get(refresh_token)
            if previous is None:
                continue
            if priority < previous - _QUOTA_DROP_EPSILON:
                dropped_items.append((refresh_token, previous, priority, quota))

        if not current_priorities:
            return

        quota_drop_message = ""
        target_drop_message = ""
        with self._auto_load_lock:
            self._quota_priority_by_refresh_token.update(current_priorities)
            if not dropped_items or not self._auto_load_enabled:
                return

            target_refresh_token = self._auto_load_target_refresh_token
            if not target_refresh_token:
                return

            now = time.monotonic()
            if now - self._auto_load_target_selected_at < _QUOTA_DROP_GRACE_SECONDS:
                return

            for refresh_token, previous, current, quota in dropped_items:
                if refresh_token == target_refresh_token:
                    if self._proxy_kill_pending_reason == "quota_drop":
                        self._clear_proxy_kill_pending_locked()
                        target_drop_message = (
                            "[AutoLoad] 当前选举 token 已出现额度下降，取消因额度未命中的断连等待"
                        )
                    continue

                state_changed = (
                    not self._proxy_kill_pending
                    or self._proxy_kill_pending_reason != "quota_drop"
                    or self._proxy_kill_pending_used_access_token != refresh_token
                    or self._proxy_kill_pending_access_token != target_refresh_token
                )
                self._proxy_kill_pending = True
                self._proxy_kill_pending_reason = "quota_drop"
                self._proxy_kill_pending_access_token = target_refresh_token
                self._proxy_kill_pending_used_access_token = refresh_token
                if state_changed:
                    self._proxy_kill_attempt_in_flight = False
                    quota_drop_message = (
                        "[AutoLoad] 额度下降命中非当前选举 token，等待下一次 WebSocket ping/pong 断开旧连接: "
                        f"actual_refresh_token={refresh_token} target_refresh_token={target_refresh_token} "
                        f"quota={previous:g}%->{current:g}% raw={quota}"
                    )
                break

        if target_drop_message:
            print(target_drop_message, flush=True)
        if quota_drop_message:
            print(quota_drop_message, flush=True)

    def _on_auto_load_access_token_used(self, access_token: str) -> None:
        self._update_proxy_kill_pending_for_used_token(access_token)
        if not self.auth_sync_service.increment_traffic_by_access_token(access_token):
            return
        self._schedule_refresh_traffic()

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
        self._clear_proxy_kill_pending()
        self.refresh_auth_files(update_status=False)

    def _recompute_auto_load_target(self) -> None:
        with self._auto_load_lock:
            if not self._auto_load_enabled:
                return
            current_refresh_token = self._auto_load_target_refresh_token
            current_access_token = self._auto_load_target_access_token
        rows = self.auth_sync_service.list_auth_rows()
        rows = [row for row in rows if not row.disabled]
        if not rows:
            self._set_auto_load_target("", "")
            self._clear_proxy_kill_pending()
            print("[AutoLoad] 当前没有可选授权文件", flush=True)
            return
        row = max(rows, key=lambda item: (self._quota_priority(item.quota), item.refresh_token))
        target_changed = row.refresh_token != current_refresh_token or row.access_token != current_access_token
        self._set_auto_load_target(row.refresh_token, row.access_token)
        if target_changed:
            print(
                f"[AutoLoad] 负载目标: account_id={row.account_id} refresh_token={row.refresh_token} quota={row.quota}",
                flush=True,
            )
        self._sync_proxy_kill_pending_with_target(row.access_token)
        self.refresh_auth_files(update_status=False)

    def _set_auto_load_target(self, refresh_token: str, access_token: str) -> None:
        with self._auto_load_lock:
            target_changed = (
                refresh_token != self._auto_load_target_refresh_token
                or access_token != self._auto_load_target_access_token
            )
            self._auto_load_target_refresh_token = refresh_token
            self._auto_load_target_access_token = access_token
            if target_changed:
                self._auto_load_target_selected_at = time.monotonic()
                if self._proxy_kill_pending_reason == "quota_drop":
                    self._clear_proxy_kill_pending_locked()

    def _restart_proxy_server_async(self) -> bool:
        with self._proxy_restart_lock:
            if self._proxy_restart_pending:
                return False
            if not (self.service.process and self.service.process.poll() is None):
                return False
            self._proxy_restart_pending = True
        Thread(target=self._restart_proxy_server_worker, daemon=True).start()
        return True

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

    def _update_proxy_kill_pending_for_used_token(self, used_access_token: str) -> None:
        used_access_token = used_access_token.strip()
        if not used_access_token:
            return
        mismatch_detected = False
        mismatch_resolved = False
        with self._auto_load_lock:
            self._last_used_access_token = used_access_token
            if not self._auto_load_enabled:
                self._clear_proxy_kill_pending_locked()
                return
            target_access_token = self._auto_load_target_access_token.strip()
            if not target_access_token:
                self._clear_proxy_kill_pending_locked()
                return
            if used_access_token == target_access_token:
                if self._proxy_kill_pending_reason != "quota_drop":
                    mismatch_resolved = self._proxy_kill_pending
                    self._clear_proxy_kill_pending_locked()
            else:
                state_changed = (
                    not self._proxy_kill_pending
                    or self._proxy_kill_pending_access_token != target_access_token
                    or self._proxy_kill_pending_used_access_token != used_access_token
                )
                self._proxy_kill_pending = True
                self._proxy_kill_pending_reason = "token_mismatch"
                self._proxy_kill_pending_access_token = target_access_token
                self._proxy_kill_pending_used_access_token = used_access_token
                if state_changed:
                    self._proxy_kill_attempt_in_flight = False
                    mismatch_detected = True
        if mismatch_resolved:
            print("[AutoLoad] 实际消耗 token 已与当前选举目标一致，停止等待断连", flush=True)
            return
        if mismatch_detected:
            print(
                "[AutoLoad] 检测到 token 消耗未命中当前选举目标，等待下一次 WebSocket ping/pong 断开旧连接",
                flush=True,
            )

    def _sync_proxy_kill_pending_with_target(self, target_access_token: str) -> None:
        with self._auto_load_lock:
            if not self._auto_load_enabled or not target_access_token:
                self._clear_proxy_kill_pending_locked()
                return
            if (
                self._proxy_kill_pending_reason != "quota_drop"
                and self._last_used_access_token
                and self._last_used_access_token == target_access_token
            ):
                self._clear_proxy_kill_pending_locked()

    def _clear_proxy_kill_pending(self) -> None:
        with self._auto_load_lock:
            self._clear_proxy_kill_pending_locked()

    def _consume_proxy_kill_pending(self) -> bool:
        kill_message = ""
        with self._auto_load_lock:
            if not self._is_proxy_kill_pending_locked():
                return False
            if self._proxy_kill_attempt_in_flight:
                return False
            now = time.monotonic()
            if now < self._proxy_kill_next_allowed_at:
                return False
            self._proxy_kill_attempt_in_flight = True
            self._proxy_kill_next_allowed_at = now + _AUTO_LOAD_MISMATCH_WINDOW_SECONDS
            kill_message = (
                "[AutoLoad] WebSocket ping/pong 触发断开旧连接: "
                f"reason={self._proxy_kill_pending_reason or '-'} "
                f"target={self._proxy_kill_pending_access_token or '-'} "
                f"actual={self._proxy_kill_pending_used_access_token or '-'}"
            )
        if kill_message:
            print(kill_message, flush=True)
        return True

    def _record_proxy_kill_result(self, killed: int) -> None:
        with self._auto_load_lock:
            self._proxy_kill_attempt_in_flight = False
            if killed > 0:
                self._clear_proxy_kill_pending_locked(preserve_cooldown=True)
                return
            if self._proxy_kill_pending:
                print("[AutoLoad] 本次未找到可断开的旧连接，保留断连等待并按冷却时间重试", flush=True)

    def _request_manual_proxy_kill(self) -> None:
        with self._auto_load_lock:
            self._manual_proxy_kill_pending = True
        print("[ProxyWindow] 已请求断开代理长连接", flush=True)

    def _consume_manual_proxy_kill_pending(self) -> bool:
        with self._auto_load_lock:
            if not self._manual_proxy_kill_pending:
                return False
            self._manual_proxy_kill_pending = False
        return True

    def _record_manual_proxy_kill_result(self, killed: int, tracked: int, reset: int) -> None:
        print(f"[ProxyWindow] 已断开代理连接: tracked={tracked} reset={reset} killed={killed}", flush=True)

    def _is_proxy_kill_pending_locked(self) -> bool:
        return self._proxy_kill_pending

    def _clear_proxy_kill_pending_locked(self, preserve_cooldown: bool = False) -> None:
        next_allowed_at = self._proxy_kill_next_allowed_at if preserve_cooldown else 0.0
        self._proxy_kill_pending = False
        self._proxy_kill_pending_access_token = ""
        self._proxy_kill_pending_used_access_token = ""
        self._proxy_kill_pending_reason = ""
        self._proxy_kill_next_allowed_at = next_allowed_at
        self._proxy_kill_attempt_in_flight = False

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
        if not self._refresh_config():
            return
        if self.service.process and self.service.process.poll() is None:
            print("[ProxyWindow] 二级代理配置已变更，正在重启代理使配置立即生效", flush=True)
            self._restart_proxy_server_async()

    def _get_auto_load_target_refresh_token(self) -> str:
        with self._auto_load_lock:
            return self._auto_load_target_refresh_token if self._auto_load_enabled else ""

    def _get_auto_load_marks(self) -> tuple[str, str]:
        current_refresh_token = ""
        for row in self.auth_sync_service.list_auth_rows():
            if row.current:
                current_refresh_token = row.refresh_token
                break
        return current_refresh_token, self._get_auto_load_target_refresh_token()

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
                        payload = conn.recv(65536).decode("utf-8", errors="ignore").strip()
                    except socket.timeout:
                        payload = ""
                    if payload.startswith("USED "):
                        parts = payload.split()
                        token = parts[1] if len(parts) >= 2 else ""
                        if token:
                            self._post_ui(lambda value=token: self._on_auto_load_access_token_used(value))
                        continue
                    if payload.startswith("KILL_RESULT "):
                        parts = payload.split()
                        try:
                            killed = int(parts[1]) if len(parts) >= 2 else 0
                        except ValueError:
                            killed = 0
                        self._post_ui(lambda value=killed: self._record_proxy_kill_result(value))
                        continue
                    if payload.startswith("MANUAL_KILL_RESULT "):
                        parts = payload.split()
                        try:
                            killed = int(parts[1]) if len(parts) >= 2 else 0
                            tracked = int(parts[2]) if len(parts) >= 3 else killed
                            reset = int(parts[3]) if len(parts) >= 4 else killed
                        except ValueError:
                            killed = 0
                            tracked = 0
                            reset = 0
                        self._post_ui(
                            lambda value=killed, total=tracked, reset_count=reset: self._record_manual_proxy_kill_result(
                                value,
                                total,
                                reset_count,
                            )
                        )
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
                    if payload == "RESELECT":
                        self._post_ui(self._recompute_auto_load_target)
                        continue
                    if payload == "PINGPONG":
                        conn.sendall(("1\n" if self._consume_proxy_kill_pending() else "0\n").encode("utf-8"))
                        continue
                    if payload == "MANUAL_KILL":
                        conn.sendall(("1\n" if self._consume_manual_proxy_kill_pending() else "0\n").encode("utf-8"))
                        continue
                    if payload == "IDLE_TIMEOUT":
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

    def update_auth(self) -> None:
        if not messagebox.askyesno("更新授权", "是否创建新的授权文件？\n注意：本次操作会结束Codex进程"):
            return
        auth_path = Path(os.environ.get("USERPROFILE", str(Path.home()))) / ".codex" / "auth.json"
        try:
            if auth_path.exists():
                auth_path.unlink()
                print(f"[ProxyWindow] 已删除授权文件: {auth_path}", flush=True)
            else:
                print(f"[ProxyWindow] 授权文件不存在: {auth_path}", flush=True)
        except OSError as exc:
            messagebox.showerror("更新授权失败", f"删除授权文件失败: {exc}")
            return

        for image_name in ("Codex.exe", "code.exe"):
            self._kill_process_by_image_name(image_name)

        launch_row = self._get_default_codex_launch_row()
        if launch_row is None:
            messagebox.showerror("更新授权失败", "未找到可启动的 Codex 安装项。")
            return
        self._launch_codex(launch_row)

    def correct_traffic(self) -> None:
        if self._correct_traffic_refreshing:
            return
        self._correct_traffic_refreshing = True
        if self._correct_traffic_button is not None:
            self._correct_traffic_button.config(text="刷新额度中", state="disabled")
        Thread(target=self._refresh_quota_before_correct_traffic_worker, daemon=True).start()

    def _refresh_quota_before_correct_traffic_worker(self) -> None:
        error = ""
        try:
            self.auth_usage_service.refresh_once()
        except Exception as exc:
            error = str(exc)
        try:
            self._post_ui(lambda value=error: self._finish_correct_traffic_quota_refresh(value))
        except tk.TclError:
            pass

    def _finish_correct_traffic_quota_refresh(self, error: str) -> None:
        self._correct_traffic_refreshing = False
        if error:
            print(f"[ProxyWindow] 矫正流量前刷新额度失败: {error}", flush=True)
        self._apply_pending_quota_items()
        self.refresh_auth_files(update_status=False)
        self._request_manual_proxy_kill()
        self._start_correct_traffic_countdown()

    def clean_auth_files(self) -> None:
        if self._clean_auth_refreshing:
            return
        self._clean_auth_refreshing = True
        if self._clean_auth_button is not None:
            self._clean_auth_button.config(text="刷新额度中", state="disabled")
        Thread(target=self._clean_auth_files_worker, daemon=True).start()

    def _clean_auth_files_worker(self) -> None:
        refreshed_items: list[AuthQuotaItem] = []
        error = ""
        try:
            refreshed_items = self.auth_usage_service.refresh_once()
        except Exception as exc:
            error = str(exc)
        try:
            self._post_ui(lambda items=refreshed_items, value=error: self._finish_clean_auth_refresh(items, value))
        except tk.TclError:
            pass

    def _finish_clean_auth_refresh(self, refreshed_items: list[AuthQuotaItem], error: str) -> None:
        self._clean_auth_refreshing = False
        if self._clean_auth_button is not None:
            self._clean_auth_button.config(text="清理授权", state="normal")
        if error:
            messagebox.showerror("清理授权失败", f"刷新额度失败: {error}")
            return

        self._apply_pending_quota_items()
        rows = self.auth_sync_service.list_auth_rows()
        refreshed_tokens = {
            item.refresh_token
            for item in refreshed_items
            if item.refresh_token and self._is_normal_quota(item.quota)
        }
        delete_rows = self._build_clean_auth_delete_rows(rows, refreshed_tokens)
        self._print_clean_auth_plan(rows, refreshed_tokens, delete_rows)
        self.refresh_auth_files(update_status=False)

        if not delete_rows:
            messagebox.showinfo("清理授权", "没有发现需要清理的无效或重复授权。")
            return

        message = self._build_clean_auth_confirm_message(delete_rows)
        if not messagebox.askyesno("清理授权", message):
            return
        self._delete_clean_auth_rows(delete_rows)

    def open_low_price_window(self) -> None:
        if self._low_price_window is not None and self._low_price_window.winfo_exists():
            self._low_price_window.lift()
            self._low_price_window.focus_force()
            return

        dialog = tk.Toplevel(self.root)
        self._low_price_window = dialog
        dialog.title("低价购号")
        dialog.transient(self.root)
        dialog.resizable(True, True)
        dialog.protocol("WM_DELETE_WINDOW", self._close_low_price_window)
        self._bind_dialog_close_keys(dialog)
        self._center_child_window(dialog, 560, 420)

        body = ttk.Frame(dialog, padding=12)
        body.pack(fill="both", expand=True)

        action_row = ttk.Frame(body)
        action_row.pack(fill="x", pady=(0, 8))
        self._low_price_refresh_button = ttk.Button(action_row, text="刷新", command=self.refresh_low_price_accounts)
        self._low_price_refresh_button.pack(side="right")

        tree_frame = ttk.Frame(body)
        tree_frame.pack(fill="both", expand=True)

        columns = ("productId", "title", "price", "sales")
        self._low_price_tree = ttk.Treeview(tree_frame, columns=columns, show="headings", selectmode="browse")
        self._low_price_tree.heading("productId", text="产品id")
        self._low_price_tree.heading("title", text="标题")
        self._low_price_tree.heading("price", text="价格")
        self._low_price_tree.heading("sales", text="销量")
        self._low_price_tree.column("productId", width=90, anchor="center", stretch=False)
        self._low_price_tree.column("title", width=230, anchor="w", stretch=False)
        self._low_price_tree.column("price", width=110, anchor="center", stretch=False)
        self._low_price_tree.column("sales", width=110, anchor="center", stretch=False)
        self._low_price_tree.tag_configure("planKeyword", background="#fff4d6", foreground="#5c4300")
        self._low_price_tree.bind("<Motion>", self._on_low_price_tree_motion)
        self._low_price_tree.bind("<Leave>", lambda _event: self._hide_tooltip())
        self._low_price_tree.bind("<Double-1>", self._on_low_price_tree_double_click)
        y_scroll = ttk.Scrollbar(tree_frame, orient="vertical", command=self._low_price_tree.yview)
        self._low_price_tree.configure(yscrollcommand=y_scroll.set)
        self._low_price_tree.grid(row=0, column=0, sticky="nsew")
        y_scroll.grid(row=0, column=1, sticky="ns")
        tree_frame.rowconfigure(0, weight=1)
        tree_frame.columnconfigure(0, weight=1)
        dialog.bind("<Destroy>", self._on_low_price_window_destroy, add="+")
        self.refresh_low_price_accounts()

    def refresh_low_price_accounts(self) -> None:
        if self._low_price_tree is None or self._low_price_refreshing:
            return
        self._low_price_refreshing = True
        self._set_low_price_buttons_state()
        proxy_url = self._get_proxy_for_usage_request()
        Thread(target=self._refresh_low_price_accounts_worker, args=(proxy_url,), daemon=True).start()

    def _refresh_low_price_accounts_worker(self, proxy_url: str) -> None:
        try:
            items_by_product_id: dict[str, LowPriceAccount] = {}
            for page in range(1, 6):
                for item in self.low_price_account_service.fetch_accounts(proxy_url, page=page):
                    items_by_product_id.setdefault(item.product_id, item)
            items = list(items_by_product_id.values())
            error = ""
        except Exception as exc:
            items = []
            error = str(exc)
        try:
            self._post_ui(lambda: self._finish_low_price_refresh(items, error))
        except tk.TclError:
            pass

    def _finish_low_price_refresh(self, items: list[LowPriceAccount], error: str) -> None:
        self._low_price_refreshing = False
        self._set_low_price_buttons_state()
        if self._low_price_tree is None:
            return
        if error:
            messagebox.showerror("低价购号刷新失败", error)
            return
        self._low_price_items_by_product_id.clear()
        for item in items:
            self._low_price_items_by_product_id[item.product_id] = item
        self._render_low_price_items()

    def _set_low_price_buttons_state(self) -> None:
        if self._low_price_refresh_button is None:
            return
        if not self._low_price_refreshing:
            self._low_price_refresh_button.config(text="刷新", state="normal")
            return
        self._low_price_refresh_button.config(text="刷新中", state="disabled")

    def _render_low_price_items(self) -> None:
        if self._low_price_tree is None:
            return
        for item_id in self._low_price_tree.get_children():
            self._low_price_tree.delete(item_id)
        self._low_price_product_id_by_item.clear()
        self._low_price_titles_by_item.clear()
        for item in sorted(self._low_price_items_by_product_id.values(), key=self._low_price_sort_key):
            item_id = self._low_price_tree.insert(
                "",
                "end",
                values=(item.product_id, self._format_low_price_title(item.title), item.price, item.sales),
                tags=("planKeyword",) if self._is_low_price_plan_title(item.title) else (),
            )
            self._low_price_product_id_by_item[item_id] = item.product_id
            self._low_price_titles_by_item[item_id] = item.title

    def _low_price_sort_key(self, item: LowPriceAccount) -> tuple[int, int, str]:
        plan_priority = 0 if self._is_low_price_plan_title(item.title) else 1
        return (plan_priority, self._price_sort_value(item.price), -self._sales_sort_value(item.sales), item.product_id)

    def _price_sort_value(self, price: str) -> float:
        normalized = price.replace(",", ".")
        match = re.search(r"\d+(?:\.\d+)?", normalized)
        if not match:
            return float("inf")
        return float(match.group(0))

    def _sales_sort_value(self, sales: str) -> int:
        digits = "".join(re.findall(r"\d+", sales))
        if not digits:
            return 0
        return int(digits)

    def _format_low_price_title(self, title: str) -> str:
        max_length = 34
        if len(title) <= max_length:
            return title
        return f"{title[:max_length - 3]}..."

    def _is_low_price_plan_title(self, title: str) -> bool:
        return re.search(r"\b(team|plus|pro|business)\b", title, re.IGNORECASE) is not None

    def _on_low_price_tree_motion(self, event: tk.Event) -> None:
        if self._low_price_tree is None:
            self._hide_tooltip()
            return
        row_id = self._low_price_tree.identify_row(event.y)
        column = self._low_price_tree.identify_column(event.x)
        title = self._low_price_titles_by_item.get(row_id, "")
        if row_id and column == "#2" and title:
            self._show_tooltip(event.x_root, event.y_root, title)
            return
        self._hide_tooltip()

    def _on_low_price_tree_double_click(self, event: tk.Event) -> None:
        if self._low_price_tree is None:
            return
        row_id = self._low_price_tree.identify_row(event.y)
        product_id = self._low_price_product_id_by_item.get(row_id, "")
        item = self._low_price_items_by_product_id.get(product_id)
        if item is None or not item.href:
            return
        webbrowser.open(self._build_low_price_url(item.href))

    def _build_low_price_url(self, href: str) -> str:
        path = href if href.startswith("/") else f"/{href}"
        separator = "&" if "?" in path else "?"
        return f"https://plati.market{path}{separator}ai=1426781"

    def _close_low_price_window(self) -> None:
        if self._low_price_window is not None and self._low_price_window.winfo_exists():
            self._low_price_window.destroy()

    def _on_low_price_window_destroy(self, event: tk.Event) -> None:
        if event.widget is self._low_price_window:
            self._low_price_window = None
            self._low_price_tree = None
            self._low_price_refresh_button = None
            self._low_price_items_by_product_id.clear()
            self._low_price_product_id_by_item.clear()
            self._low_price_titles_by_item.clear()

    def _start_correct_traffic_countdown(self) -> None:
        if self._correct_traffic_after_id is not None:
            try:
                self.root.after_cancel(self._correct_traffic_after_id)
            except tk.TclError:
                pass
            self._correct_traffic_after_id = None
        self._update_correct_traffic_countdown(5)

    def _update_correct_traffic_countdown(self, seconds: int) -> None:
        if self._correct_traffic_button is None:
            return
        if seconds <= 0:
            self._correct_traffic_button.config(text="矫正流量", state="normal")
            self._correct_traffic_after_id = None
            return
        self._correct_traffic_button.config(text=f"重置中 {seconds}", state="disabled")
        self._correct_traffic_after_id = self.root.after(
            1000,
            lambda: self._update_correct_traffic_countdown(seconds - 1),
        )

    def _kill_process_by_image_name(self, image_name: str) -> None:
        result = subprocess.run(
            ["taskkill", "/im", image_name, "/f"],
            capture_output=True,
            text=True,
            encoding="gbk",
            errors="replace",
        )
        output = (result.stdout or result.stderr).strip()
        if result.returncode == 0:
            print(f"[ProxyWindow] 已结束进程: {image_name}", flush=True)
        else:
            print(f"[ProxyWindow] 结束进程跳过: {image_name} {output}", flush=True)

    def _get_default_codex_launch_row(self) -> CodexInstallRow | None:
        rows = list(self._rows_by_item.values()) or self._scan_codex_installs()
        if not rows:
            return None
        for row in rows:
            if row.path.replace("/", "\\").endswith("\\app\\Codex.exe"):
                return row
        return rows[0]

    def kill_codex_processes(self) -> None:
        process_rows = self._list_codex_process_rows()
        dialog = tk.Toplevel(self.root)
        dialog.title("结束进程")
        dialog.transient(self.root)
        dialog.resizable(True, True)
        dialog.grab_set()
        self._bind_dialog_close_keys(dialog)
        self._center_child_window(dialog, 460, 360)

        body = ttk.Frame(dialog, padding=12)
        body.pack(fill="both", expand=True)

        ttk.Label(body, text="选择要强制结束的进程").pack(anchor="w")
        tree_frame = ttk.Frame(body)
        tree_frame.pack(fill="both", expand=True, pady=(8, 10))

        process_tree = ttk.Treeview(tree_frame, columns=("pid",), selectmode="extended")
        process_tree.heading("#0", text="进程名称")
        process_tree.heading("pid", text="PID")
        process_tree.column("#0", width=280, anchor="w")
        process_tree.column("pid", width=120, anchor="center", stretch=False)
        y_scroll = ttk.Scrollbar(tree_frame, orient="vertical", command=process_tree.yview)
        process_tree.configure(yscrollcommand=y_scroll.set)
        process_tree.grid(row=0, column=0, sticky="nsew")
        y_scroll.grid(row=0, column=1, sticky="ns")
        tree_frame.rowconfigure(0, weight=1)
        tree_frame.columnconfigure(0, weight=1)

        item_depths: dict[str, int] = {}

        def insert_process(parent: str, row: CodexProcessRow, depth: int) -> None:
            item_id = str(row.pid)
            process_tree.insert(parent, "end", iid=item_id, text=row.name, values=(row.pid,), open=True)
            item_depths[item_id] = depth
            for child in row.children:
                insert_process(item_id, child, depth + 1)

        for process_row in process_rows:
            insert_process("", process_row, 0)
        all_items = tuple(item_depths)
        root_items = process_tree.get_children("")
        if all_items:
            process_tree.selection_set(root_items)
        else:
            process_tree.insert("", "end", text="未找到 Codex.exe 或 codex.exe 进程", values=("",))

        button_row = ttk.Frame(body)
        button_row.pack(fill="x")
        button_row.columnconfigure(0, weight=1)
        button_row.columnconfigure(1, weight=0)
        button_row.columnconfigure(2, weight=1)

        def kill_selected() -> None:
            selected_items = [item for item in process_tree.selection() if item in item_depths]
            if not selected_items:
                messagebox.showwarning("结束进程", "请先选择要结束的进程。", parent=dialog)
                return
            messages = self._kill_process_ids([int(item) for item in selected_items], item_depths)
            if messages:
                messagebox.showerror("结束进程失败", "\n".join(messages), parent=dialog)
            dialog.destroy()

        end_button = ttk.Button(button_row, text="结束选中进程", command=kill_selected)
        end_button.grid(row=0, column=1)
        if not all_items:
            end_button.config(state="disabled")

        dialog.protocol("WM_DELETE_WINDOW", dialog.destroy)
        dialog.wait_window()

    def _list_codex_process_rows(self) -> list[CodexProcessRow]:
        target_names = {"Codex.exe", "codex.exe"}
        root_processes: list[psutil.Process] = []
        for process in psutil.process_iter(["name"]):
            try:
                if process.info.get("name") in target_names:
                    root_processes.append(process)
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
        child_pids = {
            child.pid
            for process in root_processes
            for child in self._safe_children(process)
        }
        roots = [process for process in root_processes if process.pid not in child_pids]
        return [self._build_process_row(process) for process in sorted(roots, key=lambda item: item.pid)]

    def _build_process_row(self, process: psutil.Process) -> CodexProcessRow:
        try:
            process_name = process.name()
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            process_name = "<无法读取>"
        children = [self._build_process_row(child) for child in self._safe_children(process)]
        children.sort(key=lambda row: row.pid)
        return CodexProcessRow(pid=process.pid, name=process_name, children=children)

    def _safe_children(self, process: psutil.Process) -> list[psutil.Process]:
        try:
            return process.children()
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            return []

    def _kill_process_ids(self, process_ids: list[int], item_depths: dict[str, int]) -> list[str]:
        failed_messages: list[str] = []
        ordered_process_ids = sorted(process_ids, key=lambda pid: item_depths.get(str(pid), 0), reverse=True)
        for process_id in ordered_process_ids:
            result = subprocess.run(
                ["taskkill", "/pid", str(process_id), "/f"],
                capture_output=True,
                text=True,
                encoding="gbk",
                errors="replace",
            )
            output = (result.stdout or result.stderr).strip()
            if result.returncode != 0:
                failed_messages.append(f"PID {process_id}: {output or '结束失败'}")
        return failed_messages

    def refresh_installs(self, update_status: bool = True) -> int:
        self._destroy_launch_buttons()
        for item in self.tree.get_children():
            self.tree.delete(item)
        self._rows_by_item.clear()
        rows = self._scan_codex_installs()
        for row in rows:
            item = self.tree.insert("", "end", values=(row.name, row.display_path, row.size, row.version, ""))
            self._rows_by_item[item] = row
        self.root.after_idle(self._refresh_launch_buttons)
        signature = tuple((row.name, row.display_path, row.size, row.version) for row in rows)
        if update_status and signature != self._last_install_rows_signature:
            print(f"已刷新 {len(rows)} 项")
        self._last_install_rows_signature = signature
        return len(rows)

    def refresh_auth_files(self, update_status: bool = True) -> int:
        selected_row = self._get_selected_auth_row()
        selected_refresh_token = selected_row.refresh_token if selected_row is not None else ""
        selected_item = ""
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
                    "¤" if row.disabled else "●" if row.refresh_token == load_refresh_token else "",
                    self._shorten_middle(row.account_id, 16, 10),
                    self._format_last_refresh(row.last_refresh),
                    self._format_last_refresh(row.quota_refresh_time_5h),
                    row.quota,
                    row.plan_type or "",
                    row.traffic,
                ),
            )
            self._auth_rows_by_item[item] = row
            if row.refresh_token == selected_refresh_token:
                selected_item = item
        if selected_item:
            self.auth_tree.selection_set(selected_item)
            self.auth_tree.focus(selected_item)
        signature = tuple(
            (
                row.current,
                row.disabled,
                row.refresh_token == load_refresh_token,
                row.account_id,
                row.last_refresh,
                row.quota_refresh_time_5h,
                row.quota,
                row.plan_type or "",
                row.traffic,
            )
            for row in rows
        )
        if update_status and signature != self._last_auth_rows_signature:
            print(f"已刷新 {len(rows)} 个文件")
        self._last_auth_rows_signature = signature
        self._refresh_tray_icon_tooltip(rows)
        return len(rows)

    def _scan_codex_installs(self) -> list[CodexInstallRow]:
        rows: list[CodexInstallRow] = []
        rows.extend(self._scan_windowsapps_codex())
        rows.extend(self._scan_node_codex())
        return [row for row in rows if self._is_supported_codex_executable(row.path)]

    def _is_supported_codex_executable(self, path: str) -> bool:
        normalized_path = path.replace("/", "\\")
        return normalized_path.endswith("\\app\\Codex.exe") or normalized_path.endswith("\\codex\\codex.exe")

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

    def _is_normal_quota(self, quota: str) -> bool:
        value = quota.strip()
        return bool(value and value != "—")

    def _last_refresh_sort_key(self, text: str) -> float:
        if not text:
            return 0.0
        try:
            return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
        except ValueError:
            return 0.0

    def _build_clean_auth_delete_rows(
        self,
        rows: list[AuthFileRow],
        refreshed_tokens: set[str],
    ) -> list[AuthFileRow]:
        delete_rows_by_token: dict[str, AuthFileRow] = {
            row.refresh_token: row
            for row in rows
            if row.refresh_token and row.refresh_token not in refreshed_tokens
        }
        rows_by_account_id: dict[str, list[AuthFileRow]] = {}
        for row in rows:
            if not row.account_id:
                continue
            rows_by_account_id.setdefault(row.account_id, []).append(row)

        for account_rows in rows_by_account_id.values():
            if len(account_rows) <= 1:
                continue
            normal_rows = [row for row in account_rows if row.refresh_token in refreshed_tokens]
            if not normal_rows:
                continue
            keep_row = max(normal_rows, key=lambda item: (self._last_refresh_sort_key(item.last_refresh), item.refresh_token))
            for row in account_rows:
                if row.refresh_token != keep_row.refresh_token:
                    delete_rows_by_token[row.refresh_token] = row
        return list(delete_rows_by_token.values())

    def _print_clean_auth_plan(
        self,
        rows: list[AuthFileRow],
        refreshed_tokens: set[str],
        delete_rows: list[AuthFileRow],
    ) -> None:
        delete_tokens = {row.refresh_token for row in delete_rows}
        print("[ProxyWindow] 清理授权刷新成功队列:", flush=True)
        for row in rows:
            if row.refresh_token not in refreshed_tokens:
                continue
            print(
                f"  account_id={row.account_id} refresh_token={row.refresh_token} "
                f"last_refresh={row.last_refresh} quota={row.quota}",
                flush=True,
            )
        print("[ProxyWindow] 清理授权待删除队列:", flush=True)
        if not delete_rows:
            print("  无", flush=True)
            return
        for row in delete_rows:
            print(
                f"  account_id={row.account_id} refresh_token={row.refresh_token} "
                f"last_refresh={row.last_refresh} quota={row.quota} "
                f"{'current' if row.current else ''} {'disabled' if row.disabled else ''}",
                flush=True,
            )
        kept_rows = [row for row in rows if row.account_id and row.refresh_token not in delete_tokens]
        print("[ProxyWindow] 清理授权保留队列:", flush=True)
        for row in kept_rows:
            if any(item.account_id == row.account_id for item in delete_rows):
                print(
                    f"  account_id={row.account_id} refresh_token={row.refresh_token} "
                    f"last_refresh={row.last_refresh} quota={row.quota}",
                    flush=True,
                )

    def _build_clean_auth_confirm_message(self, delete_rows: list[AuthFileRow]) -> str:
        lines = [
            "确认清理无效授权吗？",
            "",
            f"将删除 {len(delete_rows)} 个授权文件：额度刷新失败的授权会被清理；同账户多个授权都有效时，仅保留令牌刷新时间最新的授权。",
            "",
            "待删除:",
        ]
        for row in delete_rows[:12]:
            lines.append(
                f"- {self._shorten_middle(row.account_id, 16, 10)} "
                f"{self._format_last_refresh(row.last_refresh)} "
                f"{self._redact_middle(row.refresh_token)}"
            )
        if len(delete_rows) > 12:
            lines.append(f"... 其余 {len(delete_rows) - 12} 个已打印到日志")
        return "\n".join(lines)

    def _delete_clean_auth_rows(self, delete_rows: list[AuthFileRow]) -> None:
        deleted_tokens: set[str] = set()
        errors: list[str] = []
        for row in delete_rows:
            ok, message = self.auth_sync_service.delete_auth_file(row.refresh_token)
            if ok:
                deleted_tokens.add(row.refresh_token)
                continue
            errors.append(f"{row.refresh_token}: {message}")

        if self._get_auto_load_target_refresh_token() in deleted_tokens:
            self._set_auto_load_target("", "")
        self.auth_usage_service.remove_tokens(deleted_tokens)
        for refresh_token in deleted_tokens:
            self._quota_priority_by_refresh_token.pop(refresh_token, None)
        if self.auto_load_var.get():
            self._recompute_auto_load_target()
        self.refresh_auth_files(update_status=False)

        if errors:
            messagebox.showerror("清理授权失败", "\n".join(errors[:8]))
            return
        messagebox.showinfo("清理授权", f"已清理 {len(deleted_tokens)} 个授权文件。")

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
            f"状态: {'禁用' if row.disabled else '启用'}",
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
        self._auth_menu.entryconfig(1, label="启用" if row.disabled else "禁用")
        try:
            self._auth_menu.tk_popup(event.x_root, event.y_root)
        finally:
            self._auth_menu.grab_release()

    def _on_auth_tree_delete_key(self, _event: tk.Event) -> str:
        self._delete_selected_auth_row()
        return "break"

    def _activate_selected_auth_row(self) -> None:
        row = self._get_selected_auth_row()
        if row is not None:
            self._activate_auth_row(row)

    def _delete_selected_auth_row(self) -> None:
        row = self._get_selected_auth_row()
        if row is not None:
            self._delete_auth_row(row)

    def _toggle_selected_auth_row_disabled(self) -> None:
        row = self._get_selected_auth_row()
        if row is not None:
            self._set_auth_row_disabled(row, not row.disabled)

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

    def _set_auth_row_disabled(self, row: AuthFileRow, disabled: bool) -> None:
        ok, message = self.auth_sync_service.set_auth_disabled(row.refresh_token, disabled)
        if not ok:
            messagebox.showerror("更新状态失败", message)
            return
        if disabled and row.refresh_token == self._get_auto_load_target_refresh_token():
            self._set_auto_load_target("", "")
            self._clear_proxy_kill_pending()
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

    def _bind_widget_tooltip(self, widget: tk.Widget, text: str) -> None:
        widget.bind("<Enter>", lambda event: self._show_tooltip(event.x_root, event.y_root, text))
        widget.bind("<Motion>", lambda event: self._show_tooltip(event.x_root, event.y_root, text))
        widget.bind("<Leave>", lambda _event: self._hide_tooltip())

    def _destroy_launch_buttons(self) -> None:
        for button in self._launch_buttons.values():
            button.destroy()
        self._launch_buttons.clear()

    def _refresh_launch_buttons(self) -> None:
        if not hasattr(self, "tree"):
            return
        for item in self.tree.get_children():
            row = self._rows_by_item.get(item)
            if row is None:
                continue
            button = self._launch_buttons.get(item)
            if button is None:
                button = ttk.Button(
                    self.tree,
                    text="启动",
                    command=lambda launch_row=row: self._launch_codex(launch_row),
                )
                self._launch_buttons[item] = button
            bbox = self.tree.bbox(item, "action")
            if not bbox:
                button.place_forget()
                continue
            x, y, width, height = bbox
            button_width = 56
            button_height = 24
            button.place(
                x=x + max((width - button_width) // 2, 0),
                y=y + max((height - button_height) // 2, 0),
                width=button_width,
                height=button_height,
            )
        stale_items = set(self._launch_buttons) - set(self.tree.get_children())
        for item in stale_items:
            self._launch_buttons.pop(item).destroy()

    def _on_tree_click(self, event: tk.Event) -> None:
        self.tree.selection_remove(self.tree.selection())
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

    def _on_root_unmap(self, _event: tk.Event) -> None:
        if not self._tray_icon_visible and self.root.state() == "iconic":
            self.root.after(0, self._hide_to_tray)

    def _hide_to_tray(self) -> None:
        self._add_tray_icon()
        self.root.withdraw()

    def _restore_from_tray(self) -> None:
        self._remove_tray_icon()
        self.root.deiconify()
        self.root.lift()
        self.root.focus_force()

    def _add_tray_icon(self) -> None:
        if self._tray_icon_visible:
            return
        menu = pystray.Menu(pystray.MenuItem("显示", self._on_tray_show, default=True))
        self._tray_icon = pystray.Icon(_TRAY_ICON_TIP, self._load_tray_icon(), self._build_tray_icon_tip(), menu)
        self._tray_icon.run_detached()
        self._tray_icon_visible = True

    def _remove_tray_icon(self) -> None:
        if not self._tray_icon_visible:
            return
        if self._tray_icon is not None:
            self._tray_icon.stop()
            self._tray_icon = None
        self._tray_icon_visible = False

    def _on_tray_show(self, _icon: pystray.Icon, _item: pystray.MenuItem) -> None:
        self._post_ui(self._restore_from_tray)

    def _refresh_tray_icon_tooltip(self, rows: list[AuthFileRow] | None = None) -> None:
        if not self._tray_icon_visible or self._tray_icon is None:
            return
        self._tray_icon.title = self._build_tray_icon_tip(rows)

    def _build_tray_icon_tip(self, rows: list[AuthFileRow] | None = None) -> str:
        auth_rows = rows if rows is not None else self.auth_sync_service.list_auth_rows()
        if not auth_rows:
            return _TRAY_ICON_TIP
        current_refresh_token, load_refresh_token = self._get_auto_load_marks()
        lines = [_TRAY_ICON_TIP]
        for row in auth_rows[:_TRAY_ICON_MAX_ROWS]:
            marks = []
            if row.refresh_token == current_refresh_token:
                marks.append("当前")
            if row.refresh_token == load_refresh_token:
                marks.append("负载")
            mark_text = f"[{'/'.join(marks)}] " if marks else ""
            account_id = self._shorten_middle(row.account_id, 6, 4) if row.account_id else "-"
            quota = row.quota or "-"
            lines.append(f"{mark_text}{account_id} {quota}")
        if len(auth_rows) > _TRAY_ICON_MAX_ROWS:
            lines.append("...")
        tip = "\n".join(lines)
        if len(tip) <= _TRAY_ICON_MAX_TIP_LENGTH:
            return tip
        truncated_lines = [lines[0]]
        for line in lines[1:]:
            candidate = "\n".join(truncated_lines + [line])
            if len(candidate) > _TRAY_ICON_MAX_TIP_LENGTH - 4:
                truncated_lines.append("...")
                break
            truncated_lines.append(line)
        return "\n".join(truncated_lines)

    def _load_tray_icon(self) -> Image.Image:
        icon_path = self._get_icon_path()
        if icon_path.exists():
            return Image.open(icon_path)
        return Image.new("RGBA", (64, 64), "#2d8cff")

    def _get_icon_path(self) -> Path:
        if getattr(sys, "frozen", False):
            base_dir = Path(sys.executable).resolve().parent
        else:
            base_dir = Path(__file__).resolve().parents[2]
        return base_dir / "icon" / "tray_icon.ico"

    def _on_close(self) -> None:
        if not messagebox.askyesno("退出确认", "确认退出程序吗？"):
            return
        self._closing = True
        self._remove_tray_icon()
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
