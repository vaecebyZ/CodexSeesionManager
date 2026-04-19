import json
import os
import socket
import subprocess
import time
import ctypes
from pathlib import Path
import urllib.request

import psutil


class ChromeService:
    def __init__(self, remote_debugging_port: int = 9222, debug_wait_seconds: float = 300.0) -> None:
        self.remote_debugging_port = remote_debugging_port
        self.debug_wait_seconds = debug_wait_seconds

    def find_chrome(self) -> str | None:
        candidates = self._candidate_paths()
        for candidate in candidates:
            if candidate and Path(candidate).exists():
                return candidate
        return self._find_in_path()

    def is_debug_port_open(self) -> bool:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(0.3)
            return sock.connect_ex(("127.0.0.1", self.remote_debugging_port)) == 0

    def is_debug_session_alive(self) -> bool:
        if not self.is_debug_port_open():
            return False
        try:
            with urllib.request.urlopen(
                f"http://127.0.0.1:{self.remote_debugging_port}/json/list",
                timeout=2,
            ) as response:
                tabs = json.loads(response.read().decode("utf-8"))
            return isinstance(tabs, list)
        except OSError:
            return False
        except json.JSONDecodeError:
            return False

    def launch_chrome(self, chrome_path: str) -> bool:
        return self.open_chrome(chrome_path)

    def open_chrome(self, chrome_path: str) -> bool:
        user_data_dir = self._default_user_data_dir()
        if not user_data_dir:
            return False
        self._last_chrome_signature = self._build_chrome_signature(user_data_dir)
        try:
            subprocess.Popen(
                [
                    chrome_path,
                    f"--remote-debugging-port={self.remote_debugging_port}",
                    f"--user-data-dir={user_data_dir}",
                    "--disable-background-mode",
                    "--no-first-run",
                    "--no-default-browser-check",
                    "--disable-session-crashed-bubble",
                    "--new-window",
                    "https://chatgpt.com/",
                ],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        except OSError:
            return False
        return True

    def get_running_chrome_pid(self) -> int | None:
        signature = getattr(self, "_last_chrome_signature", None)
        if not signature:
            return None
        for proc in psutil.process_iter(["pid", "name", "cmdline"]):
            try:
                if self._process_matches(proc, signature):
                    return proc.info["pid"]
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
        return None

    def wait_for_debug_port(self) -> bool:
        return self._wait_for_debug_port(self.debug_wait_seconds)

    def _wait_for_debug_port(self, timeout_seconds: float = 10.0) -> bool:
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            if self.is_debug_port_open():
                return True
            time.sleep(0.2)
        return False

    def _candidate_paths(self) -> list[str]:
        program_files = os.environ.get("ProgramFiles", r"C:\Program Files")
        program_files_x86 = os.environ.get("ProgramFiles(x86)", r"C:\Program Files (x86)")
        local_app_data = os.environ.get("LOCALAPPDATA") or str(Path.home() / "AppData" / "Local")
        return [
            self._chrome_from_env_path(),
            rf"{program_files}\Google\Chrome\Application\chrome.exe",
            rf"{program_files_x86}\Google\Chrome\Application\chrome.exe",
            rf"{local_app_data}\Google\Chrome\Application\chrome.exe",
        ]

    def _chrome_from_env_path(self) -> str | None:
        for folder in os.environ.get("PATH", "").split(os.pathsep):
            chrome = Path(folder) / "chrome.exe"
            if chrome.exists():
                return str(chrome)
        return None

    def _find_in_path(self) -> str | None:
        from shutil import which

        return which("chrome") or which("chrome.exe")

    def _default_user_data_dir(self) -> str | None:
        local_app_data = os.environ.get("LOCALAPPDATA")
        if not local_app_data:
            return None
        chrome_user_data = Path(local_app_data) / "Google" / "Chrome" / "User Data"
        if chrome_user_data.exists():
            return str(chrome_user_data)
        return None

    def is_process_running(self, pid: int | None) -> bool:
        if not pid:
            return False
        try:
            return psutil.Process(int(pid)).is_running()
        except (psutil.NoSuchProcess, psutil.AccessDenied, ValueError):
            return False

    def stop_chrome_processes(self) -> None:
        signature = getattr(self, "_last_chrome_signature", None)
        if not signature:
            return

        matched: list[psutil.Process] = []
        for proc in psutil.process_iter(["pid", "name", "cmdline"]):
            try:
                if self._process_matches(proc, signature, include_child_processes=True):
                    matched.append(proc)
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue

        for proc in matched:
            self._terminate_process_tree(proc)

    def _terminate_process_tree(self, proc: psutil.Process) -> None:
        try:
            children = proc.children(recursive=True)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            children = []

        for child in children:
            try:
                child.terminate()
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass

        _, alive = psutil.wait_procs(children, timeout=3)
        for child in alive:
            try:
                child.kill()
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass

        try:
            proc.terminate()
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
        try:
            proc.wait(timeout=3)
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.TimeoutExpired):
            try:
                proc.kill()
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass

    def is_browser_window_open(self, pid: int | None) -> bool:
        if not pid:
            return False

        found = {"value": False}

        @ctypes.WINFUNCTYPE(ctypes.c_bool, ctypes.c_void_p, ctypes.c_void_p)
        def enum_proc(hwnd, lparam):
            window_pid = ctypes.c_ulong()
            ctypes.windll.user32.GetWindowThreadProcessId(hwnd, ctypes.byref(window_pid))
            if window_pid.value != int(pid):
                return True
            if not ctypes.windll.user32.IsWindowVisible(hwnd):
                return True
            class_name = ctypes.create_unicode_buffer(256)
            ctypes.windll.user32.GetClassNameW(hwnd, class_name, 256)
            if "Chrome_WidgetWin" in class_name.value:
                found["value"] = True
                return False
            return True

        ctypes.windll.user32.EnumWindows(enum_proc, 0)
        return found["value"]

    def _build_chrome_signature(self, user_data_dir: str) -> dict[str, str]:
        return {
            "remote_port": f"--remote-debugging-port={self.remote_debugging_port}",
            "user_data_dir": f"--user-data-dir={user_data_dir}",
        }

    def _process_matches(
        self,
        proc: psutil.Process,
        signature: dict[str, str],
        include_child_processes: bool = False,
    ) -> bool:
        name = (proc.info.get("name") or "").lower()
        if "chrome" not in name:
            return False
        cmdline = " ".join(proc.info.get("cmdline") or [])
        matched = (
            signature["remote_port"] in cmdline
            and signature["user_data_dir"] in cmdline
        )
        if not matched:
            return False
        if include_child_processes:
            return True
        return "--type=" not in cmdline
