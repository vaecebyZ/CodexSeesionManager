import ctypes
import os
import subprocess
import sys
from pathlib import Path

_MB_YESNO = 0x00000004
_MB_OK = 0x00000000
_MB_ICONWARNING = 0x00000030
_MB_DEFBUTTON2 = 0x00000100
_IDYES = 6


def _get_resource_path(*parts: str) -> str:
    if getattr(sys, "frozen", False):
        base_dir = Path(sys.executable).resolve().parent
    else:
        base_dir = Path(__file__).resolve().parent
    return str(base_dir.joinpath(*parts))


def _is_admin() -> bool:
    try:
        return bool(ctypes.windll.shell32.IsUserAnAdmin())
    except (AttributeError, OSError):
        return False


def _show_admin_prompt() -> bool:
    try:
        result = ctypes.windll.user32.MessageBoxW(
            None,
            "当前不是管理员身份运行。\n\n点击“是”将以管理员身份重新启动，点击“否”将直接退出。",
            "需要管理员权限",
            _MB_YESNO | _MB_ICONWARNING | _MB_DEFBUTTON2,
        )
        return result == _IDYES
    except (AttributeError, OSError):
        return False


def _restart_as_admin() -> bool:
    executable = sys.executable
    if getattr(sys, "frozen", False):
        params = subprocess.list2cmdline(sys.argv[1:])
    else:
        script_path = os.path.abspath(sys.argv[0])
        params = subprocess.list2cmdline([script_path, *sys.argv[1:]])
    try:
        result = ctypes.windll.shell32.ShellExecuteW(None, "runas", executable, params, None, 1)
        return result > 32
    except (AttributeError, OSError):
        return False


def main() -> None:
    if not _is_admin():
        if _show_admin_prompt():
            if _restart_as_admin():
                return
            ctypes.windll.user32.MessageBoxW(
                None,
                "无法以管理员身份重新启动程序。",
                "启动失败",
                _MB_OK | _MB_ICONWARNING,
            )
        sys.exit(1)

    from app.services.log_service import get_log_service

    log_service = get_log_service()
    log_service.install()

    from app.ui.proxy_window import ProxyWindow
    import tkinter as tk

    root = tk.Tk()
    icon_path = _get_resource_path("icon", "icon.ico")
    if os.path.exists(icon_path):
        try:
            root.iconbitmap(icon_path)
        except tk.TclError:
            pass
    ProxyWindow(root)
    root.mainloop()


if __name__ == "__main__":
    main()
