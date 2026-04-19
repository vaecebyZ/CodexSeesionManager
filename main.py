import ctypes
import sys
import tkinter as tk

from app.services.log_service import get_log_service


def _is_admin() -> bool:
    try:
        return bool(ctypes.windll.shell32.IsUserAnAdmin())
    except OSError:
        return False


def main() -> None:
    log_service = get_log_service()
    log_service.install()
    from app.ui.proxy_window import ProxyWindow

    if not _is_admin():
        print("请以管理员权限启动。")
        sys.exit(1)
    root = tk.Tk()
    ProxyWindow(root)
    root.mainloop()


if __name__ == "__main__":
    main()
