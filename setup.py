import sys
import os
from pathlib import Path

from cx_Freeze import setup, Executable

project_root = Path(__file__).resolve().parent
include_files = [
    (str(Path(sys.prefix) / "Scripts" / "mitmdump.exe"), "mitmdump.exe"),
    ("app/services/proxy_logger_addon.py", "proxy_logger_addon.py"),
]
mitmproxy_dir = project_root / ".mitmproxy"
if mitmproxy_dir.exists():
    include_files.append((str(mitmproxy_dir), ".mitmproxy"))

setup(
    name="codex_session",
    version="1.0",
    description="Codex Session Manager",
    executables=[
        Executable("main.py", target_name="codex_session.exe", base="gui"),
    ],
    options={
        "build_exe": {
            "build_exe": os.environ.get("BUILD_EXE_DIR", str(project_root / "build" / "exe.win-amd64-3.11")),
            "include_files": include_files,
            "packages": ["mitmproxy", "psutil", "websocket"],
            "excludes": []  # 不需要的库可以排除掉
        }
    }
)
