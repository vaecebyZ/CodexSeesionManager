@echo off
REM ================================
REM Codex build tool
REM ================================

set "ROOT=%~dp0"
for /f %%I in ('powershell -NoProfile -Command "Get-Date -Format yyyyMMddHHmmss"') do set "BUILD_EXE_DIR=%ROOT%build\exe.%%I"

conda run -n codex_session python setup.py build
