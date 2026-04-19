from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path


@dataclass
class AppConfig:
    port: int = 8080
    upstream_proxy: str = "127.0.0.1:1088"
    use_upstream_proxy: bool = True
    auto_load: bool = True


class AppConfigService:
    def __init__(self, config_path: Path | None = None) -> None:
        self.config_path = config_path or Path(__file__).resolve().parents[2] / "config.json"

    def exists(self) -> bool:
        return self.config_path.exists()

    def load(self) -> AppConfig | None:
        if not self.config_path.exists():
            return None
        try:
            data = json.loads(self.config_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
        if not isinstance(data, dict):
            return None
        try:
            port = int(data.get("port") or 8080)
        except (TypeError, ValueError):
            port = 8080
        upstream_proxy = str(data.get("upstream_proxy") or "127.0.0.1:1088")
        use_upstream_proxy = bool(data.get("use_upstream_proxy", True))
        auto_load = bool(data.get("auto_load", True))
        return AppConfig(
            port=port,
            upstream_proxy=upstream_proxy,
            use_upstream_proxy=use_upstream_proxy,
            auto_load=auto_load,
        )

    def save(self, config: AppConfig) -> None:
        self.config_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "port": config.port,
            "upstream_proxy": config.upstream_proxy,
            "use_upstream_proxy": config.use_upstream_proxy,
            "auto_load": config.auto_load,
        }
        self.config_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
