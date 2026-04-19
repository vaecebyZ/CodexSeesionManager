import json
import urllib.request

from app.models import AccountToken, SessionViewData, UserProfile
import websocket


class ChatGPTService:
    def open_browser(self, chrome_service) -> str:
        chrome_path = chrome_service.find_chrome()
        if not chrome_path:
            return "未找到系统中的 Chrome。"
        if chrome_service.open_chrome(chrome_path):
            return ""
        return "Chrome 已找到，但无法启动或启用远程调试。"

    def attach_and_fetch(self, chrome_service) -> tuple[SessionViewData, str]:
        if not chrome_service.is_debug_port_open():
            if not chrome_service.wait_for_debug_port():
                return SessionViewData(profile=UserProfile(), accounts=[]), "附加失败：未检测到可用的远程调试 Chrome。"
        page_text = self._read_chatgpt_page_text(chrome_service.remote_debugging_port)
        view_data = self._parse_view_data(page_text)
        return view_data, ""

    def fetch_session_view_data(self, chrome_service) -> tuple[SessionViewData, str]:
        return self.attach_and_fetch(chrome_service)

    def _read_chatgpt_page_text(self, remote_debugging_port: int) -> str:
        endpoint = f"http://127.0.0.1:{remote_debugging_port}/json/list"
        with urllib.request.urlopen(endpoint, timeout=300) as response:
            tabs = json.loads(response.read().decode("utf-8"))
        for tab in tabs:
            if tab.get("url", "").startswith("https://chatgpt.com/"):
                socket_url = tab.get("webSocketDebuggerUrl")
                if not socket_url:
                    return ""
                return self._evaluate_page_text(socket_url)
        return ""

    def _evaluate_page_text(self, websocket_url: str) -> str:
        ws = websocket.create_connection(websocket_url, timeout=300)
        try:
            self._send_cdp(ws, "Runtime.enable")
            self._send_cdp(ws, "Page.enable")
            result = self._send_cdp(
                ws,
                "Runtime.evaluate",
                {"expression": "document.body ? document.body.innerText : ''", "returnByValue": True},
            )
            return result.get("result", {}).get("result", {}).get("value", "")
        finally:
            ws.close()

    def _send_cdp(self, ws, method: str, params: dict | None = None) -> dict:
        message = {"id": 1, "method": method}
        if params:
            message["params"] = params
        ws.send(json.dumps(message))
        while True:
            response = json.loads(ws.recv())
            if response.get("id") == 1:
                return response

    def _parse_view_data(self, page_text: str) -> SessionViewData:
        accounts = self._extract_accounts(page_text)
        return SessionViewData(
            profile=UserProfile(
                user_id="未提取",
                user_name="未提取",
                user_email="未提取",
                expire_time="未提取",
            ),
            accounts=accounts,
        )

    def _extract_accounts(self, page_text: str) -> list[AccountToken]:
        if not page_text:
            return []
        candidates: list[AccountToken] = []
        for line in page_text.splitlines():
            text = line.strip()
            if not text:
                continue
            if any(keyword in text.lower() for keyword in ("workspace", "team", "group")):
                candidates.append(
                    AccountToken(
                        account_id=text,
                        plan_type="未知",
                        structure="未知",
                        access_token="页面文本",
                        session_token="页面文本",
                    )
                )
        return candidates[:20]
