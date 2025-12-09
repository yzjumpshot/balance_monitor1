"""Helpers to talk to Lark open APIs (local copy for balance monitor)."""

from __future__ import annotations

import json
import os
import time
from typing import Any, Dict
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

_TOKEN_CACHE: Dict[str, Any] = {"token": None, "expires_at": 0.0}


def _get_credentials() -> tuple[str, str]:
    app_id = os.getenv("LARK_APP_ID")
    app_secret = os.getenv("LARK_APP_SECRET")
    if not app_id or not app_secret:
        raise RuntimeError("LARK_APP_ID or LARK_APP_SECRET is not configured")
    return app_id, app_secret


def get_tenant_access_token() -> str:
    """Return cached tenant_access_token or fetch a new one."""
    now = time.time()
    cached_token = _TOKEN_CACHE.get("token")
    expires_at = float(_TOKEN_CACHE.get("expires_at") or 0.0)
    if cached_token and expires_at - now > 60:
        return str(cached_token)

    app_id, app_secret = _get_credentials()
    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    payload = {"app_id": app_id, "app_secret": app_secret}
    body = json.dumps(payload).encode("utf-8")
    request = Request(
        url, data=body, headers={"Content-Type": "application/json; charset=utf-8"}, method="POST"
    )
    try:
        with urlopen(request, timeout=5) as response:
            data = json.load(response)
    except HTTPError as exc:
        raise RuntimeError(f"token request failed: HTTP {exc.code}") from exc
    except URLError as exc:
        raise RuntimeError(f"token request failed: {exc}") from exc

    if data.get("code") != 0:
        raise RuntimeError(f"token request error: {data}")

    token = data.get("tenant_access_token")
    expire = float(data.get("expire") or 0.0)
    if not token:
        raise RuntimeError("token response missing tenant_access_token")

    _TOKEN_CACHE["token"] = token
    _TOKEN_CACHE["expires_at"] = now + expire
    return str(token)


def reply_to_message(message_id: str, text: str) -> None:
    """Reply inside the original chat thread."""
    token = get_tenant_access_token()
    url = f"https://open.feishu.cn/open-apis/im/v1/messages/{message_id}/reply"
    payload = {
        "msg_type": "text",
        "content": json.dumps({"text": text}, ensure_ascii=False),
    }
    _post_json(url, payload, token)


def send_text_to_chat(chat_id: str, text: str) -> None:
    """Send a plain text message to the specified chat."""
    token = get_tenant_access_token()
    url = "https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type=chat_id"
    payload = {
        "receive_id": chat_id,
        "msg_type": "text",
        "content": json.dumps({"text": text}, ensure_ascii=False),
    }
    _post_json(url, payload, token)


def reply_card(message_id: str, card: Dict[str, Any]) -> None:
    """Reply with an interactive card."""
    token = get_tenant_access_token()
    url = f"https://open.feishu.cn/open-apis/im/v1/messages/{message_id}/reply"
    payload = {
        "msg_type": "interactive",
        "card": card,
        "content": json.dumps(card, ensure_ascii=False),
    }
    _post_json(url, payload, token)


def send_card_to_chat(chat_id: str, card: Dict[str, Any]) -> None:
    """Send an interactive card to the specified chat."""
    token = get_tenant_access_token()
    url = "https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type=chat_id"
    payload = {
        "receive_id": chat_id,
        "msg_type": "interactive",
        "card": card,
        "content": json.dumps(card, ensure_ascii=False),
    }
    _post_json(url, payload, token)


def _post_json(url: str, payload: Dict[str, Any], token: str) -> None:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    request = Request(
        url,
        data=body,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json; charset=utf-8",
        },
        method="POST",
    )
    try:
        with urlopen(request, timeout=5) as response:
            data = json.load(response)
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="ignore")
        raise RuntimeError(
            f"Lark API request failed: HTTP {exc.code} body={body}"
        ) from exc
    except URLError as exc:
        raise RuntimeError(f"Lark API request failed: {exc}") from exc

    if data.get("code") != 0:
        raise RuntimeError(f"Lark API error: {data}")
