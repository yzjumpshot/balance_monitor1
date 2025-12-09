import asyncio
from typing import Optional, Iterable, Any, Hashable
from ..base_client import BaseWsClient
from uuid import uuid4
from ..data_type import WssConfig, MarketMeta, AccountMeta, AccountConfig, RestConfig
from ..get_client import get_rest_client


class KucoinWsClient(BaseWsClient):
    def __init__(self, market_meta: MarketMeta, wss_config: WssConfig):
        if not wss_config.name:
            wss_config.name = str(market_meta)
        super().__init__(wss_config)
        self._market_type = market_meta.market_type
        self._rest_client = get_rest_client(market_meta)
        self._token = ""
        self._connect_id = ""

    @property
    def req_id_key(self):
        return "id"

    @property
    def url(self):
        return f"{self._url}?token={self._token}&connectId={self._connect_id}"

    def skip_msg(self, msg: dict[str, Any]) -> bool:
        return msg.get("type") in ("pong", "ack", "welcome")

    async def get_token(self):
        return await self._rest_client.get_public_bullet()

    async def on_connected(self):
        await super().on_connected()

    async def request_heartbeat(self, timeout: float) -> float:
        _start_ts = self._loop.time()
        await self.request({"type": "ping"}, timeout=timeout)
        _end_ts = self._loop.time()
        latency = _end_ts - _start_ts
        return latency

    async def request_subscribe(self, topics: Iterable[str]):
        tp_info = {}
        for tp in topics:
            info = tp.split(":")
            if len(info) == 1:
                (prefix,) = info
                tp_info[prefix] = None
            elif len(info) == 2:
                prefix, symbols = info
                tp_info.setdefault(prefix, []).append(symbols)

        req_msgs = []
        for prefix, symbols in tp_info.items():
            if symbols:
                for i in range(0, len(symbols), 100):
                    tp = f"{prefix}:{','.join(symbols[i:i+100])}"
                    req_msgs.append({"type": "subscribe", "topic": tp, "response": True})
            else:
                tp = prefix
                req_msgs.append({"type": "subscribe", "topic": tp, "response": True})

        resps = await asyncio.gather(*[self.request(req_msg) for req_msg in req_msgs])

        if not all(resp.get("type") == "ack" for resp in resps):
            raise Exception(f"subscribe - failed - {resps}")

    async def request_unsubscribe(self, topics: Iterable[str]):
        tp_info = {}
        for tp in topics:
            info = tp.split(":")
            if len(info) == 1:
                (prefix,) = info
                tp_info[prefix] = None
            elif len(info) == 2:
                prefix, symbols = info
                tp_info.setdefault(prefix, []).append(symbols)

        req_msgs = []
        for prefix, symbols in tp_info.items():
            if symbols:
                for i in range(0, len(symbols), 100):
                    tp = f"{prefix}:{','.join(symbols[i:i+100])}"
                    req_msgs.append({"type": "unsubscribe", "topic": tp, "response": True})
            else:
                tp = prefix
                req_msgs.append({"type": "unsubscribe", "topic": tp, "response": True})

        resps = await asyncio.gather(*[self.request(req_msg) for req_msg in req_msgs])

        if not all(resp.get("type") == "ack" for resp in resps):
            raise Exception(f"unsubscribe - failed - {resps}")

    async def before_connect(self):
        token_info = await self.get_token()
        self._token = token_info["data"]["token"]
        # tips: 官方sdk里面是没有用到Ping interval这个参数的
        # self.ping_interval = int(token_info["data"]["instanceServers"][0]["pingInterval"]) // 1000 - 2
        # self.ping_timeout = int(token_info["data"]["instanceServers"][0]["pingTimeout"]) // 1000 - 2
        self._url = token_info["data"]["instanceServers"][0]["endpoint"]
        # self._encryption = token_info["data"]["instanceServers"][0]["encrypt"]
        self._connect_id = uuid4().hex


class KucoinPrivateWsClient(KucoinWsClient):
    def __init__(
        self,
        account_meta: AccountMeta,
        account_config: AccountConfig,
        wss_config: WssConfig,
    ):
        if not wss_config.name:
            wss_config.name = str(account_meta)
            
        super().__init__(account_meta.market, wss_config)
        self.api_key = account_config.api_key
        self.secret_key = account_config.secret_key
        rest_config = RestConfig(bind_ips=[wss_config.bind_ip])
        self._rest_client = get_rest_client(account_meta, account_config, rest_config=rest_config)

    async def get_token(self):
        return await self._rest_client.get_private_bullet()

    async def send(self, payload: dict[str, Any]) -> None:
        return await super().send({"privateChannel": True} | payload)
