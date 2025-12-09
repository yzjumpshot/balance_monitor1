import time
import hmac
import asyncio

from ..base_client import BaseWsClient
from ..get_client import get_rest_client
from typing import Any, Iterable
from ..data_type import WssConfig, MarketMeta, AccountMeta, AccountConfig, RestConfig


class BybitWsClient(BaseWsClient):
    def __init__(self, market_meta: MarketMeta, wss_config: WssConfig):
        if not wss_config.name:
            wss_config.name = str(market_meta)
        super().__init__(wss_config)

    @property
    def req_id_key(self):
        return "req_id"

    def skip_msg(self, msg: dict[str, Any]) -> bool:
        return msg.get("op") in ("ping", "pong")

    async def on_connected(self):
        await super().on_connected()

    async def request_heartbeat(self, timeout: float) -> float:
        loop = self._loop or asyncio.get_running_loop()
        _start_ts = loop.time()
        await self.request({"op": "ping"}, timeout=timeout)
        _end_ts = loop.time()
        latency = _end_ts - _start_ts
        return latency

    async def request_subscribe(self, topics: Iterable[str]):
        topics_list = list(topics)
        resps = await asyncio.gather(
            *[
                self.request({"op": "subscribe", "args": topics_list[i : i + 10]})
                for i in range(0, len(topics_list), 10)
            ]
        )

        if not all(resp.get("success", False) for resp in resps):
            raise Exception(f"subscribe - failed - {resps}")

    async def request_unsubscribe(self, topics: Iterable[str]):
        topics_list = list(topics)
        resps = await asyncio.gather(
            *[
                self.request({"op": "unsubscribe", "args": topics_list[i : i + 10]})
                for i in range(0, len(topics_list), 10)
            ]
        )

        if not all(resp.get("success", False) for resp in resps):
            raise Exception(f"unsubscribe - failed - {resps}")


class BybitPrivateWsClient(BybitWsClient):
    def __init__(
        self,
        account_meta: AccountMeta,
        account_config: AccountConfig,
        wss_config: WssConfig,
    ):
        if not wss_config.name:
            wss_config.name = str(account_meta)
        super().__init__(account_meta.market, wss_config)
        rest_config = RestConfig(bind_ips=[wss_config.bind_ip])
        self._rest_client = get_rest_client(account_meta, account_config, rest_config=rest_config)
        self.api_key = account_config.api_key
        self.secret_key = account_config.secret_key

    async def on_connected(self):
        await self.login()
        await super().on_connected()

    def _sign_message(self, expires):
        signature = str(
            hmac.new(
                bytes(self.secret_key, "utf-8"), bytes(f"GET/realtime{expires}", "utf-8"), digestmod="sha256"
            ).hexdigest()
        )
        return signature

    async def login(self):
        expires = int((time.time() + 1) * 1000)
        self.logger.info(f"login - start")
        payload = {"op": "auth", "args": [self.api_key, expires, self._sign_message(expires)]}
        resp = await self.request(payload)
        if not resp.get("success", False):
            raise Exception(f"login - failed - {resp}")

        self.logger.info(f"login - end")
