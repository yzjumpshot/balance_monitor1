import time
import asyncio
from contextlib import suppress
from typing import Any, Optional, Iterable, Union

from .rest import DeribitRestClient
from ..base_client import BaseWsClient
from ..data_type import WssConfig, MarketMeta, AccountMeta, AccountConfig, RestConfig
from ..get_client import get_rest_client


class DeribitWsClient(BaseWsClient):
    def __init__(self, market_meta: MarketMeta, wss_config: WssConfig):
        if not wss_config.name:
            wss_config.name = str(market_meta)
        super().__init__(wss_config)
        self._heartbeat_needed = asyncio.Event()

    @property
    def req_id_key(self):
        return "id"

    def skip_msg(self, msg: dict[str, Any]) -> bool:
        return msg.get("method") in ("heartbeat", None)

    async def on_raw_msg(self, msg: Union[list[Any], dict[str, Any]]):
        if isinstance(msg, dict) and msg.get("method") == "heartbeat" and msg["params"]["type"] == "test_request":
            self._heartbeat_needed.set()

    async def on_connected(self):
        await self.login()
        await super().on_connected()

    async def time_for_heartbeat(self):
        with suppress(asyncio.TimeoutError):
            await asyncio.wait_for(self._heartbeat_needed.wait(), timeout=self._heartbeat_interval * 2)

    async def request_heartbeat(self, timeout: float) -> float:
        loop = self._loop or asyncio.get_running_loop()
        ping_payload = {
            "method": "public/test",
            "params": {},
        }
        _start_ts = loop.time()
        await self.request(ping_payload, timeout=timeout)
        _end_ts = loop.time()
        latency = _end_ts - _start_ts
        self._heartbeat_needed.clear()

        return latency

    async def login(self) -> None:
        # set heartbeat
        self.logger.info(f"set heartbeat")
        payload = {
            "method": "public/set_heartbeat",
            "params": {"interval": self._heartbeat_interval},
        }
        set_heartbeat_resp = await self.request(payload)
        if set_heartbeat_resp.get("result") != "ok":
            raise Exception(f"set heartbeat - failed - {set_heartbeat_resp}")

    async def request_subscribe(self, topics: Iterable[str]):
        public_topics = []
        private_topics = []
        for t in topics:
            if t.split(".")[0] == "user":
                private_topics.append(t)
            else:
                public_topics.append(t)

        sub_payloads = []
        if public_topics:
            public_payload = {
                "method": "public/subscribe",
                "params": {"channels": public_topics},
            }
            sub_payloads.append(public_payload)

        if private_topics:
            private_payload = {
                "method": "private/subscribe",
                "params": {"channels": private_topics},
            }
            sub_payloads.append(private_payload)

        resps = await asyncio.gather(*[self.request(payload) for payload in sub_payloads])
        subed_topics = []
        for resp in resps:
            subed_topics.extend(resp["result"])

        if set(topics) != set(subed_topics):
            raise Exception(f"subscribe - failed - topics={topics} subed_topics={subed_topics}")

    async def request_unsubscribe(self, topics: Iterable[str]):
        public_topics = []
        private_topics = []
        for t in topics:
            if t.split(".")[0] == "user":
                private_topics.append(t)
            else:
                public_topics.append(t)

        unsub_payloads = []
        if public_topics:
            public_payload = {
                "method": "public/unsubscribe",
                "params": {"channels": public_topics},
            }
            unsub_payloads.append(public_payload)

        if private_topics:
            private_payload = {
                "method": "private/unsubscribe",
                "params": {"channels": private_topics},
            }
            unsub_payloads.append(private_payload)

        resps = await asyncio.gather(*[self.request(payload) for payload in unsub_payloads])
        unsubed_topics = []
        for resp in resps:
            unsubed_topics.extend(resp["result"])

        if set(topics) != set(unsubed_topics):
            raise Exception(f"unsubscribe - failed - topics={topics} unsubed_topics={unsubed_topics}")

    async def send(self, payload: dict[str, Any]) -> None:
        return await super().send({"jsonrpc": "2.0"} | payload)


class DeribitPrivateWsClient(DeribitWsClient):
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
        self._access_token = None
        self._refresh_token = None
        self._expired_ts = 0

    async def fetch_access_token(self, expires_in: int = 24 * 60 * 60):
        data = await self._rest_client.auth(
            "client_credentials", client_id=self.api_key, client_secret=self.secret_key, scope=f"expires:{expires_in}"
        )
        self._access_token = data["result"]["access_token"]
        self._refresh_token = data["result"]["refresh_token"]
        self._expired_ts = data["result"]["expires_in"] + time.time()
        self.logger.info(f"fetch access token. token would be expired at {self._expired_ts}")

    async def refresh_access_token(self):
        data = await self._rest_client.auth("refresh_token", refresh_token=self._refresh_token)
        self._access_token = data["result"]["access_token"]
        self._refresh_token = data["result"]["refresh_token"]
        self._expired_ts = data["result"]["expires_in"] + time.time()
        self.logger.info(f"refresh access token. token would be expired at {self._expired_ts}")

    async def _maintain_access_token(self, expires_in: int = 24 * 60 * 60):
        while not self.closed:
            await self.sleep_or_closed(60)
            now_ts = time.time()

            if self._expired_ts - now_ts < expires_in / 3:
                try:
                    await self.refresh_access_token()
                except Exception as e:
                    self.logger.error(f"refresh access token failed - {e}")
                    await self.fetch_access_token(expires_in)

    async def login(self):
        await super().login()
        expires_in = 24 * 60 * 60
        await self.fetch_access_token(expires_in)

    async def send(self, payload: dict[str, Any]) -> None:
        method = payload.get("method", "")
        if "private" in method:
            params = payload.setdefault("params", {})
            params["access_token"] = self._access_token

        return await super().send(payload)

    def init_tasks(self):
        super().init_tasks()
        expires_in = 24 * 60 * 60
        self.add_task(self._maintain_access_token(expires_in), name=f"maintain_access_token#{self._client_id}")


if __name__ == "__main__":

    async def callback_func(obj):
        print(obj)

    url = "wss://www.deribit.com/ws/api/v2"
    topic = ["incremental_ticker.BTC-PERPETUAL"]
    # client = DeribitWsClient(url, topic)
    # client.register_msg_callback(callback_func)
    # asyncio.get_event_loop().run_until_complete(client.run())
