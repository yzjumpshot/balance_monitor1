import asyncio
import time
from collections import defaultdict
from typing import Optional, Any, Iterable
import hmac
import hashlib
import gzip

from ..base_client import BaseWsClient
from ..data_type import WssConfig, MarketMeta, AccountMeta, AccountConfig


class CoinexWsClient(BaseWsClient):
    def __init__(self, market_meta: MarketMeta, wss_config: WssConfig):
        if not wss_config.name:
            wss_config.name = str(market_meta)
        super().__init__(wss_config)

    @property
    def req_id_key(self):
        return "id"

    async def _recv(self) -> str:
        if self._ws is not None:
            msg = await self._ws.recv()
            if isinstance(msg, str):
                msg = msg.encode()
            msg = gzip.decompress(msg)  # decompress with gzip is needed
            self._logger.debug("{uname} recv - msg={msg}", uname=self, msg=msg)
            msg = msg.decode("utf-8")
            return msg
        return ""

    def skip_msg(self, msg: dict[str, Any]) -> bool:
        return msg.get("id") is not None

    async def on_connected(self):
        await super().on_connected()

    async def request_heartbeat(self, timeout: float) -> float:
        loop = self._loop or asyncio.get_running_loop()
        _start_ts = loop.time()
        await self.request({"method": "server.ping", "params": {}}, timeout=timeout)
        _end_ts = loop.time()
        latency = _end_ts - _start_ts
        return latency

    async def request_subscribe(self, topics: Iterable[str]):
        # topic: "balance", "balance@BTC"
        # topic: "state", "state@BTCUSDT"
        # topic: "depth@BTCUSDT@10@0@true"
        coros = []
        sub_info = defaultdict(list)
        for tp in topics:
            chan, *pl = tp.split("@")
            if len(pl) == 0:
                sub_info[chan] = []
            elif len(pl) == 1:
                sub_info[chan].append(pl[0])
            else:
                # only depth case
                assert chan == "depth" and len(pl) == 4
                sub_info[chan].append((pl[0], int(pl[1]), pl[2], pl[3].lower() == "true"))

        for c, pl in sub_info.items():
            if c in ("balance",):
                msg = {"method": f"{c}.subscribe", "params": {"ccy_list": pl}}
            else:
                msg = {"method": f"{c}.subscribe", "params": {"market_list": pl}}

            coros.append(self.request(msg))

        resps = await asyncio.gather(*coros)

        if not all(resp["code"] == 0 for resp in resps):
            raise Exception(f"subscribe - failed - {resps}")

    async def request_unsubscribe(self, topics: Iterable[str]):
        # topic: "balance", "balance@BTC"
        # topic: "state", "state@BTCUSDT"
        # topic: "depth@BTCUSDT@10@0@true" or "depth@BTCUSDT"
        coros = []
        unsub_info = defaultdict(list)
        for tp in topics:
            chan, *pl = tp.split("@")
            if len(pl) == 0:
                unsub_info[chan] = []
            elif len(pl) == 1:
                unsub_info[chan].append(pl[0])
            else:
                # only depth case
                assert chan == "depth"
                unsub_info[chan].append(pl[0])  # unsub depth only accepct market name

        for c, pl in unsub_info.items():
            if c in ("balance",):
                msg = {"method": f"{c}.unsubscribe", "params": {"ccy_list": pl}}
            else:
                msg = {"method": f"{c}.unsubscribe", "params": {"market_list": pl}}

            coros.append(self.request(msg))

        resps = await asyncio.gather(*coros)

        if not all(resp["code"] == 0 for resp in resps):
            raise Exception(f"unsubscribe - failed - {resps}")


class CoinexPrivateWsClient(CoinexWsClient):
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

    async def on_connected(self):
        await self.login()
        await super().on_connected()

    async def login(self):
        self.logger.info(f"login - start")
        ts = int(time.time() * 1000)
        prepared_str = str(ts)
        signed_str = (
            hmac.new(
                self.secret_key.encode("utf-8"),
                msg=prepared_str.encode("utf-8"),
                digestmod=hashlib.sha256,
            )
            .hexdigest()
            .lower()
        )
        payload = {
            "method": "server.sign",
            "params": {
                "access_id": self.api_key,
                "signed_str": signed_str,
                "timestamp": ts,
            },
        }
        resp = await self.request(payload)
        if resp["code"] != 0:
            raise Exception(f"login - failed - {resp}")

        self.logger.info(f"login - end")
