import asyncio
import time
from collections import defaultdict
from typing import Any, Iterable
import hmac
import hashlib
from ..enum_type import MarketType
from ..base_client import BaseWsClient
from ..data_type import WssConfig, MarketMeta, AccountMeta, AccountConfig


class GateWsClient(BaseWsClient):
    def __init__(self, market_meta: MarketMeta, wss_config: WssConfig):
        if not wss_config.name:
            wss_config.name = str(market_meta)
        super().__init__(wss_config)
        self._market_type = market_meta.market_type

        if self._market_type in (MarketType.SPOT, MarketType.MARGIN):
            self._chan_prefix = "spot"
        elif self._market_type in (MarketType.UPERP, MarketType.CPERP, MarketType.CDELIVERY, MarketType.UDELIVERY):
            self._chan_prefix = "futures"
        elif self._market_type == MarketType.OPTIONS:
            self._chan_prefix = "options"

        if wss_config.extra_params.get("channel_type", "") == "ANNOUNCEMENT":
            self._chan_prefix = "announcement"

    @property
    def req_id_key(self):
        return "id"

    def skip_msg(self, msg: dict[str, Any]) -> bool:
        return msg.get("event") in ("subscribe", "unsubscribe", "")

    async def on_connected(self):
        await super().on_connected()

    async def request_heartbeat(self, timeout: float) -> float:
        loop = self._loop or asyncio.get_running_loop()
        _start_ts = loop.time()
        await self.request({"channel": f"{self._chan_prefix}.ping"}, timeout=timeout)
        _end_ts = loop.time()
        latency = _end_ts - _start_ts
        return latency

    async def request_subscribe(self, topics: Iterable[str]):
        coros = []
        sub_info = defaultdict(list)
        for tp in topics:
            chan, *pl = tp.split("@")
            if chan in ("candlesticks", "order_book_update", "order_book"):
                msg: dict[str, Any] = {"event": "subscribe", "channel": f"{self._chan_prefix}.{chan}", "payload": pl}
                coros.append(self.request(msg))
            else:
                sub_info[chan].extend(pl)

        for c, pl in sub_info.items():
            msg: dict[str, Any] = {"event": "subscribe", "channel": f"{self._chan_prefix}.{c}"}

            msg["payload"] = pl

            coros.append(self.request(msg))

        resps = await asyncio.gather(*coros)

        if not all(resp.get("error") is None for resp in resps):
            raise Exception(f"subscribe - failed - {resps}")

    async def request_unsubscribe(self, topics: Iterable[str]):
        coros = []
        unsub_info = defaultdict(list)
        for tp in topics:
            chan, *pl = tp.split("@")
            if chan in ("candlesticks", "order_book_update", "order_book"):
                msg: dict[str, Any] = {"event": "unsubscribe", "channel": f"{self._chan_prefix}.{chan}", "payload": pl}
                coros.append(self.request(msg))
            else:
                unsub_info[chan].extend(pl)

        for c, pl in unsub_info.items():
            msg: dict[str, Any] = {"event": "unsubscribe", "channel": f"{self._chan_prefix}.{c}"}
            msg["payload"] = pl

            coros.append(self.request(msg))

        resps = await asyncio.gather(*coros)

        if not all(resp.get("error") is None for resp in resps):
            raise Exception(f"unsubscribe - failed - {resps}")

    async def send(self, payload: dict[str, Any]) -> None:
        return await super().send({"time": int(time.time())} | payload)


class GatePrivateWsClient(GateWsClient):
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

    def gen_sign(self, channel: str, event: str, timestamp: float) -> dict[str, str]:
        s = "channel=%s&event=%s&time=%d" % (channel, event, timestamp)
        sign = hmac.new(self.secret_key.encode("utf-8"), s.encode("utf-8"), hashlib.sha512).hexdigest()
        return {"method": "api_key", "KEY": self.api_key, "SIGN": sign}

    async def send(self, payload: dict[str, Any]) -> None:
        extra_payload = {}
        _time = int(time.time())
        extra_payload["time"] = _time
        if any(
            s in payload["channel"]
            for s in [
                "balance",
                "order",
                "position",
                "usertrade",
                "liquidates",
                "auto_deleverages",
                "reduce_risk_limits",
                "autoorders",
            ]
        ):
            _auth = self.gen_sign(payload["channel"], payload["event"], _time)
            extra_payload["auth"] = _auth
        return await super().send(payload | extra_payload)
