import asyncio
import base64
import hashlib
import hmac
import time
from typing import Optional, Iterable, Any, Union

import orjson
from ..base_client import BaseWsClient
from ..data_type import WssConfig, MarketMeta, AccountMeta, AccountConfig
from ..enum_type import MarketType


class OKXWsClient(BaseWsClient):
    def __init__(self, market_meta: MarketMeta, wss_config: WssConfig):
        if not wss_config.name:
            wss_config.name = str(market_meta)
        super().__init__(wss_config)

    @property
    def req_id_key(self):
        return "id"

    async def send(self, payload: Union[dict[str, Any], str]) -> None:
        await asyncio.wait_for(self._ws_ready.wait(), timeout=60)
        if isinstance(payload, str):
            raw_msg = payload
        else:
            raw_msg = orjson.dumps(payload).decode()
        await self._send(raw_msg)

    async def request(self, payload: str | dict[str, Any], timeout: Optional[float] = None) -> dict[str, Any]:
        # lot of tricky work for okx:
        # - ping for heartbeat is just a plain str, not dump json if msg is a "ping"
        # - such request like "login", "subscribe", "unsubscribe", can not carry a "id" to identify the response, use it`s own name as req_id instead
        # by handling msg like this as above
        # there would be a corner case that the responses for multiple request at the same time might be pairing failed
        if payload == "ping":
            req_id = "heartbeat"
        elif isinstance(payload, dict):
            if (op := payload.get("op")) in ("login", "subscribe", "unsubscribe"):
                req_id = op
            else:
                req_id = self.next_req_id
                payload[self.req_id_key] = req_id
        else:
            raise ValueError(f"invalid payload: {payload}")

        self.logger.debug("request - start - req_id={req_id}", req_id=req_id)
        loop = self._loop or asyncio.get_running_loop()
        fut = loop.create_future()
        self._req_futures[str(req_id)] = fut

        await self.send(payload)
        timeout = timeout or self._req_timeout
        result = await asyncio.wait_for(fut, timeout=timeout)
        self.logger.debug("request - end - req_id={req_id}", req_id=req_id)
        return result

    async def handle_raw_msg(self, raw_msg: str) -> Optional[dict[str, Any]]:
        _id = None
        if raw_msg == "pong":
            _id = "heartbeat"
            msg = None
        else:
            msg = orjson.loads(raw_msg)
            await self.on_raw_msg(msg)

            if (ev := msg.get("event")) in ("login", "subscribe", "unsubscribe"):
                _id = ev
            else:
                _id = msg.get(self.req_id_key)

            if self.skip_msg(msg):
                msg = None

        if _id and (fut := self._req_futures.pop(str(_id), None)) and (not fut.done()):
            fut.set_result(msg)

        return msg

    async def on_connected(self):
        await super().on_connected()

    async def request_heartbeat(self, timeout: float) -> float:
        loop = self._loop or asyncio.get_running_loop()
        _start_ts = loop.time()
        await self.request("ping", timeout=timeout)
        _end_ts = loop.time()
        latency = _end_ts - _start_ts
        return latency

    async def request_subscribe(self, topics: Iterable[str]):
        print(topics, "#################")
        # topic example: "account", "orders@MarketType:SPOT", "tickers@instId:BTC-USDT"
        payload: dict[str, Any] = {"op": "subscribe"}
        args = []
        for tp in topics:
            tp_data = tp.split("@")
            if len(tp_data) == 1:
                (chan,) = tp_data
                arg = {"channel": chan}
            elif len(tp_data) == 2:
                chan, s = tp_data
                k, v = s.split(":")
                arg = {"channel": chan, k: v}
            else:
                continue
            print(arg)
            args.append(arg)

        payload["args"] = args

        _ = await self.request(payload)

    async def request_unsubscribe(self, topics: Iterable[str]):
        # topic example: "account", "orders@MarketType:SPOT", "tickers@instId:BTC-USDT"
        payload: dict[str, Any] = {"op": "unsubscribe"}
        args = []
        for tp in topics:
            tp_data = tp.split("@")
            if len(tp_data) == 1:
                (chan,) = tp_data
                arg = {"channel": chan}
            elif len(tp_data) == 2:
                chan, s = tp_data
                k, v = s.split(":")
                arg = {"channel": chan, k: v}
            else:
                continue

            args.append(arg)

        payload["args"] = args

        _ = await self.request(payload)


class OKXPrivateWsClient(OKXWsClient):
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
        self.passphrase = account_config.passphrase

    def _sign_message(self, secret_key):
        message = str(int(time.time())) + "GET" + "/users/self/verify"
        hmac_digest = hmac.new(secret_key.encode("utf-8"), message.encode("utf-8"), hashlib.sha256).digest()
        return base64.b64encode(hmac_digest).decode("ascii")

    def _timestamp(self):
        return str(int(time.time()))

    async def on_connected(self):
        await self.login()
        await super().on_connected()

    async def login(self):
        payload = {
            "op": "login",
            "args": [
                {
                    "apiKey": self.api_key,
                    "passphrase": self.passphrase,
                    "timestamp": self._timestamp(),
                    "sign": self._sign_message(self.secret_key),
                }
            ],
        }
        _ = await self.request(payload)
