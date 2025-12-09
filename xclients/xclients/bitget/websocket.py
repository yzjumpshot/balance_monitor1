import asyncio
import base64
import hashlib
import hmac
import time
from typing import (
    Any,
    Iterable,
    Optional,
    Union,
)

import orjson

from ..base_client import BaseWsClient
from ..enum_type import MarketType
from ..data_type import WssConfig, MarketMeta, AccountMeta, AccountConfig


class BitgetWsClient(BaseWsClient):
    def __init__(self, market_meta: MarketMeta, wss_config: WssConfig):
        if not wss_config.name:
            wss_config.name = str(market_meta)
        super().__init__(wss_config)
        self._market_type = market_meta.market_type
        self._product_type = self.get_product_type()

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
        # lot of tricky work for bitget:
        # - ping for heartbeat is just a plain str, not dump json if msg is a "ping"
        if payload == "ping":
            req_id = "heartbeat"
        elif isinstance(payload, dict):
            if (op := payload.get("op")) in ("login", "subscribe", "unsubscribe"):
                req_id = op
            else:
                req_id = self.next_req_id
                payload[self.req_id_key] = req_id
        else:
            raise ValueError(f"invalid payload type: {type(payload)}")

        self.logger.debug("request - start - req_id={req_id}", req_id=req_id)
        loop = self._loop or asyncio.get_running_loop()
        fut = loop.create_future()
        self._req_futures[str(req_id)] = fut

        await self.send(payload)
        timeout = timeout or self._req_timeout
        result = await asyncio.wait_for(fut, timeout=timeout)
        self.logger.debug("request - end - req_id={req_id}", req_id=req_id)
        return result

    def skip_msg(self, msg: dict[str, Any]) -> bool:
        return msg.get("event") in ("login", "subscribe", "unsubscribe")

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

    def get_product_type(self):
        match self._market_type:
            case MarketType.SPOT:
                pt = "SPOT"
            case MarketType.UPERP | MarketType.UDELIVERY:
                pt = "USDT-FUTURES"  # support usdc
            case MarketType.CPERP | MarketType.CDELIVERY:
                pt = "COIN-FUTURES"
            case _:
                pt = None

        return pt

    async def request_subscribe(self, topics: Iterable[str]):
        args_lst = []
        for tp in topics:
            tp_data = tp.split("@")
            if "account" in tp_data:
                _id_name = "coin"
            else:
                _id_name = "instId"

            if len(tp_data) == 1:
                args = {"channel": tp_data[0], _id_name: "default"}
            elif len(tp_data) == 2:
                channel, symbol = tp_data
                args = {"channel": channel, _id_name: symbol}
            else:
                raise ValueError(f"invalid topic: {tp}")

            if self._product_type:
                args["instType"] = self._product_type
            args_lst.append(args)

        payload = {"op": "subscribe", "args": args_lst}
        await self.request(payload)

    async def request_unsubscribe(self, topics: Iterable[str]):
        args_lst = []
        for tp in topics:
            tp_data = tp.split("@")
            if "account" in tp_data:
                _id_name = "coin"
            else:
                _id_name = "instId"
            if len(tp_data) == 1:
                args = {"channel": tp_data[0], _id_name: "default"}
            elif len(tp_data) == 2:
                channel, symbol = tp_data
                args = {"channel": channel, _id_name: symbol}
            else:
                raise ValueError(f"invalid topic: {tp}")

            if self._product_type:
                args["instType"] = self._product_type
            args_lst.append(args)

        payload = {"op": "unsubscribe", "args": args_lst}
        await self.request(payload)


class BitgetPrivateWsClient(BitgetWsClient):
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

    def _sign_message(self, secret_key, timestamp):
        message = timestamp + "GET" + "/user/verify"
        hmac_digest = hmac.new(secret_key.encode("utf-8"), message.encode("utf-8"), hashlib.sha256).digest()
        return base64.b64encode(hmac_digest).decode("ascii")

    def _timestamp(self):
        return str(int(time.time()))

    async def on_connected(self):
        await self.login()
        await super().on_connected()

    async def login(self):
        timestamp = self._timestamp()
        payload = {
            "op": "login",
            "args": [
                {
                    "apiKey": self.api_key,
                    "passphrase": self.passphrase,
                    "timestamp": timestamp,
                    "sign": self._sign_message(self.secret_key, timestamp),
                }
            ],
        }
        _ = await self.request(payload)
