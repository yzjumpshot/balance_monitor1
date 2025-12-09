from decimal import Decimal
import time
from typing import Any
from ..base_wrapper import BaseMarketWssWrapper
from loguru import logger
from ..enum_type import Event
from ..data_type import *
from .constants import *


class BitgetMarketWssWrapper(BaseMarketWssWrapper):
    def __init__(self, account_meta: AccountMeta, account_config: AccountConfig, wss_config: WssConfig):
        super().__init__(account_meta, account_config, wss_config)

    def get_product_type(self):
        if MarketType.UPERP == self._market_type:
            return "USDT-FUTURES"
        if MarketType.CPERP == self._market_type:
            return "COIN-FUTURES"
        elif MarketType.SPOT == self._market_type:
            return "SPOT"
        else:
            raise ValueError(f"Unsupported market type: {self._market_type}")

    async def subscribe_stream(self, symbols: list[str] | None = None) -> None:
        if not symbols:
            symbols = list(self._subscribed_symbols)
        assert self._ws_client, "WebSocket client is not initialized"

        for event in self.registered_events:
            if event == Event.BOOK:
                # origin channel: books5
                topics = [f"books1@{symbol}" for symbol in symbols]

                await self._ws_client.subscribe(topics)

    async def _process_message(self, message: dict[str, Any]):
        if "ping" in message or "pong" in message:
            return
        if not message.get("data") or not message.get("arg", {}).get("channel", ""):
            return
        await super()._process_message(message)

    def _is_orderbook_message(self, message: dict[str, Any]) -> bool:
        return message["arg"]["channel"].startswith("books")

    def _orderbook_handler(self, message: dict[str, Any]) -> OrderBook | None:
        symbol = message["arg"]["instId"]
        orderbook = OrderBook(symbol)
        action = message["action"]
        orderbook.book_type = BookType.DIFF
        if action == "snapshot":
            orderbook.book_update_type = BookUpdateType.SNAPSHOT
        else:
            orderbook.book_update_type = BookUpdateType.DELTA

        for data in message["data"]:
            orderbook.exch_seq = int(data["ts"])
            orderbook.exch_ts = int(data["ts"])
            orderbook.recv_ts = int(time.time() * 1000)

            for bid in data["bids"]:
                orderbook.bids.append((Decimal(bid[0]), Decimal(bid[1])))
            for ask in data["asks"]:
                orderbook.asks.append((Decimal(ask[0]), Decimal(ask[1])))
        return orderbook
