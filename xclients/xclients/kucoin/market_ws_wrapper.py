import time
from typing import Any
from ..base_wrapper import BaseMarketWssWrapper
from loguru import logger
from ..enum_type import Event
from ..data_type import *


class KucoinMarketWssWrapper(BaseMarketWssWrapper):
    def __init__(self, account_meta: AccountMeta, account_config: AccountConfig, wss_config: WssConfig):
        super().__init__(account_meta, account_config, wss_config)

    async def subscribe_stream(self, symbols: list[str] | None = None) -> None:
        if not symbols:
            symbols = list(self._subscribed_symbols)
        assert self._ws_client, "WebSocket client is not initialized"

        topic_list = []
        for event in self.registered_events:
            for s in symbols:
                match event:
                    case Event.BOOK:
                        if self._market_type == MarketType.SPOT:
                            topic_list.append(f"/spotMarket/level2Depth50:{s}")
                        else:
                            topic_list.append(f"/contractMarket/level2Depth50:{s}")

        await self._ws_client.subscribe(topic_list)

    async def _process_message(self, message: dict[str, Any]):
        if not message.get("data"):
            msg_type = message.get("type", None)
            if msg_type not in ["pong", "ack", "welcome"]:
                logger.debug(f"public recv unknown msg: {message}")
            return
        if not message.get("topic"):
            logger.warning(f"public recv unknown topic: {message}")
            return
        await super()._process_message(message)

    def _is_orderbook_message(self, message: dict[str, Any]) -> bool:
        return (
            any(
                [
                    message["topic"].startswith("/spotMarket/level2Depth50:"),
                    message["topic"].startswith("/contractMarket/level2Depth50:"),
                ]
            )
            and message.get("data") is not None
        )

    def _orderbook_handler(self, message: dict[str, Any]) -> OrderBook | None:
        data = message["data"]
        if message["topic"].startswith("/contractMarket/level2Depth50:"):
            symbol = message["topic"].split(":")[-1]
            orderbook = OrderBook(symbol)
            orderbook.stream = message["topic"]
            orderbook.book_type = BookType.FIXED
            orderbook.book_update_type = BookUpdateType.SNAPSHOT
            orderbook.exch_seq = 0  # unused seq id
            orderbook.exch_ts = int(data["ts"])
            orderbook.recv_ts = int(time.time() * 1000)

            for bid in data["bids"]:
                orderbook.bids.append((Decimal(bid[0]), Decimal(bid[1])))
            for ask in data["asks"]:
                orderbook.asks.append((Decimal(ask[0]), Decimal(ask[1])))
        elif message["topic"].startswith("/spotMarket/level2Depth50:"):
            symbol = message["topic"].split(":")[-1]
            orderbook = OrderBook(symbol)
            orderbook.stream = message["topic"]
            orderbook.book_type = BookType.FIXED
            orderbook.book_update_type = BookUpdateType.SNAPSHOT
            orderbook.exch_seq = 0  # no sequence
            orderbook.exch_ts = data["timestamp"]
            orderbook.recv_ts = int(time.time() * 1000)

            for bid in data["bids"]:
                orderbook.bids.append((Decimal(bid[0]), Decimal(bid[1])))
            for ask in data["asks"]:
                orderbook.asks.append((Decimal(ask[0]), Decimal(ask[1])))
        return orderbook
