import time
from typing import Any
from ..base_wrapper import BaseMarketWssWrapper
from loguru import logger
from ..enum_type import Event
from ..data_type import *


class CoinexMarketWssWrapper(BaseMarketWssWrapper):
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
                        topic_list.append(f"depth@{s}@10@0@true")
                    case Event.TICKER:
                        if self._market_type.is_derivative:
                            topic_list.append(f"state@{s}")
                        else:
                            topic_list.append(f"bbo@{s}")
        await self._ws_client.subscribe(topic_list)

    async def _process_message(self, message: dict[str, Any]):
        if not message.get("data", {}):
            return
        if message.get("data", {}).get("result") == "pong":
            return
        await super()._process_message(message)

    def _is_orderbook_message(self, message: dict[str, Any]) -> bool:
        return message.get("method") == "depth.update"

    def _orderbook_handler(self, message: dict[str, Any]) -> OrderBook | None:
        symbol = message["data"]["market"]
        orderbook = OrderBook(symbol)
        full_depth = message["data"]["is_full"]

        data = message["data"]["depth"]
        orderbook.exch_seq = int(data["updated_at"])
        orderbook.exch_ts = int(data["updated_at"])
        orderbook.recv_ts = int(time.time() * 1000)
        if full_depth:
            orderbook.book_type = BookType.FIXED
            orderbook.book_update_type = BookUpdateType.SNAPSHOT
        else:
            orderbook.book_type = BookType.DIFF
            orderbook.book_update_type = BookUpdateType.DELTA

        for bid in data["bids"]:
            orderbook.bids.append((Decimal(bid[0]), Decimal(bid[1])))
        for ask in data["asks"]:
            orderbook.asks.append((Decimal(ask[0]), Decimal(ask[1])))

        return orderbook

    def _is_ticker_message(self, message: dict[str, Any]) -> bool:
        return bool(message.get("data")) and (
            message.get("method", "") == "bbo.update" or message.get("method", "") == "state.update"
        )

    def _ticker_handler(self, message: dict[str, Any]) -> Tickers | None:
        tickers = Tickers()
        if message["method"] == "bbo.update":
            info = message["data"]
            symbol = info["market"]
            tickers[symbol] = Ticker(
                symbol,
                bid=float(info["best_bid_price"]),
                ask=float(info["best_ask_price"]),
                ts=info["updated_at"],  # no event ts, use update ts
                update_ts=info["updated_at"],
                bid_qty=float(info["best_bid_size"]),
                ask_qty=float(info["best_ask_size"]),
            )
        elif message["method"] == "state.update":
            for info in message["data"]["state_list"]:
                symbol = info["market"]
                tickers[symbol] = Ticker(
                    symbol,
                    index_price=float(info["index_price"]),
                    ts=int(time.time() * 1000),
                    update_ts=int(time.time() * 1000),
                    fr=float(info["latest_funding_rate"]),
                    fr_ts=float(info["latest_funding_time"]),
                )
        return tickers
