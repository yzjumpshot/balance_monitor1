import time
from typing import Any
from ..base_wrapper import BaseMarketWssWrapper
from loguru import logger
from ..enum_type import Event
from ..data_type import *


class OKXMarketWssWrapper(BaseMarketWssWrapper):
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
                        topic_list.append(f"books5@instId:{s}")
                    case Event.TICKER:
                        topic_list.append(f"tickers@instId:{s}")
                        if self._market_type.is_derivative:
                            topic_list.append(f"funding-rate@instId:{s}")
                            index_symbol = s.replace("-SWAP", "")
                            topic_list.append(f"index-tickers@instId:{index_symbol}")

        await self._ws_client.subscribe(topic_list)

    async def _process_message(self, message: dict[str, Any]):
        if "ping" in message or "pong" in message:
            return
        if not message.get("data"):
            return
        await super()._process_message(message)

    def _is_orderbook_message(self, message: dict[str, Any]) -> bool:
        topic = message.get("arg", {}).get("channel")
        return topic == "books5"

    def _orderbook_handler(self, message: dict[str, Any]) -> OrderBook | None:
        symbol = message["arg"]["instId"]
        orderbook = OrderBook(symbol)
        orderbook.book_type = BookType.FIXED
        orderbook.book_update_type = BookUpdateType.SNAPSHOT

        # action = message["action"]  # 订阅books5没有action字段，每次定量推送5档行情
        for data in message["data"]:
            orderbook.exch_seq = int(data["seqId"])
            orderbook.exch_ts = int(data["ts"])
            orderbook.recv_ts = int(time.time() * 1000)

            for bid in data["bids"]:
                orderbook.bids.append((Decimal(bid[0]), Decimal(bid[1])))
            for ask in data["asks"]:
                orderbook.asks.append((Decimal(ask[0]), Decimal(ask[1])))
        return orderbook

    def _is_ticker_message(self, message: dict[str, Any]) -> bool:
        topic = message.get("arg", {}).get("channel")
        return topic in ["tickers", "funding-rate", "index-tickers"]

    def _ticker_handler(self, message: dict[str, Any]) -> Tickers | None:
        tickers = Tickers()
        topic = message.get("arg", {}).get("channel")
        data = message["data"]
        symbol = message["arg"]["instId"]
        if topic == "index-tickers":
            symbol += "-SWAP"

        if topic == "tickers":
            for info in data:
                tickers[symbol] = Ticker(
                    symbol,
                    bid=float(info["bidPx"]),
                    ask=float(info["askPx"]),
                    bid_qty=float(info["bidSz"]),
                    ask_qty=float(info["askSz"]),
                    ts=float(info["ts"]),
                    update_ts=float(info["ts"]),
                )
        elif topic == "funding-rate":
            for info in data:
                tickers[symbol] = Ticker(
                    symbol,
                    fr=float(info["fundingRate"]),
                    fr_ts=float(info["fundingTime"]),
                    ts=float(info["ts"]),
                    update_ts=float(info["ts"]),
                )
        elif topic == "index-tickers":
            for info in data:
                tickers[symbol] = Ticker(
                    symbol,
                    index_price=float(info["idxPx"]),
                    ts=float(info["ts"]),
                    update_ts=float(info["ts"]),
                )
        return tickers
