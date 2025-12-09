import time
from decimal import Decimal
from typing import Any

from loguru import logger

from ..base_wrapper import BaseMarketWssWrapper
from ..data_type import *
from ..enum_type import (
    Event,
)
from .constants import *


class BinanceMarketWssWrapper(BaseMarketWssWrapper):
    def __init__(self, account_meta: AccountMeta, account_config: AccountConfig, wss_config: WssConfig):
        super().__init__(account_meta, account_config, wss_config)

    async def subscribe_stream(self, symbols: list[str] | None = None) -> None:
        if not symbols:
            symbols = list(self._subscribed_symbols)
        assert self._ws_client, "WebSocket client is not initialized"

        topics: list[str] = []
        for event in self.registered_events:
            if event == Event.BOOK:
                for symbol in symbols:
                    topics.append(f"{symbol.lower()}@bookTicker")

            elif event == Event.TICKER:
                if self._market_type in [MarketType.SPOT, MarketType.MARGIN]:
                    topics.append("!ticker@arr")
                else:
                    batch_size = 100
                    for i in range(0, len(symbols), batch_size):
                        batch = symbols[i : i + batch_size]
                        for symbol in batch:
                            topics.append(f"{symbol.lower()}@depth5@100ms")
                    topics.append("!markPrice@arr@1s")
            elif event == Event.PREMIUM_INDEX:
                if self._market_type in [MarketType.UPERP]:
                    for interval in self._wss_config.extra_params["kline_intervals"]:
                        batch_size = 100
                        for i in range(0, len(symbols), batch_size):
                            batch = symbols[i : i + batch_size]
                            topics.extend([f"p{symbol}@kline_{interval}" for symbol in batch])
                else:
                    logger.error("Premium index is not supported for this market type")
            elif event == Event.KLINE:
                batch_size = 100
                for interval in self._wss_config.extra_params["kline_intervals"]:
                    for i in range(0, len(symbols), batch_size):
                        batch = symbols[i : i + batch_size]
                        topics.extend([f"{symbol.lower()}@kline_{interval}" for symbol in batch])
            else:
                logger.error(f"Event {event} is not supported")

        await self._ws_client.subscribe(topics)

    async def _process_message(self, message: dict[str, Any]):
        if not message.get("data"):
            return
        if not (message.get("stream")):
            return
        await super()._process_message(message)

    def _is_ticker_message(self, message: Any) -> bool:
        if self._market_type in [MarketType.SPOT, MarketType.MARGIN]:
            return message["stream"] == "!ticker@arr"
        if self._market_type in [MarketType.UPERP, MarketType.UDELIVERY]:
            return message["stream"] == "!markPrice@arr@1s" or message["stream"].endswith("@depth5@100ms")
        return False

    def _ticker_handler(self, message: dict[str, Any]) -> Tickers | None:
        if self._market_type in [MarketType.SPOT, MarketType.MARGIN]:
            return self._spot_ticker_handler(message)
        elif self._market_type in [MarketType.UPERP, MarketType.UDELIVERY]:
            return self._uperp_ticker_handler(message)
        return None

    def _spot_ticker_handler(self, message: dict[str, Any]) -> Tickers | None:
        tickers = Tickers()
        for info in message["data"]:
            if not info["s"] in self._subscribed_symbols:
                continue
            tickers[info["s"]] = Ticker(
                exch_symbol=info["s"],
                bid=float(info["b"]),
                ask=float(info["a"]),
                ts=info["E"],
                update_ts=info["E"],  # exch update ts = exch event ts
                bid_qty=float(info["B"]),
                ask_qty=float(info["A"]),
            )
        return tickers

    def _uperp_ticker_handler(self, message: dict[str, Any]) -> Tickers | None:
        tickers = Tickers()
        data = message["data"]
        if message["stream"] == "!markPrice@arr@1s":
            for info in message["data"]:
                symbol = info["s"]
                if symbol not in self._subscribed_symbols:
                    continue
                tickers[symbol] = Ticker(
                    exch_symbol=symbol,
                    index_price=float(info["p"]),
                    ts=info["E"],
                    update_ts=info["E"],
                    fr=float(info["r"]),
                    fr_ts=info["T"],  # 下个资金时间
                )
        elif message["stream"].endswith("@depth5@100ms"):
            symbol = data["s"]
            if symbol not in self._subscribed_symbols:
                return None
            tickers[symbol] = Ticker(
                exch_symbol=symbol,
                bid=float(data["b"][0][0]),
                ask=float(data["a"][0][0]),
                ts=data["E"],  # 事件时间 exch event ts
                update_ts=data["T"],  # 交易时间 exch update ts
                bid_qty=float(data["b"][0][1]),
                ask_qty=float(data["a"][0][1]),
            )
        return tickers

    def _is_orderbook_message(self, message: Any) -> bool:
        if self._market_type in [MarketType.SPOT, MarketType.MARGIN]:
            return message["stream"].endswith("@bookTicker") or message["stream"].endswith("@depth5@100ms")
        if self._market_type in [MarketType.UPERP, MarketType.UDELIVERY]:
            return message["data"]["e"] == "bookTicker" or message["data"]["e"] == "depthUpdate"
        return False

    def _orderbook_handler(self, message: Any) -> OrderBook | None:
        if self._market_type in [MarketType.SPOT, MarketType.MARGIN]:
            return self._spot_orderbook_handler(message)
        elif self._market_type in [MarketType.UPERP, MarketType.UDELIVERY]:
            return self._uperp_orderbook_handler(message)

    def _spot_orderbook_handler(self, message: Any) -> OrderBook | None:
        data = message.get("data")
        symbol = data["s"]
        if symbol not in self._subscribed_symbols:
            logger.warning(f"Received orderbook message for unsubscribed symbol: {symbol}")
            return None
        orderbook = OrderBook(symbol)
        orderbook.stream = message["stream"]
        orderbook.book_type = BookType.FIXED
        orderbook.book_update_type = BookUpdateType.SNAPSHOT
        orderbook.exch_ts = int(time.time() * 1000)
        orderbook.recv_ts = orderbook.exch_ts

        if message["stream"].endswith("@bookTicker"):
            orderbook.exch_seq = data["u"]
            orderbook.bids.append((Decimal(data["b"]), Decimal(data["B"])))
            orderbook.asks.append((Decimal(data["a"]), Decimal(data["A"])))
            return orderbook
        elif message["stream"].endswith("@depth5@100ms"):
            orderbook.exch_seq = data["lastUpdateId"]
            for bid in data["bids"]:
                orderbook.bids.append((Decimal(bid[0]), Decimal(bid[1])))
            for ask in data["asks"]:
                orderbook.asks.append((Decimal(ask[0]), Decimal(ask[1])))
            return orderbook
        else:
            logger.warning(f"_orderbook_handler received non-orderbook message: {message}")
            return None

    def _uperp_orderbook_handler(self, message: Any) -> OrderBook | None:
        data = message["data"]
        symbol = data["s"]
        if symbol not in self._subscribed_symbols:
            logger.warning(f"Received orderbook message for unsubscribed symbol: {symbol}")
            return None
        orderbook = OrderBook(symbol)
        orderbook.stream = data["e"]
        orderbook.exch_seq = data["u"]
        orderbook.exch_ts = data["T"] * 1000
        orderbook.recv_ts = int(time.time() * 1000)
        orderbook.book_type = BookType.FIXED
        orderbook.book_update_type = BookUpdateType.SNAPSHOT
        if data["e"] == "bookTicker":
            orderbook.bids.append((Decimal(data["b"]), Decimal(data["B"])))
            orderbook.asks.append((Decimal(data["a"]), Decimal(data["A"])))
            return orderbook
        elif data["e"] == "depthUpdate":
            for bid in data["b"]:
                orderbook.bids.append((Decimal(bid[0]), Decimal(bid[1])))
            for ask in data["a"]:
                orderbook.asks.append((Decimal(ask[0]), Decimal(ask[1])))
            return orderbook

    def _is_kline_message(self, message: dict[str, Any]) -> bool:
        return (
            bool(message.get("data"))
            and message.get("stream", "").split("_")[-1] in self._wss_config.extra_params["kline_intervals"]
            and message.get("stream", "").islower()  # check
            and message.get("data", {}).get("s", "") in self._subscribed_symbols
        )

    def _kline_handler(self, message: Any) -> list[Kline]:
        interval = message["stream"].split("_")[-1]  # Stream: <symbol>@kline_<interval>
        kline = message["data"]
        symbol = kline["s"]
        data = Kline(
            exch_symbol=symbol,
            interval=interval,
            start_ts=float(kline["k"]["t"]),
            open=float(kline["k"]["o"]),
            close=float(kline["k"]["c"]),
            high=float(kline["k"]["h"]),
            low=float(kline["k"]["l"]),
            volume=float(kline["k"]["v"]),
            turnover=float(kline["k"]["q"]),
            taker_buy_base_asset_volume=float(kline["k"]["V"]),
            taker_buy_quote_asset_volume=float(kline["k"]["Q"]),
            trade_num=float(kline["k"]["n"]),
            ts=time.time() * 1000,
            confirm=kline["k"].get("x", False),
        )
        return [data]

    def _is_premium_index_message(self, message: dict[str, Any]) -> bool:
        return (
            bool(message.get("data"))
            and message.get("stream", "").split("_")[-1] in self._wss_config.extra_params["kline_intervals"]
            and message.get("stream", "").startswith("p")
            and not message.get("stream", "").islower()
        )

    def _premium_index_handler(self, message: Any) -> Any:
        return message["data"]
