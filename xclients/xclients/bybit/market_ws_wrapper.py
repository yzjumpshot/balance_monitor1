from decimal import Decimal
import time
from typing import Any
from ..base_wrapper import BaseMarketWssWrapper
from loguru import logger
from ..enum_type import Event
from ..data_type import *
from .constants import *


class BybitMarketWssWrapper(BaseMarketWssWrapper):
    def __init__(self, account_meta: AccountMeta, account_config: AccountConfig, wss_config: WssConfig):
        super().__init__(account_meta, account_config, wss_config)

    async def subscribe_stream(self, symbols: list[str] | None = None) -> None:
        if not symbols:
            symbols = list(self._subscribed_symbols)
        assert self._ws_client, "WebSocket client is not initialized"

        for event in self.registered_events:
            if event == Event.BOOK:
                suffixes: list[str] = []
                for symbol in symbols:
                    suffixes.append(f"orderbook.1.{symbol.upper()}")
                payload: dict[str, Any] = {"op": "subscribe", "args": suffixes, "req_id": 1}
                await self._ws_client.request(payload)
            elif event == Event.KLINE:
                for interval in self._wss_config.extra_params["kline_intervals"]:
                    bybit_interval = INTERVAL_MAP.get(interval, interval)
                    if self._market_type in [MarketType.SPOT, MarketType.MARGIN]:
                        batch_size = 10
                    else:
                        batch_size = 100
                    for i in range(0, len(symbols), batch_size):
                        batch = symbols[i : i + batch_size]
                        suffixes = [f"kline.{bybit_interval}.{symbol.upper()}" for symbol in batch]
                        payload = {"op": "subscribe", "args": suffixes, "req_id": 2}
                        await self._ws_client.request(payload)

    def _is_orderbook_message(self, message: dict[str, Any]):
        return message.get("topic", "").startswith("orderbook")

    def _orderbook_handler(self, message: dict[str, Any]) -> OrderBook | None:
        topic: str = message["topic"]
        data = message["data"]
        symbol = data["s"]
        if symbol not in self._subscribed_symbols:
            logger.warning(f"Received orderbook message for unsubscribed symbol: {symbol}")
        orderbook = OrderBook(symbol)
        orderbook.stream = topic
        if topic.startswith("orderbook.1."):
            orderbook.book_type = BookType.DIFF

            orderbook.exch_seq = data["seq"]
            orderbook.exch_ts = message["ts"]
            orderbook.recv_ts = int(time.time() * 1000)

            if message["type"] == "snapshot" or ("data" in message and message["data"]["u"] == 1):
                orderbook.book_update_type = BookUpdateType.SNAPSHOT
            elif message["type"] == "delta":
                orderbook.book_update_type = BookUpdateType.DELTA
            else:
                return

            for bid in data["b"]:
                orderbook.bids.append((Decimal(bid[0]), Decimal(bid[1])))
            for ask in data["a"]:
                orderbook.asks.append((Decimal(ask[0]), Decimal(ask[1])))

            return orderbook
        else:
            logger.warning(f"_orderbook_handler 获取非orderbook msg: {message}")
            return None

    def _is_kline_message(self, message: dict[str, Any]) -> bool:
        return (
            bool(message.get("data"))
            and message.get("topic", "").split(".")[1]
            in [INTERVAL_MAP.get(interval, interval) for interval in self._wss_config.extra_params["kline_intervals"]]
            and message.get("topic", "").split(".")[-1] in self._subscribed_symbols
        )

    def _kline_handler(self, message: Any) -> list[Kline]:
        interval: str = message.get("topic", "").split(".")[1]  # "topic": "kline.5.BTCUSDT"

        # trans interval to meta_interval: 1 --> 1m; 5 --> 5m
        kline_lst = message["data"]
        symbol = message.get("topic", "").split(".")[-1]
        res_datas: list[Kline] = []
        for kline in kline_lst:
            res_datas.append(
                Kline(
                    exch_symbol=symbol,
                    interval=INVERT_INTERVAL_MAP.get(interval, interval),
                    start_ts=float(kline["start"]),
                    open=float(kline["open"]),
                    close=float(kline["close"]),
                    high=float(kline["high"]),
                    low=float(kline["low"]),
                    volume=float(kline["volume"]),
                    turnover=float(kline["turnover"]),
                    ts=time.time() * 1000,
                    confirm=kline["confirm"],
                )
            )
        return res_datas
