from typing import Any
from xclients.data_type import Any
from ..base_wrapper import BaseMarketWssWrapper
from ..enum_type import Event
from ..data_type import *
from loguru import logger


class GateMarketWssWrapper(BaseMarketWssWrapper):
    def __init__(self, account_meta: AccountMeta, account_config: AccountConfig, wss_config: WssConfig):
        super().__init__(account_meta, account_config, wss_config)

    async def subscribe_stream(self, symbols: list[str] | None = None) -> None:
        if not symbols:
            symbols = list(self._subscribed_symbols)
        assert self._ws_client, "WebSocket client is not initialized"

        topic_list = []
        for event in self.registered_events:
            for s in symbols:
                if not self._market_type.is_derivative:
                    match event:
                        case Event.BOOK:
                            topic_list.append(f"order_book@{s}@5@100ms")
                        case Event.TICKER:
                            topic_list.append(f"book_tickers@{s}")
                else:
                    match event:
                        case Event.BOOK:
                            topic_list.append(f"order_book@{s}@5@0")
                        case Event.TICKER:
                            topic_list.append(f"tickers@{s}")
                            topic_list.append(f"book_ticker@{s}")

        await self._ws_client.subscribe(topic_list)

    async def _process_message(self, message: dict[str, Any]):
        if not message or "result" not in message or message["result"] is None:
            return
        if "status" in message["result"]:
            if message["result"]["status"] != "success":
                logger.debug(f"收到公共WS报错 {message}")
            return
        await super()._process_message(message)

    def _is_orderbook_message(self, message: dict[str, Any]) -> bool:
        return message["channel"] in [
            "futures.order_book",
            "futures.book_ticker",
            "spot.order_book",
            "spot.book_ticker",
            "spot.order_book_update",
        ]

    def _orderbook_handler(self, message: dict[str, Any]) -> OrderBook | None:
        data = message["result"]
        if message["channel"] == "spot.order_book":
            symbol = data["s"]
            orderbook = OrderBook(symbol)
            orderbook.book_type = BookType.FIXED
            orderbook.book_update_type = BookUpdateType.SNAPSHOT

            orderbook.exch_seq = data["lastUpdateId"]
            orderbook.exch_ts = data["t"]
            orderbook.recv_ts = int(time.time() * 1000)
            for bid in data["bids"]:
                orderbook.bids.append((Decimal(bid[0]), Decimal(bid[1])))
            for ask in data["asks"]:
                orderbook.asks.append((Decimal(ask[0]), Decimal(ask[1])))
            return orderbook

        elif message["channel"] == "spot.book_ticker":
            symbol = data["s"]
            orderbook = OrderBook(symbol)
            orderbook.book_type = BookType.FIXED
            orderbook.book_update_type = BookUpdateType.SNAPSHOT
            orderbook.exch_seq = data["u"]

            bpx = Decimal(data["b"])
            bqty = Decimal(data["B"])
            apx = Decimal(data["a"])
            aqty = Decimal(data["A"])

            orderbook.bids.appendleft((bpx, bqty))
            orderbook.asks.appendleft((apx, aqty))

            orderbook.exch_seq = data["u"]
            orderbook.exch_ts = data["t"]
            orderbook.recv_ts = int(time.time() * 1000)

            return orderbook

        elif message["channel"] == "futures.order_book":  # order_book
            if message["event"] == "update":
                return
            symbol = data["contract"]
            orderbook = OrderBook(symbol)
            orderbook.stream = message["channel"]
            orderbook.book_type = BookType.FIXED
            orderbook.book_update_type = BookUpdateType.SNAPSHOT

            orderbook.exch_seq = data["id"]
            orderbook.exch_ts = data["t"]
            orderbook.recv_ts = int(time.time() * 1000)

            for bid in data["bids"]:
                orderbook.bids.append((Decimal(bid["p"]), Decimal(bid["s"])))
            for ask in data["asks"]:
                orderbook.asks.append((Decimal(ask["p"]), Decimal(ask["s"])))

            return orderbook

        elif message["channel"] == "futures.book_ticker":  # book_ticker
            symbol = data["s"]
            orderbook = OrderBook(symbol)
            orderbook.stream = message["channel"]
            orderbook.book_type = BookType.FIXED
            orderbook.book_update_type = BookUpdateType.SNAPSHOT
            orderbook.exch_seq = data["u"]

            if data["b"] != "":
                bpx = Decimal(data["b"])
            else:
                return
            bqty = Decimal(data["B"])
            if data["a"] != "":
                apx = Decimal(data["a"])
            else:
                return
            aqty = Decimal(data["A"])

            orderbook.bids.appendleft((bpx, bqty))
            orderbook.asks.appendleft((apx, aqty))
            orderbook.exch_seq = data["u"]
            orderbook.exch_ts = data["t"]
            orderbook.recv_ts = int(time.time() * 1000)

            return orderbook

    def _is_ticker_message(self, message: Any) -> bool:
        return bool(message.get("result")) and (
            message.get("channel", "")
            in ["futures.tickers", "futures.book_ticker", "spot.tickers", "spot.book_ticker"]
        )

    def _ticker_handler(self, message: dict[str, Any]) -> Tickers | None:
        tickers = Tickers()
        if message["channel"] == "spot.tickers":
            # subscribe_public_stream中对于Events.TICKER订阅spot.book_ticker，不再订阅spot.tickers
            info = message["result"]
            symbol = info["currency_pair"]
            tickers[symbol] = Ticker(
                symbol,
                bid=info["highest_bid"],
                ask=info["lowest_ask"],
                ts=message["time"] * 1000,
                update_ts=message["time"] * 1000,
                # 此频道无highest_size, lowest_size字段
            )
        elif message["channel"] == "spot.book_ticker":
            info = message["result"]
            symbol = info["s"]
            tickers[symbol] = Ticker(
                symbol,
                bid=float(info["b"]) if info["b"] else np.nan,  # 最佳买方的价格，如果没有买方，则为空串
                ask=float(info["a"]) if info["a"] else np.nan,
                ts=message["time_ms"],  # 实际数据有time_ms, 但文档未标注有time_ms
                update_ts=info["t"],
                bid_qty=float(info["B"]),  # 最佳买方的数量，如果没有买方，则为 0
                ask_qty=float(info["A"]),
            )
        elif message["channel"] == "futures.tickers":
            datas = message["result"]
            for info in datas:
                symbol = info["contract"]
                tickers[symbol] = Ticker(
                    symbol,
                    index_price=float(info["index_price"]),
                    ts=message["time_ms"],
                    update_ts=message["time_ms"],
                    fr=float(info["funding_rate"]),
                )
        elif message["channel"] == "futures.book_ticker":
            info = message["result"]
            symbol = info["s"]
            tickers[symbol] = Ticker(
                symbol,
                bid=float(info["b"]) if info["b"] else np.nan,  # 最佳买方的价格，如果没有买方，则为空串
                ask=float(info["a"]) if info["a"] else np.nan,
                ts=message["time_ms"],
                update_ts=info["t"],
                bid_qty=float(info["B"]),  # 最佳买方的数量，如果没有买方，则为 0
                ask_qty=float(info["A"]),
            )
        return tickers
