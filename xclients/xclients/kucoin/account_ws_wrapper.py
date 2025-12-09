from decimal import Decimal
from typing import Any
from collections import defaultdict

from ..base_wrapper import BaseAccountWssWrapper
from ..data_type import *
from ..enum_type import (
    AccountType,
    TimeInForce,
    OrderSide,
    OrderStatus,
    OrderType,
)


class KucoinAccountWssWrapper(BaseAccountWssWrapper):
    def __init__(self, account_meta: AccountMeta, account_config: AccountConfig, wss_config: WssConfig):
        super().__init__(account_meta, account_config, wss_config)

    async def subscribe_stream(self, symbol_list: list[str] | None = None) -> None:
        assert self._ws_client, "WebSocket client is not initialized"

        topic_list = []
        for event in self.registered_events:
            match event:
                case Event.BALANCE:
                    match self._market_type:
                        case MarketType.SPOT:
                            topic_list.append("/account/balance")
                        case MarketType.UPERP | MarketType.CPERP | MarketType.UDELIVERY | MarketType.CDELIVERY:
                            topic_list.append("/contractAccount/wallet")
                case Event.ORDER:
                    match self._market_type:
                        case MarketType.SPOT:
                            topic_list.append("/spotMarket/tradeOrdersV2")
                        case MarketType.UPERP | MarketType.CPERP | MarketType.UDELIVERY | MarketType.CDELIVERY:
                            topic_list.append("/contractMarket/tradeOrders")
                case Event.POSITION:
                    if self._market_type != MarketType.SPOT:
                        topic_list.append("/contract/positionAll")

        await self._ws_client.subscribe(topic_list)

    def _is_balance_message(self, msg: dict[str, Any]) -> bool:
        if msg.get("type") != "message":
            return False

        if msg["topic"] == "/account/balance" and msg["subject"] == "account.balance":
            return True
        elif msg["topic"] == "/contractAccount/wallet" and msg["subject"] == "walletBalance.change":
            return True

        return False

    def _balance_handler(self, msg: dict[str, Any]) -> Balances:
        data = msg["data"]
        match self._market_type:
            case MarketType.SPOT:
                asset_data = {
                    data["currency"]: Balance(
                        asset=data["currency"],
                        balance=float(data["total"]),
                        free=float(data["available"]),
                        locked=float(data["hold"]),
                        type="full",
                        ts=int(data["time"]),
                    )
                }
            case MarketType.UPERP | MarketType.CPERP | MarketType.UDELIVERY | MarketType.CDELIVERY:
                asset_data = {
                    data["currency"]: Balance(
                        asset=data["currency"],
                        balance=float(data["availableBalance"]) + float(data["holdBalance"]),
                        free=float(data["availableBalance"]),
                        locked=float(data["holdBalance"]),
                        type="full",
                        ts=int(data["timestamp"]),
                    )
                }

        return Balances(asset_data)

    def _is_order_message(self, msg: dict[str, Any]) -> bool:
        if msg.get("type") != "message":
            return False

        if msg["topic"] in ("/spotMarket/tradeOrdersV2", "/contractMarket/tradeOrders"):
            return True

        return False

    def _order_handler(self, msg: dict[str, Any]) -> list[OrderSnapshot]:
        d = msg["data"]
        if "size" in d:
            qty = Decimal(d["size"])
            left_qty = Decimal(d["remainSize"])
            filled_qty = Decimal(d.get("filledSize", "0"))
        else:
            qty = Decimal(d["originSize"])
            left_qty = Decimal(d["originSize"])
            filled_qty = Decimal(0)

        if self._market_type == MarketType.SPOT:
            c_time = int(d["orderTime"])
            u_time = int(d["ts"] / 10**6)
        else:
            c_time = int(d["orderTime"] / 10**6)
            u_time = int(d["ts"] / 10**6)

        order_type = getattr(OrderType, d.get("orderType", "UNKNOWN").upper(), OrderType.UNKNOWN)
        tif = getattr(TimeInForce, d.get("timeInForce", "UNKNOWN"), TimeInForce.UNKNOWN)

        if d["type"] == "received" and d["status"] == "new":
            status = OrderStatus.PENDING
        elif d["type"] == "open" and d["status"] == "open":
            status = OrderStatus.LIVE
        elif d["status"] == "match":
            if left_qty == 0:
                status = OrderStatus.FILLED
            else:
                status = OrderStatus.PARTIALLY_FILLED
        elif d["status"] == "done":
            if d["type"] == "filled":
                status = OrderStatus.FILLED
            elif d["type"] == "canceled":
                status = OrderStatus.CANCELED
            else:
                status = OrderStatus.UNKNOWN
        else:
            status = OrderStatus.UNKNOWN

        side = getattr(OrderSide, d["side"].upper(), OrderSide.UNKNOWN)

        o = OrderSnapshot(
            exch_symbol=d["symbol"],
            price=Decimal(d["price"]),
            qty=qty,
            avg_price=float(d["price"]),  # TODO: can not figure it out from raw data, use orig price to replace it
            filled_qty=filled_qty,
            order_side=side,
            order_status=status,
            order_type=order_type,
            order_time_in_force=tif,
            order_id=str(d["orderId"]),
            client_order_id=str(d["clientOid"]),
            place_ack_ts=c_time,
            exch_update_ts=u_time,
            local_update_ts=int(time.time() * 1000),
        )

        return [o]

    def _is_position_message(self, msg: dict[str, Any]) -> bool:
        if msg.get("type") != "message":
            return False

        if msg["topic"].startswith("/contract/position"):
            return True

        return False

    def _position_handler(self, msg: dict[str, Any]) -> Positions:
        d = msg["data"]
        qty = float(d.get("currentQty", "0"))
        if qty == 0:
            return Positions()

        p = Position(
            exch_symbol=d["symbol"],
            net_qty=qty,
            entry_price=float(d["avgEntryPrice"]),
            value=abs(qty) * float(d["avgEntryPrice"]),
            unrealized_pnl=float(d["unrealisedPnl"]),
            liq_price=float(d["liquidationPrice"]),
        )

        return Positions({d["symbol"]: p})
