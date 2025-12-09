from decimal import Decimal
from typing import Any
from collections import defaultdict
import time

from ..base_wrapper import BaseAccountWssWrapper
from ..data_type import *
from ..enum_type import (
    AccountType,
    TimeInForce,
    OrderSide,
    OrderStatus,
    OrderType,
)


class CoinexAccountWssWrapper(BaseAccountWssWrapper):
    def __init__(self, account_meta: AccountMeta, account_config: AccountConfig, wss_config: WssConfig):
        super().__init__(account_meta, account_config, wss_config)

    async def subscribe_stream(self, symbols: list[str] | None = None) -> None:
        assert self._ws_client, "WebSocket client is not initialized"

        topic_list = []
        for event in self.registered_events:
            match event:
                case Event.BALANCE:
                    topic_list.append("balance")
                case Event.ORDER:
                    topic_list.append("order")
                case Event.POSITION:
                    if self._market_type != MarketType.SPOT:
                        topic_list.append("position")
                case Event.USER_TRADE:
                    topic_list.append("user_deals")

        await self._ws_client.subscribe(topic_list)

    def _is_position_message(self, msg: dict[str, Any]) -> bool:
        return msg.get("method") == "position.update"

    def _is_balance_message(self, msg: dict[str, Any]) -> bool:
        return msg.get("method") == "balance.update"

    def _is_order_message(self, msg: dict[str, Any]) -> bool:
        return msg.get("method") == "order.update"

    def _is_trade_msg(self, msg: dict[str, Any]) -> bool:
        return msg.get("method") == "user_deals.update"

    def _balance_handler(self, msg: dict[str, Any]) -> Balances:
        data = {
            d["ccy"]: Balance(
                asset=d["ccy"],
                balance=float(d["available"]) + float(d["frozen"]),
                free=float(d["available"]),
                locked=float(d["frozen"]),
                type="full",
                ts=d.get("updated_at", int(time.time() * 1000)),
            )
            for d in msg["data"]["balance_list"]
        }

        return Balances(**data)

    def _order_handler(self, msg: dict[str, Any]) -> list[OrderSnapshot]:
        ev = msg["data"]["event"]
        ro = msg["data"]["order"]

        quantity = Decimal(ro["amount"])
        filled_quantity = Decimal(ro["filled_amount"])

        if filled_quantity != Decimal(0):
            filled_price = Decimal(ro["filled_value"]) / filled_quantity
        else:
            filled_price = Decimal(0)

        if ev == "put":
            if filled_quantity == Decimal(0):
                status = OrderStatus.LIVE
            else:
                status = OrderStatus.PARTIALLY_FILLED
        elif ev == "update":
            status = OrderStatus.PARTIALLY_FILLED
        elif ev == "modify":
            if filled_quantity == Decimal(0):
                status = OrderStatus.LIVE
            else:
                status = OrderStatus.PARTIALLY_FILLED
        elif ev == "finish":
            if quantity == filled_quantity:
                status = OrderStatus.FILLED
            else:
                status = OrderStatus.CANCELED
        else:
            status = OrderStatus.UNKNOWN  # should not reach here

        if ro["type"] == "limit":
            order_type = OrderType.LIMIT
            tif = TimeInForce.GTC
        elif ro["type"] == "market":
            order_type = OrderType.MARKET
            tif = TimeInForce.UNKNOWN
        elif ro["type"] == "maker_only":
            order_type = OrderType.LIMIT
            tif = TimeInForce.GTX
        elif ro["type"] in ("ioc", "fok"):
            order_type = OrderType.LIMIT
            tif = TimeInForce[ro["type"].upper()]
        else:
            order_type = OrderType.UNKNOWN
            tif = TimeInForce.UNKNOWN

        o = OrderSnapshot(
            exch_symbol=ro["market"],
            order_side=OrderSide[ro["side"].upper()],
            order_id=str(ro["order_id"]),
            client_order_id=str(ro["client_id"]),
            price=Decimal(ro["price"]),
            qty=Decimal(ro["amount"]),
            avg_price=float(filled_price),
            filled_qty=filled_quantity,
            order_type=order_type,
            order_time_in_force=tif,
            order_status=status,
            place_ack_ts=ro["created_at"],
            exch_update_ts=ro["updated_at"],
            local_update_ts=int(time.time() * 1000),
            fee=float(ro["fee"]),
            fee_ccy=ro["fee_ccy"],
        )

        return [o]

    def _position_handler(self, msg: dict[str, Any]) -> Positions:
        rp = msg["data"]["position"]
        sign = {"long": 1, "short": -1}.get(rp["side"], 1)
        data = {
            rp["market"]: Position(
                exch_symbol=rp["market"],
                net_qty=float(rp["open_interest"]) * sign,
                entry_price=float(rp["avg_entry_price"]),
                value=abs(float(rp["open_interest"]) * float(rp["avg_entry_price"])),
                unrealized_pnl=float(rp["unrealized_pnl"]),
                liq_price=float(rp["liq_price"]),
                ts=rp["updated_at"],
            )
        }

        return Positions(**data)

    def _trade_handler(self, msg: dict[str, Any]) -> TradeData:
        rt = msg["data"]
        t = Trade(
            create_ts=rt["created_at"],
            side=OrderSide[rt["side"].upper()],
            trade_id=rt["deal_id"],
            order_id=rt["order_id"],
            last_trd_price=Decimal(rt["price"]),
            last_trd_volume=Decimal(rt["amount"]),
            turnover=Decimal(rt["price"]) * Decimal(rt["amount"]),
            fill_ts=rt["created_at"],
            fee=Decimal(rt["fee"]),
            fee_ccy=rt["fee_ccy"],
            is_maker=rt["role"] == "maker",
        )

        return TradeData({rt["market"]: [t]})
