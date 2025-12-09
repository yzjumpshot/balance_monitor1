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


class OKXAccountWssWrapper(BaseAccountWssWrapper):
    def __init__(self, account_meta: AccountMeta, account_config: AccountConfig, wss_config: WssConfig):
        super().__init__(account_meta, account_config, wss_config)

    async def subscribe_stream(self, symbols: list[str] | None = None) -> None:
        assert self._ws_client, "WebSocket client is not initialized"

        topic_list = []
        for event in self.registered_events:
            match event:
                case Event.BALANCE:
                    topic_list.append("account")
                case Event.ORDER:
                    match self._market_type:
                        case MarketType.SPOT:
                            topic_list.append("orders@instType:SPOT")
                        case MarketType.MARGIN:
                            topic_list.append("orders@instType:MARGIN")
                        case MarketType.UPERP | MarketType.CPERP:
                            topic_list.append("orders@instType:SWAP")
                        case MarketType.UDELIVERY | MarketType.CDELIVERY:
                            topic_list.append("orders@instType:FUTURES")
                case Event.POSITION:
                    match self._market_type:
                        case MarketType.MARGIN:
                            topic_list.append("positions@instType:MARGIN")
                        case MarketType.UPERP | MarketType.CPERP:
                            topic_list.append("positions@instType:SWAP")
                        case MarketType.UDELIVERY | MarketType.CDELIVERY:
                            topic_list.append("positions@instType:FUTURES")

            await self._ws_client.subscribe(topic_list)

    def _is_balance_message(self, msg: dict[str, Any]) -> bool:
        if "arg" in msg and "channel" in msg["arg"] and msg["arg"]["channel"] == "account" and "data" in msg:
            return True
        return False

    def _balance_handler(self, msg: dict[str, Any]) -> Balances:
        data = msg["data"][0]["details"]
        asset_data: dict[str, Balance] = {
            i["ccy"]: Balance(
                asset=i["ccy"],
                balance=float(i["eq"]),
                free=float(i["availEq"]),
                locked=float(i["frozenBal"]),
                type="full",
                ts=int(i["uTime"]),
            )
            for i in data
        }
        return Balances(asset_data)

    def _is_order_message(self, msg: dict[str, Any]) -> bool:
        if "arg" in msg and msg["arg"].get("channel") == "orders" and "data" in msg:
            return True

        return False

    def _order_handler(self, msg: dict[str, Any]) -> list[OrderSnapshot]:
        odd = []
        for d in msg["data"]:
            raw_order_type = d["ordType"]

            if raw_order_type in ("market", "limit"):
                order_type = OrderType[raw_order_type.upper()]
                tif = TimeInForce.GTC
            elif raw_order_type in ("post_only", "fok", "ioc"):
                order_type = OrderType.LIMIT
                tif = TimeInForce[raw_order_type.upper()]
            elif raw_order_type == "optimal_limit_ioc":
                order_type = OrderType.MARKET
                tif = TimeInForce.IOC
            elif raw_order_type == "mmp_and_post_only":
                order_type = OrderType.LIMIT
                tif = TimeInForce.GTX
            else:
                order_type = OrderType.UNKNOWN
                tif = TimeInForce.UNKNOWN

            raw_status = d["state"]
            if raw_status in ("filled", "canceled", "partially_filled"):
                status = OrderStatus[raw_status.upper()]
            elif raw_status == "live":
                status = OrderStatus.LIVE
            elif raw_status == "mmp_canceled":
                status = OrderStatus.CANCELED
            else:
                status = OrderStatus.UNKNOWN

            side = getattr(OrderSide, d["side"].upper(), OrderSide.UNKNOWN)

            o = OrderSnapshot(
                exch_symbol=d["instId"],
                price=Decimal(d["px"]),
                qty=Decimal(d["sz"]),
                avg_price=float(d["avgPx"]),
                filled_qty=Decimal(d["accFillSz"]),
                order_side=side,
                order_status=status,
                order_type=order_type,
                order_time_in_force=tif,
                order_id=d["ordId"],
                client_order_id=d["clOrdId"],
                reduce_only=d["reduceOnly"],
                fee=float(d["fee"]),
                place_ack_ts=int(d["cTime"]),
                exch_update_ts=int(d["uTime"]),
                local_update_ts=int(time.time() * 1000),
            )

            odd.append(o)

        return odd

    def _is_position_message(self, msg: dict[str, Any]) -> bool:
        if "arg" in msg and "channel" in msg["arg"] and msg["arg"]["channel"] == "positions" and "data" in msg:
            return True
        return False

    def _position_handler(self, msg: dict[str, Any]) -> Positions:
        data = {}
        for d in msg["data"]:
            symbol = d["instId"]
            sign = {"long": 1, "short": -1}.get(d["posSide"], 1)
            data[symbol] = Position(
                exch_symbol=symbol,
                net_qty=float(d["pos"]) * sign,
                entry_price=float(d["avgPx"]),
                value=float(d["notionalUsd"]),
                unrealized_pnl=float(d["upl"]),
                liq_price=float(d["liqPx"]),
            )

        return Positions(**data)
