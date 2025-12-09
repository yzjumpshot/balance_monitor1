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

from .constants import TIF_MAP
from loguru import logger


class GateAccountWssWrapper(BaseAccountWssWrapper):
    def __init__(self, account_meta: AccountMeta, account_config: AccountConfig, wss_config: WssConfig):
        super().__init__(account_meta, account_config, wss_config)

    async def subscribe_stream(self, symbols: list[str] | None = None) -> None:
        assert self._ws_client, "WebSocket client is not initialized"

        topic_list = []
        for event in self.registered_events:
            match event:
                case Event.BALANCE:
                    topic_list.append("balances")
                case Event.ORDER:
                    topic_list.append("orders@!all")
                case Event.POSITION if self._market_type != MarketType.SPOT:
                    topic_list.append("positions@!all")
                case Event.USER_TRADE:
                    topic_list.append("usertrades@!all")

        await self._ws_client.subscribe(topic_list)

    def _is_balance_message(self, msg: dict[str, Any]) -> bool:
        if (
            (msg.get("channel") == "spot.balances" or msg.get("channel") == "futures.balances")
            and msg.get("event") == "update"
            and msg.get("result")
        ):
            return True
        return False

    def _is_order_message(self, msg: dict[str, Any]) -> bool:
        return msg["channel"] in ("spot.orders", "futures.orders") and msg["event"] == "update"

    def _is_position_message(self, msg: dict[str, Any]) -> bool:
        if msg.get("channel") == "futures.positions" and msg.get("event") == "update" and msg.get("result"):
            return True
        return False

    def _is_trade_msg(self, msg: dict[str, Any]) -> bool:
        if (
            msg["channel"] in ("spot.usertrades", "futures.usertrades")
            and msg["event"] == "update"
            and msg.get("result")
        ):
            return True
        return False

    def _balance_handler(self, msg) -> Balances:
        if msg.get("channel", "") == "spot.balances":
            data = msg["result"]
            asset_data = {
                info["currency"]: Balance(
                    asset=info["currency"],
                    balance=float(info["total"]),
                    free=float(info["available"]),
                    locked=float(info["freeze"]),
                    type="full",
                    ts=info["timestamp_ms"],
                )
                for info in data
            }

        elif msg.get("channel", "") == "futures.balances":
            data = msg["result"]
            asset_data = {
                info["currency"].upper(): Balance(
                    asset=info["currency"].upper(),
                    balance=float(info["balance"]),
                    free=float(info["balance"]),
                    locked=float(0),
                    type="full",
                    ts=info["time_ms"],
                )
                for info in data
            }
        else:
            asset_data = {}
        return Balances(asset_data)

    def _order_handler(self, msg: dict[str, Any]) -> list[OrderSnapshot]:
        odd = []
        if self._market_type == MarketType.SPOT:
            for d in msg["result"]:
                order_type = getattr(OrderType, d["type"].upper(), OrderType.UNKNOWN)
                tif = TIF_MAP.get(d["time_in_force"], TimeInForce.UNKNOWN)

                if order_type == OrderType.MARKET:
                    filled_price = Decimal(d["avg_deal_price"])
                    _left = Decimal(d["left"]) / filled_price if filled_price != 0 else Decimal("0")
                    filled_qty = Decimal(d["filled_total"]) / filled_price if filled_price != 0 else Decimal("0")
                    qty = filled_qty + _left
                else:
                    qty = Decimal(d["amount"])
                    _left = Decimal(d["left"])
                    filled_qty = qty - _left

                raw_status = d["finish_as"]
                if raw_status == "open":
                    if filled_qty == 0:
                        status = OrderStatus.LIVE
                    else:
                        status = OrderStatus.PARTIALLY_FILLED
                elif raw_status == "filled":
                    status = OrderStatus.FILLED
                elif raw_status == "cancelled":
                    status = OrderStatus.CANCELED
                elif raw_status == "ioc":
                    if _left == 0:
                        status = OrderStatus.FILLED
                    else:
                        status = OrderStatus.CANCELED
                elif raw_status == "stp":
                    status = OrderStatus.CANCELED
                else:
                    status = OrderStatus.UNKNOWN

                side = getattr(OrderSide, d["side"].upper(), OrderSide.UNKNOWN)

                o = OrderSnapshot(
                    exch_symbol=d["currency_pair"],
                    price=Decimal(d["price"]),
                    qty=qty,
                    avg_price=float(d["avg_deal_price"]),
                    filled_qty=filled_qty,
                    order_side=side,
                    order_status=status,
                    order_type=order_type,
                    order_time_in_force=tif,
                    order_id=str(d["id"]),
                    client_order_id=str(d["text"]),
                    fee=float(d["fee"]),
                    fee_ccy=d["fee_currency"],
                    rejected_message="",
                    place_ack_ts=int(d["create_time_ms"]),
                    exch_update_ts=int(d["update_time_ms"]),
                    local_update_ts=int(time.time() * 1000),
                )
                odd.append(o)
        else:
            for d in msg["result"]:
                qty = Decimal(abs(d["size"]))
                _left = Decimal(abs(d["left"]))
                filled_qty = qty - _left

                if d["size"] > 0:
                    side = OrderSide.BUY
                elif d["size"] < 0:
                    side = OrderSide.SELL
                else:
                    side = OrderSide.UNKNOWN

                order_type = OrderType.MARKET if d["price"] == 0.0 else OrderType.LIMIT
                tif = TIF_MAP.get(d["tif"], TimeInForce.UNKNOWN)
                status = OrderStatus.UNKNOWN
                if d["status"] == "open":
                    if filled_qty == 0:
                        status = OrderStatus.LIVE
                    else:
                        status = OrderStatus.PARTIALLY_FILLED
                elif d["status"] == "finished":
                    if d["finish_as"] == "filled":
                        status = OrderStatus.FILLED
                    elif d["finish_as"] in (
                        "cancelled",
                        "liquidated",
                        "ioc",
                        "auto_deleveraging",
                        "reduce_only",
                        "position_close",
                        "stp",
                    ):
                        status = OrderStatus.CANCELED
                    else:
                        status = OrderStatus.UNKNOWN

                o = OrderSnapshot(
                    exch_symbol=d["contract"],
                    price=Decimal(d["price"]),
                    qty=qty,
                    avg_price=float(d["fill_price"]),
                    filled_qty=filled_qty,
                    order_side=side,
                    order_status=status,
                    order_type=order_type,
                    order_time_in_force=tif,
                    order_id=str(d["id"]),
                    client_order_id=str(d["text"]),
                    reduce_only=d["is_reduce_only"],
                    rejected_message="",
                    place_ack_ts=int(d["create_time_ms"]),
                    exch_update_ts=int(d["finish_time_ms"]),
                    local_update_ts=int(time.time() * 1000),
                )
                odd.append(o)

        return odd

    def _position_handler(self, msg: dict[str, Any]) -> Positions:
        position_data: dict[str, Position]
        data = msg["result"]
        position_data = {
            info["contract"]: Position(
                exch_symbol=info["contract"],
                net_qty=float(info["size"]),
                entry_price=float(info["entry_price"]),
                value=abs(float(info["size"])) * float(info["entry_price"]),
                unrealized_pnl=0.0,
                liq_price=float(info["liq_price"]),
            )
            for info in data
        }
        return Positions(**position_data)

    def _trade_handler(self, msg: dict[str, Any]) -> TradeData:
        result: dict[str, list[Trade]] = {}
        if self._market_type == MarketType.SPOT:
            for data in msg["result"]:
                result.setdefault(data["currency_pair"], []).append(
                    Trade(
                        create_ts=int(float(data["create_time_ms"])),
                        side=getattr(OrderSide, data["side"].upper(), OrderSide.UNKNOWN),
                        trade_id=data["id"],
                        order_id=data["order_id"],
                        last_trd_price=Decimal(data["price"]),
                        last_trd_volume=abs(Decimal(data["amount"])),
                        turnover=Decimal(data["price"]) * abs(Decimal(data["amount"])),
                        fill_ts=int(float(data["create_time_ms"])),
                        fee=Decimal(data["fee"]),
                        fee_ccy=data["fee_currency"],
                        is_maker=True if data["role"] == "maker" else False,
                    )
                )
        else:
            for data in msg["result"]:
                side = "BUY" if data["size"] > 0 else "SELL"
                result.setdefault(data["contract"], []).append(
                    Trade(
                        create_ts=int(float(data["create_time"]) * 1000),
                        side=getattr(OrderSide, side, OrderSide.UNKNOWN),
                        trade_id=data["id"],
                        order_id=data["order_id"],
                        last_trd_price=Decimal(data["price"]),
                        last_trd_volume=abs(Decimal(data["size"])),
                        turnover=Decimal(data["price"]) * abs(Decimal(data["size"])),  # TODO consider multiplier
                        fill_ts=int(float(data["create_time"]) * 1000),
                        fee=Decimal(data["fee"]),
                        fee_ccy="",
                        is_maker=True if data["role"] == "maker" else False,
                    )
                )
        return TradeData(result)

    async def _process_extra_message(self, msg: dict[str, Any]) -> None:
        # - 追加保证金
        if msg["channel"] == "futures.reduce_risk_limits":
            logger.critical("降低风险率推送", level="critical")

        elif msg["channel"] == "futures.auto_deleverages":
            logger.critical(f"ADL推送: {msg}", level="critical")
            for adl_order in msg["result"]:
                symbol = adl_order["contract"]

                qty = abs(Decimal(adl_order["trade_size"]))
                if float(adl_order["fill_price"]) > float(adl_order["entry_price"]):
                    if qty > 0:
                        logger.critical(f"ADL推送符号异常", level="critical")
                        qty *= -1
                await self._event_bus.publish(Event.LIQUIDATION, self._account_meta, LiquidationMessage(symbol, qty))
