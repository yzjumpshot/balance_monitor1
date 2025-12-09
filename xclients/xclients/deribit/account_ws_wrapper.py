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

from .constants import STATUS_MAP, TIF_MAP


class DeribitAccountWssWrapper(BaseAccountWssWrapper):
    # balance update {'e': 'balanceUpdate', 'E': 1690252320713, 'a': 'USDT', 'd': '1.00000000', 'T': 1690252320713}
    # account update {'e': 'outboundAccountPosition', 'E': 1690252320713, 'u': 1690252320713, 'B': [{'a': 'USDT', 'f': '1.00000000', 'l': '0.00000000'}]}
    def __init__(self, account_meta: AccountMeta, account_config: AccountConfig, wss_config: WssConfig):
        super().__init__(account_meta, account_config, wss_config)

    async def subscribe_stream(self, symbols: list[str] | None = None) -> None:
        assert self._ws_client, "WebSocket client is not initialized"

        topic_list = []
        for event in self.registered_events:
            match event:
                case Event.BALANCE:
                    currency = "any"
                    topic_list.append(f"user.portfolio.{currency}")
                case Event.ORDER:
                    currency = "any"
                    match self._market_type:
                        case MarketType.SPOT | MarketType.MARGIN:
                            kind = "spot"
                        case MarketType.UPERP | MarketType.CPERP | MarketType.UDELIVERY | MarketType.CDELIVERY:
                            kind = "future"
                        case _:
                            raise Exception(f"Unsupported MarketType: {self._market_type}")

                    topic_list.append(f"user.orders.{kind}.{currency}.raw")
                case Event.POSITION:
                    currency = "any"
                    match self._market_type:
                        case MarketType.SPOT | MarketType.MARGIN:
                            kind = "spot"
                        case MarketType.UPERP | MarketType.CPERP | MarketType.UDELIVERY | MarketType.CDELIVERY:
                            kind = "future"
                        case _:
                            raise Exception(f"Unsupported MarketType: {self._market_type}")

                    topic_list.append(f"user.changes.{kind}.{currency}.raw")
                case Event.USER_TRADE:
                    currency = "any"
                    match self._market_type:
                        case MarketType.SPOT | MarketType.MARGIN:
                            kind = "spot"
                        case MarketType.UPERP | MarketType.CPERP | MarketType.UDELIVERY | MarketType.CDELIVERY:
                            kind = "future"
                        case _:
                            raise Exception(f"Unsupported MarketType: {self._market_type}")

                    topic_list.append(f"user.trades.{kind}.{currency}.raw")

        await self._ws_client.subscribe(topic_list)

    def _is_position_message(self, msg: dict[str, Any]) -> bool:
        if not msg.get("method") == "subscription":
            return False

        return msg["params"]["channel"].startswith("user.changes")

    def _is_balance_message(self, msg: dict[str, Any]) -> bool:
        if not msg.get("method") == "subscription":
            return False

        return msg["params"]["channel"].startswith("user.portfolio")

    def _is_order_message(self, msg: dict[str, Any]) -> bool:
        if not msg.get("method") == "subscription":
            return False

        return msg["params"]["channel"].startswith("user.orders")

    def _is_trade_msg(self, msg: dict[str, Any]) -> bool:
        if not msg.get("method") == "subscription":
            return False

        return msg["params"]["channel"].startswith("user.trades")

    def _account_handler(self, msg: dict[str, Any]) -> Balances:
        data = msg["params"]["data"]
        asset_data = {
            data["currency"]: Balance(
                asset=data["currency"],
                balance=float(data["balance"]),
                free=float(str(data["available_withdrawal_funds"])),
                locked=float(data["balance"]) - float(data["available_withdrawal_funds"]),
                type="full",
                ts=int(time.time() * 1000),
            )
        }
        return Balances(asset_data)

    def _order_handler(self, msg: dict[str, Any]) -> list[OrderSnapshot]:
        odd = []
        data = msg["params"]["data"]

        if data["post_only"]:
            tif = TimeInForce.GTX
        else:
            tif = TIF_MAP.get(data["time_in_force"], TimeInForce.UNKNOWN)

        status = STATUS_MAP.get(data["order_state"], OrderStatus.UNKNOWN)
        if status == OrderStatus.LIVE and data["filled_amount"] != 0:
            status = OrderStatus.PARTIALLY_FILLED

        o = OrderSnapshot(
            exch_symbol=data["instrument_name"],
            price=Decimal(data["price"]),
            qty=Decimal(data["amount"]),
            avg_price=float(data["average_price"]),
            filled_qty=Decimal(data["filled_amount"]),
            order_side=OrderSide[data["direction"].upper()],
            order_status=status,
            order_type=OrderType[data["order_type"].upper()],
            order_time_in_force=tif,
            order_id=data["order_id"],
            client_order_id=data["label"],
            fee=data["commission"],
            reduce_only=data["reduce_only"],
            rejected_message=data["reject_reason"],
            place_ack_ts=data["creation_timestamp"],
            exch_update_ts=data["last_update_timestamp"],
            local_update_ts=int(time.time() * 1000),
        )
        odd.append(o)

        return odd

    def _position_handler(self, msg: dict[str, Any]) -> Positions:
        pd = Positions()
        data = msg["params"]["data"]
        for d in data.get("positions", []):
            symbol = d["instrument_name"]
            sign = {"buy": 1, "sell": -1}.get(d["direction"], 1)
            p = Position(
                exch_symbol=symbol,
                net_qty=float(d["size"]) * sign,
                entry_price=float(d["average_price"]),
                value=float(d["size"]) * float(d["average_price"]),
                unrealized_pnl=float(d["floating_profit_loss"]),
                ts=int(time.time() * 1000),
            )
            pd[symbol] = p

        return pd

    def _trade_handler(self, msg: dict[str, Any]) -> TradeData:
        td = TradeData()
        data = msg["params"]["data"]
        for d in data:
            symbol = d["instrument_name"]
            t = Trade(
                create_ts=d["timestamp"],
                side=OrderSide[d["direction"].upper()],
                trade_id=d["trade_id"],
                order_id=d["order_id"],
                last_trd_price=Decimal(d["price"]),
                last_trd_volume=Decimal(d["amount"]),
                turnover=Decimal(d["price"]) * Decimal(d["amount"]),
                fill_ts=d["timestamp"],
                fee=Decimal(d["fee"]),
                fee_ccy=d["fee_currency"],
                is_maker=d["liquidity"] == "M",
            )
            td.setdefault(symbol, []).append(t)

        return td
