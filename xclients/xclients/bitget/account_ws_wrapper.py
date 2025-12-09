from decimal import Decimal
from typing import Any
from collections import defaultdict

from loguru import logger

from ..base_wrapper import BaseAccountWssWrapper
from ..data_type import *
from ..enum_type import (
    AccountType,
    TimeInForce,
    OrderSide,
    OrderStatus,
    OrderType,
)

from .constants import STATUS_MAP, TIF_MAP, SIDE_MAP, POSMODE_MAP


class BitgetAccountWssWrapper(BaseAccountWssWrapper):
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
                    topic_list.append("orders@default")
                case Event.POSITION:
                    if self._market_type == MarketType.UPERP:
                        topic_list.append("positions@default")
                case Event.USER_TRADE:
                    topic_list.append("fill@default")

        await self._ws_client.subscribe(topic_list)

    def _is_position_message(self, msg: dict[str, Any]) -> bool:
        arg = msg.get("arg", {})
        inst_type = arg.get("instType")
        channel = arg.get("channel")

        if self._market_type == MarketType.UPERP and inst_type == "USDT-FUTURES" and channel == "positions":
            return True

        return False

    def _is_balance_message(self, msg: dict[str, Any]) -> bool:
        arg = msg.get("arg", {})
        inst_type = arg.get("instType")
        channel = arg.get("channel")

        if self._market_type == MarketType.SPOT and inst_type == "SPOT" and channel == "account":
            return True
        elif self._market_type == MarketType.UPERP and inst_type == "USDT-FUTURES" and channel == "account":
            return True

        return False

    def _is_order_message(self, msg: dict[str, Any]) -> bool:
        arg = msg.get("arg", {})
        inst_type = arg.get("instType")
        channel = arg.get("channel")

        if self._market_type == MarketType.SPOT and inst_type == "SPOT" and channel == "orders":
            return True
        elif self._market_type == MarketType.UPERP and inst_type == "USDT-FUTURES" and channel == "orders":
            return True

        return False

    def _is_trade_msg(self, msg: dict[str, Any]) -> bool:
        arg = msg.get("arg", {})
        inst_type = arg.get("instType")
        channel = arg.get("channel")

        if self._market_type == MarketType.SPOT and inst_type == "SPOT" and channel == "fill":
            return True
        elif self._market_type == MarketType.UPERP and inst_type == "USDT-FUTURES" and channel == "fill":
            return True

        return False

    def _balance_handler(self, msg: dict[str, Any]) -> Balances:
        if self._market_type == MarketType.SPOT:
            data = {
                d["coin"]: Balance(
                    asset=d["coin"],
                    balance=float(d["available"]) + float(d["frozen"]),
                    free=float(d["available"]),
                    locked=float(d["frozen"]),
                    type="full",
                    ts=int(d["uTime"]),
                )
                for d in msg["data"]
            }
        elif self._market_type == MarketType.UPERP:
            ts = int(msg["ts"])
            data = {
                d["marginCoin"]: Balance(
                    asset=d["marginCoin"],
                    balance=float(d["available"]) + float(d["frozen"]),
                    free=float(d["available"]),
                    locked=float(d["frozen"]),
                    type="full",
                    ts=ts,
                )
                for d in msg["data"]
            }
        else:
            raise NotImplementedError

        return Balances(**data)

    def _order_handler(self, msg: dict[str, Any]) -> list[OrderSnapshot]:
        data = []
        if self._market_type == MarketType.SPOT:
            for d in msg["data"]:
                tif = TIF_MAP.get(d.get("force"), TimeInForce.UNKNOWN)
                order_type = getattr(OrderType, d["orderType"].upper(), OrderType.UNKNOWN)
                status = STATUS_MAP.get(d["status"], OrderStatus.UNKNOWN)

                if feeDetail := d.get("feeDetail", []):
                    if len(feeDetail) != 1:
                        logger.warning(f"订单有多个币种的手续费: {d}")
                    fee = float(feeDetail[0]["fee"])
                    fee_ccy = feeDetail[0]["feeCoin"]
                else:
                    fee = 0.0
                    fee_ccy = ""

                side = SIDE_MAP.get(d["side"], OrderSide.UNKNOWN)
                o = OrderSnapshot(
                    exch_symbol=d["instId"],
                    price=Decimal(d.get("price", "0")),
                    qty=Decimal(d["newSize"]),
                    avg_price=float(d.get("priceAvg", "0")),
                    filled_qty=Decimal(d["accBaseVolume"]),
                    order_side=side,
                    order_status=status,
                    order_type=order_type,
                    order_time_in_force=tif,
                    order_id=str(d["orderId"]),
                    client_order_id=str(d["clientOid"]),
                    fee=fee,
                    fee_ccy=fee_ccy,
                    place_ack_ts=float(d["cTime"]),
                    exch_update_ts=float(d["uTime"]),
                    local_update_ts=float(time.time() * 1000),
                )

                data.append(o)
        elif self._market_type == MarketType.UPERP:
            for d in msg["data"]:
                tif = TIF_MAP.get(d.get("force"), TimeInForce.UNKNOWN)
                order_type = getattr(OrderType, d["orderType"].upper(), OrderType.UNKNOWN)
                status = STATUS_MAP.get(d["status"], OrderStatus.UNKNOWN)
                side = SIDE_MAP.get(d["side"], OrderSide.UNKNOWN)

                price = Decimal(d.get("price", "0"))
                filled_quantity = Decimal(d["accBaseVolume"])

                # FIXME: order msg from bitget is pretty weird here.
                # if an order was partially_filled but being cancel finally,
                # the 'priceAvg' of the last order msg would be missing
                if "priceAvg" in d:
                    filled_price = float(d["priceAvg"])
                else:
                    if filled_quantity == Decimal("0"):
                        filled_price = 0
                    else:
                        filled_price = float(price)

                if feeDetail := d.get("feeDetail", []):
                    if len(feeDetail) != 1:
                        logger.warning(f"订单有多个币种的手续费: {d}")
                    fee = float(feeDetail[0]["fee"])
                    fee_ccy = feeDetail[0]["feeCoin"]
                else:
                    fee = 0.0
                    fee_ccy = ""

                o = OrderSnapshot(
                    exch_symbol=d["instId"],
                    price=price,
                    qty=Decimal(d["size"]),
                    avg_price=filled_price,
                    filled_qty=filled_quantity,
                    order_side=side,
                    order_status=status,
                    order_type=order_type,
                    order_time_in_force=tif,
                    order_id=str(d["orderId"]),
                    client_order_id=str(d["clientOid"]),
                    fee=fee,
                    fee_ccy=fee_ccy,
                    reduce_only=False if d["reduceOnly"] == "no" else True,
                    place_ack_ts=float(d["cTime"]),
                    exch_update_ts=float(d["uTime"]),
                    local_update_ts=float(time.time() * 1000),
                )
                data.append(o)
        else:
            raise NotImplementedError

        return data

    def _position_handler(self, msg: dict[str, Any]) -> Positions:
        data = {}
        for d in msg["data"]:
            sign = {"long": 1, "short": -1}.get(d["holdSide"], 1)
            data[d["instId"]] = Position(
                exch_symbol=d["instId"],
                net_qty=float(d["total"]) * sign,
                entry_price=float(d["openPriceAvg"]),
                value=float(d["total"]) * float(d["openPriceAvg"]),
                unrealized_pnl=float(d["unrealizedPL"]),
                liq_price=float(d["liquidationPrice"]),
                ts=int(d["uTime"]),
            )

        return Positions(**data)

    def _trade_handler(self, msg: dict[str, Any]) -> TradeData:
        data = defaultdict(list)
        if self._market_type == MarketType.SPOT:
            for d in msg["data"]:
                symbol = d["symbol"]
                side = SIDE_MAP.get(d["side"], OrderSide.UNKNOWN)

                assert len(d["feeDetail"]) == 1  # TODO: is it possible to have multiple feeDetail?

                t = Trade(
                    create_ts=int(d["cTime"]),
                    side=side,
                    trade_id=d["tradeId"],
                    order_id=d["orderId"],
                    last_trd_price=Decimal(d["priceAvg"]),
                    last_trd_volume=Decimal(d["size"]),
                    turnover=Decimal(d["amount"]),
                    fill_ts=int(d["uTime"]),
                    fee=-Decimal(d["feeDetail"][0]["totalFee"]),
                    fee_ccy=d["feeDetail"][0]["feeCoin"],
                    is_maker=d["tradeScope"] == "marker",
                )

                data[symbol].append(t)
        elif self._market_type == MarketType.UPERP:
            for d in msg["data"]:
                symbol = d["symbol"]
                side = SIDE_MAP.get(d["side"], OrderSide.UNKNOWN)

                assert len(d["feeDetail"]) == 1  # TODO: is it possible to have multiple feeDetail?

                t = Trade(
                    create_ts=int(d["cTime"]),
                    side=side,
                    trade_id=d["tradeId"],
                    order_id=d["orderId"],
                    last_trd_price=Decimal(d["price"]),
                    last_trd_volume=Decimal(d["baseVolume"]),
                    turnover=Decimal(d["quoteVolume"]),
                    fill_ts=int(d["uTime"]),
                    fee=-Decimal(d["feeDetail"][0]["totalFee"]),
                    fee_ccy=d["feeDetail"][0]["feeCoin"],
                    is_maker=d["tradeScope"] == "marker",
                )

                data[symbol].append(t)
        else:
            raise NotImplementedError

        return TradeData(**data)

    async def _process_extra_message(self, msg: dict[str, Any]) -> None:
        if self._is_order_message(msg) and msg["arg"].get("instType") == "USDT-FUTURES":
            raw_orders = msg["data"]
            for raw_order in raw_orders:
                if raw_order["tradeSide"] in [
                    "dte_sys_adl_buy_in_single_side_mode",
                    "dte_sys_adl_sell_in_single_side_mode",
                ]:
                    logger.critical(f"ADL推送: {raw_order}")
                    if raw_order.get("status") == "filled":
                        symbol = raw_order["instId"]

                        qty = Decimal(raw_order["accBaseVolume"])
                        if raw_order["tradeSide"] == "dte_sys_adl_sell_in_single_side_mode" and qty > 0:
                            qty = -qty
                        await self._event_bus.publish(
                            Event.LIQUIDATION, self._account_meta, LiquidationMessage(symbol, qty)
                        )
