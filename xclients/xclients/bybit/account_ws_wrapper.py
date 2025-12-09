from decimal import Decimal
from typing import Any
from ..base_wrapper import BaseAccountWssWrapper
from ..enum_type import (
    Event,
    TimeInForce,
    OrderSide,
    OrderStatus,
    OrderType,
)
from ..data_type import *
from .constants import *
from loguru import logger


class BybitAccountWssWrapper(BaseAccountWssWrapper):
    def __init__(self, account_meta: AccountMeta, account_config: AccountConfig, wss_config: WssConfig):
        super().__init__(account_meta, account_config, wss_config)

    async def subscribe_stream(self, symbols: list[str] | None = None) -> None:
        assert self._ws_client, "WebSocket client is not initialized"

        topic_list = []
        for event in self.registered_events:
            match event:
                case Event.BALANCE:
                    topic_list.append("wallet")
                case Event.ORDER:
                    if self._market_type in [MarketType.SPOT, MarketType.MARGIN]:
                        topic_list.append("order.spot")
                    elif self._market_type == MarketType.UPERP:
                        topic_list.append("order.linear")
                    elif self._market_type == MarketType.CPERP:
                        topic_list.append("order.inverse")
                case Event.POSITION:
                    if self._market_type == MarketType.UPERP:
                        topic_list.append("position.linear")
                    elif self._market_type == MarketType.CPERP:
                        topic_list.append("position.inverse")
                case Event.USER_TRADE:
                    if self._market_type in [MarketType.SPOT, MarketType.MARGIN]:
                        topic_list.append("execution.spot")
                    elif self._market_type == MarketType.UPERP:
                        topic_list.append("execution.linear")
                    elif self._market_type == MarketType.CPERP:
                        topic_list.append("execution.inverse")

        await self._ws_client.subscribe(topic_list)

    def _is_balance_message(self, msg: dict[str, Any]) -> bool:
        if msg.get("topic", "") == "wallet" and msg.get("data"):
            market_types = {
                "SPOT": [MarketType.SPOT, MarketType.MARGIN],
                "CONTRACT": [MarketType.UPERP, MarketType.CPERP, MarketType.UDELIVERY, MarketType.CDELIVERY],
                "UNIFIED": [
                    MarketType.SPOT,
                    MarketType.MARGIN,
                    MarketType.UPERP,
                    MarketType.CPERP,
                    MarketType.UDELIVERY,
                    MarketType.CDELIVERY,
                ],
            }.get(msg["data"][0]["accountType"], [])
            return self._market_type in market_types
        return False

    def _balance_handler(self, msg: dict[str, Any]) -> Balances:
        data = msg["data"]
        update_ts = msg["creationTime"]
        asset_data = Balances()

        # TODO: assert here for case of multi account, should fix this latter
        assert len(data) == 1, "uncovered situation for account callback"

        for info in data[0]["coin"]:
            balance = float(info["equity"])
            locked = float(info["locked"])
            borrowed = float(info["borrowAmount"])
            free = balance - locked + borrowed

            asset_data[info["coin"]] = Balance(
                asset=info["coin"],
                balance=balance,
                free=free,
                locked=locked,
                borrowed=borrowed,
                type="full",
                ts=update_ts,
            )

        return Balances(asset_data)

    def _is_order_message(self, msg: dict[str, Any]) -> bool:
        if msg.get("topic", "").startswith("order.") and msg.get("data"):
            return True
        return False

    def _order_handler(self, msg: dict[str, Any]) -> list[OrderSnapshot]:
        odd = []
        for d in msg["data"]:
            _filled_vol = Decimal(d["cumExecValue"])
            _filled_quantity = Decimal(d["cumExecQty"])
            _filled_price = _filled_vol / _filled_quantity if _filled_quantity != Decimal("0") else Decimal("0")

            order_type = getattr(OrderType, d["orderType"].upper(), OrderType.UNKNOWN)
            tif = TIF_MAP.get(d["timeInForce"], TimeInForce.UNKNOWN)
            status = STATUS_MAP.get(d["orderStatus"], OrderStatus.UNKNOWN)
            side = getattr(OrderSide, d["side"].upper(), OrderSide.UNKNOWN)

            o = OrderSnapshot(
                exch_symbol=d["symbol"],
                price=Decimal(d["price"]),
                qty=Decimal(d["qty"]),
                avg_price=float(_filled_price),
                filled_qty=_filled_quantity,
                order_side=side,
                order_status=status,
                order_type=order_type,
                order_time_in_force=tif,
                order_id=d["orderId"],
                client_order_id=d["orderLinkId"],
                fee=float(d["cumExecFee"]),
                fee_ccy="",
                reduce_only=d["reduceOnly"],
                rejected_message=d["rejectReason"],
                place_ack_ts=float(d["createdTime"]),
                exch_update_ts=float(d["updatedTime"]),
                local_update_ts=float(time.time() * 1000),
            )
            odd.append(o)

        return odd

    def _is_position_message(self, msg: dict[str, Any]) -> bool:
        if msg.get("topic", "").startswith("position") and msg.get("data"):
            return True
        return False

    def _position_handler(self, msg: dict[str, Any]) -> Positions:
        pd = Positions()
        for d in msg["data"]:
            if float(d["size"]) != 0:
                sign = {"Buy": 1, "Sell": -1}.get(d["side"], 1)
                pd[d["symbol"]] = Position(
                    exch_symbol=d["symbol"],
                    net_qty=float(d["size"]) * sign,
                    entry_price=float(d["entryPrice"]),
                    value=float(d["positionValue"]),
                    liq_price=float(d["liqPrice"]) if d["liqPrice"] else 0.0,
                    unrealized_pnl=float(d["unrealisedPnl"]),
                    ts=int(d["updatedTime"]),
                )
        return pd

    def _is_user_trade_message(self, msg: dict[str, Any]) -> bool:
        if msg.get("topic", "").startswith("execution") and msg.get("data"):
            return True
        return False

    def _trade_handler(self, msg: dict[str, Any]) -> TradeData:
        td = TradeData()

        for d in msg["data"]:
            td[d["symbol"]] = [
                Trade(
                    create_ts=0,
                    side=getattr(OrderSide, d["side"].upper(), OrderSide.UNKNOWN),
                    trade_id=str(d["execId"]),
                    order_id=str(d["orderId"]),
                    last_trd_price=Decimal(d["execPrice"]),
                    last_trd_volume=Decimal(d["execQty"]),
                    turnover=Decimal(d["execPrice"]) * Decimal(d["execQty"]),
                    fill_ts=int(d["execTime"]),
                    fee=Decimal(d["execFee"]),
                    fee_ccy="",
                    is_maker=d["isMaker"],
                )
            ]
        return td

    async def _process_extra_message(self, msg: dict[str, Any]) -> None:
        # - ADL 处理
        # ADL推送例子: {'topic': 'order.linear', 'id': '56940315_LUCEUSDT_194308502819', 'creationTime': 1745002363414, 'data': [{'category': 'linear', 'symbol': 'LUCEUSDT', 'orderId': '5803db5e-104d-4326-8b90-8706f05040ec', 'orderLinkId': '', 'blockTradeId': '', 'side': 'Buy', 'positionIdx': 0, 'orderStatus': 'Filled', 'cancelType': 'UNKNOWN', 'rejectReason': 'EC_NoError', 'timeInForce': 'GTC', 'isLeverage': '', 'price': '0.012139', 'qty': '508164', 'avgPrice': '0.012139', 'leavesQty': '0', 'leavesValue': '0', 'cumExecQty': '508164', 'cumExecValue': '6168.602796', 'cumExecFee': '0', 'orderType': 'Limit', 'stopOrderType': '', 'orderIv': '', 'triggerPrice': '', 'takeProfit': '', 'stopLoss': '', 'triggerBy': '', 'tpTriggerBy': '', 'slTriggerBy': '', 'triggerDirection': 0, 'placeType': '', 'lastPriceOnCreated': '0.00715', 'closeOnTrigger': False, 'reduceOnly': True, 'smpGroup': 0, 'smpType': 'None', 'smpOrderId': '', 'slLimitPrice': '0', 'tpLimitPrice': '0', 'tpslMode': 'UNKNOWN', 'createType': 'CreateByAdl_PassThrough', 'marketUnit': '', 'createdTime': '1745002363412', 'updatedTime': '1745002363414', 'feeCurrency': '', 'closedPnl': '4703.9573267', 'slippageTolerance': '0', 'slippageToleranceType': 'UNKNOWN'}]}
        topic = msg.get("topic", "")
        if "order" in topic:
            data = msg["data"]
            for raw_order in data:
                if "CreateByAdl" in raw_order.get("createType", ""):
                    logger.critical(f"ADL推送: {msg}")
                    # 如果Bybit ADL是一把梭, 那应该只取Filled推送来处理
                    if raw_order.get("orderStatus", "") == "Filled":
                        symbol = raw_order["symbol"]
                        qty = Decimal(raw_order["cumExecQty"])
                        if raw_order["side"] == "Sell" and qty > 0:
                            qty = -qty
                        await self._event_bus.publish(
                            Event.LIQUIDATION,
                            self._account_meta,
                            LiquidationMessage(symbol, qty),
                        )
