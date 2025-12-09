from decimal import Decimal
from typing import Any

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
from .constants import *
from websockets.exceptions import ConnectionClosedError


class BinanceAccountWssWrapper(BaseAccountWssWrapper):
    def __init__(self, account_meta: AccountMeta, account_config: AccountConfig, wss_config: WssConfig):
        super().__init__(account_meta, account_config, wss_config)

    async def subscribe_stream(self, symbols: list[str] | None = None) -> None:
        assert self._ws_client, "WebSocket client is not initialized"

    def _is_position_message(self, msg: dict[str, Any]) -> bool:
        if MarketType.UPERP == self._market_type and msg.get("e") == "ACCOUNT_UPDATE" and msg.get("a", {}).get("P"):
            return True
        return False

    def _is_balance_message(self, msg: dict[str, Any]) -> bool:
        if self._market_type in [MarketType.SPOT, MarketType.MARGIN] and msg.get("e") == "outboundAccountPosition":
            return True
        if self._market_type.is_derivative and msg.get("e") == "ACCOUNT_UPDATE" and msg.get("a", {}).get("B"):
            if self._account_meta.account_type != AccountType.UNIFIED:
                return True
            if self._market_type in [MarketType.UPERP, MarketType.UDELIVERY] and msg.get("fs") == "UM":
                return True
            elif self._market_type in [MarketType.CPERP, MarketType.CDELIVERY] and msg.get("fs") == "CM":
                return True
            return True
        return False

    def _is_order_message(self, msg: dict[str, Any]) -> bool:
        if self._market_type.is_derivative and msg["e"] == "ORDER_TRADE_UPDATE":
            if self._account_meta.account_type != AccountType.UNIFIED:
                return True
            if self._market_type in [MarketType.UPERP, MarketType.UDELIVERY] and msg.get("fs") == "UM":
                return True
            elif self._market_type in [MarketType.CPERP, MarketType.CDELIVERY] and msg.get("fs") == "CM":
                return True

        if self._market_type in [MarketType.SPOT, MarketType.MARGIN] and msg["e"] == "executionReport":
            return True

        return False

    def _is_user_trade_message(self, msg: dict[str, Any]) -> bool:
        if (
            self._market_type.is_derivative
            and msg["e"] == "ORDER_TRADE_UPDATE"
            and msg["o"]
            and msg["o"]["x"] == "TRADE"
        ):
            if self._account_meta.account_type != AccountType.UNIFIED:
                return True
            if self._market_type in [MarketType.UPERP, MarketType.UDELIVERY] and msg.get("fs") == "UM":
                return True
            elif self._market_type in [MarketType.CPERP, MarketType.CDELIVERY] and msg.get("fs") == "CM":
                return True
        if (
            self._market_type in [MarketType.SPOT, MarketType.MARGIN]
            and msg["e"] == "executionReport"
            and msg["x"] == "TRADE"
        ):
            return True
        return False

    def _balance_handler(self, msg: dict[str, Any]) -> Balances:
        if self._market_type.is_derivative:
            update_ts = msg["T"]
            data = {
                i["a"]: Balance(asset=i["a"], balance=float(i["bc"]), free=float(i["bc"]), type="delta", ts=update_ts)
                for i in msg["a"]["B"]
            }

        elif self._market_type in [MarketType.SPOT, MarketType.MARGIN]:
            update_ts = msg["E"]
            data = {
                i["a"]: Balance(
                    asset=i["a"],
                    balance=float(i["f"]) + float(i["l"]),
                    free=float(i["f"]),
                    locked=float(i["l"]),
                    type="full",
                    ts=update_ts,
                )
                for i in msg["B"]
            }
        else:
            raise NotImplementedError
        return Balances(**data)

    def _order_handler(self, msg: dict[str, Any]) -> list[OrderSnapshot]:
        if self._market_type.is_derivative:
            # u_time = msg["T"]  # check
            msg = msg["o"]

            order_type = getattr(OrderType, msg["o"], OrderType.UNKNOWN)
            tif = TIF_MAP.get(msg["f"], TimeInForce.UNKNOWN)
            status = STATUS_MAP.get(msg["X"], OrderStatus.UNKNOWN)
            side = getattr(OrderSide, msg["S"], OrderSide.UNKNOWN)

            o = OrderSnapshot(
                exch_symbol=msg["s"],
                price=Decimal(msg["p"]),
                qty=Decimal(msg["q"]),
                avg_price=float(msg["ap"]),
                filled_qty=Decimal(msg["z"]),
                order_side=side,
                order_status=status,
                order_type=order_type,
                order_time_in_force=tif,
                order_id=str(msg["i"]),
                fee=float(msg["n"]),
                fee_ccy=msg.get("N", ""),
                client_order_id=str(msg["c"]),
                reduce_only=msg["R"],
                rejected_message=msg.get("r", ""),
                place_ack_ts=float(msg["T"]),
                exch_update_ts=float(msg["T"]),
                local_update_ts=float(time.time() * 1000),
            )
        elif self._market_type in [MarketType.SPOT, MarketType.MARGIN]:
            _filled_vol = Decimal(msg["Z"])
            _filled_quantity = Decimal(msg["z"])
            _filled_price = _filled_vol / _filled_quantity if _filled_quantity != Decimal("0") else Decimal("0")

            order_type = getattr(OrderType, msg["o"], OrderType.UNKNOWN)
            tif = TIF_MAP.get(msg["f"], TimeInForce.UNKNOWN)
            status = STATUS_MAP.get(msg["X"], OrderStatus.UNKNOWN)
            side = getattr(OrderSide, msg["S"], OrderSide.UNKNOWN)

            o = OrderSnapshot(
                exch_symbol=msg["s"],
                price=Decimal(msg["p"]),
                qty=Decimal(msg["q"]),
                avg_price=float(_filled_price),
                filled_qty=_filled_quantity,
                order_side=side,
                order_status=status,
                order_type=order_type,
                order_time_in_force=tif,
                order_id=str(msg["i"]),
                fee=float(msg["n"]),
                fee_ccy=msg.get("N", ""),
                client_order_id=str(msg["c"]),
                rejected_message=msg.get("r", ""),
                place_ack_ts=float(msg["O"]),
                exch_update_ts=float(msg["E"]),
                local_update_ts=float(time.time() * 1000),
            )
        else:
            raise NotImplementedError

        return [o]

    def _position_handler(self, msg: dict[str, Any]) -> Positions:
        data = {}
        for i in msg["a"]["P"]:
            if float(i["pa"]) != 0:
                sign = {"LONG": 1, "SHORT": -1}.get(i["ps"], 1)
                data[i["s"]] = Position(
                    exch_symbol=i["s"],
                    net_qty=float(i["pa"]) * sign,
                    entry_price=float(i["ep"]),
                    value=float(i["pa"]) * float(i["ep"]),
                    unrealized_pnl=float(i["up"]),
                    liq_price=float(i.get("li", 0)),
                )

        return Positions(**data)

    def _trade_handler(self, msg: dict[str, Any]) -> TradeData:
        if self._market_type.is_derivative:
            c_time = msg["T"]
            u_time = msg["E"]
            msg = msg["o"]

            side = getattr(OrderSide, msg["S"], OrderSide.UNKNOWN)

            t = Trade(
                create_ts=int(c_time),
                side=side,
                trade_id=str(msg["t"]),
                order_id=str(msg["i"]),
                last_trd_price=Decimal(msg["L"]),
                last_trd_volume=Decimal(msg["l"]),
                turnover=Decimal(msg["L"]) * Decimal(msg["l"]),
                fill_ts=int(u_time),
                fee=Decimal(msg["n"]),
                fee_ccy=msg["N"],
                is_maker=msg["m"],
            )
        elif self._market_type in [MarketType.SPOT, MarketType.MARGIN]:
            side = getattr(OrderSide, msg["S"], OrderSide.UNKNOWN)

            t = Trade(
                create_ts=int(msg["T"]),
                side=side,
                trade_id=str(msg["t"]),
                order_id=str(msg["i"]),
                last_trd_price=Decimal(msg["L"]),
                last_trd_volume=Decimal(msg["l"]),
                turnover=Decimal(msg["L"]) * Decimal(msg["l"]),
                fill_ts=int(msg["E"]),
                fee=Decimal(msg["n"]),
                fee_ccy=msg["N"],
                is_maker=msg["m"],
            )
        else:
            raise NotImplementedError
        return TradeData({msg["s"]: [t]})

    async def _process_extra_message(self, msg: dict[str, Any]) -> None:
        if msg["e"] == "listenKeyExpired":
            logger.info("ListenKey过期")
            raise ConnectionClosedError(None, None)
        # - 追加保证金
        elif msg["e"] == "MARGIN_CALL":
            logger.critical("追加保证金通知")
        # - ADL 处理
        # 推送例子（会推送一条 X=NEW 和一条 X=FILLED, 因为BN ADL订单是一把梭，所以只需要处理最后的filled推送）
        # {'e': 'ORDER_TRADE_UPDATE', 'T': 1743503535616, 'E': 1743503535652, 'fs': 'UM', 'o': {'s': 'ACTUSDT', 'c': 'adl_autoclose', 'S': 'BUY', 'o': 'LIMIT', 'f': 'IOC', 'q': '1162327', 'p': '0.1094110', 'ap': '0', 'sp': '0', 'x': 'NEW', 'X': 'NEW', 'i': 2514755802, 'l': '0', 'z': '0', 'L': '0', 'n': '0', 'N': 'USDT', 'T': 1743503535616, 't': 0, 'b': '0', 'a': '0', 'm': False, 'R': False, 'ps': 'BOTH', 'rp': '0', 'V': 'EXPIRE_NONE', 'pm': 'PM_NONE', 'gtd': 0}}
        # {'e': 'ORDER_TRADE_UPDATE', 'T': 1743503535616, 'E': 1743503535652, 'fs': 'UM', 'o': {'s': 'ACTUSDT', 'c': 'adl_autoclose', 'S': 'BUY', 'o': 'LIMIT', 'f': 'IOC', 'q': '1162327', 'p': '0.1094110', 'ap': '0.1094110', 'sp': '0', 'x': 'CALCULATED', 'X': 'FILLED', 'i': 2514755802, 'l': '1162327', 'z': '1162327', 'L': '0.1094110', 'n': '0', 'N': 'USDT', 'T': 1743503535616, 't': 216328382, 'b': '0', 'a': '0', 'm': True, 'R': False, 'ps': 'BOTH', 'rp': '139900.39541069', 'V': 'EXPIRE_NONE', 'pm': 'PM_NONE', 'gtd': 0}}
        elif msg["e"] == "ORDER_TRADE_UPDATE":
            raw_order = msg["o"]
            if raw_order["c"] == "adl_autoclose":
                logger.critical(f"ADL推送: {msg}")
                if raw_order.get("X") == "FILLED":
                    exch_symbol = raw_order["s"]
                    qty = Decimal(raw_order["z"])
                    if raw_order["S"] == "SELL" and qty > 0:
                        qty = -qty
                    await self._event_bus.publish(
                        Event.LIQUIDATION, self._account_meta, LiquidationMessage(exch_symbol, qty)
                    )
