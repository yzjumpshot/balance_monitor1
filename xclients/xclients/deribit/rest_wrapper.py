import time
import asyncio
from decimal import Decimal
from datetime import datetime, timedelta
from dateutil import parser
from typing import Any, Callable, Literal, Optional, Union

from ..base_wrapper import BaseRestWrapper, BaseWssWrapper, catch_it
from ..data_type import *
from ..enum_type import (
    AccountType,
    ExchangeName,
    TimeInForce,
    Event,
    OrderSide,
    OrderStatus,
    OrderType,
)
from .rest import DeribitRestClient
from .constants import STATUS_MAP, TIF_MAP
import ccxt.async_support as ccxt
from ccxt.base.types import Order as ccxtOrder, ConstructorArgs
from loguru import logger


class DeribitRestWrapper(BaseRestWrapper):
    client: DeribitRestClient

    def __init__(
        self,
        account_meta: AccountMeta,
        account_config: AccountConfig,
        rest_config: RestConfig,
    ) -> None:
        super().__init__(account_meta, account_config, rest_config)
        self.init_ccxt_client()

    def init_ccxt_client(self):
        ccxt_default_type = "spot"
        match self._account_meta.market_type:
            case MarketType.SPOT:
                ccxt_default_type = "spot"
            case MarketType.UPERP:
                ccxt_default_type = "swap"
            case MarketType.CPERP:
                ccxt_default_type = "swap"
            case MarketType.CDELIVERY:
                ccxt_default_type = "future"
            case MarketType.UDELIVERY:
                ccxt_default_type = "future"

        ccxt_params = {
            "apiKey": self._account_config.api_key,
            "secret": self._account_config.secret_key,
            "enableRateLimit": True,
            "options": {
                "defaultType": ccxt_default_type,
            },
        }
        self.ccxt_client = ccxt.deribit(ConstructorArgs(ccxt_params))

    def judge_symbol_market_type(self, symbol: str):
        if symbol.find("-") == -1:
            return MarketType.SPOT
        elif symbol.endswith("-PERPETUAL"):
            prefix = symbol.removesuffix("-PERPETUAL")
            if len(prefix.split("_")) == 2:
                return MarketType.UPERP
            else:
                return MarketType.CPERP
        else:
            return MarketType.CDELIVERY

    @catch_it
    async def get_positions(self, from_redis: bool = False):
        pd = Positions()
        if self._market_type in [MarketType.SPOT, MarketType.MARGIN]:
            kind = "spot"
        elif self._market_type in [MarketType.UPERP, MarketType.UDELIVERY, MarketType.CPERP, MarketType.CDELIVERY]:
            kind = "future"
        else:
            raise ValueError(f"unsupported MarketType: {self._market_type}")

        if from_redis:
            suffix = "raw:test"
            key = f"{kind}_position"
            data = await self._load_data_from_rmx_acc(suffix, key)
            for symbol, symbol_data in data.items():
                if symbol_data["size"] == 0 or self.judge_symbol_market_type(symbol) != self._market_type:
                    continue

                sign = {"buy": 1, "sell": -1}.get(symbol_data["direction"], 1)
                pd[symbol] = Position(
                    exch_symbol=symbol,
                    net_qty=symbol_data["size"] * sign,
                    entry_price=symbol_data["average_price"],
                    value=symbol_data["size"] * symbol_data["mark_price"],
                    liq_price=symbol_data["estimated_liquidation_price"],
                    unrealized_pnl=float(str(symbol_data["floating_profit_loss"])),
                )
        else:
            raw_data = await self.client.get_positions(kind=kind)
            if raw_data is None:
                raise ValueError("fail to get positions")

            for d in raw_data["result"]:
                symbol = d["instrument_name"]
                if d["size_currency"] == 0 or self.judge_symbol_market_type(symbol) != self._market_type:
                    continue

                sign = {"buy": 1, "sell": -1}.get(d["direction"], 1)
                p = Position(
                    exch_symbol=symbol,
                    net_qty=d["size_currency"] * sign,
                    entry_price=d["average_price"],
                    value=d["size"] * d["mark_price"],
                    liq_price=d["estimated_liquidation_price"],
                    unrealized_pnl=float(str(d["floating_profit_loss"])),
                )
                pd[symbol] = p

        return pd

    @catch_it
    async def get_assets(self, from_redis: bool = False):
        ad = Balances()
        if from_redis:
            suffix = "raw:test"
            key = "account_info"
            data = await self._load_data_from_rmx_acc(suffix, key)
            for ccy, info in data.items():
                if info["balance"] <= 0:
                    continue

                ad[ccy] = Balance(
                    asset=ccy,
                    balance=info["balance"],
                    free=info["available_withdrawal_funds"],
                    locked=info["balance"] - info["available_withdrawal_funds"],
                    ts=int(time.time() * 1000),
                    type="full",
                )
        else:
            raw_data = await self.client.get_account_summaries()
            if raw_data is None:
                raise ValueError("fail to get assets")

            for d in raw_data["result"]["summaries"]:
                if d["balance"] <= 0:
                    continue

                ccy = d["currency"].lower()
                asset = Balance(
                    asset=ccy,
                    balance=d["balance"],
                    free=d["available_withdrawal_funds"],
                    locked=d["balance"] - d["available_withdrawal_funds"],
                    ts=int(time.time() * 1000),
                    type="full",
                )
                ad[ccy] = asset

        return ad

    @catch_it
    async def get_equity(self) -> float:
        asset_total = 0
        raw_data = await self.client.get_account_summaries()
        if raw_data is None:
            raise ValueError("failed to get equity")

        for d in raw_data["result"]["summaries"]:
            if d["equity"] <= 0:
                continue

            ccy = d["currency"]
            if ccy.lower() == "usdt":
                price = 1
            else:
                price_resp = await self.get_price(f"{ccy.upper()}_USDT")
                if price_resp["status"] != 0:
                    raise ValueError(price_resp["msg"])
                price = price_resp["data"]
            asset_total += float(d["equity"]) * price

        return asset_total

    # @catch_it
    # async def repay(self, asset: str, amount: Decimal, isolated_symbol: Optional[str] = None) -> bool:
    #     if MarketType.MARGIN != self._market_type:
    #         raise ValueError(f"Market type {self._market_type} is not supported(only supported for MARGIN)")
    #     if isolated_symbol:
    #         resp = await self.client.sapi_margin_repay(asset, str(amount), isolated="TRUE", symbol=isolated_symbol)
    #     else:
    #         resp = await self.client.sapi_margin_repay(asset, str(amount))
    #     if resp.get("code"):
    #         raise ValueError(resp["msg"])
    #     else:
    #         return True

    @catch_it
    async def get_price(self, symbol: str, from_redis: bool = False) -> float:
        resp = await self.client.get_book_summary_by_instrument(symbol)
        if isinstance(resp, dict) and resp.get("result"):
            return float(str(resp["result"][0]["mark_price"]))
        elif resp:
            raise ValueError(resp.get("msg"))
        else:
            raise ValueError("fail to get price")

    @catch_it
    async def get_prices(self) -> Prices:
        if MarketType.SPOT == self._market_type:
            resp = await self.client.get_book_summary_by_currency("USDT", "spot")
            if resp is None:
                raise ValueError("failed to get prices")

            return Prices({item["instrument_name"]: Decimal(str(item["mark_price"])) for item in resp["result"]})
        elif MarketType.UPERP == self._market_type:
            resp = await self.client.get_book_summary_by_currency("USDT", "future")
            if resp is None:
                raise ValueError("failed to get prices")

            return Prices({item["instrument_name"]: Decimal(str(item["mark_price"])) for item in resp["result"]})
        else:
            btc_resp = await self.client.get_book_summary_by_currency("BTC", "future")
            if btc_resp is None:
                raise ValueError("failed to get prices")

            eth_resp = await self.client.get_book_summary_by_currency("ETH", "future")
            if eth_resp is None:
                raise ValueError("failed to get prices")

            result = {
                item["instrument_name"]: Decimal(str(item["mark_price"]))
                for item in btc_resp["result"]
                if self.judge_symbol_market_type(item["instrument_name"]) == self._market_type
            }
            result.update(
                {
                    item["instrument_name"]: Decimal(str(item["mark_price"]))
                    for item in eth_resp["result"]
                    if self.judge_symbol_market_type(item["instrument_name"]) == self._market_type
                }
            )
            return Prices(result)

    @catch_it
    async def get_trade_history(self, start_time: int, end_time: int, symbol_list: list[str]):
        result: dict[str, list[Trade]] = {}
        trade_data_list = []
        for symbol in symbol_list:
            while True:
                resp = await self.client.get_user_trades_by_instrument_and_time(
                    symbol,
                    start_time,
                    end_time,
                    count=300,
                    sorting="asc",
                )
                if resp is None:
                    raise ValueError("failed to get trade history")

                if trades := resp["result"].get("trades", []):
                    trade_data_list += trades
                    start_time = trades[-1]["timestamp"] + 1

                if not resp["result"].get("has_more", False):
                    break

        for data in trade_data_list:
            result.setdefault(data["instrument_name"], []).append(
                Trade(
                    create_ts=int(data["timestamp"]),
                    side=getattr(OrderSide, data["direction"].upper(), OrderSide.UNKNOWN),
                    trade_id=str(data["trade_id"]),
                    order_id=str(data["order_id"]),
                    last_trd_price=Decimal(str(data["price"])),
                    last_trd_volume=Decimal(str(data["amount"])),
                    turnover=Decimal(str(data["price"])) * Decimal(str(data["amount"])),
                    fill_ts=int(data["timestamp"]),
                    fee=Decimal(str(data["fee"])),
                    fee_ccy=data["fee_currency"],
                    is_maker=data["liquidity"] == "M",
                )
            )

        return TradeData(result)

    @catch_it
    async def place_order(
        self,
        symbol: str,
        order_side: Literal["BUY", "SELL"] | OrderSide,
        qty: Optional[Decimal] = None,
        price: Optional[Decimal] = None,
        order_type: Literal["LIMIT", "MARKET"] | OrderType = OrderType.LIMIT,
        order_time_in_force: Literal["GTC", "IOC", "FOK", "GTX"] | TimeInForce | None = None,
        client_order_id: Optional[str] = None,
        reduce_only: Optional[bool] = None,
        extras: Optional[dict[str, Any]] = None,
    ) -> OrderSnapshot:
        if isinstance(order_time_in_force, str):
            order_time_in_force = TimeInForce[order_time_in_force]
        if isinstance(order_side, str):
            order_side = OrderSide[order_side]
        if isinstance(order_type, str):
            order_type = OrderType[order_type]
        if not client_order_id:
            client_order_id = "xclients" + str(int(time.time() * 1000000))
        send_order_type = "limit" if order_type == OrderType.LIMIT else "market"
        post_only = False
        reject_post_only = False
        send_time_in_force = None if order_type == OrderType.MARKET else "good_til_cancelled"
        if order_time_in_force:
            if TimeInForce.GTX == order_time_in_force:
                send_order_type = "limit"
                post_only = True
                reject_post_only = True
            else:
                send_time_in_force = {v: k for k, v in TIF_MAP.items()}.get(order_time_in_force)

        if order_side == OrderSide.BUY:
            func = self.client.buy
        else:
            func = self.client.sell

        params = extras or {}
        quote_qty = params.pop("quote_qty", None)

        resp = await func(
            instrument_name=symbol,
            amount=qty,
            type=send_order_type,
            label=client_order_id,
            price=price,
            time_in_force=send_time_in_force,  # type: ignore
            post_only=post_only,
            reject_post_only=reject_post_only,
            reduce_only=reduce_only,
            **(params or {}),
        )
        snapshot = OrderSnapshot(
            exch_symbol=symbol,
            client_order_id=client_order_id,
            order_side=order_side,
            order_type=order_type,
            order_time_in_force=order_time_in_force or TimeInForce.UNKNOWN,
            price=price or Decimal(0),
            qty=qty or Decimal(0),
            local_update_ts=int(time.time() * 1000),
        )
        if resp is None:
            snapshot.order_status = OrderStatus.REJECTED
        elif error := resp.get("error"):
            snapshot.order_status = OrderStatus.REJECTED
            snapshot.rejected_message = error["message"]
        else:
            snapshot.order_id = str(resp["result"]["order"]["order_id"])
            snapshot.order_status = OrderStatus.LIVE
            snapshot.avg_price = resp["result"]["order"]["average_price"]
            snapshot.filled_qty = resp["result"]["order"]["filled_amount"]
            snapshot.exch_update_ts = resp["result"]["order"]["last_update_timestamp"]
            snapshot.place_ack_ts = snapshot.local_update_ts
        return snapshot

    @catch_it
    async def ccxt_place_order(
        self,
        symbol: str,
        order_side: Literal["BUY", "SELL"] | OrderSide,
        qty: Decimal,
        price: Optional[Decimal] = None,
        order_type: Literal["LIMIT", "MARKET"] | OrderType = OrderType.LIMIT,
        order_time_in_force: Literal["GTC", "IOC", "FOK", "GTX"] | TimeInForce | None = None,
        client_order_id: Optional[str] = None,
        reduce_only: Optional[bool] = None,
        extras: Optional[dict[str, Any]] = None,
    ) -> OrderSnapshot:
        params = extras or {}
        if isinstance(order_side, str):
            order_side = OrderSide[order_side]
        if isinstance(order_type, str):
            order_type = OrderType[order_type]
        if not client_order_id:
            client_order_id = "xclients" + str(int(time.time() * 1000000))
        params["label"] = client_order_id

        if order_time_in_force:
            if isinstance(order_time_in_force, str):
                order_time_in_force = TimeInForce[order_time_in_force]
            params["timeInForce"] = order_time_in_force.ccxt

        if reduce_only:
            params["reduce_only"] = reduce_only

        try:
            order_resp: ccxtOrder = await self.ccxt_client.create_order(
                symbol,
                order_type.ccxt,
                order_side.ccxt,
                float(qty),
                float(price) if price else None,
                params=params,
            )
            order_snapshot = OrderSnapshot.from_ccxt_order(order_resp, symbol)
            order_snapshot.client_order_id = order_resp["info"].get("label", client_order_id)
        except Exception as e:
            logger.error(f"Failed to place order: {e}")
            order_snapshot = OrderSnapshot(
                order_id="",
                client_order_id=client_order_id,
                exch_symbol=symbol,
                order_side=order_side,
                order_type=order_type,
                order_time_in_force=order_time_in_force or TimeInForce.UNKNOWN,
                price=price or Decimal(0),
                qty=qty,
                local_update_ts=int(time.time() * 1000),
                order_status=OrderStatus.REJECTED,
                rejected_message=str(e),
            )
        return order_snapshot

    @catch_it
    async def ccxt_cancel_order(
        self, symbol: str, order_id: Optional[str] = None, client_order_id: Optional[str] = None
    ) -> OrderSnapshot | None:
        if not order_id and not client_order_id:
            raise ValueError("Either `order_id` or `client_order_id` must be provided")

        params: dict[str, Any] = {}
        if client_order_id:
            params["label"] = client_order_id

        try:
            order_resp = await self.ccxt_client.cancel_order(order_id or "", symbol, params=params)
            order_snapshot = OrderSnapshot.from_ccxt_order(order_resp, symbol)
            order_snapshot.client_order_id = order_resp["info"].get("label", client_order_id)
            if order_snapshot.order_id == order_id or order_snapshot.client_order_id == client_order_id:
                return order_snapshot
            else:
                return None
        except Exception as e:
            logger.error(f"Failed to cancel order: {e}")
            return None

    @catch_it
    async def ccxt_cancel_all(self, symbol: str) -> bool:
        params: dict[str, Any] = {}

        try:
            await self.ccxt_client.cancel_all_orders(symbol, params=params)
            return True
        except Exception as e:
            logger.error(f"Failed to cancel all orders: {e}")
            return False

    @catch_it
    async def ccxt_sync_open_orders(self, symbol: str) -> list[OrderSnapshot]:
        params: dict[str, Any] = {}

        try:
            order_resp = await self.ccxt_client.fetch_open_orders(symbol, params=params)
            order_list = [OrderSnapshot.from_ccxt_order(order, symbol) for order in order_resp]
            return order_list
        except Exception as e:
            logger.error(f"Failed to fetch open orders: {e}")
            return []

    @catch_it
    async def ccxt_sync_order(
        self, symbol: str, order_id: str | None = None, client_order_id: str | None = None
    ) -> OrderSnapshot:
        if not order_id and not client_order_id:
            raise ValueError("Either `order_id` or `client_order_id` must be provided")

        try:
            params: dict[str, Any] = {}
            if client_order_id:
                params["label"] = client_order_id

            order_resp = await self.ccxt_client.fetch_order(order_id or "", symbol, params=params)
            order_snapshot = OrderSnapshot.from_ccxt_order(order_resp, symbol)
            order_snapshot.client_order_id = order_resp["info"].get("label", client_order_id)
            return order_snapshot
        except Exception as e:
            logger.error(f"Failed to fetch order: {e}")
            return OrderSnapshot(
                order_id=order_id or "",
                client_order_id=client_order_id or "",
                exch_symbol=symbol,
                local_update_ts=int(time.time() * 1000),
                order_status=OrderStatus.ORDER_NOT_FOUND,
            )

    @catch_it
    async def get_historical_funding_rate(
        self,
        symbol_list: list[str],
        start_time: datetime | str | int | None = None,
        days: int = 7,
    ) -> FundingRatesHis:
        assert self._market_type in (MarketType.UPERP, MarketType.CPERP), f"Invalid Market type {self._market_type}"

        if isinstance(start_time, int):
            start_ts = start_time
        else:
            if start_time is None:
                start_time = datetime.now() - timedelta(days=days)
            elif isinstance(start_time, str):
                start_time = parser.parse(start_time)
            start_ts = int(start_time.timestamp() * 1000)

        end_ts = int(datetime.now().timestamp() * 1000)
        frs: dict[str, set[FundingRateSimple]] = {}
        if not symbol_list:
            symbol_list = []
            resp = await self.client.get_instrument_info(currency="any", kind="future")
            if resp is None:
                raise ValueError("failed to get historical funding rate")

            for info in resp["result"]:
                if info["quote_currency"] not in ("USDT", "USDC", "USD"):
                    continue

                match self._market_type:
                    case MarketType.UPERP:
                        if info["instrument_type"] != "linear" or info.get("settlement_period") != "perpetual":
                            continue
                    case MarketType.CPERP:
                        if info["instrument_type"] != "reversed" or info.get("settlement_period") != "perpetual":
                            continue

                symbol_list.append(info["instrument_name"])

        for symbol in symbol_list:
            data_set: set[FundingRateSimple] = set()
            _end_ts = end_ts
            for _ in range(1000):
                resp = await self.client.get_funding_rate_history(symbol, start_ts, _end_ts)
                await asyncio.sleep(0.1)
                if resp is None:
                    continue

                datas = resp["result"] or []
                for d in datas:
                    ts = float(d["timestamp"])
                    fr = float(d["interest_1h"])

                    if not (start_ts <= ts <= end_ts):
                        continue

                    data_set.add(FundingRateSimple(funding_rate=fr, funding_ts=ts))

                if not datas:
                    break

                _end_ts = int(min(d.funding_ts for d in data_set) - 1)

                if _end_ts <= start_ts:
                    break

            frs[symbol] = data_set
        return FundingRatesHis({symbol: sorted(list(fr)) for symbol, fr in frs.items()})
