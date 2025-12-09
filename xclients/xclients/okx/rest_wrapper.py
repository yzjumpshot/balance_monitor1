import traceback
from decimal import Decimal
from typing import Any, Optional, Literal
import time
import asyncio
from loguru import logger
from datetime import datetime, timedelta
from dateutil import parser
from ..base_wrapper import BaseRestWrapper, catch_it
from ..enum_type import (
    OrderSide,
    TimeInForce,
    Interval,
    MarginMode,
    OrderStatus,
    OrderType,
)
from .rest import OKXRestClient
from ..data_type import *
import copy
import ccxt.async_support as ccxt
from ccxt.base.types import Order as ccxtOrder, ConstructorArgs


class OKXRestWrapper(BaseRestWrapper):
    client: OKXRestClient

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
        ccxt_default_sub_type = "linear"
        match self._account_meta.market_type:
            case MarketType.SPOT:
                ccxt_default_type = "spot"
            case MarketType.MARGIN:
                ccxt_default_type = "margin"
            case MarketType.UPERP:
                ccxt_default_type = "swap"
                ccxt_default_sub_type = "linear"
            case MarketType.CPERP:
                ccxt_default_type = "swap"
                ccxt_default_sub_type = "inverse"
            case MarketType.UDELIVERY:
                ccxt_default_type = "future"
                ccxt_default_sub_type = "linear"
            case MarketType.CDELIVERY:
                ccxt_default_type = "future"
                ccxt_default_sub_type = "inverse"

        ccxt_params = {
            "apiKey": self._account_config.api_key,
            "secret": self._account_config.secret_key,
            "password": self._account_config.passphrase,  # OKX需要passphrase
            "enableRateLimit": True,
            "options": {
                "defaultType": ccxt_default_type,
                "defaultSubType": ccxt_default_sub_type,
            },
        }
        self.ccxt_client = ccxt.okx(ConstructorArgs(ccxt_params))

    @catch_it
    async def get_loans(self):
        resp = await self.client.vip_loan_history(state="2")
        if resp is None or resp.get("code") != "0":
            raise ValueError(resp)

        loan_dict: dict[str, Loan] = {}
        for item in resp["data"]:
            if item["ccy"] not in loan_dict:
                loan_dict[item["ccy"]] = Loan(quantity=Decimal(item["dueAmt"]))
            else:
                loan_dict[item["ccy"]].quantity += Decimal(item["dueAmt"])

        return LoanData(loan_dict)

    @catch_it
    async def get_assets(self, from_redis: bool = False):
        result: dict[str, Balance] = {}
        if from_redis:
            suffix = "raw:test"
            key = "spot_balance"
            data = await self._load_data_from_rmx_acc(suffix, key)
        else:
            resp = await self.client.get_balance()
            if resp is None or resp.get("code") != "0":
                raise ValueError(resp)

            data = resp["data"]

        if not isinstance(data, list):
            raise ValueError

        for d in data:
            for info in d["details"]:
                if float(info["cashBal"]) == 0:
                    continue
                result[info["ccy"]] = Balance(
                    asset=info["ccy"],
                    balance=float(info["cashBal"]),
                    free=float(info["availBal"]),
                    locked=float(info["ordFrozen"]),
                    type="full",
                    ts=int(info["uTime"]),
                )

        return Balances(result)

    @catch_it
    async def get_positions(self, from_redis: bool = False):
        if not self._market_type.is_derivative:
            raise ValueError(f"Market type {self._market_type} is not supported(only supported for UPERP)")

        result: dict[str, Position] = {}
        data = None

        if from_redis:
            suffix = "raw:test"
            key = "swap_position"
            data = (await self._load_data_from_rmx_acc(suffix, key))["positions"]
        else:
            resp = await self.client.get_position()
            if resp is None or resp["code"] != "0":
                raise ValueError(resp)

            data = resp["data"]

        if not isinstance(data, list):
            raise ValueError(f"Could not find position from data[{data}]")

        for info in data:
            if Decimal(info["pos"]) != Decimal(0):
                sign = {"long": 1, "short": -1}.get(info["posSide"], 1)
                result[info["instId"]] = Position(
                    exch_symbol=info["instId"],
                    net_qty=float(info["pos"]) * sign,
                    entry_price=float(info["avgPx"]),
                    value=abs(float(info["pos"])) * float(info["avgPx"]),
                    unrealized_pnl=float(info["upl"]),
                    liq_price=float(info["liqPx"]) if info["liqPx"] else 0.0,
                    ts=int(info["uTime"]),
                )
        return Positions(result)

    @catch_it
    async def get_equity(self) -> float:
        resp = await self.client.get_balance()
        if resp is None or resp["code"] != "0":
            raise ValueError(resp)

        total_equity = resp["data"][0]["totalEq"]
        return float(total_equity)

    @catch_it
    async def get_discount_rate(self, ccy: str):
        resp = await self.client.get_discount_rate_interest_free_quota(ccy)
        if resp is None or resp["code"] != "0":
            raise ValueError(resp)

        discount_info = resp["data"][0]["discountInfo"]
        discount_rate = [DiscountRate(int(r["minAmt"]), Decimal(r["discountRate"])) for r in discount_info]
        discount_rate = DiscountRateData(sorted(discount_rate, key=lambda x: x.min_amt, reverse=True))
        return discount_rate

    @catch_it
    async def set_symbol_leverage(self, symbol: str, leverage: int, **kwargs) -> bool:
        if MarketType.UPERP != self._market_type:
            raise ValueError(f"Market type {self._market_type} is not supported(only supported for UPERP)")
        symbol = symbol.upper()
        if len(symbol.split("-")) != 3 or "-SWAP" not in symbol:
            raise ValueError(f"Invalid swap symbol {symbol}")
        logger.debug(f"Change leverage of {symbol} to {leverage}")
        resp = await self.client.set_leverage(leverage=str(leverage), mgn_mode="cross", inst_id=symbol)
        if resp is None or resp["code"] != "0":
            raise ValueError(resp)

        return True

    def get_exch_market_type(self) -> Literal["SPOT", "SWAP", "FUTURES", "OPTION"]:
        market_type_dict: dict[MarketType, Literal["SPOT", "SWAP", "FUTURES", "OPTION"]] = {
            MarketType.SPOT: "SPOT",
            MarketType.MARGIN: "SPOT",
            MarketType.UPERP: "SWAP",
            MarketType.UDELIVERY: "FUTURES",
            MarketType.CPERP: "SWAP",
            MarketType.CDELIVERY: "FUTURES",
        }
        return market_type_dict[self._market_type]

    @catch_it
    async def get_prices(self) -> Prices:
        if self._market_type in [MarketType.SPOT, MarketType.MARGIN]:
            resp = await self.client.get_tickers(inst_type="SPOT")
        elif self._market_type in [MarketType.UDELIVERY, MarketType.CDELIVERY]:
            resp = await self.client.get_tickers(inst_type="FUTURES")
        else:
            resp = await self.client.get_tickers(inst_type="SWAP")

        if resp is None or resp["code"] != "0":
            raise ValueError(resp)

        return Prices({item["instId"]: float(item["last"]) for item in resp["data"]})

    @catch_it
    async def get_price(self, symbol: str) -> float:
        resp = await self.client.get_ticker(symbol)
        if resp is None or resp["code"] != "0":
            raise ValueError(resp)

        return float(resp["data"][0]["last"])

    @catch_it
    async def get_trade_history(self, start_time: int, end_time: int, symbol_list: list[str]) -> TradeData:
        result: dict[str, list[Trade]] = {}
        data_list: list[Any] = []

        for symbol in symbol_list:
            after = None
            while True:
                resp = await self.client.get_trades(
                    self.get_exch_market_type(),
                    inst_id=symbol,
                    end_order_id=after,
                    start_ts=start_time,
                    end_ts=end_time,
                )
                await asyncio.sleep(0.04)
                if resp is None or resp["code"] != "0":
                    raise ValueError(resp)

                data_list.extend(resp["data"])

                if len(resp["data"]) != 100:
                    break

                after = resp["data"][-1]["ordId"]

        for data in data_list:
            result.setdefault(data["instId"], []).append(
                Trade(
                    create_ts=int(data["fillTime"]),
                    side=getattr(OrderSide, data["side"].upper(), OrderSide.UNKNOWN),
                    trade_id=data["tradeId"],
                    order_id=data["ordId"],
                    last_trd_price=Decimal(data["fillPx"]),
                    last_trd_volume=Decimal(data["fillSz"]),
                    turnover=Decimal(data["fillPx"]) * Decimal(data["fillSz"]),
                    fill_ts=int(data["fillTime"]),
                    fee=Decimal(data["fee"]),
                    fee_ccy=data["feeCcy"],
                    is_maker=True if data["execType"] == "M" else False,
                )
            )
        return TradeData(result)

    @catch_it
    async def get_order_history(self, start_time: int, end_time: int, symbol_list: list[str]) -> OrderSnapshotData:
        order_dict: dict[str, list[OrderSnapshot]] = {}
        order_data_list = []

        for symbol in symbol_list:
            after = None
            while True:
                resp = await self.client.get_order_history(
                    self.get_exch_market_type(),
                    inst_id=symbol,
                    end_order_id=after,
                    start_ts=start_time,
                    end_ts=end_time,
                )
                await asyncio.sleep(0.1)

                if not (isinstance(resp, dict) and resp.get("code") == "0"):
                    logger.error(
                        f"account[{self._account}] MarketType[{self._market_type}] symbol[{symbol}], error: {resp}"
                    )
                    await asyncio.sleep(0.2)
                    break

                order_data_list.extend(resp["data"])

                if len(resp["data"]) != 100:
                    break

                after = resp["data"][-1]["ordId"]
                await asyncio.sleep(0.033)

        for od in order_data_list:
            raw_order_type = od["ordType"]

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

            raw_status = od["state"]
            if raw_status in ("filled", "canceled", "partially_filled"):
                status = OrderStatus[raw_status.upper()]
            elif raw_status == "live":
                status = OrderStatus.LIVE
            elif raw_status == "mmp_canceled":
                status = OrderStatus.CANCELED
            else:
                status = OrderStatus.UNKNOWN

            side = getattr(OrderSide, od["side"].upper(), OrderSide.UNKNOWN)

            o = OrderSnapshot(
                exch_symbol=od["instId"],
                order_side=side,
                order_id=od["ordId"],
                client_order_id=od["clOrdId"].lstrip("cid"),
                price=Decimal(od["px"]) if od["px"] else Decimal(0),
                qty=Decimal(od["sz"]),
                filled_qty=Decimal(od["accFillSz"]),
                avg_price=float(od["avgPx"] if od["avgPx"] else od["fillPx"]),
                order_type=order_type,
                order_time_in_force=tif,
                order_status=status,
                place_ack_ts=int(od["cTime"]),
                exch_update_ts=int(od["uTime"]),
                local_update_ts=int(time.time() * 1000),
            )
            order_dict.setdefault(o.exch_symbol, []).append(o)

        return OrderSnapshotData(order_dict)

    @catch_it
    async def get_funding_fee(
        self,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        look_back: Optional[int] = None,
        symbol_list: Optional[list[str]] = None,
    ):
        start_time, end_time = self._parse_start_end_look_back(start_time, end_time, look_back)
        data_list: list[Any] = []
        after = None
        while True:
            resp = await self.client.get_account_bill(bill_type="8", start_ts=start_time, end_ts=end_time, after=after)
            if resp is None or resp["code"] != "0":
                raise ValueError(resp)

            if not resp["data"]:
                break
            data_list += resp["data"]
            after = resp["data"][-1]["billId"]
            await asyncio.sleep(0.25)

        funding_dict: dict[str, list[FundingFee]] = dict()
        for item in data_list:
            if symbol_list and item["instId"] not in symbol_list:
                continue

            if item["instId"] not in funding_dict:
                funding_dict[item["instId"]] = [FundingFee(Decimal(item["pnl"]), int(item["ts"]))]
            else:
                funding_dict[item["instId"]].append(FundingFee(Decimal(item["pnl"]), int(item["ts"])))

        return FundingFeeData(funding_dict)

    @catch_it
    async def get_historical_funding_rate(
        self,
        symbol_list: list[str],
        start_time: datetime | str | int | None = None,
        days: int = 7,
    ) -> FundingRatesHis:
        assert self._market_type in [MarketType.UPERP, MarketType.CPERP], f"Invalid Market type {self._market_type}"
        if start_time is None:
            start_time = datetime.now() - timedelta(days=days)
        elif isinstance(start_time, str):
            start_time = parser.parse(start_time)
        if isinstance(start_time, int):
            start_ts = start_time
        else:
            start_ts = int(start_time.timestamp() * 1000)  # type: ignore
        end_ts = int(time.time() * 1000)

        frs: dict[str, set[FundingRateSimple]] = {}

        for symbol in symbol_list:
            frs[symbol] = set()
            data_list: list[dict[str, str]] = []
            symbol_end_ts = end_ts
            while True:
                resp = await self.client.get_history_funding_rate(symbol, start_ts, symbol_end_ts)
                if resp is None or resp["code"] != "0":
                    raise ValueError(resp)

                await asyncio.sleep(0.3)
                data = resp["data"]
                if not data:
                    break

                data_list.extend(data)
                symbol_end_ts = resp["data"][-1]["fundingTime"]

            for item in data_list:
                ts = int(item["fundingTime"])
                frs[symbol].add(
                    FundingRateSimple(
                        funding_rate=float(item["realizedRate"]),
                        funding_ts=ts,
                    )
                )

        return FundingRatesHis({symbol: sorted(list(fr)) for symbol, fr in frs.items()})

    @catch_it
    async def get_historical_kline(
        self,
        symbol: str,
        interval: Interval,
        start_time: int,
        end_time: Optional[int] = None,
        contract_type: Literal["PERPETUAL", "CURRENT_QUARTER", "NEXT_QUARTER"] = "PERPETUAL",
    ):
        # this func will miss one piece of the closest data from the end_time
        result: list[KLine] = []
        interval_str = interval.name.lstrip("_").replace("h", "H")

        data_list: list[Any] = []
        while True:
            resp = await self.client.get_kline(symbol, start=start_time, end=end_time, interval=interval_str, limit=100)  # type: ignore[call-arg]
            if resp is None or resp["code"] != "0":
                raise ValueError(resp)

            if not resp["data"]:
                break

            data_list += resp["data"]
            await asyncio.sleep(0.25)

            if len(resp["data"]) != 100:
                break

            end_time = int(resp["data"][-1][0]) - 1

        for lis in data_list[::-1]:
            result.append(
                KLine(
                    start_ts=int(lis[0]),
                    open=Decimal(lis[1]),
                    high=Decimal(lis[2]),
                    low=Decimal(lis[3]),
                    close=Decimal(lis[4]),
                    volume=Decimal(lis[5]),
                    turnover=Decimal(lis[6]),
                )
            )

        return KLineData(result)

    @catch_it
    async def cancel_order(self, symbol: str, orderId: Optional[str] = None, clientOid: Optional[str] = None) -> bool:
        assert orderId or clientOid, "Either Parameters `orderId` and `clientOid` is Required"
        resp = await self.client.cancel_order(symbol, orderId, clientOid)
        if resp is None or resp["code"] != "0":
            raise ValueError(resp)

        return True

    @catch_it
    async def cancel_all(self, symbol: str) -> bool:
        resp = await self.ccxt_sync_open_orders(symbol)
        if resp["status"] != 0:
            raise ValueError(resp["msg"])
        open_orders: list[OrderSnapshot] = resp["data"]
        if symbol not in open_orders:
            return True
        order_id_list = [order.order_id for order in open_orders]
        resp = await self.client.cancel_batch_order(symbol, order_id_list)
        if resp is None:
            raise ValueError
        if isinstance(resp, dict):
            if resp.get("code") != "0":
                if resp.get("data"):
                    raise ValueError(resp["data"][0]["sMsg"])
                raise ValueError(resp["msg"])
        return True

    @catch_it
    async def get_leverage(self, symbol: str, mgnMode: MarginMode):
        mgn_mode_str: Literal["cross", "isolated"] = "cross" if mgnMode == MarginMode.CROSS else "isolated"
        resp = await self.client.get_leverage(symbol, mgn_mode_str)
        leverage = Leverage()
        if resp is None or resp["code"] != "0":
            raise ValueError(resp)

        for data in resp["data"]:
            if data["posSide"] == "long":
                leverage.long = Decimal(data["lever"])
            elif data["posSide"] == "short":
                leverage.short = Decimal(data["lever"])
            elif data["posSide"] == "net":
                leverage.long = Decimal(data["lever"])
                leverage.short = Decimal(data["lever"])

        if not (leverage.long or leverage.short):
            raise ValueError(f"fail to get leverage for symbol[{symbol}] mgnMode[{mgnMode}]")

        return leverage

    @catch_it
    async def get_max_open_quantity(self, symbol: str, mgnMode: MarginMode = MarginMode.CROSS):
        mgn_mode_str: Literal["cross", "isolated"] = "cross" if mgnMode == MarginMode.CROSS else "isolated"
        resp = await self.client.get_max_size(symbol, mgn_mode_str)
        if resp is None or resp["code"] != "0":
            raise ValueError(resp)

        data = resp["data"][0]
        return MaxOpenQty(buy=Decimal(data["maxBuy"]), sell=Decimal(data["maxSell"]))

    @catch_it
    async def get_current_funding_rate(self, symbol_list: list[str] | None = None) -> FundingRatesCur:
        assert MarketType.UPERP == self._market_type, "only support get current funding rate for UPERP"
        assert isinstance(symbol_list, list), "symbol_list must be a list"
        frs: FundingRatesCur = FundingRatesCur()
        for symbol in symbol_list:
            resp = await self.client.get_current_funding_rate(symbol)
            if resp is None or resp["code"] != "0":
                raise ValueError(resp)

            for info in resp["data"]:
                fr = float(info["fundingRate"])
                ts = float(info["fundingTime"])
                interval_hour = int((float(info["nextFundingTime"]) - float(info["fundingTime"])) / (1000 * 60 * 60))
                fr_cap = float(info["maxFundingRate"])
                fr_floor = float(info["minFundingRate"])
                frs[symbol] = FundingRate(fr, ts, interval_hour, fr_cap=fr_cap, fr_floor=fr_floor)

        return frs

    @catch_it
    async def get_current_simple_funding_rate(self, symbol_list: list[str] | None = None) -> FundingRatesSimple:
        assert MarketType.UPERP == self._market_type, "only support get current funding rate for UPERP"
        assert isinstance(symbol_list, list), "symbol_list must be a list"
        frs: FundingRatesSimple = FundingRatesSimple()
        for symbol in symbol_list:
            resp = await self.client.get_current_funding_rate(symbol)
            if resp is None or resp["code"] != "0":
                raise ValueError(resp)

            for info in resp["data"]:
                fr = float(info["fundingRate"])
                ts = float(info["fundingTime"])
                interval_hour = int((float(info["nextFundingTime"]) - float(info["fundingTime"])) / (1000 * 60 * 60))
                frs[symbol] = FundingRateSimple(fr, ts, interval_hour)
        return frs

    @catch_it
    async def get_commission_rate(self, symbol: str, from_redis: bool = False) -> CommissionRate:
        if from_redis:
            assert self._account, "account is required when from_redis is True"
            data = await self._load_data_from_rmx("trading_fee:okex", key=self._account)
            if not data:
                raise ValueError(f"Could not get current commission rate from redis for symbol[{symbol}]")

            if self._market_type in [MarketType.SPOT, MarketType.MARGIN]:
                makerfee = data["spot_maker"]
                takerfee = data["spot_taker"]
            else:
                makerfee = data["swap_maker"]
                takerfee = data["swap_taker"]
        else:
            if self._market_type in [MarketType.SPOT, MarketType.MARGIN]:
                resp = await self.client.get_account_fee(inst_type=self.get_exch_market_type(), inst_id=symbol)
                if resp is None or resp["code"] != "0":
                    raise ValueError(resp)

                resp = resp["data"][0]
                makerfee = -float(resp["maker"])
                takerfee = -float(resp["taker"])
            else:
                resp = await self.client.get_account_fee(inst_type=self.get_exch_market_type(), uly=symbol[:-5])
                if resp is None or resp["code"] != "0":
                    raise ValueError(resp)

                resp = resp["data"][0]
                if symbol[-9:-5] == "USDT":
                    makerfee = -float(resp["makerU"])
                    takerfee = -float(resp["takerU"])
                else:
                    makerfee = -float(resp["maker"])
                    takerfee = -float(resp["taker"])

        return CommissionRate(maker=Decimal(str(makerfee)), taker=Decimal(str(takerfee)))

    @catch_it
    async def get_margin_interest_rates_cur(
        self,
        vip_level: int | str | None = None,
        asset: str | None = "",
    ) -> InterestRates:
        resp = await self.client.sapi_nonvip_loanable_asset()
        if resp is None or resp["code"] != "0":
            raise ValueError(resp)

        interest_rates: InterestRates = []
        for datas in resp.get("data", []):
            for line in datas.get("basic", []):
                ccy = line["ccy"]
                if asset and asset != ccy:
                    continue
                interest_rates.append(
                    InterestRate(
                        asset=ccy,
                        vip_level="VIP0",  # 返回字段 irDiscount（利率的折扣率）已经废弃，只有基础利率信息
                        ir=Decimal(line["rate"]),  # 基础杠杆日利率
                        ts=time.time() * 1000,
                    )
                )

        return interest_rates

    def get_interval(self, interval: Interval):
        return interval.name.lstrip("_").upper()

    @catch_it
    async def get_long_short_ratio(self, symbol: str, limit: int, interval: Interval):
        assert MarketType.UPERP == self._market_type, f"Invalid Market type {self._market_type}, only support UPERP"
        assert interval in [Interval._5m, Interval._1h, Interval._1d], f"Invalid interval {interval.name}"
        if symbol.endswith("-USDT-SWAP"):
            symbol = symbol[:-10]

        interval_str = self.get_interval(interval)
        end_time = int(time.time() * 1000)
        start_time = end_time - limit * interval.value * 1000
        resp = await self.client.get_long_short_ratio(symbol, interval_str, start_time, end_time)  # type: ignore[call-arg]
        if resp is None or resp["code"] != "0":
            raise ValueError(resp)

        lis = [LongShortRatio(long_short_ratio=Decimal(data[1]), ts=int(data[0]) * 1000) for data in resp["data"]]
        return LongShortRatioData(sorted(lis, key=lambda x: x.ts))

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
    ):
        if isinstance(order_time_in_force, str):
            order_time_in_force = TimeInForce[order_time_in_force]
        if isinstance(order_side, str):
            order_side = OrderSide[order_side]
        if isinstance(order_type, str):
            order_type = OrderType[order_type]
        if not client_order_id:
            client_order_id = str(int(time.time() * 1000000))
        send_time_in_force: Literal[
            "market", "limit", "post_only", "fok", "ioc", "optimal_limit_ioc", "mmp", "mmp_and_post_only"
        ] = "limit"
        if order_time_in_force:
            if order_type == OrderType.MARKET:
                send_time_in_force = "market"
            else:
                if TimeInForce.GTX == order_time_in_force:
                    send_time_in_force = "post_only"
                elif TimeInForce.FOK == order_time_in_force:
                    send_time_in_force = "fok"
                elif TimeInForce.IOC == order_time_in_force:
                    send_time_in_force = "ioc"
                else:
                    send_time_in_force = "limit"
        else:
            if order_type == OrderType.MARKET:
                send_time_in_force = "market"
            else:
                raise ValueError("limit order time_in_force cannot be empty")
        send_order_side = "buy" if order_side == OrderSide.BUY else "sell"

        # 市价单不需要price参数
        if order_type == OrderType.MARKET and price is not None:
            raise ValueError("In market_order parameter price not required")
        params = extras or {}
        quote_qty = params.pop("quote_qty", None)
        # 验证 qty 和 quote_qty 互斥
        if qty is None and quote_qty is None:
            raise ValueError("Either qty or quote_qty must be specified")
        if qty is not None and quote_qty is not None:
            raise ValueError("qty and quote_qty are mutually exclusive")

        # 验证 quote_qty 只能用于市价单
        # use_base_qty表示市价单是使用base(true)，quote(false)
        use_base_qty = True
        if quote_qty is not None and order_type != OrderType.MARKET:
            raise ValueError("quote_qty is only supported for MARKET orders")
        elif quote_qty is not None and order_type == OrderType.MARKET:
            qty = quote_qty
            use_base_qty = False

        if self._market_type not in (MarketType.SPOT, MarketType.MARGIN) and not use_base_qty:
            raise ValueError("Only base_qty is allowed")

        if order_type == OrderType.MARKET:
            tgtCcy = "base_ccy" if use_base_qty else "quote_ccy"
        else:
            tgtCcy = None

        if self._market_type in (MarketType.SPOT, MarketType.MARGIN):
            resp = await self.client.place_order(
                symbol,
                "cross",
                send_order_side,
                send_time_in_force,
                str(qty),
                str(price) if price else None,
                clOrdId=client_order_id,
                reduce_only=reduce_only,
                tgtCcy=tgtCcy,
            )
        else:
            resp = await self.client.place_order(
                symbol,
                "cross",
                send_order_side,
                send_time_in_force,
                str(qty),
                str(price) if price else None,
                clOrdId=client_order_id,
                reduce_only=reduce_only,
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
            snapshot.rejected_message = "Failed to place order, response is None"
        elif isinstance(resp, dict):
            if resp.get("code") != "0":
                snapshot.order_status = OrderStatus.REJECTED
                if resp.get("data"):
                    snapshot.rejected_message = resp["data"][0]["sMsg"]
                else:
                    snapshot.rejected_message = resp["msg"]
            else:
                snapshot.order_status = OrderStatus.LIVE
                snapshot.order_id = resp["data"][0]["ordId"]
                snapshot.place_ack_ts = snapshot.local_update_ts
                snapshot.exch_update_ts = resp["data"][0]["ts"]
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

        if order_time_in_force:
            if isinstance(order_time_in_force, str):
                order_time_in_force = TimeInForce[order_time_in_force]
            params["timeInForce"] = order_time_in_force.ccxt

        if reduce_only:
            params["reduceOnly"] = reduce_only
        if client_order_id:
            params["clientOrderId"] = client_order_id
        else:
            params["clientOrderId"] = "xclients_" + str(int(time.time() * 1000000))
        if "tdMode" not in params:
            params["tdMode"] = "cross"
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
            params["clientOrderId"] = client_order_id

        try:
            order_resp: ccxtOrder = await self.ccxt_client.cancel_order(order_id or "", symbol, params=params)  # type: ignore[call-arg]
            order_snapshot = OrderSnapshot.from_ccxt_order(order_resp, symbol)
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
            order_resp: list[ccxtOrder] = await self.ccxt_client.fetch_open_orders(symbol, params=params)
            order_id_list = [order["id"] for order in order_resp]
            if order_id_list:
                await self.ccxt_client.cancel_orders(order_id_list, symbol)
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
                params["clientOrderId"] = client_order_id

            order_resp = await self.ccxt_client.fetch_order(order_id or "", symbol, params=params)
            return OrderSnapshot.from_ccxt_order(order_resp, symbol)
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
    async def get_tickers(self) -> Tickers:
        ticker_info = await self.client.get_tickers(inst_type=self.get_exch_market_type())  # type: ignore[call-arg]
        if ticker_info is None or ticker_info["code"] != "0":
            raise ValueError(ticker_info)

        update_ts = float(time.time() * 1_000)
        tickers = {
            ticker["instId"]: Ticker(
                ticker["instId"],
                float(ticker["bidPx"]) if ticker["bidPx"] else np.nan,
                float(ticker["askPx"]) if ticker["askPx"] else np.nan,
                ts=float(ticker["ts"]),
                update_ts=update_ts,
                bid_qty=float(ticker["bidSz"]) if ticker["bidSz"] else np.nan,
                ask_qty=float(ticker["askSz"]) if ticker["askSz"] else np.nan,
            )
            for ticker in ticker_info["data"]
        }
        if self._market_type in [MarketType.UPERP]:
            # 参数 quoteCcy和instId必须填写一个
            index_info = await self.client.get_index_tickers(quote_ccy="USDT")
            if index_info is None or index_info["code"] != "0":
                logger.warning(f"Failed to get index tickers: {index_info}")
                return tickers

            for index in index_info["data"]:
                symbol = index["instId"]
                if symbol in tickers:
                    tickers[symbol].index_price = float(index["idxPx"])
            # TODO add fr, fr_ts by request single symbol or use websockets?
        return tickers

    @catch_it
    async def get_quotations(self) -> Quotations:
        ticker_info = await self.client.get_tickers(inst_type=self.get_exch_market_type())  # type: ignore[call-arg]
        if ticker_info is None or ticker_info["code"] != "0":
            raise ValueError(ticker_info)

        update_ts = float(time.time() * 1_000)
        quotations = {
            t["instId"]: Quotation(
                exch_symbol=t["instId"],
                bid=float(t["bidPx"]) if t["bidPx"] else np.nan,
                ask=float(t["askPx"]) if t["askPx"] else np.nan,
                ts=float(t["ts"]),
                update_ts=update_ts,
                bid_qty=float(t["bidSz"]) if t["bidSz"] else np.nan,
                ask_qty=float(t["askSz"]) if t["askSz"] else np.nan,
            )
            for t in ticker_info["data"]
        }

        return Quotations(quotations)

    @catch_it
    async def get_orderbook_snapshot(self, symbol: str, limit: int = 100) -> OrderBook:
        resp = await self.client.get_orderbook(symbol, limit=limit)
        if resp is None or resp["code"] != "0":
            raise ValueError(resp)

        data = resp.get("data", [{}])[0]
        if not data:
            raise Exception(f"Get orderbook snapshot empty. resp: {resp}")

        orderbook = OrderBook(symbol)
        orderbook.exch_seq = int(data["ts"])
        orderbook.exch_ts = int(data["ts"])
        orderbook.recv_ts = int(time.time() * 1000)
        for bid in data["bids"]:
            orderbook.bids.append((Decimal(bid[0]), Decimal(bid[1])))
        for ask in data["asks"]:
            orderbook.asks.append((Decimal(ask[0]), Decimal(ask[1])))
        return orderbook

    @catch_it
    async def get_fundamentals(self) -> Fundamentals:
        tickers_resp = await self.client.get_tickers(inst_type=self.get_exch_market_type())  # type: ignore[call-arg]
        if tickers_resp is None or tickers_resp["code"] != "0":
            raise ValueError(tickers_resp)

        fundamentals: dict[str, Fundamental] = {}
        for ticker in tickers_resp["data"]:
            symbol = ticker["instId"]
            if symbol not in self._insts:
                continue
            fundamentals[symbol] = Fundamental(
                symbol,
                # invalid data eg:
                #     {
                #     'instType': 'SPOT',
                #     'instId': 'TESTG-USDT',
                #     'last': '',
                #     'lastSz': '0',
                #     'askPx': '1.1',
                #     'askSz': '1',
                #     'bidPx': '1',
                #     'bidSz': '1',
                #     'open24h': '',
                #     'high24h': '',
                #     'low24h': '',
                #     'volCcy24h': '0',
                #     'vol24h': '0',
                #     ...
                #     }
                float(ticker["last"]) / float(ticker["open24h"]) - 1 if ticker["open24h"] not in ["", "0"] else np.nan,
                (
                    int(
                        Decimal(ticker["volCcy24h"])
                        * (
                            Decimal(ticker["open24h"])
                            + Decimal(ticker["low24h"])
                            + Decimal(ticker["high24h"])
                            + Decimal(ticker["last"])
                        )
                        / 4
                    )
                    if self._market_type in [MarketType.UPERP]
                    else float(ticker["volCcy24h"])
                ),
            )
        if self._market_type in [MarketType.UPERP]:
            # 文档说uly和instFamily必须传一个，实际好像可以不用传
            oi_resp = await self.client.get_open_interest(self.get_exch_market_type())  # type: ignore[call-arg]
            if oi_resp is None or oi_resp["code"] != "0":
                raise ValueError(oi_resp)

            for data in oi_resp["data"]:
                symbol = data["instId"]
                if symbol in fundamentals:
                    fundamentals[symbol].open_interest = float(data["oiUsd"])

        return fundamentals

    @catch_it
    async def get_account_info(self) -> AccountInfo:
        resp = await self.client.get_balance()
        if resp is None or resp["code"] != "0":
            raise ValueError(resp)

        usdt_free = 0.0
        for data in resp["data"]:
            for detail in data["details"]:
                ccy = detail["ccy"]
                if ccy == "USDT":
                    usdt_free = float(detail["availBal"])
                    break

        account = resp["data"][0]
        equity = float(account["totalEq"])  # 美金层面权益
        available_balance = float(account["adjEq"])  # 美金层面有效保证金
        margin_balance = float(account["imr"])  # 美金层面占用保证金
        maintenance_margin = float(account["mmr"])  # 美金层面维持保证金
        total_position_value = float(account["notionalUsd"])  # 以美金价值为单位的持仓数量
        usdt_free = usdt_free

        if maintenance_margin > 0:
            mmr = margin_balance / maintenance_margin
        else:
            mmr = 999
        if (im := float(account["adjEq"])) > 0:
            imr = margin_balance / im
        else:
            imr = 999

        return AccountInfo(
            account=self._account_meta,
            equity=equity,
            usdt_free=usdt_free,
            imr=imr,
            mmr=mmr,
            available_balance=available_balance,
            margin_balance=margin_balance,
            total_position_value=total_position_value,
        )

    @catch_it
    async def get_interest_rates_cur(
        self,
        vip_level: int | str | None = None,
        vip_loan: bool = False,
        asset: str = "",
        days: int = -1,
    ) -> InterestRates:
        assert self._market_type == MarketType.SPOT, f"Invalid Market type {self._market_type}, only support SPOT"
        assert days == -1, "param days only support -1"
        interest_rates: list[InterestRate] = []
        resp = await self.client.get_lending_rate_summary(ccy=asset)
        if resp is None or resp["code"] != "0":
            raise ValueError(resp)

        for info in resp.get("data", []):
            ccy = info["ccy"]
            if asset and ccy != asset:
                continue
            interest_rates.append(
                InterestRate(
                    asset=ccy,
                    days=days,
                    ir=Decimal(info["estRate"]) / 365,  # estRate:下一次预估借贷年利率
                    ts=time.time() * 1000,
                )
            )
        return interest_rates

    @catch_it
    async def get_interest_rates_his(
        self,
        vip_level: int | str | None = None,
        vip_loan: bool = False,
        asset: str = "",
        start_time: int | None = None,
        end_time: int | None = None,
    ) -> InterestRates:
        assert self._market_type == MarketType.SPOT, f"Invalid Market type {self._market_type}, only support SPOT"
        interest_rates: list[InterestRate] = []
        data_list: list[dict[str, str]] = []
        if not end_time:
            end_time = int(time.time() * 1000)
        if not start_time:
            start_time = end_time - 30 * 24 * 60 * 60 * 1000  # 30 days
        tmp_end_time = str(end_time)
        while True:
            resp = await self.client.get_lending_rate_history(
                ccy=asset, start_time=str(start_time), end_time=tmp_end_time
            )
            if resp is None or resp["code"] != "0":
                raise ValueError(resp)

            await asyncio.sleep(0.3)
            data = resp["data"]
            if not data:
                break

            data_list.extend(data)
            tmp_end_time = resp["data"][-1]["ts"]

        for info in data_list:
            ccy = info["ccy"]
            if asset and ccy != asset:
                continue
            ir_ts = float(info["ts"])
            if ir_ts < start_time or ir_ts > end_time:
                continue
            interest_rates.append(
                InterestRate(
                    asset=ccy,
                    ir=Decimal(info["rate"]) / 365,  # rate:出借年利率
                    ts=ir_ts,
                )
            )

        return interest_rates

    @catch_it
    async def get_staking_interest_rates_his(
        self,
        asset: Literal["SOL", "ETH"],
        start_time: int | None = None,
        end_time: int | None = None,
    ) -> InterestRates:
        assert self._market_type == MarketType.SPOT, f"Invalid Market type {self._market_type}, only support SPOT"
        if not end_time:
            end_time = int(datetime.now().timestamp() * 1000)
        if not start_time:
            start_time = end_time - 30 * 24 * 60 * 60 * 1000

        if asset == "SOL":
            func = self.client.get_staking_sol_rate_history
        elif asset == "ETH":
            func = self.client.get_staking_eth_rate_history
        else:
            raise NotImplementedError("asset only support SOL,ETH")

        days = min([int((time.time() - start_time / 1000) / (24 * 60 * 60)) + 1, 365])
        resp = await func(days)
        if resp is None or resp["code"] != "0":
            raise ValueError(resp)

        interest_rates: InterestRates = []
        for info in resp.get("data", []):
            data_ts = float(info["ts"])
            if data_ts < start_time or data_ts > end_time:
                continue
            interest_rates.append(
                InterestRate(
                    asset=asset,
                    ir=Decimal(info["rate"]) / 365,
                    ts=data_ts,
                )
            )

        return interest_rates
