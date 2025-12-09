from decimal import Decimal
import time
import asyncio
from typing import Optional, Union, Callable, Any, Literal
from loguru import logger
from datetime import datetime, timedelta
from dateutil import parser
import ccxt.async_support as ccxt
from ccxt.base.types import Order as ccxtOrder, ConstructorArgs

from .rest import *
from .websocket import GatePrivateWsClient
from ..base_wrapper import BaseRestWrapper, BaseWssWrapper, catch_it
from ..enum_type import (
    AccountType,
    ExchangeName,
    MarketType,
    Interval,
    TimeInForce,
    OrderSide,
    Event,
    MarginMode,
    OrderStatus,
    OrderType,
)
from ..data_type import *
from ..common.exceptions import UnsupportedOperationError
from .constants import TIF_MAP


class GateRestWrapper(BaseRestWrapper):
    client: Any

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

        ccxt_params = {
            "apiKey": self._account_config.api_key,
            "secret": self._account_config.secret_key,
            "enableRateLimit": True,
            "options": {
                "defaultType": ccxt_default_type,
                "defaultSubType": ccxt_default_sub_type,
            },
        }
        self.ccxt_client = ccxt.gateio(ConstructorArgs(ccxt_params))

    @catch_it
    async def get_positions(self, from_redis: bool = False):
        match self._market_type:
            case MarketType.UPERP:
                positions = await self.get_lps_positions(from_redis)
            case MarketType.CPERP:
                positions = await self.get_lps_positions(from_redis, settle="btc")
            case _:
                raise ValueError(f"Market type {self._market_type} is not supported")

        if positions is None:
            raise ValueError("fail to get positions")

        if (data := positions.get("data")) is None:
            raise ValueError(positions.get("msg", "unknown error"))

        return data

    @catch_it
    async def get_lps_positions(self, from_redis: bool = False, settle: Literal["usdt", "btc"] = "usdt"):
        """
        param settle: usdt --> lps; btc --> ps;
        """
        result: dict[str, Position] = {}
        data = None
        if from_redis:
            if settle != "usdt":
                raise ValueError(f"MarketType: {self._market_type} have no redis data")

            suffix = "raw:test"
            key = "futures_positions"
            data = await self._load_data_from_rmx_acc(suffix, key)
        else:
            cli: GateFutureRestClient = self.client
            data = await cli.get_positions(settle=settle)
        if data is None:
            raise ValueError("fail to get data")

        if not isinstance(data, list):
            raise ValueError(data)

        for i in data:
            if Decimal(i["size"]) != Decimal(0):
                if i["contract"] not in result:
                    result[i["contract"]] = Position(
                        exch_symbol=i["contract"],
                        net_qty=float(i["size"]),
                        entry_price=float(i["entry_price"]),
                        value=abs(float(i["size"])) * float(i["entry_price"]),
                        liq_price=float(i["liq_price"]),
                        unrealized_pnl=float(i["unrealised_pnl"]),
                    )
                else:
                    result[i["contract"]].net_qty += float(i["size"])

        return Positions(result)

    @catch_it
    async def get_assets(self, from_redis: bool = False) -> Balances:
        match (self._account_type, self._market_type):
            case (AccountType.UNIFIED, _):
                return await self._get_unified_assets(from_redis)
            case (AccountType.NORMAL, MarketType.UPERP):
                return await self._get_lps_assets(from_redis)
            case (AccountType.NORMAL, MarketType.CPERP):
                return await self._get_lps_assets(from_redis, settle="btc")
            case (AccountType.NORMAL, MarketType.SPOT | MarketType.MARGIN):
                return await self._get_sp_assets(from_redis)
            case _:
                raise ValueError(f"Unsupported market type: {self._market_type}")

    async def _get_unified_assets(self, from_redis: bool = False) -> Balances:
        result: dict[str, Balance] = {}
        if from_redis:
            suffix = "raw:test"
            key = "uta_balance"
            resp = await self._load_data_from_rmx_acc(suffix, key)
        else:
            cli = self.client
            match self._market_type:
                case MarketType.SPOT | MarketType.MARGIN if isinstance(cli, GateUnifiedSpotRestClient):
                    resp = await cli.get_account()
                case MarketType.UPERP | MarketType.CPERP if isinstance(cli, GateUnifiedFutureRestClient):
                    resp = await cli.get_account()
                case MarketType.UDELIVERY | MarketType.CDELIVERY if isinstance(cli, GateUnifiedDeliveryRestClient):
                    resp = await cli.get_account()
                case _:
                    raise ValueError(f"market type {self._market_type} is not supported")

            if isinstance(resp, dict) and resp.get("label"):
                raise Exception(resp.get("message"))
        update_time = int(time.time() * 1000)
        if isinstance(resp, dict):
            for asset, info in resp["balances"].items():
                tot_balance = float(info["equity"])
                if tot_balance == 0:
                    continue
                result[asset] = Balance(
                    asset=asset,
                    balance=tot_balance,
                    free=float(info["available"]),
                    borrowed=float(info["borrowed"]),
                    locked=float(info["freeze"]),
                    type="full",
                    ts=update_time,
                )
            return Balances(result)
        raise ValueError(f"Invalid data received from the API")

    async def _get_sp_assets(self, from_redis: bool = False) -> Balances:
        result: dict[str, Balance] = {}
        if from_redis:
            suffix = "raw:test"
            key = "spot_balance"
            resp = await self._load_data_from_rmx_acc(suffix, key)
        else:
            cli: GateSpotRestClient = self.client
            resp = await cli.get_account()
            if isinstance(resp, dict) and resp.get("label"):
                raise Exception(resp.get("message"))
        if resp is None:
            raise ValueError("fail to get data")
        update_time = int(time.time() * 1000)
        if isinstance(resp, list):
            for info in resp:
                tot_balance = float(info["available"]) + float(info["locked"])
                if tot_balance == 0:
                    continue
                result[info["currency"]] = Balance(
                    asset=info["currency"],
                    balance=tot_balance,
                    free=float(info["available"]),
                    locked=float(info["locked"]),
                    type="full",
                    ts=update_time,
                )
            return Balances(result)
        raise ValueError(f"Invalid data [{resp}] received from the API for {self._market_type} assets")

    async def _get_lps_assets(self, from_redis: bool = False, settle: Literal["usdt", "btc"] = "usdt") -> Balances:
        """
        param settle: usdt --> lps; btc --> ps;
        """
        if self._market_type not in [MarketType.UPERP, MarketType.CPERP]:
            raise ValueError(f"Account type {self._market_type} is not supported(only supported for UFTURES,CPERP)")

        result: dict[str, Balance] = {}
        if from_redis:
            suffix = "raw:test"
            if self._market_type == MarketType.UPERP:
                key = "futures_balance"
            else:
                raise ValueError(f"MarketType: {self._market_type} have no redis data")

            resp = await self._load_data_from_rmx_acc(suffix, key)
        else:
            cli: GateFutureRestClient = self.client
            resp = await cli.get_account(settle=settle)

        if resp is None or resp.get("message"):
            raise ValueError(resp)

        update_time = resp["update_time"] * 1000
        result[settle.upper()] = Balance(
            asset=settle.upper(),
            balance=float(resp["total"]),
            free=float(resp["available"]),
            locked=float(resp["total"]) - float(resp["available"]),
            ts=update_time,
            type="full",
        )
        return Balances(result)

    @catch_it
    async def get_equity(self) -> float:
        """
        U合约的totalMarginBalance + 现货free+locked + 杠杆BTC net asset
        """
        if self._market_type in [MarketType.SPOT, MarketType.MARGIN] and self._account_type != AccountType.UNIFIED:
            cli: GateSpotRestClient = self.client
            resp = await cli.get_total_balance()
            if resp is None or resp.get("message"):
                raise ValueError(resp)

            return float(resp["total"]["amount"])
        else:
            assets = await self.get_assets()
            price_resp = await self.get_prices()
            equity = 0
            if assets["status"] == 0 and price_resp["status"] == 0:
                price_dict = price_resp["data"]
                for coin, info in assets["data"].items():
                    if info.balance == 0:
                        continue
                    if price_dict.get(coin + "_USDT"):
                        equity += info.balance * price_dict[coin + "_USDT"]
                    elif coin == "USDT":
                        equity += info.balance
                    elif coin not in ["POINT", "FTM", "XMR", "BADAI"]:
                        logger.error("invalid coin {} {}", coin, info.balance)
            elif assets["status"] != 0:
                raise Exception(assets["msg"])
            elif price_resp["status"] != 0:
                raise Exception(price_resp["msg"])
            return equity

    @catch_it
    async def set_symbol_leverage(self, symbol: str, leverage: int, **kwargs) -> bool:
        assert self._market_type in [MarketType.UPERP, MarketType.CPERP], "Invalid market type"
        cli = self.client
        symbol = symbol.upper()
        match self._market_type:
            case MarketType.UPERP | MarketType.CPERP if isinstance(cli, GateFutureRestClient):
                resp = await cli.set_leverage(contract=symbol, leverage=str(leverage))
            case MarketType.UDELIVERY | MarketType.CDELIVERY if isinstance(cli, GateDeliveryRestClient):
                resp = await cli.set_leverage(contract=symbol, leverage=str(leverage))
            case _:
                raise ValueError(f"market type {self._market_type} is not supported")

        if resp is None or resp.get("message"):
            raise ValueError(resp)

        return True

    @catch_it
    async def universal_transfer(
        self,
        qty: Decimal,
        asset: str = "USDT",
        from_market_type: MarketType | None = None,
        to_market_type: MarketType | None = None,
        from_account_type: AccountType | None = None,
        to_account_type: AccountType | None = None,
    ) -> TransferResponse:
        assert from_market_type and to_market_type, "gate只支持使用market_type进行转账"
        market_type_dict: dict[
            MarketType, Literal["spot", "margin", "futures", "delivery", "cross_margin", "options"]
        ] = {
            MarketType.SPOT: "spot",
            MarketType.UPERP: "futures",
            MarketType.MARGIN: "margin",
            MarketType.CPERP: "futures",
            MarketType.UDELIVERY: "delivery",
            MarketType.CDELIVERY: "delivery",
        }
        assert from_market_type in market_type_dict and to_market_type in market_type_dict, "Invalid acct type"
        assert self._market_type == MarketType.SPOT, "Only SPOT account support universal_transfer"
        if isinstance(from_market_type, str):
            from_market_type = MarketType[from_market_type]
        elif isinstance(to_market_type, str):
            to_market_type = MarketType[to_market_type]
        settle = None
        if from_market_type == MarketType.UPERP or to_market_type == MarketType.UPERP:
            settle = "usdt"
        elif from_market_type == MarketType.CPERP or to_market_type == MarketType.CPERP:
            settle = "btc"

        cli: GateSpotRestClient = self.client
        resp = await cli.transfer(
            asset, str(qty), market_type_dict[from_market_type], market_type_dict[to_market_type], settle=settle
        )
        if resp is None or resp.get("message"):
            raise ValueError(resp)

        return TransferResponse(apply_id=resp["tx_id"])

    @catch_it
    async def get_price(self, symbol: str, from_redis: bool = False) -> float:
        cli = self.client
        match self._market_type:
            case MarketType.SPOT | MarketType.MARGIN if isinstance(cli, GateSpotRestClient):
                resp = await cli.get_market(symbol)
            case MarketType.UPERP if isinstance(cli, GateFutureRestClient):
                resp = await cli.get_tickers(contract=symbol)
            case MarketType.CPERP if isinstance(cli, GateFutureRestClient):
                resp = await cli.get_tickers(contract=symbol, settle="btc")
            case _:
                raise ValueError(f"market type {self._market_type} is not supported")

        if resp is None or resp.get("message"):
            raise ValueError(resp)

        return float(resp[0]["last"])

    @catch_it
    async def get_prices(self) -> Prices:
        cli = self.client
        match self._market_type:
            case MarketType.SPOT | MarketType.MARGIN if isinstance(cli, GateSpotRestClient):
                resp = await cli.get_market()
                if resp is None or resp.get("message"):
                    raise ValueError(resp)

                return Prices({item["currency_pair"]: float(item["last"]) for item in resp})
            case MarketType.UPERP if isinstance(cli, GateFutureRestClient):
                resp = await cli.get_tickers()
                if resp is None or resp.get("message"):
                    raise ValueError(resp)

                return Prices({item["contract"]: float(item["last"]) for item in resp})
            case MarketType.CPERP if isinstance(cli, GateFutureRestClient):
                resp = await cli.get_tickers(settle="btc")
                if resp is None or resp.get("message"):
                    raise ValueError(resp)

                return Prices({item["contract"]: float(item["last"]) for item in resp})
            case _:
                raise ValueError(f"market type {self._market_type} is not supported")

    @catch_it
    async def get_trade_history(self, start_time: int, end_time: int, symbol_list: list[str]):
        end_time //= 1000
        start_time //= 1000
        result: dict[str, list[Trade]] = {}
        trade_data_list = []
        trade_limit = 100
        for symbol in symbol_list:
            tmp_end_time = end_time
            while True:
                cli = self.client
                match self._market_type:
                    case MarketType.SPOT | MarketType.MARGIN if isinstance(cli, GateSpotRestClient):
                        resp = await cli.get_trades(
                            symbol=symbol,
                            start_time=start_time,
                            end_time=tmp_end_time,
                            limit=trade_limit,
                        )
                    case MarketType.UPERP | MarketType.CPERP if isinstance(cli, GateFutureRestClient):
                        if MarketType.CPERP == self._market_type:
                            settle = "btc"
                        else:
                            settle = "usdt"
                        resp = await cli.get_trades(
                            symbol=symbol,
                            start_time=start_time,
                            end_time=tmp_end_time,
                            limit=trade_limit,
                            settle=settle,
                        )
                    case _:
                        raise ValueError(f"market type {self._market_type} is not supported")

                if resp is None or resp.get("message"):
                    raise ValueError(resp)

                orig_resp_len = len(resp)
                if trade_data_list:
                    last_trade_id = (
                        int(trade_data_list[-1]["id"])
                        if self._market_type in [MarketType.SPOT, MarketType.MARGIN]
                        else int(trade_data_list[-1]["trade_id"])
                    )
                    resp = [info for info in resp if int(info.get("id", info.get("trade_id", 0))) < last_trade_id]
                trade_data_list.extend(resp)

                if orig_resp_len != trade_limit:
                    break

                tmp_end_time = int(trade_data_list[-1]["create_time"]) + 1

            match self._market_type:
                case MarketType.SPOT | MarketType.MARGIN:
                    for data in trade_data_list[::-1]:
                        result.setdefault(symbol, []).append(
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
                case MarketType.UPERP | MarketType.CPERP:
                    for data in trade_data_list[::-1]:
                        side = "BUY" if data["size"] > 0 else "SELL"
                        result.setdefault(symbol, []).append(
                            Trade(
                                create_ts=int(float(data["create_time"]) * 1000),
                                side=getattr(OrderSide, side, OrderSide.UNKNOWN),
                                trade_id=data["trade_id"],
                                order_id=data["order_id"],
                                last_trd_price=Decimal(data["price"]),
                                last_trd_volume=abs(Decimal(data["size"])),
                                turnover=Decimal(data["price"])
                                * abs(Decimal(data["size"])),  # TODO consider multiplier
                                fill_ts=int(float(data["create_time"]) * 1000),
                                fee=Decimal(data["fee"]),
                                fee_ccy="",
                                is_maker=True if data["role"] == "maker" else False,
                            )
                        )

        return TradeData(result)

    @catch_it
    async def get_order_history(self, start_time: int, end_time: int, symbol_list: list[str]) -> OrderSnapshotData:
        order_dict: dict[str, list[OrderSnapshot]] = {}
        order_data_list = []

        cli = self.client
        match self._market_type:
            case MarketType.SPOT | MarketType.MARGIN if isinstance(cli, GateSpotRestClient):
                start_time //= 1000
                end_time //= 1000
                limit = 100
                for symbol in symbol_list:
                    page = 1
                    while True:
                        resp = await cli.get_orders(
                            symbol, "finished", start_time=start_time, end_time=end_time, page=page, limit=limit
                        )
                        if not isinstance(resp, list):
                            logger.error(
                                f"account[{self._account}] MarketType[{self._market_type}] symbol[{symbol}], error: {resp}"
                            )
                            await asyncio.sleep(0.2)
                            break

                        order_data_list.extend(resp)
                        if len(resp) < limit:
                            break
                        page += 1
                        await asyncio.sleep(0.2)

                for od in order_data_list:
                    order_type = getattr(OrderType, od["type"].upper(), OrderType.UNKNOWN)
                    tif = TIF_MAP.get(od["time_in_force"], TimeInForce.UNKNOWN)

                    if order_type == OrderType.MARKET:
                        filled_price = Decimal(od["avg_deal_price"])
                        _left = Decimal(od["left"]) / filled_price if filled_price != 0 else Decimal("0")
                        filled_qty = Decimal(od["filled_total"]) / filled_price if filled_price != 0 else Decimal("0")
                        qty = filled_qty + _left
                    else:
                        qty = Decimal(od["amount"])
                        _left = Decimal(od["left"])
                        filled_qty = qty - _left

                    raw_status = od["finish_as"]
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

                    side = getattr(OrderSide, od["side"].upper(), OrderSide.UNKNOWN)
                    o = OrderSnapshot(
                        exch_symbol=od["currency_pair"],
                        order_side=side,
                        order_id=str(od["id"]),
                        client_order_id=str(od["text"]),
                        price=Decimal(od["price"]),
                        qty=qty,
                        avg_price=float(od["avg_deal_price"]),
                        filled_qty=filled_qty,
                        order_type=order_type,
                        order_time_in_force=tif,
                        order_status=status,
                        place_ack_ts=int(od["create_time_ms"]),
                        exch_update_ts=int(od["update_time_ms"]),
                        local_update_ts=int(time.time() * 1000),
                    )
                    order_dict.setdefault(o.exch_symbol, []).append(o)
            case MarketType.UPERP | MarketType.CPERP if isinstance(cli, GateFutureRestClient):
                start_time //= 1000
                end_time //= 1000
                limit = 100
                if self._market_type == MarketType.UPERP:
                    settle = "usdt"
                else:
                    settle = "btc"
                for symbol in symbol_list:
                    offset = 0
                    while True:
                        resp = await cli.get_future_orders(
                            contract=symbol,
                            start_time=start_time,
                            end_time=end_time,
                            limit=limit,
                            offset=offset,
                            settle=settle,
                        )
                        if not isinstance(resp, list):
                            raise ValueError(resp)
                        order_data_list.extend(resp)
                        if len(resp) < limit:
                            break
                        offset += limit
                        await asyncio.sleep(0.2)

                    for od in order_data_list:
                        qty = Decimal(abs(od["size"]))
                        _left = Decimal(od["left"])
                        filled_qty = qty - _left

                        if od["size"] > 0:
                            side = OrderSide.BUY
                        elif od["size"] < 0:
                            side = OrderSide.SELL
                        else:
                            side = OrderSide.UNKNOWN

                        order_type = OrderType.MARKET if od["price"] == 0.0 else OrderType.LIMIT
                        tif = TIF_MAP.get(od["tif"], TimeInForce.UNKNOWN)
                        status = OrderStatus.UNKNOWN
                        if od["status"] == "open":
                            if filled_qty == 0:
                                status = OrderStatus.LIVE
                            else:
                                status = OrderStatus.PARTIALLY_FILLED
                        elif od["status"] == "finished":
                            if od["finish_as"] == "filled":
                                status = OrderStatus.FILLED
                            elif od["finish_as"] in (
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
                            exch_symbol=od["contract"],
                            order_side=side,
                            order_id=str(od["id"]),
                            client_order_id=str(od["text"]),
                            price=Decimal(od["price"]),
                            qty=qty,
                            avg_price=float(od["fill_price"]),
                            filled_qty=filled_qty,
                            order_type=order_type,
                            order_time_in_force=tif,
                            order_status=status,
                            place_ack_ts=float(od["create_time"]) * 1000,
                            exch_update_ts=float(od["finish_time"]) * 1000,
                            local_update_ts=int(time.time() * 1000),
                        )
                        order_dict.setdefault(o.exch_symbol, []).append(o)
            case _:
                raise ValueError(f"market type {self._market_type} is not supported")

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
        start_time //= 1000
        end_time //= 1000
        info_list = []
        while True:
            cli: GateFutureRestClient = self.client
            resp = await cli.get_account_book(
                settle="usdt", type="fund", start_time=start_time, end_time=end_time, limit=1000, offset=len(info_list)
            )
            if isinstance(resp, dict) and resp.get("label"):
                logger.error(resp.get("message"))
            elif resp:
                info_list.extend(resp)
            else:
                break
        funding_dict: dict[str, list[FundingFee]] = {}
        for item in info_list:
            symbol = item["text"].split(":")[0]
            if symbol_list and symbol not in symbol_list:
                continue
            if symbol in funding_dict.keys():
                funding_dict[symbol].append(FundingFee(Decimal(item["change"]), int(item["time"] * 1000)))
            else:
                funding_dict[symbol] = [FundingFee(Decimal(item["change"]), int(item["time"] * 1000))]
        return FundingFeeData(funding_dict)

    @catch_it
    async def get_historical_funding_rate(
        self,
        symbol_list: list[str],
        start_time: datetime | str | int | None = None,
        days: int = 7,
    ) -> FundingRatesHis:
        assert self._market_type in (MarketType.UPERP, MarketType.CPERP), f"Invalid Market type {self._market_type}"
        cli: GateFutureRestClient = self.client
        match self._market_type:
            case MarketType.UPERP:
                settle = "usdt"
            case MarketType.CPERP:
                settle = "btc"

        if isinstance(start_time, int):
            start_ts = int(start_time / 1000)
        else:
            if start_time is None:
                start_time = datetime.now() - timedelta(days=days)
            elif isinstance(start_time, str):
                start_time = parser.parse(start_time)
            start_ts = int(start_time.timestamp())

        end_ts = int(datetime.now().timestamp())
        frs: dict[str, set[FundingRateSimple]] = {}
        if not symbol_list:
            symbol_list = []
            resp = await cli.get_exchange_info(settle=settle)
            if resp is None or resp.get("message"):
                raise ValueError(resp)

            for info in resp:
                symbol_list.append(info["name"])

        for symbol in symbol_list:
            frs[symbol] = set()
            data_list = []
            symbol_end_ts = end_ts
            while True:
                resp = await cli.get_funding_rate(symbol, settle=settle, start_time=start_ts, end_time=symbol_end_ts)
                if isinstance(resp, dict) and resp.get("label"):
                    raise ValueError(resp.get("message"))
                data_list.extend(resp)
                if not resp:
                    break
                symbol_end_ts = int(resp[-1]["t"]) - 1
                if symbol_end_ts <= start_ts:
                    break
                for item in data_list:
                    ts = item["t"] * 1000
                    frs[symbol].add(FundingRateSimple(funding_rate=float(item["r"]), funding_ts=ts))
            await asyncio.sleep(0.3)

        return FundingRatesHis({symbol: sorted(list(fr)) for symbol, fr in frs.items()})

    def get_interval(self, interval):
        interval = interval.name.lstrip("_")
        if interval[-1] == "M":
            interval = str(int(interval[:-1]) * 30) + "d"
        return interval

    def get_request_num(self, interval):
        num = 0
        if interval[-1] == "s":
            num = int(interval[:-1])
        elif interval[-1] == "m":
            num = int(interval[:-1]) * 60
        elif interval[-1] == "h":
            num = int(interval[:-1]) * 3600
        elif interval[-1] == "d":
            num = int(interval[:-1]) * 3600 * 24
        else:
            raise ValueError("wrong interval")
        return num

    @catch_it
    async def get_historical_kline(
        self,
        symbol: str,
        interval: Interval,
        start_time: int,
        end_time: Optional[int] = None,
        contract_type: Literal["PERPETUAL", "CURRENT_QUARTER", "NEXT_QUARTER"] = "PERPETUAL",
    ):
        result: list[KLine] = []
        data_list: list[Any] = []

        interval_str = self.get_interval(interval)
        interval_gap = interval.value

        start_time = int(start_time / 1000)
        if end_time is None:
            end_time = int(time.time() * 1000)
        end_time = int(end_time / 1000)

        start_time_origin = start_time
        end_time_origin = end_time

        time_list: list[tuple[int, int]] = []
        if (end_time - start_time) / interval_gap < 1000:
            time_list.append((start_time, end_time))
        else:
            while (end_time - start_time) / interval_gap > 1000:
                time_list.append((start_time, start_time + interval_gap * 999))
                start_time += interval_gap * 999 + 1
            time_list.append((start_time, end_time))

        for start_time, end_time in time_list:
            cli = self.client
            match self._market_type:
                case MarketType.SPOT | MarketType.MARGIN if isinstance(cli, GateSpotRestClient):
                    resp = await cli.get_history_kline(
                        currency_pair=symbol, start_time=start_time, end_time=end_time, interval=interval_str  # type: ignore
                    )
                    if not isinstance(resp, list):
                        raise ValueError(resp)

                    if resp:
                        if int(resp[0][0]) < start_time:
                            resp.pop(0)
                        if int(resp[-1][0]) > end_time:
                            resp.pop()
                        data_list += resp

                    await asyncio.sleep(0.25)
                case MarketType.UPERP | MarketType.CPERP if isinstance(cli, GateFutureRestClient):
                    resp = await cli.get_history_kline(
                        contract=symbol, start_time=start_time, end_time=end_time, interval=interval_str
                    )
                    if not isinstance(resp, list):
                        raise ValueError(resp)

                    if resp:
                        if int(resp[0]["t"]) < start_time:
                            resp.pop(0)
                        if int(resp[-1]["t"]) > end_time:
                            resp.pop()
                        data_list += resp
                    await asyncio.sleep(0.25)
                case _:
                    raise ValueError(f"market type {self._market_type} is not supported")

        for d in data_list:
            if isinstance(d, dict):
                if not start_time_origin < int(d["t"]) < end_time_origin:
                    continue
                result.append(
                    KLine(
                        start_ts=int(int(d["t"]) * 1000),
                        open=Decimal(d["o"]),
                        high=Decimal(d["h"]),
                        low=Decimal(d["l"]),
                        close=Decimal(d["c"]),
                        volume=Decimal(d["v"]),
                        turnover=Decimal(d["sum"]),
                    )
                )
            else:
                # spot
                if not start_time_origin < int(d[0]) < end_time_origin:
                    continue
                result.append(
                    KLine(
                        start_ts=int(int(d[0]) * 1000),
                        open=Decimal(d[5]),
                        high=Decimal(d[3]),
                        low=Decimal(d[4]),
                        close=Decimal(d[2]),
                        volume=Decimal(d[6]),
                        turnover=Decimal(d[1]),
                    )
                )
        return KLineData(result)

    @catch_it
    async def get_current_funding_rate(self, symbol_list: list[str] | None = None) -> FundingRatesCur:
        assert self._market_type == MarketType.UPERP, "only support get current funding rate for UPERP"
        cli: GateFutureRestClient = self.client
        resp = await cli.get_exchange_info()
        if not isinstance(resp, list):
            raise ValueError(resp)

        funding_rates_dict = {info["name"]: info for info in resp}
        if not symbol_list:
            symbol_list = list(funding_rates_dict.keys())

        frs: FundingRatesCur = FundingRatesCur()
        for symbol in symbol_list:
            symbol_info = funding_rates_dict.get(symbol, {})
            fr = float(symbol_info.get("funding_rate", 0))
            fr_ts = int(symbol_info.get("funding_next_apply", 0))
            ts = fr_ts * 1000 if fr_ts else 0
            interval = symbol_info.get("funding_interval", 8 * 60 * 60) / 60 / 60
            # 资金费率上限 = (1/市场最大杠杆 - 维持保证金率) * funding_cap_ratio
            if (
                symbol_info.get("leverage_max")
                and symbol_info.get("maintenance_rate")
                and symbol_info.get("funding_cap_ratio")
            ):
                fr_cap = (1 / float(symbol_info["leverage_max"]) - float(symbol_info["maintenance_rate"])) * float(
                    symbol_info["funding_cap_ratio"]
                )
                fr_floor = -fr_cap
            else:
                fr_cap = fr_floor = np.nan
            frs[symbol] = FundingRate(fr, funding_ts=ts, interval_hour=int(interval), fr_cap=fr_cap, fr_floor=fr_floor)

        return frs

    @catch_it
    async def get_current_simple_funding_rate(self, symbol_list: list[str] | None = None) -> FundingRatesSimple:
        assert self._market_type == MarketType.UPERP, "only support get current funding rate for UPERP"
        cli: GateFutureRestClient = self.client
        resp = await cli.get_exchange_info()
        if not isinstance(resp, list):
            raise ValueError(resp)

        frs: FundingRatesSimple = FundingRatesSimple()
        for info in resp:
            exch_symbol = info["name"]
            if symbol_list and exch_symbol not in symbol_list:
                continue
            frs[exch_symbol] = FundingRateSimple(
                float(info.get("funding_rate", 0)),
                float(info.get("funding_next_apply", 0)) * 1000,
                int(info.get("funding_interval", 8 * 60 * 60) / (60 * 60)),
            )

        return frs

    @catch_it
    async def cancel_order(self, symbol: str, orderId: Optional[str] = None, clientOid: Optional[str] = None) -> bool:
        assert orderId or clientOid, "Either Parameters `orderId` or `clientOid` is needed"
        cli = self.client
        match self._market_type:
            case MarketType.SPOT | MarketType.MARGIN if isinstance(cli, GateSpotRestClient):
                resp = await cli.cancel_order(symbol, order_id=orderId, custom_id=clientOid)
            case MarketType.UPERP | MarketType.CPERP if isinstance(cli, GateFutureRestClient):
                if self._market_type == MarketType.UPERP:
                    settle = "usdt"
                else:
                    settle = "btc"
                resp = await cli.cancel_order(order_id=orderId, custom_id=clientOid, settle=settle)
            case _:
                raise ValueError(f"market type {self._market_type} is not supported")

        if resp is None:
            raise ValueError
        if isinstance(resp, dict) and resp.get("label"):
            raise ValueError(resp.get("message", resp.get("detail", resp["label"])))
        else:
            return True

    @catch_it
    async def get_leverage(self, symbol: str, mgnMode: MarginMode) -> Leverage:
        cli = self.client
        match self._market_type:
            case MarketType.UPERP | MarketType.CPERP if isinstance(cli, GateFutureRestClient):
                if self._market_type == MarketType.UPERP:
                    settle = "usdt"
                else:
                    settle = "btc"
                resp = await cli.get_position(symbol, settle=settle)
                leverage = Leverage()
                if resp is None:
                    raise ValueError
                if isinstance(resp, dict):
                    if resp.get("label"):
                        raise ValueError(f"fail to get leverage: {resp.get('message', resp['label'])}")
                    if resp["contract"] == symbol:
                        lever_str = "leverage" if mgnMode == MarginMode.ISOLATED else "cross_leverage_limit"
                        leverage.long = Decimal(resp[lever_str])
                        leverage.short = Decimal(resp[lever_str])
                if leverage.long or leverage.short:
                    return leverage
                raise ValueError(f"fail to get leverage for symbol[{symbol}] mgnMode[{mgnMode}]")
            case _:
                raise ValueError(f"market type {self._market_type} is not supported")

    @catch_it
    async def get_max_open_notional(self, symbol: str, mgnMode: MarginMode = MarginMode.CROSS):
        cli = self.client
        match self._market_type:
            case MarketType.UPERP | MarketType.CPERP if isinstance(cli, GateFutureRestClient):
                if self._market_type == MarketType.UPERP:
                    settle = "usdt"
                else:
                    settle = "btc"
                resp = await cli.get_position(symbol, settle=settle)
                max_open_notional = MaxOpenNotional()
                if resp is None:
                    raise ValueError
                if isinstance(resp, dict):
                    if resp.get("label"):
                        raise ValueError(f"fail to get max_open_notional: {resp.get('message', resp['label'])}")
                    if resp["contract"] == symbol:
                        max_open_notional.buy = Decimal(resp["risk_limit"])
                        max_open_notional.sell = Decimal(resp["risk_limit"])
                if max_open_notional.buy or max_open_notional.sell:
                    return max_open_notional
                raise ValueError(f"fail to get max_open_notional for symbol[{symbol}] mgnMode[{mgnMode}]")
            case _:
                raise ValueError(f"market type {self._market_type} is not supported")

    @catch_it
    async def get_commission_rate(self, symbol: str, from_redis: bool = False) -> CommissionRate:
        if from_redis:
            assert self._account, "Account is required to get commission rate from redis"
            data = await self._load_data_from_rmx("trading_fee:gate", key=self._account)
            if data is None:
                raise ValueError(f"Could not get current commission rate from redis for symbol[{symbol}]")

            if self._market_type in [MarketType.SPOT, MarketType.MARGIN]:
                makerfee = data["spot_maker"]
                takerfee = data["spot_taker"]
            else:
                makerfee = data["swap_maker"]
                takerfee = data["swap_taker"]
        else:
            cli = self.client
            match self._market_type:
                case MarketType.SPOT | MarketType.MARGIN if isinstance(cli, GateSpotRestClient):
                    resp = await cli.get_commission_rate(symbol=symbol)
                    if not isinstance(resp, dict):
                        raise ValueError(resp)

                    makerfee = resp["maker_fee"]
                    takerfee = resp["taker_fee"]
                case MarketType.UPERP | MarketType.CPERP if isinstance(cli, GateFutureRestClient):
                    if self._market_type == MarketType.UPERP:
                        settle = "usdt"
                    else:
                        settle = "btc"
                    resp = await cli.get_commission_rate(symbol=symbol, settle=settle)
                    if not isinstance(resp, dict):
                        raise ValueError(resp)

                    makerfee = resp[symbol]["maker_fee"]
                    takerfee = resp[symbol]["taker_fee"]
                case _:
                    raise ValueError(f"market type {self._market_type} is not supported")

        return CommissionRate(maker=Decimal(str(makerfee)), taker=Decimal(str(takerfee)))

    @catch_it
    async def get_long_short_ratio(self, symbol: str, limit: int, interval: Interval):
        assert self._market_type == MarketType.UPERP, f"Invalid market type {self._market_type}, only support UPERP"
        assert interval in [
            Interval._5m,
            Interval._15m,
            Interval._30m,
            Interval._1h,
            Interval._4h,
            Interval._1d,
        ], f"Invalid interval {interval.name}"
        interval_str = self.get_interval(interval)
        cli: GateFutureRestClient = self.client
        resp = await cli.get_long_short_ratio(symbol, interval_str, limit=limit)  # type: ignore
        if not isinstance(resp, list):
            raise ValueError(resp)

        lis = [
            LongShortRatio(long_short_ratio=Decimal(str(data["lsr_account"])), ts=int(data["time"]) * 1000)
            for data in resp
        ]
        return LongShortRatioData(lis[-limit:])

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
        send_order_type = "limit" if order_type == OrderType.LIMIT else "market"
        send_order_side = "buy" if order_side == OrderSide.BUY else "sell"
        if not client_order_id or not client_order_id.startswith("t-"):
            client_order_id = "t-xclients_" + str(int(time.time() * 1000000))

        # 市价单的 time_in_force 处理
        if order_type == OrderType.MARKET:
            if order_time_in_force is None:
                send_time_in_force = "ioc"
            elif order_time_in_force in [TimeInForce.IOC, TimeInForce.FOK]:
                send_time_in_force = order_time_in_force.name.lower()
            else:
                send_time_in_force = "ioc"
        # 限价单的 time_in_force 处理
        elif order_type == OrderType.LIMIT:
            if order_time_in_force:
                if TimeInForce.GTX == order_time_in_force:
                    send_time_in_force = "poc"
                else:
                    send_time_in_force = order_time_in_force.name.lower()

        # 市价单不需要price参数
        if order_type == OrderType.MARKET and price is not None:
            raise ValueError("In market_order parameter price not required")
        # 从 extras 中提取 quote_qty
        params = extras or {}
        quote_qty = params.pop("quote_qty", None)

        use_base_qty = True
        if self._market_type in (MarketType.SPOT, MarketType.MARGIN):
            # - LIMIT 订单和 MARKET SELL：size = qty (base coin)
            # - MARKET BUY：size = quote_qty (quote coin)
            if order_type == OrderType.MARKET and order_side == OrderSide.BUY:
                # 市价买单：使用 quote_qty
                if quote_qty is not None and qty is not None:
                    raise ValueError("Either qty or quote_qty must be specified")
                if quote_qty is None:
                    raise ValueError("For MARKET BUY orders, only quote_qty should be specified")
                size_value = quote_qty
                use_base_qty = False
            else:
                # 限价单或市价卖单：只能使用 qty
                if quote_qty is not None:
                    raise ValueError("quote_qty is only supported for MARKET BUY orders in Gate")
                size_value = qty

            if size_value is None:
                raise ValueError("Either qty or quote_qty must be specified")
        else:
            if quote_qty is not None:
                raise ValueError("Only base_qty is allowed")
            if qty is None:
                raise ValueError("base_qty is allowed")
            size_value = qty

        cli = self.client
        match self._market_type:
            case MarketType.SPOT | MarketType.MARGIN if isinstance(cli, GateSpotRestClient):
                resp = await cli.place_order(
                    cid=client_order_id,
                    side=send_order_side,
                    symbol=symbol,
                    type=send_order_type,
                    price=str(price) if price else None,
                    size=str(size_value),
                    time_in_force=send_time_in_force,  # type: ignore
                    **(params or {}),
                )
            case MarketType.UPERP | MarketType.CPERP if isinstance(cli, GateFutureRestClient):
                if not use_base_qty:
                    raise ValueError("Only base_qty is allowed")
                if self._market_type == MarketType.CPERP:
                    settle = "btc"
                else:
                    settle = "usdt"
                resp = await cli.place_order(
                    cid=client_order_id,
                    side=send_order_side,
                    symbol=symbol,
                    type=send_order_type,
                    price=str(price) if price else None,
                    size=str(size_value),
                    time_in_force=send_time_in_force,  # type: ignore
                    reduce_only=reduce_only,
                    settle=settle,
                )
            case MarketType.UDELIVERY | MarketType.CDELIVERY if isinstance(cli, GateDeliveryRestClient):
                if not use_base_qty:
                    raise ValueError("Only base_qty is allowed")
                if self._market_type == MarketType.CDELIVERY:
                    settle = "btc"
                else:
                    settle = "usdt"
                resp = await cli.place_order(
                    cid=client_order_id,
                    side=send_order_side,
                    symbol=symbol,
                    type=send_order_type,
                    price=str(price) if price else None,
                    size=str(size_value),
                    time_in_force=send_time_in_force,  # type: ignore
                    reduce_only=reduce_only,
                    settle=settle,
                )
            case _:
                raise ValueError(f"market type {self._market_type} is not supported")

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
        elif isinstance(resp, dict) and resp.get("label"):
            snapshot.order_status = OrderStatus.REJECTED
            snapshot.rejected_message = resp.get("message", resp.get("detail", resp["label"]))
        else:
            snapshot.order_id = str(resp["id"])
            snapshot.order_status = OrderStatus.LIVE
            snapshot.place_ack_ts = snapshot.local_update_ts
            snapshot.exch_update_ts = float(resp["update_time"]) * 1000
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
        params["clientOrderId"] = client_order_id

        if order_time_in_force:
            if isinstance(order_time_in_force, str):
                order_time_in_force = TimeInForce[order_time_in_force]
            params["time_in_force"] = order_time_in_force.ccxt

        if reduce_only:
            params["reduce_only"] = reduce_only

        try:
            order_resp: ccxtOrder = await self.ccxt_client.create_order(
                symbol,
                order_type.ccxt,
                order_side.ccxt,
                float(qty),
                price,
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
            order_resp = await self.ccxt_client.cancel_order(order_id or "", symbol, params=params)
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
    async def get_collateral_ratio(self) -> CollateralRatios:
        crs: list[CollateralRatio] = []
        assert (
            self._account_type == AccountType.UNIFIED and self._market_type.is_derivative
        ), "only support unified account for derivative"
        resp = await self.client.get_discount_tiers()  # FIXME: missing api
        if resp:
            crs: list[CollateralRatio] = []
            for i in resp:
                asset = i["currency"]
                cr = {float(j["lower_limit"]): float(j["discount"]) for j in i["discount_tiers"]}
                crs.append(CollateralRatio(asset, cr))
        return crs

    @catch_it
    async def get_tickers(self) -> Tickers:
        if self._market_type in [MarketType.SPOT, MarketType.MARGIN]:
            tickers = await self.client.get_market()
        else:
            tickers = await self.client.get_tickers()
        if not isinstance(tickers, list):
            raise ValueError(f"unexpected response[{tickers}]")

        update_ts = float(time.time() * 1000)
        symbol_name = "currency_pair" if self._market_type in [MarketType.SPOT, MarketType.MARGIN] else "contract"
        processed_tickers = {
            ticker[symbol_name]: Ticker(
                ticker[symbol_name],
                (float(ticker["highest_bid"]) if ticker["highest_bid"] else np.nan),
                (float(ticker["lowest_ask"]) if ticker["lowest_ask"] else np.nan),
                (float(ticker["index_price"]) if ticker.get("index_price") else np.nan),
                ts=update_ts,
                update_ts=update_ts,
                bid_qty=(float(ticker["highest_size"]) if ticker.get("highest_size") else np.nan),
                ask_qty=(float(ticker["lowest_size"]) if ticker.get("lowest_size") else np.nan),
            )
            for ticker in tickers
        }

        return processed_tickers

    @catch_it
    async def get_quotations(self) -> Quotations:
        cli = self.client
        match self._market_type:
            case MarketType.SPOT | MarketType.MARGIN if isinstance(cli, GateSpotRestClient):
                resp = await cli.get_market()
            case MarketType.UPERP | MarketType.CPERP if isinstance(cli, GateFutureRestClient):
                if self._market_type == MarketType.CPERP:
                    settle = "btc"
                else:
                    settle = "usdt"
                resp = await cli.get_tickers(settle=settle)
            case MarketType.UDELIVERY | MarketType.CDELIVERY if isinstance(cli, GateDeliveryRestClient):
                if self._market_type == MarketType.CDELIVERY:
                    settle = "btc"
                else:
                    settle = "usdt"
                resp = await cli.get_tickers(settle=settle)
            case _:
                raise ValueError(f"market type {self._market_type} is not supported")

        if not isinstance(resp, list):
            raise ValueError(resp)

        update_ts = float(time.time() * 1000)
        symbol_name = "currency_pair" if self._market_type in [MarketType.SPOT, MarketType.MARGIN] else "contract"
        quotations = {
            t[symbol_name]: Quotation(
                exch_symbol=t[symbol_name],
                bid=(float(t["highest_bid"]) if t["highest_bid"] else np.nan),
                ask=(float(t["lowest_ask"]) if t["lowest_ask"] else np.nan),
                ts=update_ts,
                update_ts=update_ts,
                bid_qty=(float(t["highest_size"]) if t.get("highest_size") else np.nan),
                ask_qty=(float(t["lowest_size"]) if t.get("lowest_size") else np.nan),
            )
            for t in resp
        }

        return Quotations(quotations)

    @catch_it
    async def get_orderbook_snapshot(self, symbol: str, limit: int = 100) -> OrderBook:
        cli = self.client
        match self._market_type:
            case MarketType.SPOT | MarketType.MARGIN if isinstance(cli, GateSpotRestClient):
                resp = await cli.get_orderbook(symbol, limit=limit)
            case MarketType.UPERP | MarketType.CPERP if isinstance(cli, GateFutureRestClient):
                if self._market_type == MarketType.UPERP:
                    settle = "usdt"
                else:
                    settle = "btc"
                resp = await cli.get_orderbook(symbol, settle=settle, limit=limit)
            case _:
                raise ValueError(f"market type {self._market_type} is not supported")

        if not (isinstance(resp, dict) and "asks" in resp):
            raise Exception(f"Get orderbook snapshot failed. err_msg={resp}, symbol={symbol}")
        orderbook = OrderBook(symbol)

        orderbook.exch_seq = int(resp["id"])
        if self._market_type in [MarketType.SPOT, MarketType.MARGIN]:
            orderbook.exch_ts = int(resp["update"])
        else:
            orderbook.exch_ts = int(resp["update"]) * 1_000
        orderbook.recv_ts = int(time.time() * 1_000)
        if self._market_type in [MarketType.SPOT, MarketType.MARGIN]:
            for bid in resp["bids"]:
                orderbook.bids.append((Decimal(bid[0]), Decimal(bid[1])))
            for ask in resp["asks"]:
                orderbook.asks.append((Decimal(ask[0]), Decimal(ask[1])))
        else:
            for bid in resp["bids"]:
                orderbook.bids.append((Decimal(bid["p"]), Decimal(bid["s"])))
            for ask in resp["asks"]:
                orderbook.asks.append((Decimal(ask["p"]), Decimal(ask["s"])))
        return orderbook

    @catch_it
    async def get_fundamentals(self) -> Fundamentals:
        cli = self.client
        match self._market_type:
            case MarketType.SPOT | MarketType.MARGIN if isinstance(cli, GateSpotRestClient):
                resp = await cli.get_market()
            case MarketType.UPERP | MarketType.CPERP if isinstance(cli, GateFutureRestClient):
                if self._market_type == MarketType.CPERP:
                    settle = "btc"
                else:
                    settle = "usdt"
                resp = await cli.get_tickers(settle=settle)
            case MarketType.UDELIVERY | MarketType.CDELIVERY if isinstance(cli, GateDeliveryRestClient):
                if self._market_type == MarketType.CDELIVERY:
                    settle = "btc"
                else:
                    settle = "usdt"
                resp = await cli.get_tickers(settle=settle)
            case _:
                raise ValueError(f"market type {self._market_type} is not supported")

        if not isinstance(resp, list):
            raise ValueError(resp)

        fundamentals: dict[str, Fundamental] = {}
        if self._market_type.is_derivative:
            symbol_name = "contract"
            notional_name = "volume_24h_quote"
        else:
            symbol_name = "currency_pair"
            notional_name = "quote_volume"

        for t in resp:
            symbol = t[symbol_name]
            if (symbol not in self._insts) or (self._insts[symbol].status != InstStatus.TRADING):
                continue

            fundamentals[symbol] = Fundamental(
                symbol,
                float(t["change_percentage"]) / 100,
                float(t[notional_name]),
                (
                    float(t["total_size"]) * float(self._insts[symbol].quantity_multiplier) * float(t["last"])
                    if "total_size" in t
                    else np.nan
                ),
            )
        return fundamentals

    @catch_it
    async def get_account_info(self) -> AccountInfo:
        cli = self.client
        match (self._account_type, self._market_type):
            case (AccountType.UNIFIED, MarketType.SPOT | MarketType.MARGIN) if isinstance(
                cli, GateUnifiedSpotRestClient
            ):
                resp = await cli.get_account()
                if resp is None:
                    raise ValueError("Could not get account info")

                usdt_balance = 0.0
                usdt_free = 0.0
                usdt_borrowed = 0.0
                for asset, info in resp["balances"].items():
                    if asset == "USDT":
                        usdt_balance = float(info["equity"])
                        usdt_free = min(float(info["available"]), usdt_balance)
                        usdt_borrowed = float(info["total_liab"])
                        break
                equity = float(resp["unified_account_total_equity"])
                available_margin = float(resp["total_available_margin"])
                margin_balance = float(resp["total_margin_balance"])
                imr = float(resp["total_initial_margin_rate"])
                mmr = float(resp["total_maintenance_margin_rate"])

                return AccountInfo(
                    account=self._account_meta,
                    equity=equity,
                    usdt_free=usdt_free,
                    imr=imr,
                    mmr=mmr,
                    available_balance=available_margin,
                    margin_balance=margin_balance,
                    usdt_borrowed=usdt_borrowed,
                )
            case (AccountType.UNIFIED, MarketType.UPERP | MarketType.CPERP) if isinstance(
                cli, GateUnifiedFutureRestClient
            ):
                resp = await cli.get_account()
                if resp is None:
                    raise ValueError("Could not get account info")

                usdt_balance = 0.0
                usdt_free = 0.0
                usdt_borrowed = 0.0
                for asset, info in resp["balances"].items():
                    if asset == "USDT":
                        usdt_balance = float(info["equity"])
                        usdt_free = min(float(info["available"]), usdt_balance)
                        usdt_borrowed = float(info["total_liab"])
                        break
                equity = float(resp["unified_account_total_equity"])
                available_margin = float(resp["total_available_margin"])
                margin_balance = float(resp["total_margin_balance"])
                imr = float(resp["total_initial_margin_rate"])
                mmr = float(resp["total_maintenance_margin_rate"])

                return AccountInfo(
                    account=self._account_meta,
                    equity=equity,
                    usdt_free=usdt_free,
                    imr=imr,
                    mmr=mmr,
                    available_balance=available_margin,
                    margin_balance=margin_balance,
                    usdt_borrowed=usdt_borrowed,
                )
            case (AccountType.UNIFIED, MarketType.UDELIVERY | MarketType.CDELIVERY) if isinstance(
                cli, GateUnifiedDeliveryRestClient
            ):
                resp = await cli.get_account()
                if resp is None:
                    raise ValueError("Could not get account info")

                usdt_balance = 0.0
                usdt_free = 0.0
                usdt_borrowed = 0.0
                for asset, info in resp["balances"].items():
                    if asset == "USDT":
                        usdt_balance = float(info["equity"])
                        usdt_free = min(float(info["available"]), usdt_balance)
                        usdt_borrowed = float(info["total_liab"])
                        break
                equity = float(resp["unified_account_total_equity"])
                available_margin = float(resp["total_available_margin"])
                margin_balance = float(resp["total_margin_balance"])
                imr = float(resp["total_initial_margin_rate"])
                mmr = float(resp["total_maintenance_margin_rate"])

                return AccountInfo(
                    account=self._account_meta,
                    equity=equity,
                    usdt_free=usdt_free,
                    imr=imr,
                    mmr=mmr,
                    available_balance=available_margin,
                    margin_balance=margin_balance,
                    usdt_borrowed=usdt_borrowed,
                )
            case (AccountType.NORMAL, MarketType.SPOT | MarketType.MARGIN) if isinstance(cli, GateSpotRestClient):
                balances_resp = await self.get_assets()
                if balances_resp["status"] != 0:
                    raise ValueError(f"Could not get account info, {balances_resp['msg']}")
                balances = balances_resp["data"]

                tickers_resp = await self.get_tickers()
                if tickers_resp["status"] != 0:
                    raise ValueError(f"Could not get account info, {tickers_resp['msg']}")
                tickers = tickers_resp["data"]

                usdt = balances.get("USDT", Balance("USDT")).free
                total_position_value = 0
                for asset, balance in balances.items():
                    if asset == "USDT":
                        continue

                    symbol = asset + "_USDT"
                    total = balance.balance
                    ticker = tickers.get(symbol)
                    if ticker is None:
                        logger.warning(f"{symbol} 获取ticker失败")
                        continue
                    total_position_value += abs(total) * ticker.mpx
                equity = total_position_value + usdt

                return AccountInfo(
                    account=self._account_meta,
                    equity=equity,
                    usdt_free=usdt,
                    margin_balance=equity,
                    total_position_value=total_position_value,
                )
            case _:
                raise ValueError(
                    f"market type {self._market_type} and account type {self._account_type} is not supported"
                )

    @catch_it
    async def adjust_risk_limits(self):
        assert self._market_type == MarketType.UPERP, "only support adjust risk limits for UPERP"
        cli: GateFutureRestClient = self.client
        positions_info = await cli.get_positions()
        exch_info = await cli.get_exchange_info()
        if not positions_info:
            logger.info("当前无持仓，跳过风险限额调整")
            return
        if not exch_info:
            logger.warning("获取交易所信息失败，跳过风险限额调整")
            return
        for position_info in positions_info:
            if position_info["size"] == 0:
                continue
            symbol = position_info["contract"]
            if symbol not in self._insts:
                logger.debug(f"交易对 {symbol} 不存在")
                continue
            inst = self._insts[symbol]
            total = Decimal(position_info["size"]) * inst.quantity_multiplier
            position_value = abs(total) * Decimal(position_info["mark_price"])
            risk_limit = Decimal(position_info["risk_limit"])
            if position_value > risk_limit * Decimal("0.9") or position_value + Decimal("10000") > risk_limit:
                symbol_info = next((i for i in exch_info if i["name"] == position_info["contract"]), None)
                if not symbol_info:
                    logger.warning(f"交易对 {position_info['contract']} 获取交易所信息失败")
                    continue
                risk_limit_max = Decimal(symbol_info["risk_limit_max"])
                if risk_limit >= risk_limit_max:
                    continue
                logger.warning(f"风险限额不足: {symbol}, {position_value} > {risk_limit}")
                risk_limit_tiers = await self.client.get_risk_limit_tiers(position_info["contract"])

                if not risk_limit_tiers:
                    logger.warning(f"获取风险限额失败: {position_info['contract']}")
                    continue

                for tier in risk_limit_tiers:
                    if Decimal(tier["risk_limit"]) > risk_limit:
                        await self.client.update_risk_limit(position_info["contract"], tier["risk_limit"])
                        logger.info(
                            f"调整风险限额: {position_info['contract']} {position_info['risk_limit']}->{tier['risk_limit']}"
                        )
                        await asyncio.sleep(1)
                        break

            elif position_value < risk_limit * Decimal("0.5") and position_value < risk_limit - Decimal("20000"):
                symbol_info = next((i for i in exch_info if i["name"] == position_info["contract"]), None)
                if not symbol_info:
                    logger.warning(f"交易对 {position_info['contract']} 获取交易所信息失败")
                    continue
                risk_limit_min = Decimal(symbol_info["risk_limit_base"])
                if risk_limit <= risk_limit_min:
                    continue
                logger.warning(f"风险限额冗余: {symbol}, {position_value} < {risk_limit}")
                risk_limit_tiers = await self.client.get_risk_limit_tiers(position_info["contract"])

                if not risk_limit_tiers:
                    logger.warning(f"获取风险限额失败: {position_info['contract']}")
                    continue

                for tier in risk_limit_tiers:
                    if (
                        Decimal(tier["risk_limit"]) < risk_limit
                        and Decimal(tier["risk_limit"]) > position_value * Decimal("1.5")
                        and Decimal(tier["risk_limit"]) > position_value + Decimal("15000")
                    ):
                        await self.client.update_risk_limit(position_info["contract"], tier["risk_limit"])
                        logger.info(
                            f"调整风险限额: {position_info['contract']} {position_info['risk_limit']}->{tier['risk_limit']}"
                        )
                        await asyncio.sleep(1)
                        break

    @catch_it
    async def get_margin_interest_rates_his(
        self,
        vip_level: int | None = None,
        asset: str | None = "",
        start_time: int | None = None,
        end_time: int | None = None,
    ) -> InterestRates:
        assert asset, "param `asset` is empty"
        assert self._market_type == MarketType.MARGIN, f"Invalid Market type {self._market_type}, only support MARGIN"
        if not end_time:
            end_time = int(datetime.now().timestamp() * 1000)
        if not start_time:
            start_time = end_time - 30 * 24 * 60 * 60 * 1000
        interest_rates: list[InterestRate] = []
        data_list: list[dict[str, str]] = []
        page = 1
        while True:
            cli: GateUnifiedSpotRestClient = self.client
            resp = await cli.get_history_loan_rate(
                currency=asset, tier=str(vip_level) if vip_level is not None else None, page=page, limit=100
            )
            if not (isinstance(resp, dict) and "rates" in resp):
                raise ValueError(resp)
            await asyncio.sleep(0.5)

            if datas := resp["rates"]:
                if vip_level is not None:
                    tier_up_rate = resp["tier_up_rate"]
                else:
                    tier_up_rate = "1"
                for ir in datas:
                    if ir["time"] < start_time or ir["time"] > end_time:
                        continue
                    ir["tier_up_rate"] = tier_up_rate
                    data_list.append(ir)
                if datas[-1]["time"] <= start_time:
                    break
            else:
                break
            page += 1

        for info in data_list:
            interest_rates.append(
                InterestRate(
                    asset=asset,
                    vip_level=str(vip_level) if vip_level is not None else "BaseRate",
                    ir=Decimal(info["tier_up_rate"]) * Decimal(info["rate"]) * 24,
                    ts=float(info["time"]),  # ms
                )
            )
        return interest_rates
