from decimal import Decimal
import traceback
from typing import Optional, Any, Literal
from loguru import logger
import time
import asyncio
import copy
import ccxt.async_support as ccxt
from ccxt.base.types import Order as ccxtOrder, ConstructorArgs

from .rest import BybitRestClient
from ..base_wrapper import BaseRestWrapper, BaseWssWrapper, BaseAccountWssWrapper, BaseMarketWssWrapper, catch_it
from ..enum_type import (
    AccountType,
    TimeInForce,
    Interval,
    OrderSide,
    MarginMode,
    OrderStatus,
    OrderType,
)
from ..data_type import *
from ..common.exceptions import UnsupportedOperationError
from .constants import *
from datetime import datetime, timedelta
from dateutil import parser


class BybitRestWrapper(BaseRestWrapper):
    client: BybitRestClient

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
                ccxt_default_type = "spot"  # Bybit treats margin similar to spot
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
            "enableRateLimit": True,
            "options": {
                "defaultType": ccxt_default_type,
                "defaultSubType": ccxt_default_sub_type,
            },
        }
        self.ccxt_client = ccxt.bybit(ConstructorArgs(ccxt_params))

    def get_category(self) -> Literal["spot", "linear", "inverse"]:
        category_dict: dict[MarketType, Literal["spot", "linear", "inverse"]] = {
            MarketType.SPOT: "spot",
            MarketType.MARGIN: "spot",
            MarketType.UPERP: "linear",
            MarketType.CPERP: "inverse",
            MarketType.CDELIVERY: "inverse",
        }
        return category_dict[self._market_type]

    def get_exch_account_type(self):
        account_type: Literal["UNIFIED", "CONTRACT", "FUND", "SPOT"] = "UNIFIED"
        if self._account_type == AccountType.UNIFIED:
            if self._market_type in [MarketType.CPERP, MarketType.CDELIVERY]:
                account_type = "CONTRACT"
        elif self._account_type == AccountType.NORMAL:
            if self._market_type in [MarketType.SPOT, MarketType.MARGIN]:
                account_type = "SPOT"
            else:
                account_type = "CONTRACT"
        elif MarketType.CDELIVERY == self._market_type:
            account_type = "FUND"
        return account_type

    @catch_it
    async def get_positions(self, from_redis: bool = False):
        if self._market_type not in [MarketType.UPERP, MarketType.CPERP, MarketType.CDELIVERY]:
            raise ValueError(
                f"Market type {self._market_type} is not supported(only supported for UPERP,CPERP,CDELIVERY)"
            )

        result: dict[str, Position] = {}
        data = []

        if from_redis:
            suffix = "raw:test"
            if self._market_type == MarketType.UPERP:
                key = "swapv5_position"
            else:
                key = ""  # TODO add in redis
            assert key, f"Account type: {self._account_type} have no redis data"

            data = await self._load_data_from_rmx_acc(suffix, key)
            data = list(data.values())
        else:
            cursor = None
            while True:
                resp = await self.client.get_position(self.get_category(), cursor=cursor)  # type: ignore
                if resp is not None and resp["retCode"] == 0:
                    data.extend(resp["result"]["list"])
                elif resp:
                    raise ValueError(resp["retMsg"])
                else:
                    raise ValueError("fail to get position from exchange")
                cursor = resp["result"]["nextPageCursor"]
                if not cursor:
                    break

        if isinstance(data, list):
            for info in data:
                if float(info["size"]) != 0:
                    sign = {"Buy": 1, "Sell": -1}.get(info["side"], 1)
                    result[info["symbol"]] = Position(
                        exch_symbol=info["symbol"],
                        net_qty=float(info["size"]) * sign,
                        entry_price=float(info["avgPrice"]),
                        value=float(info["positionValue"]),
                        liq_price=float(info["liqPrice"]) if info["liqPrice"] else 0,
                        unrealized_pnl=float(info["unrealisedPnl"]),
                        ts=int(info["updatedTime"]),
                    )
            return Positions(result)
        else:
            raise ValueError("unknown error")

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
        assert (
            from_market_type is not None or from_account_type is not None
        ), "from_market_type 和 from_account_type 不能同时为空"
        assert (
            to_market_type is not None or to_account_type is not None
        ), "to_market_type 和 to_account_type 不能同时为空"
        assert not (from_market_type and from_account_type), "from_market_type 和 from_account_type 不能同时传"
        assert not (to_market_type and to_account_type), "to_market_type 和 to_account_type 不能同时传"

        account_type_dict: dict[AccountType, Literal["FUND", "CONTRACT", "UNIFIED"]] = {
            AccountType.FUND: "FUND",
            AccountType.UNIFIED: "UNIFIED",
        }
        # 若传了 market_type 而没传 account_type，统一账户2.0的account_type默认为UNIFIED
        if from_market_type is not None and from_account_type is None:
            from_account_type = AccountType.UNIFIED
        if to_market_type is not None and to_account_type is None:
            to_account_type = AccountType.UNIFIED
        if from_account_type == to_account_type:
            raise ValueError("from_account_type 和 to_account_type 相同, 无需划转")
        assert from_account_type in account_type_dict, f"Invalid from_account_type[{from_account_type}]"
        assert to_account_type in account_type_dict, f"Invalid to_account_type[{to_account_type}]"
        resp = await self.client.inter_transfer(
            from_acct_type=account_type_dict[from_account_type],
            to_acct_type=account_type_dict[to_account_type],
            ccy=asset,
            amount=str(qty),
        )
        if resp and resp["retCode"] == 0:
            return TransferResponse(apply_id=str(resp["result"]["transferId"]))
        elif resp:
            raise ValueError(resp["retMsg"])
        else:
            raise ValueError("fail to transfer from exchange")

    @catch_it
    async def subaccount_transfer(
        self,
        from_acct_type: Literal["FUND", "UNIFIED", "CONTRACT"],
        to_acct_type: Literal["FUND", "UNIFIED", "CONTRACT"],
        from_user_id: str,
        to_user_id: str,
        ccy: str,
        amount: Decimal,
    ):
        resp = await self.client.subaccount_transfer(
            fromAccountType=from_acct_type,
            toAccountType=to_acct_type,
            coin=ccy,
            amount=str(amount),
            fromMemberId=int(from_user_id),
            toMemberId=int(to_user_id),
        )
        if isinstance(resp, dict) and resp["retCode"] == 0:
            return TransferResponse(apply_id=str(resp["result"]["transferId"]))
        elif resp:
            raise ValueError(resp["retMsg"])
        else:
            raise ValueError("fail to subaccount_transfer from exchange")

    @catch_it
    async def withdraw(
        self,
        transfer_type: Literal[0, 1, 2],
        address: str,
        ccy: str,
        amount: Decimal,
        chain: Optional[str] = None,
        vasp_entity_id: str = "others",
    ):
        """
        vaspEntityId: 接收方交易所id, 可用接口`/v5/asset/withdraw/vasp/list`查询
                      當提現至Upbit或者不在該列表內的平台時, 請使用vaspEntityId="others"
        """
        # TODO not really tested
        if transfer_type in [0, 1] and not chain:
            raise ValueError("on_chain withdraw `chain` param is needed")

        resp = await self.client.withdraw(
            address=address,
            amount=str(amount),
            coin=ccy,
            vaspEntityId=vasp_entity_id,
            chain=chain,
            forceChain=transfer_type,
        )
        if isinstance(resp, dict) and resp["retCode"] == 0:
            return WithdrawResponse(order_id=str(resp["result"]["id"]), status=WithdrawStatus.UNKNOWN)
        elif resp:
            raise ValueError(resp["retMsg"])
        else:
            raise ValueError("fail to withdraw from exchange")

    @catch_it
    async def get_withdraw_records(
        self,
        order_id: str,
        ccy: Optional[str] = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
    ):
        if not order_id:
            raise ValueError("get_withdraw_records `order_id` param is needed")
        resp = await self.client.withdraw_records(
            startTime=int(start_time) if start_time else None,
            endTime=int(end_time) if end_time else None,
            coin=ccy,
            withdrawID=str(order_id),
        )
        status_map = {
            "Pending": WithdrawStatus.PENDING,
            "BlockchainConfirmed": WithdrawStatus.PENDING,  # TODO not sure
            "SecurityCheck": WithdrawStatus.PENDING,  # TODO not sure
            "success": WithdrawStatus.SUCCESS,
            "Fail": WithdrawStatus.FAIL,
            "CancelByUser": WithdrawStatus.CANCELED,
            "MoreInformationRequired": WithdrawStatus.CANCELED,  # TODO not sure
            "Reject": WithdrawStatus.REJECT,
            "Unknown": WithdrawStatus.UNKNOWN,
        }
        if isinstance(resp, dict) and resp["retCode"] == 0:
            for record in resp["result"].get("rows", []):
                if str(record["withdrawId"]) == str(order_id):
                    return WithdrawResponse(
                        order_id=order_id, status=status_map.get(record["status"], WithdrawStatus.UNKNOWN)
                    )
        elif resp:
            raise ValueError(resp["retMsg"])
        else:
            raise ValueError("fail to withdraw from exchange")

    @catch_it
    async def get_subaccount_assets(self, user_id: str, coin: str = "USDT"):
        # 当accountType不是'FUND'时coin参数必传
        resp = await self.client.get_subaccount_assets(
            memberId=user_id,
            accountType=self.get_exch_account_type(),
            coin=coin,
        )
        result: dict[str, Balance] = {}
        if isinstance(resp, dict) and resp["retCode"] == 0:
            for info in resp["result"]["balance"]:
                balance = float(info["walletBalance"])
                if balance == 0:
                    continue

                result[info["coin"]] = Balance(
                    asset=info["coin"],
                    balance=balance,
                    free=float(info["transferBalance"]),
                    locked=balance - float(info["transferBalance"]),
                    ts=int(time.time() * 1000),
                )
            return Balances(result)
        elif resp:
            raise ValueError(resp["retMsg"])
        else:
            raise ValueError("fail to withdraw from exchange")

    @catch_it
    async def get_assets(self, from_redis: bool = False):
        result: dict[str, Balance] = {}
        data = None
        update_time = 0
        if from_redis:
            suffix = "raw:test"
            if self._market_type not in [MarketType.CPERP, MarketType.CDELIVERY]:
                key = "uta_balance"
            else:
                key = ""  # TODO add in redis
            assert key, f"Account type: {self._account_type} have no redis data"

            data = await self._load_data_from_rmx_acc(suffix, key)
            if not data:
                return Balances({})
        else:
            acct_type = "UNIFIED"
            resp = await self.client.get_balance(acct_type)
            if resp is not None and resp["retCode"] == 0:
                data = resp["result"]["list"]
                update_time = resp["time"]
            elif resp is not None:
                raise ValueError(resp.get("retMsg", ""))
            else:
                raise ValueError("fail to get assets from exchange(response is None)")
        if isinstance(data, list):
            for d in data:
                for info in d["coin"]:
                    if float(info["equity"]) == 0:
                        continue
                    result[info["coin"]] = Balance(
                        asset=info["coin"],
                        balance=float(info["equity"]),
                        free=float(info["equity"]),
                        borrowed=float(info["borrowAmount"]) if info["borrowAmount"] != "" else float(0),
                        locked=float(0),
                        type="full",
                        ts=update_time,
                    )
            return Balances(result)
        else:
            raise ValueError

    @catch_it
    async def set_swap_risk_limit(self, symbol: str, risk_limit_level: int) -> bool:
        if self._market_type is not MarketType.UPERP:
            raise ValueError(f"Market type {self._market_type} is not supported(only supported for UPERP)")
        symbol = symbol.upper()
        logger.debug(f"Change risk limit idx of {symbol} to {risk_limit_level}")
        resp = await self.client.set_risk_limit(category="linear", symbol=symbol, risk_id=risk_limit_level)

        if not (resp and resp.get("retCode") == 0):
            raise ValueError(resp)

        return True

    @catch_it
    async def get_price(self, symbol: str, from_redis: bool = False) -> float:
        if from_redis:
            if self._market_type in [MarketType.MARGIN, MarketType.SPOT]:
                std_symbol = symbol[:-4].upper() + "_USDT|SPOT|BYBIT"
            elif self._market_type == MarketType.UPERP:
                std_symbol = symbol[:-4].upper() + "_USDT|UPERP|BYBIT"
            elif self._market_type == MarketType.CPERP:
                std_symbol = symbol[:-3].upper() + "_USD|CPERP|BYBIT"
                raise ValueError(f"redis have no {self._market_type} price data")  # TODO add in redis
            elif self._market_type == MarketType.CDELIVERY:
                # TODO symbol eg: BTCUSDU24 -> std_symbol eg: BTC_USD_NQ|CDELIVERY|BYBIT
                # 反向交割:
                # BTCUSDH23 H: 第一季度; 23: 2023
                # BTCUSDM23 M: 第二季度; 23: 2023
                # BTCUSDU23 U: 第三季度; 23: 2023
                # BTCUSDZ23 Z: 第四季度; 23: 2023
                raise ValueError(f"redis have no {self._market_type} price data")  # TODO add in redis
            else:
                raise ValueError(f"Unknown account type {self._market_type}")

            data = await self._load_data_from_kit("ticker", std_symbol)
            return (float(data["apx"]) + float(data["bpx"])) / 2
        else:
            category = self.get_category()
            resp = await self.client.get_market_tickers(category, symbol)

            if not (resp and resp.get("retCode") == 0):
                raise ValueError(resp)

            return float(resp["result"]["list"][0]["lastPrice"])

    @catch_it
    async def get_prices(self) -> Prices:
        category = self.get_category()
        resp = await self.client.get_market_tickers(category)
        if not (resp and resp.get("retCode") == 0):
            raise ValueError(resp)

        result = {item["symbol"]: float(item["lastPrice"]) for item in resp["result"]["list"]}
        return Prices(result)

    @catch_it
    async def get_tickers(self) -> Tickers:
        tickers = await self.client.get_market_tickers(category=self.get_category())
        if tickers is None or not isinstance(tickers, dict):
            raise ValueError(f"fail to get tickers from exchange, resp: {tickers}")

        update_ts = float(time.time() * 1000)
        processed_tickers = {
            ticker["symbol"]: Ticker(
                ticker["symbol"],
                (float(ticker["bid1Price"]) if ticker["bid1Price"] else np.nan),
                (float(ticker["ask1Price"]) if ticker["ask1Price"] else np.nan),
                (float(ticker["indexPrice"]) if "indexPrice" in ticker else np.nan),
                ts=tickers["time"],
                update_ts=update_ts,
                fr=(float(ticker["fundingRate"]) if ticker.get("fundingRate") else np.nan),
                fr_ts=float(ticker["nextFundingTime"]) if ticker.get("nextFundingTime") else 0,
                bid_qty=(float(ticker["bid1Size"]) if ticker["bid1Size"] else np.nan),
                ask_qty=(float(ticker["ask1Size"]) if ticker["ask1Size"] else np.nan),
            )
            for ticker in tickers["result"]["list"]
        }
        return processed_tickers

    @catch_it
    async def get_quotations(self) -> Quotations:
        tickers = await self.client.get_market_tickers(category=self.get_category())
        if tickers is None or not isinstance(tickers, dict):
            raise ValueError(f"fail to get quotations from exchange, resp: {tickers}")

        update_ts = float(time.time() * 1000)
        quotations = {
            t["symbol"]: Quotation(
                exch_symbol=t["symbol"],
                bid=(float(t["bid1Price"]) if t["bid1Price"] else np.nan),
                ask=(float(t["ask1Price"]) if t["ask1Price"] else np.nan),
                ts=tickers["time"],
                update_ts=update_ts,
                bid_qty=(float(t["bid1Size"]) if t["bid1Size"] else np.nan),
                ask_qty=(float(t["ask1Size"]) if t["ask1Size"] else np.nan),
            )
            for t in tickers["result"]["list"]
        }
        return Quotations(quotations)

    @catch_it
    async def get_orderbook_snapshot(self, symbol: str, limit: int = 100) -> OrderBook:
        resp = await self.client.get_orderbook(self.get_category(), symbol, limit)
        if not resp or not (isinstance(resp, dict) and resp.get("retCode") == 0):
            raise ValueError(f"fail to get orderbook from exchange, resp: {resp}")
        orderbook = OrderBook(symbol)
        orderbook.exch_seq = int(resp["result"]["seq"])
        orderbook.exch_ts = int(resp["result"]["ts"])
        orderbook.recv_ts = int(time.time() * 1000)
        for bid in resp["result"]["b"]:
            orderbook.bids.append((Decimal(bid[0]), Decimal(bid[1])))
        for ask in resp["result"]["a"]:
            orderbook.asks.append((Decimal(ask[0]), Decimal(ask[1])))
        return orderbook

    @catch_it
    async def get_fundamentals(self) -> Fundamentals:
        resp = await self.client.get_market_tickers(category=self.get_category())
        if not resp or not (isinstance(resp, dict) and resp.get("retCode") == 0):
            raise ValueError(f"fail to get tickers from exchange, resp: {resp}")
        fundamentals: dict[str, Fundamental] = {}
        for ticker in resp["result"]["list"]:
            symbol = ticker["symbol"]
            if symbol not in self._insts:
                continue
            fundamentals[symbol] = Fundamental(
                symbol,
                float(ticker["price24hPcnt"]),
                float(ticker["turnover24h"]),
                (
                    float(ticker["openInterest"])
                    * float(self._insts[symbol].quantity_multiplier)
                    * float(ticker["lastPrice"])
                    if "openInterest" in ticker
                    else np.nan
                ),
            )
        return fundamentals

    @catch_it
    async def get_trade_history(self, start_time: int, end_time: int, symbol_list: list[str]):
        result: dict[str, list[Trade]] = {}
        next_cursor = None
        category = self.get_category()
        trade_data_list = []
        for symbol in symbol_list:
            while True:
                resp = await self.client.execution_list(
                    category,
                    symbol,
                    execType="Trade",
                    startTime=start_time,
                    endTime=end_time,
                    limit=100,
                    cursor=next_cursor,
                )

                if not (resp and resp.get("retCode") == 0):
                    raise ValueError(resp)

                next_cursor = resp["result"].get("nextPageCursor")
                trade_data_list += resp["result"]["list"]

                if not next_cursor or len(resp["result"]["list"]) < 100:
                    break

        for data in trade_data_list[::-1]:
            result.setdefault(data["symbol"], []).append(
                Trade(
                    create_ts=int(data["execTime"]),
                    side=getattr(OrderSide, data["side"].upper(), OrderSide.UNKNOWN),
                    trade_id=str(data["execId"]),
                    order_id=str(data["orderId"]),
                    last_trd_price=Decimal(data["execPrice"]),
                    last_trd_volume=Decimal(data["execQty"]),
                    turnover=Decimal(data["execPrice"]) * Decimal(data["execQty"]),
                    fill_ts=int(data["execTime"]),
                    fee=Decimal(data["execFee"]),
                    fee_ccy="",
                    is_maker=data["isMaker"],
                )
            )

        return TradeData(result)

    @catch_it
    async def get_order_history(self, start_time: int, end_time: int, symbol_list: list[str]) -> OrderSnapshotData:
        order_dict: dict[str, list[OrderSnapshot]] = {}
        order_data_list = []
        next_cursor = None

        while True:
            resp = await self.client.get_order_history(
                category=self.get_category(),
                execType="Trade",
                startTime=start_time,
                endTime=end_time,
                limit=50,
                cursor=next_cursor,
            )

            if not (resp and resp.get("retCode") == 0):
                logger.error(f"account[{self._account}] MarketType[{self._market_type}] error: {resp}")
                await asyncio.sleep(0.2)
                break

            next_cursor = resp["result"].get("nextPageCursor")
            order_data_list += resp["result"]["list"]

            if not next_cursor or len(resp["result"]["list"]) < 50:
                break

        for od in order_data_list:
            order_type = getattr(OrderType, od["orderType"].upper(), OrderType.UNKNOWN)
            tif = TIF_MAP.get(od["timeInForce"], TimeInForce.UNKNOWN)
            status = STATUS_MAP.get(od["orderStatus"], OrderStatus.UNKNOWN)
            side = getattr(OrderSide, od["side"].upper(), OrderSide.UNKNOWN)

            o = OrderSnapshot(
                place_ack_ts=int(od["createdTime"]),
                exch_symbol=od["symbol"],
                order_side=side,
                order_id=od["orderId"],
                client_order_id=od["orderLinkId"],
                price=Decimal(od["price"]) if od["price"] else Decimal(0),
                qty=Decimal(od["qty"]),
                filled_qty=Decimal(od["cumExecQty"]),
                avg_price=float(od["avgPrice"]) if od["avgPrice"] else 0.0,
                order_type=order_type,
                order_time_in_force=tif,
                order_status=status,
                exch_update_ts=int(od["updatedTime"]),
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
        assert MarketType.UPERP == self._market_type, f"Invalid Account Type {self._market_type}"
        funding_dict: dict[str, list[FundingFee]] = {}
        next_cursor = None
        raw_funding_dict = {}
        while True:
            resp = await self.client.execution_list(
                category="linear",
                execType="Funding",
                startTime=start_time,
                endTime=end_time,
                limit=100,
                cursor=next_cursor,
            )

            if not (resp and resp.get("retCode") == 0):
                raise ValueError(resp)

            next_cursor = resp["result"].get("nextPageCursor")
            dic = {item["execId"]: item for item in resp["result"]["list"]}
            raw_funding_dict.update(dic)

            if not next_cursor:
                break

        for item in raw_funding_dict.values():
            symbol = item["symbol"]
            if symbol_list and symbol not in symbol_list:
                continue

            if symbol in funding_dict:
                funding_dict[symbol].append(FundingFee(-Decimal(item["execFee"]), int(item["execTime"])))
            else:
                funding_dict[symbol] = [FundingFee(-Decimal(item["execFee"]), int(item["execTime"]))]

        return FundingFeeData(funding_dict)

    @catch_it
    async def get_historical_funding_rate(
        self,
        symbol_list: list[str],
        start_time: datetime | str | int | None = None,
        days: int = 7,
    ) -> FundingRatesHis:
        assert self._market_type in (MarketType.UPERP, MarketType.CPERP), f"Invalid Market type {self._market_type}"
        match self._market_type:
            case MarketType.UPERP:
                category = "linear"
            case MarketType.CPERP:
                category = "inverse"

        end_ts = int(time.time() * 1000)
        if isinstance(start_time, int):
            start_ts = start_time
        else:
            if start_time is None:
                start_time = datetime.now() - timedelta(days=days)
            elif isinstance(start_time, str):
                start_time = parser.parse(start_time)
            start_ts = int(start_time.timestamp() * 1000)

        frs: dict[str, set[FundingRateSimple]] = {}
        if not symbol_list:
            symbol_list = []
            resp = await self.client.get_instrument_info(category=category)

            if not (resp and resp.get("retCode") == 0):
                raise ValueError(resp)

            for info in resp["result"]["list"]:
                if info["quoteCoin"] == "USDT" and category == "linear":
                    symbol_list.append(info["symbol"])
                elif info["quoteCoin"] == "USD" and category == "inverse":
                    symbol_list.append(info["symbol"])

        for symbol in symbol_list:
            frs[symbol] = set()
            data_list = []
            symbol_end_ts = end_ts
            while True:
                resp = await self.client.get_history_funding_rate(
                    category, symbol, start_time=start_ts, end_time=symbol_end_ts
                )

                if not (resp and resp.get("retCode") == 0):
                    raise ValueError(resp)

                await asyncio.sleep(0.2)
                data = resp["result"]["list"]
                if data:
                    data_list.extend(data)
                    symbol_end_ts = int(data[-1]["fundingRateTimestamp"]) - 1
                else:
                    break
            for item in data_list:
                ts = float(item["fundingRateTimestamp"])
                frs[symbol].add(FundingRateSimple(funding_rate=float(item["fundingRate"]), funding_ts=ts))

        return FundingRatesHis({symbol: sorted(list(fr)) for symbol, fr in frs.items()})

    @catch_it
    async def get_equity(self) -> float:
        assets = await self.get_assets()
        price_resp = await self.get_prices()
        equity = 0
        if assets["status"] == 0 and price_resp["status"] == 0:
            price_dict = price_resp["data"]
            for coin, info in assets["data"].items():
                if info.balance == 0:
                    continue
                if price_dict.get(coin + "USDT"):
                    equity += info.balance * price_dict[coin + "USDT"]
                elif coin == "USDT":
                    equity += info.balance
        else:
            raise Exception(f"Failed to get assets or prices: {assets}, {price_resp}")
        return equity

    @catch_it
    async def get_discount_rate(self, ccy: str):
        resp = await self.client.get_collateral_info(ccy)
        if not (resp and resp.get("retCode") == 0):
            raise ValueError(resp)

        discount_info = resp["result"]["list"][0]
        discount_rate: list[DiscountRate] = [
            DiscountRate(min_amt=0, discount_rate=Decimal(discount_info["collateralRatio"]))
        ]
        return DiscountRateData(discount_rate)

    @catch_it
    async def set_symbol_leverage(self, symbol: str, leverage: int, **kwargs) -> bool:
        if self._market_type == MarketType.UPERP:
            logger.debug(f"Change leverage of {symbol} to {leverage}")
            symbol = symbol.upper()
            resp = await self.client.set_leverage(
                "linear", symbol, buy_leverage=str(leverage), sell_leverage=str(leverage)
            )
        elif self._market_type in [MarketType.CPERP, MarketType.CDELIVERY]:
            logger.debug(f"Change leverage of {symbol} to {leverage}")
            symbol = symbol.upper()
            resp = await self.client.set_leverage(
                "inverse", symbol, buy_leverage=str(leverage), sell_leverage=str(leverage)
            )
        else:
            logger.info(f"Account type {self._market_type} not support set leverage")
            return False

        if not (resp and resp.get("retCode") == 0):
            raise ValueError(resp)

        return True

    @catch_it
    async def cancel_order(self, symbol: str, orderId: Optional[str] = None, clientOid: Optional[str] = None) -> bool:
        assert orderId or clientOid, "Either Parameters `orderId` or `clientOid` is needed"
        resp = await self.client.cancel_order(self.get_category(), symbol, orderId, clientOid)

        if not (resp and resp.get("retCode") == 0):
            raise ValueError(resp)

        return True

    @catch_it
    async def cancel_all(self, symbol: str) -> bool:
        resp = await self.client.cancel_all_orders(self.get_category(), symbol)

        if not (resp and resp.get("retCode") == 0):
            raise ValueError(resp)

        return True

    @catch_it
    async def get_leverage(self, symbol: str, mgnMode: MarginMode) -> Leverage:
        assert self._market_type in [
            MarketType.UPERP,
            MarketType.CPERP,
            MarketType.CDELIVERY,
        ], f"Market type {self._market_type} is not supported(only supported for UPERP,CPERP,CDELIVERY)"
        leverage = Leverage()
        resp = await self.client.get_position(self.get_category(), symbol)  # type: ignore
        if not (resp and resp.get("retCode") == 0):
            raise ValueError(resp)

        if isinstance(resp["result"]["list"], list):
            for data in resp["result"]["list"]:
                if data["symbol"] == symbol:
                    if (mgnMode == mgnMode.ISOLATED and data["tradeMode"] == 1) or (
                        mgnMode == mgnMode.CROSS and data["tradeMode"] == 0
                    ):
                        if data["side"] == "Sell":
                            leverage.long = Decimal(data["leverage"])
                        elif data["side"] == "Buy":
                            leverage.short = Decimal(data["leverage"])
                        else:
                            leverage.long = Decimal(data["leverage"])
                            leverage.short = Decimal(data["leverage"])

        if not (leverage.long or leverage.short):
            raise ValueError(f"fail to get leverage for symbol[{symbol}] mgnMode[{mgnMode}]")

        return leverage

    @catch_it
    async def get_max_open_notional(self, symbol: str, mgnMode: MarginMode = MarginMode.CROSS):
        if self._market_type != MarketType.UPERP:
            raise UnsupportedOperationError("Bybit SPOT cannot get max open notional")

        max_open_notional = MaxOpenNotional()
        resp = await self.client.get_position("linear", symbol)
        if not (resp and resp.get("retCode") == 0):
            raise ValueError(resp)

        if isinstance(resp["result"]["list"], list):
            for data in resp["result"]["list"]:
                if data["symbol"] == symbol:
                    if (mgnMode == mgnMode.ISOLATED and data["tradeMode"] == 1) or (
                        mgnMode == mgnMode.CROSS and data["tradeMode"] == 0
                    ):
                        if data["side"] == "Sell":
                            max_open_notional.sell = Decimal(data["riskLimitValue"])
                        elif data["side"] == "Buy":
                            max_open_notional.buy = Decimal(data["riskLimitValue"])
                        else:
                            max_open_notional.buy = Decimal(data["riskLimitValue"])
                            max_open_notional.sell = Decimal(data["riskLimitValue"])

        if not (max_open_notional.buy or max_open_notional.sell):
            raise ValueError(f"fail to get max open notional for symbol[{symbol}] mgnMode[{mgnMode}]")

        return max_open_notional

    @catch_it
    async def get_current_funding_rate(self, symbol_list: list[str] | None = None) -> FundingRatesCur:
        assert self._market_type == MarketType.UPERP, "only support get current funding rate for UPERP"
        category = self.get_category()
        resp = await self.client.get_current_funding_rate(category=category)
        if not (resp and resp.get("retCode") == 0):
            raise ValueError(resp)

        funding_rates_dict = {
            d["symbol"]: ((float(d["fundingRate"]), int(d["nextFundingTime"])) if d["fundingRate"] else (0, 0))
            for d in resp["result"]["list"]
        }

        if not symbol_list:
            symbol_list = list(funding_rates_dict.keys())

        funding_times_dict: dict[str, tuple[int, float, float]] = {}
        cursor = None
        while True:
            resp = await self.client.get_instrument_info(self.get_category(), cursor=cursor)
            if not (resp and resp.get("retCode") == 0):
                logger.info(f"资金费率获取interval info失败: {resp}")
                break

            for info in resp["result"]["list"]:
                funding_times_dict[info["symbol"]] = (
                    int(info["fundingInterval"]),
                    float(info["upperFundingRate"]),
                    float(info["lowerFundingRate"]),
                )

            if resp["result"].get("nextPageCursor", "") == "":
                break

            cursor = resp["result"]["nextPageCursor"]

        frs: FundingRatesCur = FundingRatesCur()
        for symbol in symbol_list:
            fr, ts = funding_rates_dict.get(symbol, (0, 0))
            interval, fr_cap, fr_floor = funding_times_dict.get(symbol, (480, np.nan, np.nan))
            interval = int(interval / 60)
            frs[symbol] = FundingRate(fr, ts, interval_hour=interval, fr_cap=fr_cap, fr_floor=fr_floor)

        return frs

    @catch_it
    async def get_current_simple_funding_rate(self, symbol_list: list[str] | None = None) -> FundingRatesSimple:
        assert self._market_type == MarketType.UPERP, "only support get current funding rate for UPERP"
        category = self.get_category()
        resp = await self.client.get_current_funding_rate(category=category)
        if not (resp and resp.get("retCode") == 0):
            raise ValueError(resp)

        frs: FundingRatesSimple = FundingRatesSimple()
        for fr_info in resp["result"]["list"]:
            exch_symbol = fr_info["symbol"]
            if symbol_list and exch_symbol not in symbol_list:
                continue
            if fr_info["fundingRate"]:
                frs[exch_symbol] = FundingRateSimple(float(fr_info["fundingRate"]), int(fr_info["nextFundingTime"]))
            else:
                frs[exch_symbol] = FundingRateSimple(0, 0)

        return frs

    @catch_it
    async def get_commission_rate(self, symbol: str, from_redis: bool = False) -> CommissionRate:
        category = self.get_category()
        if from_redis:
            assert self._account, "Account type is needed to get commission rate from redis"
            data = await self._load_data_from_rmx("trading_fee:bybit", key=self._account)
            if not data:
                raise ValueError(f"Could not get current commission rate from redis for symbol[{symbol}]")

            if self._market_type in [MarketType.SPOT, MarketType.MARGIN]:
                makerfee = data["spot_maker"]
                takerfee = data["spot_taker"]
            else:
                makerfee = data["swap_maker"]
                takerfee = data["swap_taker"]
        else:
            resp = await self.client.get_commission_rate(category=category, symbol=symbol)
            if not (resp and resp.get("retCode") == 0):
                raise ValueError(resp)

            if not resp["result"]["list"]:
                raise ValueError(f"Could not get current commission rate for symbol[{symbol}]")

            takerfee = resp["result"]["list"][0]["takerFeeRate"]
            makerfee = resp["result"]["list"][0]["makerFeeRate"]

            await asyncio.sleep(0.25)

        return CommissionRate(maker=Decimal(str(makerfee)), taker=Decimal(str(takerfee)))

    @catch_it
    async def get_account_vip_level(self) -> str | int:
        resp = await self.client.get_vip_level()
        if not (resp and resp.get("retCode") == 0):
            raise ValueError(resp)

        return resp.get("result", {}).get("vipLevel")

    def get_interval(self, interval: Interval):
        interval_str = interval.name.lstrip("_")
        if interval_str[-1] == "h":
            interval_num = int(interval_str[:-1]) * 60
        elif interval_str[-1] == "m":
            interval_num = int(interval_str[:-1])
        elif interval == "3d" or interval_str[-1] == "s":
            interval_num = -1
        else:
            interval_num = interval_str[-1].upper()
        return str(interval_num)

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
        interval_str = self.get_interval(interval)
        try:
            if interval_str == -1:
                raise ValueError("unsupported interval in Bybit")
        except ValueError as v:
            traceback.format_exc()

        data_list: list[Any] = []
        category = self.get_category()
        if end_time is None:
            end_time = int(time.time() * 1000)
        end_time_origin = end_time
        while True:
            resp = await self.client.get_market_kline(
                category, symbol, start_time=start_time, end_time=end_time, interval=interval_str, limit=1000  # type: ignore
            )
            if not (resp and resp.get("retCode") == 0):
                raise ValueError(resp)

            if not resp["result"]["list"]:
                break

            data_list += resp["result"]["list"]

            if len(resp["result"]["list"]) != 1000:
                break

            end_time = int(resp["result"]["list"][-1][0]) - 1
            await asyncio.sleep(0.25)

        for lis in data_list[::-1]:
            if not start_time < int(lis[0]) < end_time_origin:
                continue

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

    def get_period(self, interval: Interval) -> Literal["5min", "15min", "30min", "1h", "4h", "1d"]:
        period_str = interval.name.lstrip("_")
        if period_str[-1] == "m":
            period_str = period_str + "in"
        return period_str  # type: ignore

    @catch_it
    async def get_long_short_ratio(self, symbol: str, limit: int, interval: Interval):
        assert self._market_type == MarketType.UPERP, f"Invalid account type {self._market_type}, only support UPERP"
        assert interval in [
            Interval._5m,
            Interval._15m,
            Interval._30m,
            Interval._1h,
            Interval._4h,
            Interval._1d,
        ], f"Invalid interval {interval.name}"
        assert not symbol.endswith("PERP"), f"Invalid symbol{symbol}"
        period_str = self.get_period(interval)
        resp = await self.client.get_long_short_ratio("linear", symbol, period_str, limit)
        if not (resp and resp.get("retCode") == 0):
            raise ValueError(resp)

        lis = [
            LongShortRatio(
                long_short_ratio=round(Decimal(data["buyRatio"]) / Decimal(data["sellRatio"]), 4),
                ts=int(data["timestamp"]),
            )
            for data in resp["result"]["list"]
        ]
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
    ) -> OrderSnapshot:
        if isinstance(order_time_in_force, str):
            order_time_in_force = TimeInForce[order_time_in_force]
        if isinstance(order_side, str):
            order_side = OrderSide[order_side]
        if isinstance(order_type, str):
            order_type = OrderType[order_type]
        if not client_order_id:
            client_order_id = "xclients" + str(int(time.time() * 1000000))
        send_order_side = "Buy" if order_side == OrderSide.BUY else "Sell"
        send_order_type = "Limit" if order_type == OrderType.LIMIT else "Market"
        category = self.get_category()

        # 市价单不需要price参数
        if order_type == OrderType.MARKET and price is not None:
            raise ValueError("In market_order parameter price not required")
        # 市价单不需要 timeInForce
        if order_type == OrderType.MARKET:
            send_time_in_force = None
        else:
            # 限价单的 timeInForce 逻辑
            send_time_in_force = (
                "PostOnly"
                if TimeInForce.GTX == order_time_in_force
                else order_time_in_force.name if order_time_in_force else None
            )

        # 从 extras 中提取 quote_qty
        params = extras or {}
        quote_qty = params.pop("quote_qty", None)

        # 验证 qty 和 quote_qty 互斥
        if qty is None and quote_qty is None:
            raise ValueError("Either qty or quote_qty must be specified")
        if qty is not None and quote_qty is not None:
            raise ValueError("qty and quote_qty are mutually exclusive")

        # 验证 quote_qty 只能用于市价单
        # use_base_qty 表示市价单是使用 baseCoin(True) 还是 quoteCoin(False)
        use_base_qty = True
        if quote_qty is not None and order_type != OrderType.MARKET:
            raise ValueError("quote_qty is only supported for MARKET orders")
        elif quote_qty is not None and order_type == OrderType.MARKET:
            qty = quote_qty
            use_base_qty = False

        if category != "spot" and not use_base_qty:
            raise ValueError("only 'spot' allow quote_qty")

        # TODO: margin_trade_mode
        is_leverage = 1 if self._market_type == MarketType.MARGIN else 0
        if order_type == OrderType.MARKET:
            resp = await self.client.v5_order_create(
                category,
                symbol,
                send_order_side,
                send_order_type,
                str(qty),
                str(price) if price else None,
                timeInForce=send_time_in_force,
                orderLinkId=client_order_id,
                isLeverage=is_leverage,
                reduceOnly=reduce_only,
                marketUnit="baseCoin" if use_base_qty else "quoteCoin",
                **params,
            )
        else:
            resp = await self.client.v5_order_create(
                category,
                symbol,
                send_order_side,
                send_order_type,
                str(qty),
                str(price) if price else None,
                timeInForce=send_time_in_force,
                orderLinkId=client_order_id,
                isLeverage=is_leverage,
                reduceOnly=reduce_only,
                **params,
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

        if not resp:
            snapshot.order_status = OrderStatus.REJECTED
            snapshot.rejected_message = "No response from exchange"
        elif resp["retCode"] != 0:
            snapshot.order_status = OrderStatus.REJECTED
            snapshot.rejected_message = resp["retMsg"]
        else:
            snapshot.order_id = resp["result"]["orderId"]
            snapshot.order_status = OrderStatus.LIVE
            snapshot.place_ack_ts = snapshot.local_update_ts
            snapshot.exch_update_ts = resp["time"]
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
            params["timeInForce"] = order_time_in_force.ccxt

        if reduce_only:
            params["reduceOnly"] = reduce_only

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
            await self.ccxt_client.cancel_order(order_id or "", symbol, params=params)
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
            # ccxt warning: bybit fetchOrder() can only access an order if it is in last 500 orders(of any status) for your account.
            # Set params["acknowledged"] = True to hide self warning.
            # Alternatively, we suggest to use fetchOpenOrder or fetchClosedOrder
            params["acknowledged"] = True

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
    async def set_account_margin_mode(self, mode: MarginMode):
        logger.info(f"Setting margin mode to {mode} for Bybit account")
        if mode != MarginMode.CROSS:
            logger.error(f"当前不支持设置保证金模式: {mode}")
            return
        resp = await self.client.set_margin_mode("REGULAR_MARGIN")
        if not (resp and resp.get("retCode") == 0):
            raise ValueError(resp)

    @catch_it
    async def set_account_leverage(self, leverage: int):
        logger.info(f"Setting leverage to {leverage} for Bybit account")
        if leverage <= 0:
            logger.error(f"无效的杠杆倍数: {leverage}")
            return
        if self._market_type.is_derivative:
            category: Literal["linear", "inverse"] = self.get_category()  # type: ignore
            positions_info = await self.client.get_position(category)
            positions_leverages = {}
            if positions_info:
                for position_info in positions_info["result"]["list"]:
                    try:
                        leverage = int(position_info["leverage"])
                    except:
                        logger.warning(f"获取杠杆失败: {position_info['symbol']}, {position_info}")
                        continue
                    positions_leverages[position_info["symbol"]] = leverage
            for symbol, current_leverage in positions_leverages.items():
                if leverage != current_leverage:
                    await self.client.set_leverage(category, symbol, str(leverage), str(leverage))
                    await asyncio.sleep(0.1)
        else:
            await self.client.uta_spot_margin_leverage(str(leverage))

    @catch_it
    async def set_uta_mode(self):
        logger.info("Setting UTA mode for Bybit account")
        resp = await self.client.get_account_config()
        if not (resp and resp.get("retCode") == 0):
            logger.critical("获取账户UTA状态失败")
            raise ValueError(resp)

        if (uta_status := resp["result"]["unifiedMarginStatus"]) == 6:
            logger.info("账户为UTA2.0 Pro")
        elif uta_status == 3:
            logger.critical("账户当前为UTA1.0, 无法通过API升级")
        else:
            resp = await self.client.upgrade_to_uta()
            try:
                if resp is not None and resp["result"]["unifiedUpdateStatus"] == "SUCCESS":
                    logger.info("账户升级为UTA2.0 Pro成功")
                elif resp is not None:
                    logger.critical(f"账户升级为UTA2.0 Pro失败: {resp['result']['unifiedUpdateMsg']['msg']}")
                else:
                    logger.critical("账户升级为UTA2.0 Pro失败")
            except Exception:
                logger.critical(f"账户升级为UTA2.0 Pro失败: {resp}")

    @catch_it
    async def enable_margin_trading(self):
        logger.info("Enabling margin trading for Bybit account")
        resp = await self.client.uta_spot_margin_switch("1")
        if not (resp and resp.get("retCode") == 0):
            raise ValueError(resp)

    @catch_it
    async def enable_account_collaterals(self):
        logger.info("Enabling account collaterals for Bybit account")
        resp = await self.client.get_collateral_info()
        if not (resp and resp.get("retCode") == 0):
            raise ValueError(resp)

        collateral_info = resp["result"]["list"]
        for info in collateral_info:
            if info["collateralSwitch"] is False:
                logger.warning(f"账户{info['currency']}的抵押开关未打开")
                await self.client.set_collateral_switch(info["currency"], True)
                await asyncio.sleep(0.5)

    @catch_it
    async def get_collateral_ratio(self) -> CollateralRatios:
        crs: list[CollateralRatio] = []
        if self._account_config.extra_params.get("has_loan", False):
            resp = await self.client.ensure_tokens_convert()
            if not (resp and resp.get("retCode") == 0):
                raise ValueError(resp)

            for i in resp["result"]["marginToken"][0]["tokenInfo"]:
                asset = i["token"]
                cr = {float(j["ladder"].split("-")[0]): float(j["convertRatio"]) for j in i["convertRatioList"]}
                crs.append(CollateralRatio(asset, cr))

        if not self._account_config.api_key:
            return CollateralRatios(crs)

        resp = await self.client.get_collateral_info()
        if not (resp and resp.get("retCode") == 0):
            raise ValueError(resp)

        for i in resp["result"]["list"]:
            asset = i["currency"]
            ratio = float(i["collateralRatio"])
            for cr in crs:
                if cr.asset == asset:
                    for k, v in cr.cr.items():
                        if ratio < v:
                            cr.cr[k] = ratio
                    break
            else:
                crs.append(CollateralRatio(asset, {0: ratio}))

        return CollateralRatios(crs)

    @catch_it
    async def get_account_info(self) -> AccountInfo:
        resp = await self.client.get_balance("UNIFIED")
        if not (resp and resp.get("retCode") == 0):
            raise ValueError(resp)

        result = resp["result"]["list"][0]
        equity = float(result["totalEquity"])
        for coin in result["coin"]:
            if coin["coin"] == "USDT":
                usdt_free = float(coin["equity"])
                usdt_borrowed = float(coin["borrowAmount"])
                break
        else:
            usdt_free = 0
            usdt_borrowed = 0

        available_balance = float(result["totalAvailableBalance"])
        margin_balance = float(result["totalMarginBalance"])
        maintenance_margin = float(result["totalMaintenanceMargin"])
        if maintenance_margin > 0:
            mmr = margin_balance / maintenance_margin
        else:
            mmr = 999

        if (raw_imr := float(result["accountIMRate"])) != 0:
            imr = 1 / raw_imr
        else:
            imr = 999

        if self._market_type in [MarketType.SPOT, MarketType.MARGIN]:
            if self._account_config.extra_params.get("has_loan", False):
                ltv_info = await self.client.get_ltv()
                if (
                    ltv_info
                    and ltv_info.get("retCode") == 0
                    and (raw_ltv := float(ltv_info["result"]["ltvInfo"][0]["ltv"]))
                ):
                    ltv = 1 - raw_ltv
                    margin_balance = min(margin_balance, float(ltv_info["result"]["ltvInfo"][0]["balance"]))
                else:
                    raise Exception(f"获取质押率失败 返回值：{ltv_info}")
            else:
                ltv = 999
        else:
            ltv = 999

        return AccountInfo(
            account=self._account_meta,
            equity=equity,
            usdt_free=usdt_free,
            imr=imr,
            mmr=mmr,
            ltv=ltv,
            available_balance=available_balance,
            margin_balance=margin_balance,
            usdt_borrowed=usdt_borrowed,
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
        assert vip_level in [
            None,
            "VIP0",
            "VIP1",
            "VIP2",
            "VIP3",
            "VIP4",
            "VIP5",
            "VIP99",
            "PRO1",
            "PRO2",
            "PRO3",
            "PRO4",
            "PRO5",
            "PRO6",
        ], f"Invalid vip_level:{vip_level}"
        interest_type_maps = {
            -1: "flexibleAnnualizedInterestRate",
            7: "annualizedInterestRate7D",
            14: "annualizedInterestRate14D",
            30: "annualizedInterestRate30D",
            60: "annualizedInterestRate60D",
            90: "annualizedInterestRate90D",
            180: "annualizedInterestRate180D",
        }
        assert days in interest_type_maps, f"Invalid days param {days}, only support {list(interest_type_maps.keys())}"

        interest_rates: list[InterestRate] = []
        resp = await self.client.get_loanable_data(currency=asset, vipLevel=str(vip_level) if vip_loan else None)
        if not (resp and resp.get("retCode") == 0):
            raise ValueError(resp)

        for info in resp["result"]["list"]:
            if vip_level and info["vipLevel"] != vip_level:
                continue
            currency = info["currency"]
            if asset and currency != asset:
                continue
            interest_rates.append(
                InterestRate(
                    asset=currency,
                    days=days,
                    ir=(
                        Decimal(info[interest_type_maps[days]]) / 365
                        if info[interest_type_maps[days]]
                        else Decimal(-1)
                    ),
                    ts=float(resp["time"]),
                )
            )
        return interest_rates

    @catch_it
    async def get_margin_interest_rates_cur(
        self,
        vip_level: int | str | None = None,
        asset: str | None = "",
    ):
        assert asset, "param `asset` is empty"
        assert self._market_type == MarketType.MARGIN, f"Invalid Market type {self._market_type}, only support MARGIN"

        resp = await self.client.get_margin_trade_data(currency=asset, vipLevel=str(vip_level) if vip_level else None)
        if not (resp and resp.get("retCode") == 0):
            raise ValueError(resp)

        interest_rates: list[InterestRate] = []
        if resp.get("result") is None:
            # None result eg: {'retCode': 0, 'retMsg': 'success', 'result': None, 'retExtInfo': '{}', 'time': 1756888587571}
            return interest_rates
        for info in resp.get("result", {}).get("vipCoinList", []):
            if vip_level and info["vipLevel"] != vip_level:
                continue
            for ir_info in info["list"]:
                coin = ir_info["currency"]
                if coin != asset or not ir_info["borrowable"]:
                    continue
                interest_rates.append(
                    InterestRate(
                        asset=asset,
                        vip_level=info["vipLevel"],
                        ir=Decimal(ir_info["hourlyBorrowRate"]) * 24,
                        ts=time.time() * 1000,
                    )
                )
        return interest_rates

    @catch_it
    async def get_margin_interest_rates_his(
        self,
        vip_level: int | str | None = None,
        asset: str | None = "",
        start_time: int | None = None,
        end_time: int | None = None,
    ) -> InterestRates:
        assert asset, "param `asset` is empty"
        assert self._market_type == MarketType.MARGIN, f"Invalid Market type {self._market_type}, only support MARGIN"
        window_size = 30 * 24 * 60 * 60 * 1000  # 最多支持30天的時間跨度
        if not end_time:
            end_time = int(datetime.now().timestamp() * 1000)
        if not start_time:
            start_time = end_time - 30 * 24 * 60 * 60 * 1000
        else:
            # 可以查詢最多過去6個月的借貸利率數據
            start_time = max([start_time, int(time.time() - 179 * 24 * 60 * 60) * 1000])
        interest_rates: list[InterestRate] = []
        data_list: list[dict[str, str]] = []
        tmp_e_time = end_time
        tmp_s_time = end_time - window_size if end_time - start_time > window_size else start_time
        while True:
            resp = await self.client.get_margin_interest_history(
                currency=asset,
                vipLevel=str(vip_level) if vip_level else None,
                startTime=tmp_s_time,
                endTime=tmp_e_time,
            )
            if not (resp and resp.get("retCode") == 0):
                raise ValueError(resp)
            await asyncio.sleep(1)
            data = resp.get("result", {}).get("list", [])
            if data:
                data_list.extend(data)
            else:
                break
            if tmp_s_time <= start_time:
                break
            tmp_e_time = tmp_s_time - 1
            tmp_s_time -= window_size
            if tmp_s_time < start_time:
                tmp_s_time = start_time

        for info in data_list:
            ccy = info["currency"]
            if asset and ccy != asset:
                continue
            interest_rates.append(
                InterestRate(
                    asset=ccy,
                    vip_level=str(info["vipLevel"]),
                    ir=Decimal(info["hourlyBorrowRate"]) * 24,
                    ts=float(info["timestamp"]),  # ms
                )
            )
        return interest_rates
