from decimal import Decimal
import traceback
from typing import Optional, Union, Any, Literal, Union
from collections import defaultdict
from datetime import datetime, timedelta
from dateutil import parser
from loguru import logger
import copy
import time
import asyncio
from .rest import BitgetSpotRestClient, BitgetFutureRestClient
from ..base_wrapper import BaseRestWrapper, catch_it
from ..enum_type import (
    TimeInForce,
    Interval,
    OrderSide,
    MarginMode,
    OrderStatus,
    OrderType,
)
from ..get_client import get_rest_client
from ..data_type import *
from .constants import STATUS_MAP, TIF_MAP
import ccxt.async_support as ccxt
from ccxt.base.types import Order as ccxtOrder, ConstructorArgs


class BitgetRestWrapper(BaseRestWrapper):
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
            "password": self._account_config.passphrase,  # Bitget需要passphrase
            "enableRateLimit": True,
            "options": {
                "defaultType": ccxt_default_type,
                "defaultSubType": ccxt_default_sub_type,
            },
        }

        self.ccxt_client = ccxt.bitget(ConstructorArgs(ccxt_params))

    def get_product_type(self):
        if MarketType.UPERP == self._market_type:
            return "USDT-FUTURES"
        if MarketType.CPERP == self._market_type:
            return "COIN-FUTURES"
        else:
            return ""

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
        assert from_market_type and to_market_type, "bitget 没有资金账户, 只支持使用market_type进行转账"
        if isinstance(from_market_type, str):
            from_market_type = MarketType[from_market_type]
        if isinstance(to_market_type, str):
            to_market_type = MarketType[to_market_type]
        market_type_dict: dict[MarketType, Literal["spot", "usdt_futures", "crossed_margin", "coin_futures"]] = {
            MarketType.SPOT: "spot",
            MarketType.UPERP: "usdt_futures",
            MarketType.MARGIN: "crossed_margin",
            MarketType.CPERP: "coin_futures",
        }
        if self._market_type.is_derivative:
            account_meta = copy.deepcopy(self._account_meta)
            account_meta.market_type = MarketType.SPOT
            sp_client: BitgetSpotRestClient = get_rest_client(account_meta, self._account_config, self._rest_config)
        else:
            sp_client: BitgetSpotRestClient = self.client
        resp = await sp_client.transfer(
            fromType=market_type_dict[from_market_type],
            toType=market_type_dict[to_market_type],
            coin=asset,
            amount=str(qty),
        )
        if isinstance(resp, dict) and resp["code"] == "00000":
            return TransferResponse(apply_id=str(resp["data"]["transferId"]))
        elif resp:
            raise ValueError(resp["msg"])
        else:
            raise ValueError("fail to transfer from exchange")

    @catch_it
    async def subaccount_transfer(
        self,
        from_market_type: Union[str, MarketType],
        to_market_type: Union[str, MarketType],
        from_user_id: str,
        to_user_id: str,
        ccy: str,
        amount: Decimal,
    ):
        if isinstance(from_market_type, str):
            from_market_type = MarketType[from_market_type]
        if isinstance(to_market_type, str):
            to_market_type = MarketType[to_market_type]
        market_type_dict: dict[MarketType, Literal["spot", "usdt_futures", "crossed_margin", "coin_futures"]] = {
            MarketType.SPOT: "spot",
            MarketType.UPERP: "usdt_futures",
            MarketType.MARGIN: "crossed_margin",
            MarketType.CPERP: "coin_futures",
        }

        cli: BitgetSpotRestClient = self.client
        resp = await cli.subaccount_transfer(
            fromType=market_type_dict[from_market_type],
            toType=market_type_dict[to_market_type],
            fromUserId=from_user_id,
            toUserId=to_user_id,
            amount=str(amount),
            coin=ccy,
        )
        if isinstance(resp, dict) and resp["code"] == "00000":
            return TransferResponse(apply_id=str(resp["data"]["transferId"]))
        elif resp:
            raise ValueError(resp["msg"])
        else:
            raise ValueError("fail to subaccount_transfer from exchange")

    @catch_it
    async def withdraw(
        self,
        transfer_type: Literal["on_chain", "internal_transfer"],
        address: str,
        ccy: str,
        amount: Decimal,
        chain: Optional[str] = None,
    ):
        # TODO not really tested
        if transfer_type == "on_chain":
            if not chain:
                raise ValueError("on_chain withdraw `chain` param is needed")

        cli: BitgetSpotRestClient = self.client
        resp = await cli.withdraw(
            transferType=transfer_type,
            address=address,
            coin=ccy,
            size=str(amount),
            chain=chain,
        )
        if isinstance(resp, dict) and resp["code"] == "00000":
            return WithdrawResponse(order_id=str(resp["data"]["orderId"]), status=WithdrawStatus.UNKNOWN)
        elif resp:
            raise ValueError(resp["msg"])
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

        cli: BitgetSpotRestClient = self.client
        resp = await cli.withdraw_records(
            startTime=int(start_time) if start_time else None,
            endTime=int(end_time) if end_time else None,
            coin=ccy,
            orderId=str(order_id),
        )
        status_map = {
            "pending": WithdrawStatus.PENDING,
            "fail": WithdrawStatus.FAIL,
            "success": WithdrawStatus.SUCCESS,
        }
        if isinstance(resp, dict) and resp["code"] == "00000":
            for record in resp["data"]:
                if str(record["orderId"]) == str(order_id):
                    return WithdrawResponse(
                        order_id=record["orderId"], status=status_map.get(record["status"], WithdrawStatus.UNKNOWN)
                    )
        elif resp:
            raise ValueError(resp["msg"])
        else:
            raise ValueError("fail to get withdraw_records from exchange")

    def get_other_client(self, market_type: MarketType):
        other_account_meta = AccountMeta(
            exch_name=self._exchange, market_type=market_type, account_type=self._account_type
        )
        return get_rest_client(other_account_meta, self._account_config, self._rest_config)

    @catch_it
    async def get_equity(self) -> float:
        if isinstance(self.client, BitgetSpotRestClient):
            sp_client: BitgetSpotRestClient = self.client
            lps_client: BitgetFutureRestClient = self.get_other_client(MarketType.UPERP)
        else:
            sp_client: BitgetSpotRestClient = self.get_other_client(MarketType.SPOT)
            lps_client: BitgetFutureRestClient = self.client
        sp_assets_resp, lps_assets_resp, price_resp = await asyncio.gather(
            sp_client.get_assets(),
            lps_client.get_accounts("USDT-FUTURES"),
            self.get_prices(),
        )

        if not (sp_assets_resp and lps_assets_resp and price_resp):
            raise ValueError("Failed to get response")

        if sp_assets_resp["code"] != "00000":
            raise ValueError(sp_assets_resp["msg"])

        if lps_assets_resp["code"] != "00000":
            raise ValueError(lps_assets_resp["msg"])

        if price_resp["status"] != 0:
            raise ValueError(price_resp["msg"])

        balances = defaultdict(float)
        for b in sp_assets_resp.get("data", []):
            balances[b["coin"]] += float(b["available"])
            balances[b["coin"]] += float(b["frozen"])
            balances[b["coin"]] += float(b["locked"])

        for b in lps_assets_resp.get("data", []):
            balances[b["marginCoin"]] += float(b["available"])
            balances[b["marginCoin"]] += float(b["unrealizedPL"])
            for a in b.get("assetList", []):
                balances[a["coin"]] += float(a["balance"])

        equity = 0
        prices = price_resp["data"]
        for coin, qty in balances.items():
            symbol = coin + "USDT"
            if symbol in prices:
                equity += qty * prices[symbol]
            elif coin == "USDT":
                equity += qty
            else:
                logger.warning(f"Could not get price for {symbol}")

        return equity

    @catch_it
    async def get_positions(self, from_redis: bool = False):
        if self._market_type not in [MarketType.UPERP]:
            raise ValueError(f"Market type {self._market_type} is not supported(only supported for UPERP)")

        result: dict[str, Position] = {}
        data = None

        if from_redis:
            suffix = "raw:test"
            if MarketType.UPERP == self._market_type:
                key = "u_contract_position"
            else:
                raise ValueError(f"Market type {self._market_type} have no positions api")

            data = await self._load_data_from_rmx_acc(suffix, key)
        else:
            if MarketType.UPERP == self._market_type:
                cli: BitgetFutureRestClient = self.client
                resp = await cli.get_positions(self.get_product_type(), "USDT")
                if resp is not None:
                    if resp["code"] == "00000":
                        data = resp["data"]
                    else:
                        raise ValueError(resp["msg"])

        if isinstance(data, list):
            for info in data:
                if float(info["total"]) != 0:
                    result[info["symbol"]] = Position(
                        exch_symbol=info["symbol"],
                        net_qty=float(info["total"]) if info["holdSide"] == "long" else -float(info["total"]),
                        entry_price=float(info["openPriceAvg"]),
                        value=float(info["total"]) * float(info["markPrice"]),
                        liq_price=float(info["liquidationPrice"]),
                        unrealized_pnl=float(info["unrealizedPL"]),
                        ts=int(time.time() * 1000),
                    )
            return Positions(result)
        else:
            raise ValueError("unknown error")

    @catch_it
    async def get_assets(self, from_redis: bool = False):
        if MarketType.UPERP == self._market_type:
            rtn = await self.get_lps_assets(from_redis)
        elif self._market_type in [MarketType.SPOT, MarketType.MARGIN]:
            rtn = await self.get_sp_assets(from_redis)
        else:
            raise ValueError(f"Market type {self._market_type} is not supported")
        if (data := rtn.get("data")) is not None:
            return data
        else:
            raise ValueError(rtn.get("msg", "unknown error"))

    @catch_it
    async def get_sp_assets(self, from_redis: bool = False):
        result: dict[str, Balance] = {}
        if from_redis:
            suffix = "raw:test"
            key = "spot_balance"
            data = await self._load_data_from_rmx_acc(suffix, key)
        else:
            cli: BitgetSpotRestClient = self.client
            resp = await cli.get_assets()
            if not resp:
                raise ValueError("unknown error")

            if resp["code"] != "00000":
                raise ValueError(resp["msg"])

            data = resp["data"]

        for info in data:
            balance = float(info["available"]) + float(info["locked"]) + float(info["frozen"])
            if balance == 0:
                continue
            result[info["coin"]] = Balance(
                asset=info["coin"],
                balance=balance,
                free=float(info["available"]),
                locked=float(info["locked"]) + float(info["frozen"]),
                type="full",
                ts=int(info["uTime"]) if info["uTime"] else 0,  # raw data may have 'uTime': None
            )
        return Balances(result)

    @catch_it
    async def get_lps_assets(self, from_redis: bool = False):
        result: dict[str, Balance] = {}
        if from_redis:
            suffix = "raw:test"
            key = "u_contract_balance"
            data = await self._load_data_from_rmx_acc(suffix, key)
        else:
            cli: BitgetFutureRestClient = self.client
            resp = await cli.get_accounts(self.get_product_type())
            if not resp:
                raise ValueError("Failed to get response")

            if resp["code"] != "00000":
                raise ValueError(resp["msg"])

            data = resp["data"]
        for info in data:
            if info["marginCoin"] not in result:
                result[info["marginCoin"]] = Balance(info["marginCoin"])
            result[info["marginCoin"]].balance += float(info["available"]) + float(info["locked"])
            result[info["marginCoin"]].free += float(info["available"])
            result[info["marginCoin"]].locked += float(info["locked"])
            result[info["marginCoin"]].ts = int(time.time() * 1000)

            for a in info.get("assetList", []):
                result[a["coin"]].balance += float(a["balance"])
                result[a["coin"]].locked += float(a["balance"])

        return Balances(result)

    @catch_it
    async def _get_lps_subaccount_assets(self, user_id: Union[str, int]) -> dict[str, Balance]:
        result: dict[str, Balance] = {}
        cli: BitgetFutureRestClient = self.client
        resp = await cli.get_lps_subaccount_assets(productType=self.get_product_type())
        if isinstance(resp, dict) and resp["code"] == "00000":
            for sub_data in resp["data"]:
                u_id = sub_data["userId"]
                if str(u_id) == str(user_id):
                    for info in sub_data["assetList"]:
                        balance = float(info["available"]) + float(info["locked"])
                        if balance == 0:
                            continue
                        result[info["marginCoin"]] = Balance(
                            asset=info["marginCoin"],
                            balance=balance,
                            free=float(info["available"]),
                            locked=float(info["locked"]),
                            ts=int(time.time() * 1000),
                        )
            return Balances(result)
        elif resp:
            raise ValueError(resp["msg"])
        else:
            raise ValueError("fail to get withdraw_records from exchange")

    @catch_it
    async def _get_sp_subaccount_assets(self, user_id: Union[str, int]) -> dict[str, Balance]:
        result: dict[str, Balance] = {}
        cli: BitgetSpotRestClient = self.client
        resp = await cli.get_sp_subaccount_assets()
        if isinstance(resp, dict) and resp["code"] == "00000":
            for sub_data in resp["data"]:
                u_id = sub_data["userId"]
                if str(u_id) == str(user_id):
                    for info in sub_data["assetsList"]:
                        balance = float(info["available"]) + float(info["locked"]) + float(info["frozen"])
                        if balance == 0:
                            continue
                        result[info["coin"]] = Balance(
                            asset=info["coin"],
                            balance=balance,
                            free=float(info["available"]),
                            locked=float(info["locked"]) + float(info["frozen"]),
                            ts=(
                                int(info["uTime"]) if info["uTime"] else int(time.time() * 1000)
                            ),  # raw data may have 'uTime': None
                        )
            return Balances(result)
        elif resp:
            raise ValueError(resp["msg"])
        else:
            raise ValueError("fail to get withdraw_records from exchange")

    @catch_it
    async def get_subaccount_assets(self, user_id: str):
        if MarketType.UPERP == self._market_type:
            result = await self._get_lps_subaccount_assets(user_id=user_id)
        elif self._market_type in [MarketType.SPOT, MarketType.MARGIN]:
            result = await self._get_sp_subaccount_assets(user_id=user_id)
        else:
            raise ValueError(f"Market type {self._market_type} is not supported")
        if (data := result.get("data")) is not None:
            return data
        else:
            raise ValueError(result.get("msg", "unknown error"))

    @catch_it
    async def get_prices(self) -> Prices:
        cli = self.client
        match self._market_type:
            case MarketType.SPOT | MarketType.MARGIN if isinstance(cli, BitgetSpotRestClient):
                resp = await cli.get_tickers()
            case MarketType.UPERP | MarketType.UDELIVERY | MarketType.CPERP | MarketType.CDELIVERY if isinstance(
                cli, BitgetFutureRestClient
            ):
                product_type = self.get_product_type()
                resp = await cli.get_tickers(product_type)
            case _:
                raise ValueError(f"Market type {self._market_type} is not supported")

        if not (resp and resp["code"] == "00000"):
            raise ValueError(resp)

        return Prices({d["symbol"]: float(d["lastPr"]) for d in resp["data"]})

    @catch_it
    async def get_tickers(self) -> Tickers:
        cli = self.client
        match self._market_type:
            case MarketType.SPOT | MarketType.MARGIN if isinstance(cli, BitgetSpotRestClient):
                resp = await cli.get_tickers()
            case MarketType.UPERP | MarketType.UDELIVERY | MarketType.CPERP | MarketType.CDELIVERY if isinstance(
                cli, BitgetFutureRestClient
            ):
                product_type = self.get_product_type()
                resp = await cli.get_tickers(product_type)
            case _:
                raise ValueError(f"Market type {self._market_type} is not supported")

        if resp is None or not (isinstance(resp, dict) and resp["code"] == "00000"):
            raise ValueError(f"Failed to get tickers, response: {resp}")

        update_ts = float(time.time() * 1000)
        tickers = {
            t["symbol"]: Ticker(
                t["symbol"],
                float(t["bidPr"]) if t["bidPr"] else np.nan,
                float(t["askPr"]) if t["askPr"] else np.nan,
                (float(t["indexPrice"]) if "indexPrice" in t else np.nan),
                ts=float(t["ts"]),
                update_ts=update_ts,
                fr=float(t["fundingRate"]) if "fundingRate" in t else np.nan,
                bid_qty=float(t["bidSz"]) if t["bidSz"] else np.nan,
                ask_qty=float(t["askSz"]) if t["askSz"] else np.nan,
            )
            for t in resp["data"]
        }
        return tickers

    @catch_it
    async def get_quotations(self) -> Quotations:
        cli = self.client
        match self._market_type:
            case MarketType.SPOT | MarketType.MARGIN if isinstance(cli, BitgetSpotRestClient):
                resp = await cli.get_tickers()
            case MarketType.UPERP | MarketType.UDELIVERY | MarketType.CPERP | MarketType.CDELIVERY if isinstance(
                cli, BitgetFutureRestClient
            ):
                product_type = self.get_product_type()
                resp = await cli.get_tickers(product_type)
            case _:
                raise ValueError(f"Market type {self._market_type} is not supported")

        if resp is None or not (isinstance(resp, dict) and resp["code"] == "00000"):
            raise ValueError(f"Failed to get tickers, response: {resp}")

        update_ts = float(time.time() * 1000)
        quotations = {
            t["symbol"]: Quotation(
                exch_symbol=t["symbol"],
                bid=float(t["bidPr"]) if t["bidPr"] else np.nan,
                ask=float(t["askPr"]) if t["askPr"] else np.nan,
                ts=float(t["ts"]),
                update_ts=update_ts,
                bid_qty=float(t["bidSz"]) if t["bidSz"] else np.nan,
                ask_qty=float(t["askSz"]) if t["askSz"] else np.nan,
            )
            for t in resp["data"]
        }

        return Quotations(quotations)

    @catch_it
    async def get_orderbook_snapshot(self, symbol: str, limit: int = 100) -> OrderBook:
        cli = self.client
        match self._market_type:
            case MarketType.SPOT | MarketType.MARGIN if isinstance(cli, BitgetSpotRestClient):
                resp = await cli.get_orderbook(symbol, limit)
            case MarketType.UPERP | MarketType.UDELIVERY | MarketType.CPERP | MarketType.CDELIVERY if isinstance(
                cli, BitgetFutureRestClient
            ):
                resp = await cli.get_orderbook(self.get_product_type(), symbol, limit)
            case _:
                raise ValueError(f"Market type {self._market_type} is not supported")

        if not (isinstance(resp, dict) and resp.get("code") == "00000"):
            raise Exception(f"Get orderbook snapshot failed. err_mgs={resp}")

        orderbook = OrderBook(symbol)
        orderbook.exch_seq = int(resp["data"]["ts"])  # no sequence id
        orderbook.exch_ts = int(resp["data"]["ts"])
        orderbook.recv_ts = int(time.time() * 1000)

        for bid in resp["data"]["bids"]:
            orderbook.bids.append((Decimal(bid[0]), Decimal(bid[1])))
        for ask in resp["data"]["asks"]:
            orderbook.asks.append((Decimal(ask[0]), Decimal(ask[1])))

        return orderbook

    @catch_it
    async def get_fundamentals(self) -> Fundamentals:
        cli = self.client
        match self._market_type:
            case MarketType.SPOT | MarketType.MARGIN if isinstance(cli, BitgetSpotRestClient):
                resp = await cli.get_tickers()
            case MarketType.UPERP | MarketType.UDELIVERY | MarketType.CPERP | MarketType.CDELIVERY if isinstance(
                cli, BitgetFutureRestClient
            ):
                product_type = self.get_product_type()
                resp = await cli.get_tickers(product_type)
            case _:
                raise ValueError(f"Market type {self._market_type} is not supported")

        if resp is None or not (isinstance(resp, dict) and resp["code"] == "00000"):
            raise ValueError(f"Failed to get tickers, response: {resp}")

        fundamentals: dict[str, Fundamental] = {}
        for t in resp["data"]:
            symbol = t["symbol"]
            if (symbol not in self._insts) or (self._insts[symbol].status != InstStatus.TRADING):
                continue

            fundamentals[symbol] = Fundamental(
                symbol,
                float(t["change24h"]),
                float(t["quoteVolume"]),
                (
                    float(t["holdingAmount"]) * float(self._insts[symbol].quantity_multiplier) * float(t["lastPr"])
                    if "holdingAmount" in t
                    else np.nan
                ),
            )
        return fundamentals

    @catch_it
    async def get_trade_history(self, start_time: int, end_time: int, symbol_list: list[str]):
        result: dict[str, list[Trade]] = {}
        trade_data_list = []
        limit = 100
        end_id = None
        for symbol in symbol_list:
            while True:
                if self._market_type in [MarketType.SPOT, MarketType.MARGIN]:
                    resp = await self.client.get_fills_history(
                        symbol, startTime=str(start_time), endTime=str(end_time), limit=limit, idLessThan=end_id
                    )
                else:
                    resp = await self.client.get_fills_history(
                        self.get_product_type(),
                        symbol,
                        startTime=str(start_time),
                        endTime=str(end_time),
                        limit=limit,
                        idLessThan=end_id,
                    )
                cli = self.client
                match self._market_type:
                    case MarketType.SPOT | MarketType.MARGIN if isinstance(cli, BitgetSpotRestClient):
                        resp = await cli.get_fills_history(
                            symbol, startTime=str(start_time), endTime=str(end_time), limit=limit, idLessThan=end_id
                        )
                    case (
                        MarketType.UPERP | MarketType.UDELIVERY | MarketType.CPERP | MarketType.CDELIVERY
                    ) if isinstance(cli, BitgetFutureRestClient):
                        resp = await cli.get_fills_history(
                            self.get_product_type(),
                            symbol,
                            startTime=str(start_time),
                            endTime=str(end_time),
                            limit=limit,
                            idLessThan=end_id,
                        )
                    case _:
                        raise ValueError(f"Market type {self._market_type} is not supported")

                if not (resp and resp["code"] == "00000"):
                    raise ValueError(resp)

                if self._market_type in [MarketType.SPOT, MarketType.MARGIN]:
                    data = resp["data"]
                else:
                    data = resp["data"]["fillList"]

                trade_data_list.extend(data)

                if len(data) != limit:
                    break

                end_id = data[-1]["tradeId"]
                time.sleep(1)

            for data in trade_data_list[::-1]:
                if self._market_type in [MarketType.SPOT, MarketType.MARGIN]:
                    price = Decimal(data["priceAvg"])
                    volume = Decimal(data["size"])
                    fee = -Decimal(data["feeDetail"]["totalFee"])
                    if (data["feeDetail"]["deduction"]) == "yes":
                        fee += Decimal(data["feeDetail"]["totalDeductionFee"])
                    fee_ccy = data["feeDetail"]["feeCoin"]
                else:
                    price = Decimal(data["price"])
                    volume = Decimal(data["baseVolume"])
                    fee = -Decimal(data["feeDetail"][0]["totalFee"])
                    if (data["feeDetail"][0]["deduction"]) == "yes":
                        fee += Decimal(data["feeDetail"][0]["totalDeductionFee"])
                    fee_ccy = data["feeDetail"][0]["feeCoin"]
                result.setdefault(data["symbol"], []).append(
                    Trade(
                        create_ts=int(data["cTime"]),
                        side=getattr(OrderSide, data["side"].upper(), OrderSide.UNKNOWN),
                        trade_id=str(data["tradeId"]),
                        order_id=str(data["orderId"]),
                        last_trd_price=price,
                        last_trd_volume=volume,
                        turnover=price * volume,
                        fill_ts=int(data.get("uTime", data["cTime"])),
                        fee=fee,
                        fee_ccy=fee_ccy,
                        is_maker=data["tradeScope"] == "maker",
                    )
                )
        return TradeData(result)

    @catch_it
    async def get_commission_rate(self, symbol: str, from_redis: bool = False) -> CommissionRate:
        if from_redis:
            assert self._account, "Account is required to get commission rate from redis"
            data = await self._load_data_from_rmx("trading_fee:bitget", key=self._account)
            if not data:
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
                case MarketType.SPOT | MarketType.MARGIN if isinstance(cli, BitgetSpotRestClient):
                    resp = await cli.get_commission_rate(symbol, "spot")
                case MarketType.UPERP | MarketType.UDELIVERY | MarketType.CPERP | MarketType.CDELIVERY if isinstance(
                    cli, BitgetFutureRestClient
                ):
                    resp = await cli.get_commission_rate(symbol, "mix")
                case _:
                    raise ValueError(f"Market type {self._market_type} is not supported")

            if not (resp and resp["code"] == "00000"):
                raise ValueError(f"Could not get current commission rate")

            data = resp["data"]
            makerfee = data["makerFeeRate"]
            takerfee = data["takerFeeRate"]

        return CommissionRate(maker=Decimal(str(makerfee)), taker=Decimal(str(takerfee)))

    @catch_it
    async def get_funding_fee(
        self,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        look_back: Optional[int] = None,
        symbol_list: Optional[list[str]] = None,
    ):
        start_time, end_time = self._parse_start_end_look_back(start_time, end_time, look_back)
        assert MarketType.UPERP == self._market_type, f"Invalid Market type {self._market_type}"
        funding_dict: dict[str, list[FundingFee]] = {}
        id_less_than = None
        limit = 100
        cli: BitgetFutureRestClient = self.client
        while True:
            resp = await cli.get_account_bill(
                productType=self.get_product_type(),
                businessType="contract_settle_fee",
                startTime=str(start_time),
                endTime=str(end_time),
                limit=limit,
                idLessThan=id_less_than,
            )

            if not (resp and resp["code"] == "00000"):
                raise ValueError(resp)

            id_less_than = resp["data"].get("endId")
            raw_data_list = resp["data"].get("bills", [])
            for item in raw_data_list:
                symbol = item["symbol"]
                if symbol_list and symbol not in symbol_list:
                    continue

                if symbol not in funding_dict:
                    funding_dict[symbol] = [FundingFee(Decimal(item["amount"]), int(item["cTime"]))]
                else:
                    funding_dict[symbol].append(FundingFee(Decimal(item["amount"]), int(item["cTime"])))

            if not id_less_than or len(raw_data_list) < limit:
                break

            await asyncio.sleep(1)

        return FundingFeeData(funding_dict)

    @catch_it
    async def get_price(self, symbol: str, from_redis: bool = False) -> float:
        if from_redis:
            if self._market_type in [MarketType.SPOT, MarketType.MARGIN]:
                std_symbol = symbol[:-4].upper() + "_USDT|SPOT|BITGET"
            elif MarketType.UPERP == self._market_type:
                std_symbol = symbol[:-4].upper() + "_USDT|UPERP|BITGET"
            else:
                std_symbol = ""

            data = await self._load_data_from_kit(name="ticker", key=std_symbol)
            return (float(data["apx"]) + float(data["bpx"])) / 2
        else:
            cli = self.client
            match self._market_type:
                case MarketType.SPOT | MarketType.MARGIN if isinstance(cli, BitgetSpotRestClient):
                    resp = await cli.get_tickers(symbol)
                case MarketType.UPERP | MarketType.UDELIVERY | MarketType.CPERP | MarketType.CDELIVERY if isinstance(
                    cli, BitgetFutureRestClient
                ):
                    product_type = self.get_product_type()
                    resp = await cli.get_ticker(product_type, symbol)
                case _:
                    raise ValueError(f"Market type {self._market_type} is not supported")

            if not (resp and resp["code"] == "00000"):
                raise ValueError(resp)

            return float(resp["data"][0]["lastPr"])

    @catch_it
    async def get_historical_kline(
        self,
        symbol: str,
        interval: Interval,
        start_time: int,
        end_time: Optional[int] = None,
        contract_type: Literal["PERPETUAL", "CURRENT_QUARTER", "NEXT_QUARTER"] = "PERPETUAL",
        limit: int = 200,
    ):
        kline_list = []
        result: list[KLine] = []
        if not end_time:
            end_time = int(time.time() * 1000)

        interval_str = interval.name.lstrip("_")

        cli = self.client
        match self._market_type:
            case MarketType.SPOT | MarketType.MARGIN if isinstance(cli, BitgetSpotRestClient):
                interval_suffix_map = {"m": "min", "h": "h", "d": "day", "w": "week", "M": "M"}
                granularity = interval_str[:-1] + interval_suffix_map.get(interval_str[-1], interval_str[-1])
                params: dict[str, Any] = {
                    "symbol": symbol,
                    "granularity": granularity,
                    "limit": str(limit),
                }
            case MarketType.UPERP | MarketType.UDELIVERY | MarketType.CPERP | MarketType.CDELIVERY if isinstance(
                cli, BitgetFutureRestClient
            ):
                interval_suffix_map = {"m": "m", "h": "H", "d": "D", "w": "W", "M": "M"}
                granularity = interval_str[:-1] + interval_suffix_map.get(interval_str[-1], interval_str[-1])
                params: dict[str, Any] = {
                    "productType": self.get_product_type(),
                    "symbol": symbol,
                    "granularity": granularity,
                    "startTime": str(start_time),
                    "limit": str(limit),
                }
            case _:
                raise ValueError(f"Market type {self._market_type} is not supported")

        end_time_origin = end_time
        while True:
            params["endTime"] = str(end_time)
            resp = await cli.get_history_kline(**params)
            if not (resp and resp.get("code") == "00000"):
                raise ValueError(resp)

            kline_list += resp["data"][::-1]
            s_time = int(resp["data"][0][0])
            if len(resp["data"]) == limit and s_time > start_time:
                end_time = s_time
            else:
                break

        for lis in kline_list[::-1]:
            if not (start_time < int(lis[0]) < end_time_origin):
                continue
            result.append(
                KLine(
                    start_ts=int(lis[0]),
                    open=Decimal(lis[1]),
                    high=Decimal(lis[2]),
                    low=Decimal(lis[3]),
                    close=Decimal(lis[4]),
                    volume=Decimal(lis[5]),
                    turnover=(
                        Decimal(lis[7])
                        if self._market_type in [MarketType.SPOT, MarketType.MARGIN]
                        else Decimal(lis[6])
                    ),
                )
            )
        return KLineData(result)

    @catch_it
    async def get_long_short_ratio(self, symbol: str, limit: int, interval: Interval):
        assert MarketType.UPERP == self._market_type, f"Invalid Market type {self._market_type}, only support UPERP"
        assert interval in [
            Interval._5m,
            Interval._15m,
            Interval._30m,
            Interval._1h,
            Interval._2h,
            Interval._4h,
            Interval._6h,
            Interval._12h,
            Interval._1d,
        ], f"Invalid interval {interval.name}"
        interval_str = interval.name.lstrip("_")
        cli: BitgetFutureRestClient = self.client
        resp = await cli.get_long_short_ratio(symbol=symbol, period=interval_str)  # type: ignore
        if not (resp and resp.get("code") == "00000"):
            raise ValueError(resp)

        if isinstance(resp.get("data"), list):
            return LongShortRatioData(
                [
                    LongShortRatio(long_short_ratio=Decimal(data["longShortAccountRatio"]), ts=int(data["ts"]))
                    for data in resp["data"][-limit:]
                ]
            )
        raise ValueError(f"unexpected response[{resp}]")

    @catch_it
    async def cancel_order(self, symbol: str, orderId: Optional[str] = None, clientOid: Optional[str] = None) -> bool:
        assert orderId or clientOid, "Either Parameters `orderId` or `clientOid` is needed"

        cli = self.client
        match self._market_type:
            case MarketType.SPOT | MarketType.MARGIN if isinstance(cli, BitgetSpotRestClient):
                resp = await cli.cancel_order(symbol, orderId, clientOid)
            case MarketType.UPERP | MarketType.CPERP | MarketType.UDELIVERY | MarketType.CDELIVERY if isinstance(
                cli, BitgetFutureRestClient
            ):
                resp = await cli.cancel_order(self.get_product_type(), symbol, orderId, clientOid)
            case _:
                raise ValueError(f"Market type {self._market_type} is not supported")

        if not (resp and resp.get("code") == "00000"):
            raise ValueError(resp)

        return True

    @catch_it
    async def get_order_history(self, start_time: int, end_time: int, symbol_list: list[str]) -> OrderSnapshotData:
        order_dict: dict[str, list[OrderSnapshot]] = {}
        order_data_list = []
        limit = 100

        cli = self.client
        match self._market_type:
            case MarketType.SPOT | MarketType.MARGIN if isinstance(cli, BitgetSpotRestClient):
                params = {"startTime": start_time, "endTime": end_time, "limit": str(limit)}
            case MarketType.UPERP | MarketType.UDELIVERY | MarketType.CPERP | MarketType.CDELIVERY if isinstance(
                cli, BitgetFutureRestClient
            ):
                params = {
                    "productType": self.get_product_type(),
                    "startTime": start_time,
                    "endTime": end_time,
                    "limit": str(limit),
                }
            case _:
                raise ValueError(f"Market type {self._market_type} is not supported")

        def _get_id_cursor(resp: dict) -> tuple[Optional[str], list]:
            if self._market_type in [MarketType.SPOT, MarketType.MARGIN]:
                if resp["data"]:
                    return resp["data"][-1]["orderId"], resp["data"]
                else:
                    return None, []
            elif self._market_type in [MarketType.UPERP, MarketType.UDELIVERY, MarketType.CPERP, MarketType.CDELIVERY]:
                if resp["data"]:
                    return resp["data"]["endId"], resp["data"]["entrustedList"] or []
                else:
                    return None, []
            else:
                return None, []

        id_less_than = None
        if symbol_list:
            for symbol in symbol_list:
                while True:
                    params["symbol"] = symbol
                    params["idLessThan"] = id_less_than
                    resp = await cli.get_order_history(**params)
                    if not (resp and resp.get("code") == "00000"):
                        logger.error(
                            f"account[{self._account}] MarketType[{self._market_type}] symbol[{symbol}], error: {resp}"
                        )
                        await asyncio.sleep(0.2)
                        break

                    id_less_than, order_datas = _get_id_cursor(resp)
                    order_data_list.extend(order_datas)
                    await asyncio.sleep(1)
                    if not id_less_than or len(order_datas) < limit:
                        break
        else:
            while True:
                params["symbol"] = None
                params["idLessThan"] = id_less_than
                resp = await cli.get_order_history(**params)
                if not (resp and resp.get("code") == "00000"):
                    logger.error(f"account[{self._account}] MarketType[{self._market_type}] error: {resp}")
                    await asyncio.sleep(0.2)
                    break

                id_less_than, order_datas = _get_id_cursor(resp)
                order_data_list.extend(order_datas)
                if not id_less_than or len(order_datas) < limit:
                    break
                await asyncio.sleep(1)

        for od in order_data_list:
            order_type = getattr(OrderType, od["orderType"].upper(), OrderType.UNKNOWN)
            tif = TIF_MAP.get(od["force"], TimeInForce.UNKNOWN) if "force" in od else TimeInForce.UNKNOWN
            status = STATUS_MAP.get(od["status"], OrderStatus.UNKNOWN)
            side = getattr(OrderSide, od["side"].upper(), OrderSide.UNKNOWN)

            o = OrderSnapshot(
                place_ack_ts=int(od["cTime"]),
                exch_symbol=od["symbol"],
                order_side=side,
                order_id=od["orderId"],
                client_order_id=od["clientOid"],
                price=Decimal(od["price"]) if od["price"] else Decimal(0),
                qty=Decimal(od["size"]),
                filled_qty=Decimal(od["baseVolume"]),
                avg_price=float(od["priceAvg"]) if od["priceAvg"] else 0.0,
                order_type=order_type,
                order_time_in_force=tif,
                order_status=status,
                exch_update_ts=int(od["uTime"]),
                local_update_ts=int(time.time() * 1000),
            )
            order_dict.setdefault(o.exch_symbol, []).append(o)

        return OrderSnapshotData(order_dict)

    @catch_it
    async def set_symbol_leverage(self, symbol: str, leverage: int, **kwargs) -> bool:
        if MarketType.UPERP != self._market_type:
            raise ValueError(f"Market type {self._market_type} is not supported(only supported for UPERP)")
        logger.debug(f"Change leverage of {symbol} to {leverage}")

        marginCoin = kwargs.get("marginCoin", "USDT")
        cli: BitgetFutureRestClient = self.client

        resp = await cli.set_leverage(self.get_product_type(), marginCoin, symbol, leverage=str(leverage))
        if not (resp and resp.get("code") == "00000"):
            raise ValueError(resp)

        return True

    @catch_it
    async def get_current_funding_rate(self, symbol_list: list[str] | None = None) -> FundingRatesCur:
        cli: BitgetFutureRestClient = self.client
        resp = await cli.get_current_funding_rate(productType=self.get_product_type())
        if not (resp and resp.get("code") == "00000"):
            raise ValueError(resp)

        if not symbol_list:
            symbol_list = [item["symbol"] for item in resp["data"]]

        frs: FundingRatesCur = FundingRatesCur()
        for item in resp["data"]:
            if item["symbol"] not in symbol_list:
                continue
            ts = int(item["nextUpdate"])
            frs[item["symbol"]] = FundingRate(
                funding_rate=float(item["fundingRate"]),
                funding_ts=ts,
                interval_hour=int(item["fundingRateInterval"]),
                fr_cap=float(item["maxFundingRate"]) if item["maxFundingRate"] else np.nan,
                fr_floor=float(item["minFundingRate"]) if item["minFundingRate"] else np.nan,
            )

        return frs

    @catch_it
    async def get_current_simple_funding_rate(self, symbol_list: list[str] | None = None) -> FundingRatesSimple:
        cli: BitgetFutureRestClient = self.client
        resp = await cli.get_current_funding_rate(productType=self.get_product_type())
        if not (resp and resp.get("code") == "00000"):
            raise ValueError(resp)

        if not symbol_list:
            symbol_list = [item["symbol"] for item in resp["data"]]

        frs: FundingRatesSimple = FundingRatesSimple()
        for item in resp["data"]:
            if item["symbol"] not in symbol_list:
                continue
            ts = int(item["nextUpdate"])
            frs[item["symbol"]] = FundingRateSimple(
                funding_rate=float(item["fundingRate"]), funding_ts=ts, interval_hour=int(item["fundingRateInterval"])
            )

        return frs

    @catch_it
    async def get_historical_funding_rate(
        self,
        symbol_list: list[str],
        start_time: datetime | str | int | None = None,
        days: int = 7,
    ) -> FundingRatesHis:
        assert self._market_type in (MarketType.UPERP, MarketType.CPERP), f"Invalid Market type {self._market_type}"
        cli: BitgetFutureRestClient = self.client
        if not isinstance(start_time, int):
            if start_time is None:
                start_time = datetime.now() - timedelta(days=days)
            elif isinstance(start_time, str):
                start_time = parser.parse(start_time)

            start_ts = int(start_time.timestamp() * 1000)
        else:
            start_ts = start_time
        if not symbol_list:
            resp = await cli.get_tickers(productType=self.get_product_type())
            if not (resp and resp.get("code") == "00000"):
                raise ValueError(resp)

            symbol_list = [item["symbol"] for item in resp["data"]]

        page_size = 100
        frs: dict[str, set[FundingRateSimple]] = {}
        for symbol in symbol_list:
            data: list[dict[str, Any]] = []
            page_no = 1
            for _ in range(1000):
                resp = await cli.get_history_funding_rate(
                    self.get_product_type(), symbol, pageSize=page_size, pageNo=page_no
                )

                await asyncio.sleep(0.05)
                if not (resp and resp.get("code") == "00000"):
                    break

                data.extend(resp["data"])
                if len(resp["data"]) < page_size:
                    break

                if int(resp["data"][-1]["fundingTime"]) <= start_ts:
                    break

                page_no += 1

            frs[symbol] = set()
            for d in data:
                if int(d["fundingTime"]) <= start_ts:
                    continue
                symbol = d["symbol"]
                fr = float(d["fundingRate"])
                frs[symbol].add(FundingRateSimple(fr, int(d["fundingTime"])))

        return FundingRatesHis({symbol: sorted(list(fr)) for symbol, fr in frs.items()})

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
        # if isinstance(marginMode, str):
        #     marginMode = MarginMode[marginMode]
        if isinstance(order_type, str):
            order_type = OrderType[order_type]
        send_order_type = "limit" if order_type == OrderType.LIMIT else "market"
        send_order_side = "buy" if order_side == OrderSide.BUY else "sell"
        if not client_order_id:
            client_order_id = "xclients" + str(int(time.time() * 1000000))

        # 市价单不需要price参数
        if order_type == OrderType.MARKET and price is not None:
            raise ValueError("In market_order parameter price not required")
        # 市价单不需要 timeInForce
        send_time_in_force = None
        if order_type == OrderType.LIMIT and order_time_in_force:
            if order_time_in_force == TimeInForce.GTX:
                send_time_in_force = "post_only"
            else:
                send_time_in_force = order_time_in_force.name.lower()

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
                    raise ValueError("quote_qty is only supported for MARKET BUY orders in Bitget")
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
            case MarketType.SPOT | MarketType.MARGIN if isinstance(cli, BitgetSpotRestClient):
                resp = await cli.spot_order(
                    symbol=symbol,
                    side=send_order_side,
                    orderType=send_order_type,
                    force=send_time_in_force,  # type: ignore
                    price=str(price) if price else None,
                    size=str(
                        size_value
                    ),  # 对于Limit和Market-Sell订单，此参数表示base coin数量; 对于Market-Buy订单，此参数表示quote coin数量；
                    clientOid=client_order_id,
                )

            case MarketType.UPERP | MarketType.CPERP | MarketType.UDELIVERY | MarketType.CDELIVERY if isinstance(
                cli, BitgetFutureRestClient
            ):
                if params.get("stop_loss_price"):
                    params["presetStopLossPrice"] = str(params["stop_loss_price"])
                    params.pop("stop_loss_price", None)
                if params.get("take_profit_price"):
                    params["presetStopSurplusPrice"] = str(params["take_profit_price"])
                    params.pop("take_profit_price", None)

                # 只能使用base_qty
                if use_base_qty:
                    resp = await cli.futures_order(
                        symbol=symbol,
                        productType=self.get_product_type(),
                        marginMode="crossed",
                        marginCoin="USDT",
                        side=send_order_side,
                        orderType=send_order_type,
                        force=send_time_in_force,  # type: ignore
                        price=str(price) if price else None,
                        size=str(size_value),
                        clientOid=client_order_id,
                        reduceOnly="YES" if reduce_only else "NO",
                        **params,
                    )
                else:
                    raise ValueError("Only base_qty is allowed")
            case _:
                raise ValueError(f"Market type {self._market_type} is not supported")

        snapshot = OrderSnapshot(
            exch_symbol=symbol,
            client_order_id=client_order_id,
            order_side=order_side,
            order_type=order_type,
            order_time_in_force=order_time_in_force or TimeInForce.UNKNOWN,
            price=price or Decimal(0),
            qty=qty if qty else Decimal(0),
            local_update_ts=int(time.time() * 1000),
        )

        if not resp:
            snapshot.order_status = OrderStatus.REJECTED
            snapshot.rejected_message = "No response from server"
        elif not (isinstance(resp, dict) and resp["code"] == "00000"):
            snapshot.order_status = OrderStatus.REJECTED
            if resp["code"] in ["43012"]:
                snapshot.rejected_reason = RejectedReason.INSUFFICIENT_BALANCE
            elif resp["code"] in ["59044"]:  # TODO may have other error code
                snapshot.rejected_reason = RejectedReason.RATE_LIMIT
            elif resp["code"] in ["22029"]:
                snapshot.rejected_reason = RejectedReason.EXCHANGE_RESTRICTED
            snapshot.rejected_message = resp.get("msg", "Unknown error")
        else:
            snapshot.order_id = resp["data"]["orderId"]
            snapshot.order_status = OrderStatus.LIVE
            snapshot.place_ack_ts = snapshot.local_update_ts
            snapshot.exch_update_ts = resp["requestTime"]
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
        # 添加Bitget特有的参数
        if self._market_type == MarketType.UPERP:
            params["productType"] = "USDT-FUTURES"
        elif self._market_type == MarketType.CPERP:
            params["productType"] = "COIN-FUTURES"

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
    async def set_account_position_mode(self, mode: PositionMode):
        logger.info(f"设置合约单向持仓")
        if self._market_type != MarketType.SPOT:
            ret = await self.client.set_position_mode(
                productType=self.get_product_type(),
                posMode="one_way_mode" if mode == PositionMode.ONE_WAY else "hedge_mode",
            )
            if ret and ret["code"] != "00000":
                logger.warning("设置合约单向持仓失败, 返回 {}".format(ret))

    @catch_it
    async def set_fee_coin_burn(self, enable: bool) -> None:
        assert not self._market_type.is_derivative, "Only SPOT and MARGIN market support set_fee_coin_burn"
        bgb_on_resp = await self.client.switch_deduct(enable)
        if bgb_on_resp and bgb_on_resp["code"] == "00000":
            logger.info(f"开启BGB抵扣成功")
        else:
            if bgb_on_resp and bgb_on_resp["code"] == "40401":
                logger.debug("无法开启BGB抵扣, 返回 {}".format(bgb_on_resp))
            else:
                logger.error("开启BGB抵扣失败, 返回 {}".format(bgb_on_resp))

    @catch_it
    async def enable_union_asset_mode(self):
        logger.info(f"设置合约联合保证金模式")
        cli: BitgetFutureRestClient = self.client
        resp = await cli.set_asset_mode(self.get_product_type(), "union")
        if resp and resp["code"] == "00000":
            pass
        else:
            logger.warning("设置合约联合保证金模式失败, 返回 {}".format(resp))

    @catch_it
    async def get_symbol_leverage_and_margin_mode(self, symbol: str) -> tuple[int, MarginMode]:
        assert self._market_type.is_derivative, f"Invalid Market type {self._market_type}, only support derivative"
        cli: BitgetFutureRestClient = self.client
        resp = await cli.get_account(self.get_product_type(), symbol, "USDT")
        if resp is None or resp.get("code") != "00000":
            raise ValueError(f"无法获取杠杆倍数, 返回 {resp}")

        current_leverage = int(resp["data"]["crossedMarginLeverage"])
        current_margin_mode = MarginMode.CROSS if resp["data"]["marginMode"] == "crossed" else MarginMode.ISOLATED
        return current_leverage, current_margin_mode

    @catch_it
    async def set_symbol_margin_mode(self, symbol: str, mode: MarginMode):
        logger.info(f"设置合约保证金模式为 {mode.name}")
        cli: BitgetFutureRestClient = self.client
        if self._market_type.is_derivative:
            resp = await cli.set_margin_mode(
                productType=self.get_product_type(),
                marginCoin="USDT",
                symbol=symbol,
                marginMode="crossed" if mode == MarginMode.CROSS else "isolated",
            )
            if resp and resp["code"] != "00000":
                raise Exception("设置合约保证金模式失败, 返回 {}".format(resp))

    @catch_it
    async def get_account_info(self) -> AccountInfo:
        cli = self.client
        match self._market_type:
            case MarketType.SPOT | MarketType.MARGIN if isinstance(cli, BitgetSpotRestClient):
                sp_tickers_resp = await cli.get_tickers()
                if sp_tickers_resp is None or sp_tickers_resp.get("code") != "00000":
                    raise ValueError(f"无法获取账户信息, 返回 {sp_tickers_resp}")
                balances_resp = await self.get_sp_assets()
                if balances_resp["status"] != 0:
                    raise ValueError(f"无法获取账户信息, 返回 {balances_resp['msg']}")
                balances = balances_resp["data"]
                sp_px = {}
                for ticker in sp_tickers_resp["data"]:
                    sp_px[ticker["symbol"]] = float(ticker["lastPr"])

                total_sp_eqty = 0
                usdt = balances.get("USDT", Balance("USDT")).balance
                for ccy in balances.keys():
                    if ccy == "USDT":
                        total_sp_eqty += balances[ccy].balance
                    else:
                        symbol = ccy + "USDT"
                        if symbol in sp_px:
                            total_sp_eqty += balances[ccy].balance * sp_px[symbol]
                        else:
                            logger.warning(f"无法获取币种 {ccy} 的USDT价格, 该币种余额不计入总资产")

                # BitGet 现货账户无借贷
                ltv = 999
                margin_balance = float("nan")
                total_position_value = total_sp_eqty - usdt

                return AccountInfo(
                    account=self._account_meta,
                    equity=total_sp_eqty,
                    usdt_free=usdt,
                    ltv=ltv,
                    margin_balance=margin_balance,
                    total_position_value=total_position_value,
                )
            case MarketType.UPERP | MarketType.UDELIVERY | MarketType.CPERP | MarketType.CDELIVERY if isinstance(
                cli, BitgetFutureRestClient
            ):
                equity_resp = await self.get_equity()
                if equity_resp["status"] == 0:
                    equity = equity_resp["data"]
                else:
                    raise ValueError(f"无法获取账户权益, 返回 {equity_resp['msg']}")

                acct_resp = await self.client.get_accounts(self.get_product_type())
                if acct_resp is None or acct_resp.get("code", None) != "00000":
                    raise ValueError(f"无法获取账户信息, 返回 {acct_resp}")

                lps_pos_rsp = await self.client.get_positions(self.get_product_type())
                if lps_pos_rsp is None or lps_pos_rsp.get("code", None) != "00000":
                    raise ValueError(f"无法获取仓位信息, 返回 {lps_pos_rsp}")

                usdt_margin_info = acct_resp["data"][0]
                if usdt_margin_info["marginCoin"] != "USDT":
                    raise Exception("获取USDT保证金数据失败")
                usdt_free: float = float(acct_resp["data"][0]["maxTransferOut"])

                available_balance = float(acct_resp["data"][0]["unionAvailable"])
                margin_balance = float(acct_resp["data"][0]["unionTotalMargin"])

                # 初始保证金额 = SUM 所有仓位的 qty*px/leverage
                # 维持保证金额 = SUM 所有仓位的 qty*px*keepMarginRate
                initial_margin = 0
                maintenance_margin = 0
                total_position_value = 0

                for pos in lps_pos_rsp["data"]:
                    initial_margin += float(pos["available"]) * float(pos["markPrice"]) / float(pos["leverage"])
                    maintenance_margin += (
                        float(pos["available"]) * float(pos["markPrice"]) * float(pos["keepMarginRate"])
                    )
                    total_position_value += float(pos["available"]) * float(pos["markPrice"])

                if initial_margin > 0:
                    imr = margin_balance / initial_margin
                else:
                    imr = 999

                if maintenance_margin > 0:
                    mmr = margin_balance / maintenance_margin
                else:
                    mmr = 999
                return AccountInfo(
                    account=self._account_meta,
                    equity=equity,  # 返回不带折扣率的total equity
                    usdt_free=usdt_free,  # 返回最大可转出的 USDT
                    imr=imr,  # margin_balance / initial_margin
                    mmr=mmr,  # margin_balance / maint_margin
                    available_balance=available_balance,  # 考虑了折扣率的available balance
                    margin_balance=margin_balance,  # 考虑了折扣率的total equity
                    total_position_value=total_position_value,
                )
            case _:
                raise ValueError(f"Market type {self._market_type} is not supported")

    @catch_it
    async def get_interest_rates_cur(
        self,
        vip_level: int | str | None = None,
        vip_loan: bool = False,
        asset: str = "",
        days: int = -1,
    ) -> InterestRates:
        assert self._market_type == MarketType.SPOT, f"Invalid Market type {self._market_type}, only support SPOT"
        assert asset, "param `asset` is empty"
        interest_type_maps = {-1: "FLEXIBLE", 7: "SEVEN", 30: "THIRTY"}
        assert days in interest_type_maps, f"Invalid days param {days}, only support {list(interest_type_maps.keys())}"
        interest_rates: list[InterestRate] = []
        cli: BitgetSpotRestClient = self.client
        resp = await cli.get_loan_interest(loanCoin=asset, daily=interest_type_maps[days])  # type: ignore
        if not (resp and resp.get("code") == "00000"):
            raise ValueError(f"unexpected response[{resp}]")

        interest_rates = [
            InterestRate(
                asset=asset,
                days=days,
                ir=Decimal(resp["data"]["hourInterest"]) / Decimal(resp["data"]["loanAmount"]) * 24,
                ts=time.time() * 1000,
            )
        ]
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
        assert asset, "param `asset` is empty"
        interest_rates: list[InterestRate] = []
        cli: BitgetSpotRestClient = self.client
        resp = await cli.get_loan_interest_history(loanCoin=asset)
        if not (isinstance(resp, dict) and resp.get("code") == "200"):
            raise ValueError(f"unexpected response[{resp}]")

        for info in resp.get("data", {}).get("item", []):
            if info["coinName"] != asset:
                continue
            rate_ts = float(info["bizTime"])
            if start_time and rate_ts < start_time:
                continue
            if end_time and rate_ts > end_time:
                continue
            interest_rates.append(
                InterestRate(
                    asset=asset,
                    ir=Decimal(info["rate"]) / 365,  # rate:年化利率
                    ts=rate_ts,
                )
            )
        return interest_rates

    @catch_it
    async def get_margin_interest_rates_cur(
        self,
        vip_level: int | None = None,
        asset: str | None = "",
    ):
        assert asset, "param `asset` is empty"
        assert self._market_type == MarketType.MARGIN, f"Invalid Market type {self._market_type}, only support MARGIN"
        cli: BitgetSpotRestClient = self.client
        resp = await cli.get_margin_interest_rate_cur(coin=asset)
        if not (isinstance(resp, dict) and resp.get("code") == "00000"):
            raise ValueError(resp)
        interest_rates: list[InterestRate] = [
            InterestRate(
                asset=resp["data"]["coin"],
                vip_level="VIP0",
                ir=Decimal(resp["data"]["dailyInterestRate"]),
                ts=float(resp["data"]["updatedTime"]),
            )
        ]
        return interest_rates
