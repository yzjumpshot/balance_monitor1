import asyncio
import copy
import time
import traceback
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any, Literal, Optional
from loguru import logger
from dateutil import parser

from ..base_wrapper import BaseRestWrapper, catch_it
from ..data_type import *
from ..enum_type import (
    AccountType,
    ExchangeName,
    TimeInForce,
    OrderSide,
    Interval,
    MarginMode,
    OrderStatus,
    OrderType,
)
from .rest import BinanceSpotRestClient, BinanceInverseRestClient, BinanceLinearRestClient, BinanceUnifiedRestClient
from ..common.exceptions import UnsupportedOperationError
from ..get_client import get_rest_client
from .constants import *
from ccxt.base.types import Order as ccxtOrder, ConstructorArgs
import ccxt.async_support as ccxt


class BinanceRestWrapper(BaseRestWrapper):
    def __init__(
        self,
        account_meta: AccountMeta,
        account_config: AccountConfig,
        rest_config: RestConfig,
    ) -> None:
        super().__init__(account_meta, account_config, rest_config)
        normal_account_meta = copy.deepcopy(account_meta)
        normal_account_meta.account_type = AccountType.NORMAL
        self.market_client = get_rest_client(normal_account_meta, account_config, rest_config)
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
                ccxt_default_type = "future"
                ccxt_default_sub_type = "linear"
            case MarketType.CPERP:
                ccxt_default_type = "future"
                ccxt_default_sub_type = "inverse"
            case MarketType.UDELIVERY:
                ccxt_default_type = "delivery"
                ccxt_default_sub_type = "linear"
            case MarketType.CDELIVERY:
                ccxt_default_type = "delivery"
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

        self.ccxt_client = ccxt.binance(ConstructorArgs(ccxt_params))
        # self.ccxt_client.load_markets()

    def get_unified_category(self) -> Literal["spot", "linear", "inverse"]:
        if self._market_type in [MarketType.SPOT, MarketType.MARGIN]:
            category = "spot"
        elif self._market_type in [MarketType.UPERP, MarketType.UDELIVERY]:
            category = "linear"
        elif self._market_type in [MarketType.CPERP, MarketType.CDELIVERY]:
            category = "inverse"
        else:
            raise ValueError(f"Unknown Market type {self._market_type}")
        return category

    @catch_it
    async def get_positions(self, from_redis: bool = False) -> Positions:
        if self._market_type not in [MarketType.UPERP, MarketType.UDELIVERY, MarketType.CPERP, MarketType.CDELIVERY]:
            raise ValueError(f"Market type {self._market_type} is not supported for positions")
        if AccountType.UNIFIED == self._account_type:
            return await self._get_unified_positions(from_redis)
        if MarketType.UPERP == self._market_type:
            return await self._get_lps_positions(from_redis)
        elif MarketType.CPERP == self._market_type:
            return await self._get_ps_positions(from_redis)
        elif MarketType.CDELIVERY == self._market_type:
            return await self._get_fu_positions(from_redis)
        raise ValueError(f"Invalid MarketType {self._market_type} for positions")

    async def _get_unified_positions(self, from_redis: bool = False) -> Positions:
        cli: BinanceUnifiedRestClient = self.client

        result: Positions = {}
        if from_redis:
            suffix = "raw:test"
            if self._market_type in [MarketType.UPERP, MarketType.UDELIVERY]:
                key = "pm_um_position"
            elif self._market_type in [MarketType.CPERP, MarketType.CDELIVERY]:
                key = "pm_cm_position"
            else:
                raise ValueError(f"Market type {self._market_type} have no positions api")
            data = await self._load_data_from_rmx_acc(suffix, key)
        else:
            if self._market_type in [MarketType.UPERP, MarketType.UDELIVERY]:
                data = await cli.get_um_position_risk()
            elif self._market_type in [
                MarketType.CPERP,
                MarketType.CDELIVERY,
            ]:
                data = await cli.get_cm_position_risk()
            else:
                raise ValueError(f"Market type {self._market_type} have no positions api")
        if isinstance(data, list):
            for info in data:
                if self._market_type in [MarketType.UPERP, MarketType.UDELIVERY]:
                    value = float(info["notional"])
                else:
                    value = float(info["notionalValue"])

                if Decimal(info["positionAmt"]) != Decimal(0):
                    sign = {"LONG": 1, "SHORT": -1}.get(info["positionSide"], 1)
                    result[info["symbol"]] = Position(
                        exch_symbol=info["symbol"],
                        net_qty=float(info["positionAmt"]) * sign,
                        entry_price=float(info["entryPrice"]),
                        value=value,
                        unrealized_pnl=float(info["unRealizedProfit"]),
                        ts=int(info["updateTime"]),
                    )
            return result
        else:
            raise ValueError(data)

    async def _get_lps_positions(self, from_redis: bool = False) -> Positions:
        cli: BinanceLinearRestClient = self.client

        result: Positions = {}
        if from_redis:
            suffix = "raw:test"
            key = "u_contract_position"
            data = await self._load_data_from_rmx_acc(suffix, key)
        else:
            data = await cli.get_linear_swap_position()
        if isinstance(data, list):
            for info in data:
                if Decimal(info["positionAmt"]) != Decimal(0):
                    sign = {"LONG": 1, "SHORT": -1}.get(info["positionSide"], 1)
                    result[info["symbol"]] = Position(
                        exch_symbol=info["symbol"],
                        net_qty=float(info["positionAmt"]) * sign,
                        entry_price=float(info["entryPrice"]),
                        value=float(info["notional"]),
                        unrealized_pnl=float(info["unRealizedProfit"]),
                        ts=int(info["updateTime"]),
                    )
            return result
        else:
            if isinstance(data, dict) and "msg" in data.keys():
                err_msg = data["msg"]
            else:
                err_msg = str(data)
            raise ValueError(err_msg)

    async def _get_ps_positions(self, from_redis: bool = False) -> Positions:
        cli: BinanceInverseRestClient = self.client

        result: Positions = {}
        if from_redis:
            suffix = "raw:test"
            key = "coin_contract_position"
            data = await self._load_data_from_rmx_acc(suffix, key)
        else:
            data = await cli.get_inverse_swap_position()
        if isinstance(data, list):
            for info in data:
                if Decimal(info["positionAmt"]) != Decimal(0) and info["symbol"].endswith("PERP"):
                    result[info["symbol"]] = Position(
                        exch_symbol=info["symbol"],
                        net_qty=float(info["positionAmt"]),
                        entry_price=float(info["entryPrice"]),
                        value=float(info["notionalValue"]),
                        unrealized_pnl=float(info["unRealizedProfit"]),
                        ts=int(info["updateTime"]),
                    )
            return result
        else:
            if isinstance(data, dict) and "msg" in data.keys():
                err_msg = data["msg"]
            else:
                err_msg = str(data)
            raise ValueError(err_msg)

    async def _get_fu_positions(self, from_redis: bool = False) -> Positions:
        cli: BinanceInverseRestClient = self.client

        result: Positions = {}
        if from_redis:
            suffix = "raw:test"
            key = "coin_contract_position"
            data = await self._load_data_from_rmx_acc(suffix, key)
        else:
            data = await cli.get_inverse_swap_position()
        if isinstance(data, list):
            for info in data:
                if Decimal(info["positionAmt"]) != Decimal(0) and not info["symbol"].endswith("PERP"):
                    result[info["symbol"]] = Position(
                        exch_symbol=info["symbol"],
                        net_qty=float(info["positionAmt"]),
                        entry_price=float(info["entryPrice"]),
                        value=float(info["notional"]),
                        unrealized_pnl=float(info["unRealizedProfit"]),
                        ts=int(info["updateTime"]),
                    )
            return result
        else:
            if isinstance(data, dict) and "msg" in data.keys():
                err_msg = data["msg"]
            else:
                err_msg = str(data)
            raise ValueError(err_msg)

    @catch_it
    async def get_loans(self):
        cli: BinanceSpotRestClient = self.client

        resp = await cli.sapi_loan_vip_ongoing_order()
        loan_dict: dict[str, Loan] = {}
        if isinstance(resp, dict) and "rows" in resp:
            for item in resp["rows"]:
                if item["loanCoin"] not in loan_dict:
                    loan_dict[item["loanCoin"]] = Loan(
                        quantity=Decimal(item["totalDebt"]), interest=Decimal(item["residualInterest"])
                    )
                else:
                    loan_dict[item["loanCoin"]].quantity += Decimal(item["totalDebt"])
                    loan_dict[item["loanCoin"]].interest += Decimal(item["residualInterest"])
            return LoanData(loan_dict)
        else:
            if isinstance(resp, dict) and "msg" in resp.keys():
                err_msg = resp["msg"]
            else:
                err_msg = str(resp)
            raise ValueError(err_msg)

    @catch_it
    async def get_equity(self) -> float:
        """
        U合约:totalMarginBalance + 现货free+locked + 杠杆BTC net asset
        """
        if AccountType.UNIFIED == self._account_type:
            return await self._get_unified_equity()
        elif self._market_type in [MarketType.SPOT, MarketType.MARGIN]:
            sp_equity = await self._get_sp_equity()
            account_meta = AccountMeta(
                exch_name=self._exchange, account_type=self._account_type, market_type=MarketType.UPERP
            )
            lps_client = get_rest_client(account_meta, self._account_config, rest_config=self._rest_config)
            account_meta.market_type = MarketType.CPERP
            ps_client = get_rest_client(account_meta, self._account_config, rest_config=self._rest_config)

            lps_equity = await self._get_lps_equity(lps_client)
            ps_equity = await self._get_ps_equity(ps_client)
            margin_equity = await self._get_margin_equity(self.client)
            return sp_equity + lps_equity + ps_equity + margin_equity
        else:
            raise ValueError(f"Invalid market type: {self._market_type}")

    async def _get_unified_equity(self):
        cli: BinanceUnifiedRestClient = self.client
        mkt_cli: BinanceSpotRestClient = self.market_client

        prices_resp = await mkt_cli.get_price()
        if not prices_resp:
            raise ValueError("Failed to get equity")

        price_dict = {item["symbol"]: float(item["price"]) for item in prices_resp}

        bal_resp = await cli.get_balance()
        if not isinstance(bal_resp, list):
            raise ValueError(f"Invalid return data[{bal_resp}]")

        equity = 0
        for info in bal_resp:
            equity_in_ccy = (
                float(info["totalWalletBalance"])
                - float(info["crossMarginBorrowed"])
                - float(info["crossMarginInterest"])
                + float(info["umUnrealizedPNL"])
            )
            if equity_in_ccy == 0:
                continue
            if info["asset"] not in ["USDT", "USDC", "FDUSD", "BFUSD"]:
                equity += equity_in_ccy * price_dict[info["asset"] + "USDT"]
            else:
                equity += equity_in_ccy
        return equity

    async def _get_sp_equity(self) -> float:
        mkt_cli: BinanceSpotRestClient = self.market_client

        asset_total = 0
        asset_data = await self._get_sp_assets()

        price_resp = await mkt_cli.get_price()
        if not price_resp:
            raise ValueError("Failed to get equity")

        price_dict = {item["symbol"]: float(item["price"]) for item in price_resp}
        for coin, info in asset_data.items():
            if info.balance == 0:
                continue
            if coin != "USDT":
                asset_total += price_dict.get(coin + "USDT", 0) * info.balance
            else:
                asset_total += info.balance

        return asset_total

    async def _get_margin_equity(self, margin_client=None) -> float:
        if not margin_client:
            cli: BinanceSpotRestClient = self.client
        else:
            cli = margin_client

        data = await cli.sapi_margin_account()
        if not data:
            raise ValueError("Failed to get equity")

        price_resp = await cli.get_price()
        if not price_resp:
            raise ValueError("Failed to get equity")

        price_dict = {item["symbol"]: float(item["price"]) for item in price_resp}
        asset_total = 0
        for info in data["userAssets"]:
            net_asset = float(info["netAsset"])
            if net_asset == 0 and net_asset != "USDT":
                continue
            coin = info["asset"]
            if coin != "USDT":
                asset_total += price_dict[coin + "USDT"] * net_asset
            else:
                asset_total += net_asset
        return asset_total

    async def _get_lps_equity(self, lps_client=None) -> float:
        if not lps_client:
            cli: BinanceLinearRestClient = self.client
        else:
            cli = lps_client

        data = await cli.fapi_v3_account()
        if not data:
            raise ValueError("Failed to get equity")

        asset_total = 0
        for asset in data["assets"]:
            if float(asset["marginBalance"]) == 0:
                continue
            if asset["asset"] not in ["USDT", "USDC", "FDUSD", "BFUSD"]:
                asset_price = await self.get_price(asset["asset"] + "USDT")
                if asset_price["status"] != 0:
                    logger.error(asset_price.get("msg"))
                else:
                    asset_total += asset_price["data"] * float(asset["marginBalance"])
            else:
                asset_total += float(asset["marginBalance"])
        return asset_total

    async def _get_ps_equity(self, ps_client=None) -> float:
        if not ps_client:
            cli: BinanceInverseRestClient = self.client
        else:
            cli = ps_client

        data = await cli.dapi_v1_account()
        if not data:
            raise ValueError("Failed to get equity")

        asset_total = 0
        if not data or not data.get("assets"):
            raise ValueError(data)
        for info in data["assets"]:
            try:
                if float(info["marginBalance"]) == 0:
                    continue
                coin = info["asset"]
                if coin != "USDT":
                    asset_price = await self.get_price(coin + "USDT", from_redis=True)
                    if asset_price["status"] != 0:
                        logger.error(asset_price["msg"])
                    else:
                        asset_total += asset_price["data"] * float(info["marginBalance"])
                else:
                    asset_total += float(info["marginBalance"])
            except:
                raise ValueError(traceback.format_exc())
        return asset_total

    @catch_it
    async def get_assets(self, from_redis: bool = False) -> Balances:
        if AccountType.UNIFIED == self._account_type:
            return await self._get_unified_assets(from_redis)
        elif MarketType.UPERP == self._market_type:
            return await self._get_lps_assets(from_redis)
        elif MarketType.SPOT == self._market_type:
            return await self._get_sp_assets(from_redis)
        elif MarketType.MARGIN == self._market_type:
            return await self._get_margin_assets(from_redis)
        elif self._market_type in [MarketType.CPERP, MarketType.CDELIVERY]:
            return await self._get_ps_assets(from_redis)

        raise ValueError(f"Market type {self._market_type} is not supported")

    async def _get_unified_assets(self, from_redis: bool = False) -> Balances:
        cli: BinanceUnifiedRestClient = self.client
        result: dict[str, Balance] = {}

        if from_redis:
            suffix = "raw:test"
            key = "pm_balance"
            data = await self._load_data_from_rmx_acc(suffix, key)
        else:
            data = await cli.get_balance()

        if self._market_type in [MarketType.MARGIN, MarketType.SPOT]:
            free_key = "crossMarginFree"
            frozen_key = "crossMarginLocked"
        elif self._market_type in [MarketType.UPERP, MarketType.UDELIVERY]:
            free_key = "umWalletBalance"
            frozen_key = "umUnrealizedPNL"
        elif self._market_type in [MarketType.CPERP, MarketType.CDELIVERY]:
            free_key = "cmWalletBalance"
            frozen_key = "cmUnrealizedPNL"
        else:
            raise ValueError(f"Market type {self._market_type} is not supported")

        if not isinstance(data, list):
            raise ValueError(f"Invalid return data[{data}]")

        for info in data:
            balance = float(info[free_key]) + float(info[frozen_key])
            if balance == 0:
                continue
            result[info["asset"]] = Balance(
                asset=info["asset"],
                balance=balance,
                free=float(info[free_key]),
                locked=float(info[frozen_key]),
                type="full",
                ts=info["updateTime"],
            )

        return Balances(result)

    async def _get_sp_assets(self, from_redis: bool = False) -> Balances:
        cli: BinanceSpotRestClient = self.client

        data = None
        result: dict[str, Balance] = {}
        if from_redis:
            suffix = "raw:test"
            key = "spot_balance"
            data = await self._load_data_from_rmx_acc(suffix, key)
        else:
            data = await cli.get_spot_account()

        if data is not None and data.get("balances"):
            for info in data["balances"]:
                balance = float(info["free"]) + float(info["locked"])
                if balance == 0:
                    continue
                result[info["asset"]] = Balance(
                    asset=info["asset"],
                    balance=balance,
                    free=float(info["free"]),
                    locked=float(info["locked"]),
                    type="full",
                    ts=data["updateTime"],
                )
            return Balances(result)
        else:
            if isinstance(data, dict) and "msg" in data.keys():
                err_msg = data["msg"]
            else:
                err_msg = str(data)
            raise ValueError(err_msg)

    async def _get_margin_assets(self, from_redis: bool = False) -> Balances:
        cli: BinanceSpotRestClient = self.client

        result: dict[str, Balance] = {}
        data = None
        if from_redis:
            suffix = "raw:test"
            key = "cross_margin_balance"
            data = await self._load_data_from_rmx_acc(suffix, key)
        else:
            data = await cli.sapi_margin_account()

        update_time = int(time.time() * 1000)
        if data and data.get("userAssets"):
            for info in data["userAssets"]:
                balance = float(info["free"]) + float(info["locked"])
                if balance == 0:
                    continue
                result[info["asset"]] = Balance(
                    asset=info["asset"],
                    balance=balance,
                    free=float(info["free"]),
                    locked=float(info["locked"]),
                    type="full",
                    ts=update_time,
                )
            return Balances(result)
        else:
            if isinstance(data, dict) and "msg" in data.keys():
                err_msg = data["msg"]
            else:
                err_msg = str(data)
            raise ValueError(err_msg)

    async def _get_lps_assets(self, from_redis: bool = False) -> Balances:
        cli: BinanceLinearRestClient = self.client

        result: dict[str, Balance] = {}
        data = None
        if from_redis:
            suffix = "raw:test"
            key = "u_contract_balance"
            data = await self._load_data_from_rmx_acc(suffix, key)
        else:
            data = await cli.fapi_v2_balance()
        if isinstance(data, list):
            for info in data:
                if float(info["balance"]) == 0:
                    continue
                result[info["asset"]] = Balance(
                    asset=info["asset"],
                    balance=float(info["balance"]),
                    free=float(info["maxWithdrawAmount"]),
                    locked=float(info["balance"]) - float(info["maxWithdrawAmount"]),
                    ts=info["updateTime"],
                    type="full",
                )
            return Balances(result)
        else:
            if isinstance(data, dict) and "msg" in data.keys():
                err_msg = data["msg"]
            else:
                err_msg = str(data)
            raise ValueError(err_msg)

    async def _get_ps_assets(self, from_redis: bool = False) -> Balances:
        cli: BinanceInverseRestClient = self.client

        result: dict[str, Balance] = {}
        data = None
        if from_redis:
            suffix = "raw:test"
            key = "coin_contract_balance"
            data = await self._load_data_from_rmx_acc(suffix, key)
        else:
            data = await cli.dapi_v2_balance()
        if isinstance(data, list):
            for info in data:
                if float(info["balance"]) == 0:
                    continue
                result[info["asset"]] = Balance(
                    asset=info["asset"],
                    balance=float(info["balance"]),
                    free=float(info["withdrawAvailable"]),
                    locked=float(info["balance"]) - float(info["withdrawAvailable"]),
                    ts=info["updateTime"],
                    type="full",
                )
            return Balances(result)
        else:
            if isinstance(data, dict) and "msg" in data.keys():
                err_msg = data["msg"]
            else:
                err_msg = str(data)
            raise ValueError(err_msg)

    @catch_it
    async def set_symbol_leverage(self, symbol: str, leverage: int, **kwargs) -> bool:
        assert self._market_type in [MarketType.UPERP, MarketType.UDELIVERY], ValueError(
            f"Market type {self._market_type} is not supported(only supported for UPERP and UDELIVERY)"
        )
        assert self._account_type == AccountType.NORMAL, "Unified account 不支持逐个合约设置杠杆"

        cli: BinanceLinearRestClient = self.client

        symbol = symbol.upper()
        logger.debug(f"Change leverage of {symbol} to {leverage}")
        resp = await cli.set_leverage(symbol=symbol, leverage=leverage)

        if resp is None:
            raise ValueError("Failed to set leverage")

        if resp.get("code"):
            raise ValueError(resp["msg"])
        else:
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
        assert self._account_type == AccountType.NORMAL, "Unified account 不支持划转"
        assert (
            from_market_type is not None or from_account_type is not None
        ), "from_market_type 和 from_account_type 不能同时为空"
        assert (
            to_market_type is not None or to_account_type is not None
        ), "to_market_type 和 to_account_type 不能同时为空"
        assert not (from_market_type and from_account_type), "from_market_type 和 from_account_type 不能同时传"
        assert not (to_market_type and to_account_type), "to_market_type 和 to_account_type 不能同时传"

        transfer_from = ""
        transfer_to = ""
        account_type_dict: dict[AccountType, str] = {
            AccountType.FUND: "FUNDING",
        }
        market_type_dict: dict[MarketType, str] = {
            MarketType.SPOT: "MAIN",
            MarketType.MARGIN: "MARGIN",
            MarketType.UPERP: "UMFUTURE",
            MarketType.UDELIVERY: "UMFUTURE",
            MarketType.CPERP: "CMFUTURE",
            MarketType.CDELIVERY: "CMFUTURE",
        }
        account_meta = copy.deepcopy(self._account_meta)
        account_meta.market_type = MarketType.SPOT
        if self._market_type.is_derivative:
            spot_client: BinanceSpotRestClient = get_rest_client(
                account_meta, self._account_config, rest_config=self._rest_config
            )
        else:
            spot_client: BinanceSpotRestClient = self.client

        if from_market_type is not None:
            assert from_market_type in market_type_dict, f"Invalid from_market_type[{from_market_type}]"
            transfer_from = market_type_dict[from_market_type]
        if to_market_type is not None:
            assert to_market_type in market_type_dict, f"Invalid to_market_type[{to_market_type}]"
            transfer_to = market_type_dict[to_market_type]
        if from_account_type is not None:
            assert from_account_type in account_type_dict, f"Invalid from_account_type[{from_account_type}]"
            transfer_from = account_type_dict[from_account_type]
        if to_account_type is not None:
            assert to_account_type in account_type_dict, f"Invalid to_account_type[{to_account_type}]"
            transfer_to = account_type_dict[to_account_type]
        if transfer_from == transfer_to:
            raise Exception(f"transfer_from[{transfer_from}] 和 transfer_to[{transfer_to}] 相同, 无需划转")
        ret = await spot_client.sapi_asset_transfer(transfer_from + "_" + transfer_to, asset, str(qty))
        if ret and ("tranId" in ret or "txnId" in ret):
            logger.info(f"划转成功: {transfer_from} -> {transfer_to}, {qty} {asset}")
            return TransferResponse(apply_id=ret["tranId"] if "tranId" in ret else ret["txnId"])
        else:
            if not ret or ret["code"] != -11015:
                raise Exception(f"{transfer_from}->{transfer_to}转账失败{'' if not ret else f' 返回: {ret}'}")
            else:
                raise Exception(f"{transfer_from}->{transfer_to}转账失败: {ret['msg']}")

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
        if self._account_type == AccountType.UNIFIED:
            params["portfolioMargin"] = True
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
            # market = self.ccxt_client.market(order_snapshot.symbol)
            # order_snapshot.symbol = market["id"] or ""
        except Exception as e:
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
    async def place_order(
        self,
        symbol: str,
        order_side: Literal["BUY", "SELL"] | OrderSide,
        qty: Decimal | None = None,
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
        send_order_type = order_type.name

        # 市价单不需要price参数
        if order_type == OrderType.MARKET and price is not None:
            raise ValueError("In market_order parameter price not required")

        send_tif = None if order_type == OrderType.MARKET else "GTC"
        if order_time_in_force:
            if (
                TimeInForce.GTX == order_time_in_force
                and self._market_type == MarketType.SPOT
                and self._account_type != AccountType.UNIFIED
            ):
                send_order_type = "LIMIT_MAKER"
                send_tif = None
            else:
                send_tif = order_time_in_force.name
        params = extras or {}
        if reduce_only and self._market_type.is_derivative:
            params["reduceOnly"] = "true"
        if not extras or not extras.get("newOrderRespType"):
            params["newOrderRespType"] = "RESULT"

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

        cli = self.client
        match (self._account_type, self._market_type):
            case (AccountType.NORMAL, MarketType.SPOT) if isinstance(cli, BinanceSpotRestClient):
                func = cli.spot_order
            case (AccountType.NORMAL, MarketType.MARGIN) if isinstance(cli, BinanceSpotRestClient):
                func = cli.sapi_margin_order
            case (AccountType.NORMAL, MarketType.UPERP | MarketType.UDELIVERY) if isinstance(
                cli, BinanceLinearRestClient
            ):
                # 只能使用base_qty
                if use_base_qty:
                    func = cli.lps_order
                else:
                    raise ValueError("Only base_qty is allowed")
            case (AccountType.NORMAL, MarketType.CPERP | MarketType.CDELIVERY) if isinstance(
                cli, BinanceInverseRestClient
            ):
                # 只能使用base_qty
                if use_base_qty:
                    func = cli.ps_order
                else:
                    raise ValueError("Only base_qty is allowed")
            case (AccountType.UNIFIED, MarketType.SPOT | MarketType.MARGIN) if isinstance(
                cli, BinanceUnifiedRestClient
            ):
                func = cli.place_margin_order
            case (AccountType.UNIFIED, MarketType.UPERP | MarketType.UDELIVERY) if isinstance(
                cli, BinanceUnifiedRestClient
            ):
                # 只能使用base_qty
                if use_base_qty:
                    func = cli.place_um_order
                else:
                    raise ValueError("Only base_qty is allowed")
            case (AccountType.UNIFIED, MarketType.CPERP | MarketType.CDELIVERY) if isinstance(
                cli, BinanceUnifiedRestClient
            ):
                # 只能使用base_qty
                if use_base_qty:
                    func = cli.place_cm_order
                else:
                    raise ValueError("Only base_qty is allowed")
            case _:
                raise ValueError(f"{self._account_type}-{self._market_type} is not supported")

        if order_side == OrderSide.UNKNOWN:
            raise ValueError("order_side cannot be UNKNOWN")

        match order_type:
            case OrderType.MARKET:
                if use_base_qty:
                    resp = await func(
                        symbol,
                        side=order_side.name,
                        type=send_order_type,  # type: ignore
                        price=str(price) if price is not None else None,
                        quantity=str(qty) if qty is not None else None,
                        timeInForce=send_tif,  # type: ignore
                        newClientOrderId=client_order_id,
                        **params,
                    )
                else:
                    resp = await func(
                        symbol,
                        side=order_side.name,
                        type=send_order_type,  # type: ignore
                        price=str(price) if price is not None else None,
                        quoteOrderQty=str(qty) if qty is not None else None,
                        timeInForce=send_tif,  # type: ignore
                        newClientOrderId=client_order_id,
                        **params,
                    )

            case _:
                resp = await func(
                    symbol,
                    side=order_side.name,
                    type=send_order_type,  # type: ignore
                    price=str(price) if price is not None else None,
                    quantity=str(qty) if qty is not None else None,
                    timeInForce=send_tif,  # type: ignore
                    newClientOrderId=client_order_id,
                    **params,
                )

        snapshot = OrderSnapshot(
            exch_symbol=symbol,
            client_order_id=client_order_id,
            order_side=order_side,
            order_type=order_type,
            order_time_in_force=order_time_in_force or TimeInForce.UNKNOWN,
            price=price or Decimal(0),
            qty=qty if qty is not None else Decimal(0),
            local_update_ts=int(time.time() * 1000),
        )
        if resp is None:
            snapshot.order_status = OrderStatus.REJECTED
            snapshot.rejected_message = "Failed to place order, response is None"
        elif resp.get("code"):
            if resp["code"] in [-2011, -1001]:
                pass
            elif resp["code"] in [-5022]:
                snapshot.rejected_reason = RejectedReason.POC
            elif resp["code"] in [-2010, -2019]:
                snapshot.rejected_reason = RejectedReason.INSUFFICIENT_BALANCE
            elif resp["code"] in [-1015]:
                snapshot.rejected_reason = RejectedReason.RATE_LIMIT
            snapshot.order_status = OrderStatus.REJECTED
            snapshot.rejected_message = resp["msg"]
        else:
            snapshot.order_id = str(resp["orderId"])
            snapshot.order_status = STATUS_MAP.get(resp["status"], OrderStatus.UNKNOWN)
            snapshot.price = Decimal(resp["price"])
            snapshot.qty = Decimal(resp["origQty"])
            snapshot.filled_qty = Decimal(resp["executedQty"])
            avg_price = 0
            if snapshot.filled_qty > 0:
                if resp.get("cummulativeQuoteQty"):
                    avg_price = float(resp["cummulativeQuoteQty"]) / float(snapshot.filled_qty)
                elif resp.get("avgPrice"):
                    avg_price = float(resp["avgPrice"])
            snapshot.avg_price = avg_price
            if resp.get("transactTime"):
                snapshot.exch_update_ts = float(resp["transactTime"])
            elif resp.get("updateTime"):
                snapshot.exch_update_ts = float(resp["updateTime"])
            snapshot.place_ack_ts = snapshot.local_update_ts
        return snapshot

    @catch_it
    async def ccxt_cancel_order(
        self, symbol: str, order_id: Optional[str] = None, client_order_id: Optional[str] = None
    ) -> OrderSnapshot | None:
        if not order_id and not client_order_id:
            raise ValueError("Either `order_id` or `client_order_id` must be provided")
        params: dict[str, Any] = {}
        if client_order_id:
            params = {"clientOrderId": client_order_id}
        if self._account_type == AccountType.UNIFIED:
            params["portfolioMargin"] = True
        order_resp = await self.ccxt_client.cancel_order(order_id or "", symbol, params=params)
        order_snapshot = OrderSnapshot.from_ccxt_order(order_resp, symbol)
        if order_snapshot.order_id == order_id or order_snapshot.client_order_id == client_order_id:
            return order_snapshot
        else:
            return None

    @catch_it
    async def ccxt_cancel_all(self, symbol: str) -> bool:
        params: dict[str, Any] = {}
        if self._account_type == AccountType.UNIFIED:
            params["portfolioMargin"] = True
        await self.ccxt_client.cancel_all_orders(symbol, params=params)
        return True

    @catch_it
    async def ccxt_sync_open_orders(self, symbol: str) -> list[OrderSnapshot]:
        params: dict[str, Any] = {}
        if self._account_type == AccountType.UNIFIED:
            params["portfolioMargin"] = True
        order_resp = await self.ccxt_client.fetch_open_orders(symbol, params=params)
        order_list = [OrderSnapshot.from_ccxt_order(order, symbol) for order in order_resp]
        return order_list

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
            if self._account_type == AccountType.UNIFIED:
                params["portfolioMargin"] = True
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
    async def cancel_order(self, symbol: str, orderId: Optional[str] = None, clientOid: Optional[str] = None) -> bool:
        assert orderId or clientOid, "Either Parameters `orderId` and `clientOid` is required."
        oid = int(orderId) if isinstance(orderId, str) else None

        cli = self.client
        match (self._account_type, self._market_type):
            case (AccountType.NORMAL, MarketType.SPOT) if isinstance(cli, BinanceSpotRestClient):
                resp = await cli.cancel_order(symbol, orderId, clientOid)
            case (AccountType.NORMAL, MarketType.MARGIN) if isinstance(cli, BinanceSpotRestClient):
                resp = await cli.sapi_cancel_order(symbol, orderId, clientOid)
            case (AccountType.NORMAL, MarketType.UPERP | MarketType.UDELIVERY) if isinstance(
                cli, BinanceLinearRestClient
            ):
                resp = await cli.cancel_order(symbol, orderId, clientOid)
            case (AccountType.NORMAL, MarketType.CPERP | MarketType.CDELIVERY) if isinstance(
                cli, BinanceInverseRestClient
            ):
                resp = await cli.cancel_order(symbol, orderId, clientOid)
            case (AccountType.UNIFIED, MarketType.SPOT | MarketType.MARGIN) if isinstance(
                cli, BinanceUnifiedRestClient
            ):
                resp = await cli.cancel_margin_order(symbol, oid, clientOid)
            case (AccountType.UNIFIED, MarketType.UPERP | MarketType.UDELIVERY) if isinstance(
                cli, BinanceUnifiedRestClient
            ):
                if MarketType.CPERP == self._market_type and (not symbol.endswith("_PERP")):
                    symbol = symbol + "_PERP"
                resp = await cli.cancel_um_order(symbol, oid, clientOid)
            case (AccountType.UNIFIED, MarketType.CPERP | MarketType.CDELIVERY) if isinstance(
                cli, BinanceUnifiedRestClient
            ):
                resp = await cli.cancel_cm_order(symbol, oid, clientOid)
            case _:
                raise ValueError(f"{self._account_type}-{self._market_type} is not supported")

        if resp is None:
            raise ValueError("Failed to cancel order, response is None")
        if resp.get("code"):
            raise ValueError(resp["msg"])
        else:
            return True

    @catch_it
    async def cancel_all(self, symbol: str) -> bool:
        # TODO add unified logic
        cli = self.client
        match (self._account_type, self._market_type):
            case (AccountType.NORMAL, MarketType.MARGIN) if isinstance(cli, BinanceSpotRestClient):
                resp = await cli.sapi_cancel_all_orders(symbol)
            case (AccountType.NORMAL, MarketType.SPOT) if isinstance(cli, BinanceSpotRestClient):
                resp = await cli.cancel_all_orders(symbol)
            case (AccountType.NORMAL, MarketType.UPERP | MarketType.UDELIVERY) if isinstance(
                cli, BinanceLinearRestClient
            ):
                resp = await cli.cancel_all_orders(symbol)
            case (AccountType.NORMAL, MarketType.CPERP | MarketType.CDELIVERY) if isinstance(
                cli, BinanceInverseRestClient
            ):
                resp = await cli.cancel_all_orders(symbol)
            case _:
                raise ValueError(f"{self._account_type}-{self._market_type} is not supported")

        if resp is None:
            raise ValueError
        if isinstance(resp, dict) and resp.get("code") != 200 and resp.get("code") != 0 and resp.get("code") != -2011:
            raise ValueError(resp["msg"])
        else:
            return True

    @catch_it
    async def repay(self, asset: str, amount: Decimal, isolated_symbol: Optional[str] = None) -> bool:
        if MarketType.MARGIN != self._market_type:
            raise ValueError(f"Market type {self._market_type} is not supported(only supported for MARGIN)")

        cli: BinanceSpotRestClient = self.client
        if isolated_symbol:
            resp = await cli.sapi_margin_repay(asset, str(amount), isIsolated=True, symbol=isolated_symbol)
        else:
            resp = await cli.sapi_margin_repay(asset, str(amount))

        if resp is None:
            raise ValueError("Failed to repay")

        if resp.get("code"):
            raise ValueError(resp["msg"])
        else:
            return True

    @catch_it
    async def get_price(self, symbol: str, from_redis: bool = False) -> float:
        if from_redis:
            if MarketType.SPOT == self._market_type or MarketType.MARGIN == self._market_type:
                std_symbol = symbol[:-4].upper() + "_USDT|SPOT|BINANCE"
            elif MarketType.UPERP == self._market_type:
                std_symbol = symbol[:-4].upper() + "_USDT|UPERP|BINANCE"
            elif MarketType.CPERP == self._market_type:
                if symbol.endswith("_PERP"):
                    std_symbol = symbol[:-8].upper() + "_USD|CPERP|BINANCE"
                else:
                    std_symbol = symbol[:-3].upper() + "_USD|CPERP|BINANCE"
            elif MarketType.CDELIVERY == self._market_type:
                if len(symbol.split("_")) == 2 and str(symbol.split("_")[1]).isdigit():
                    # symbol eg: BTCUSD_240628 -> std_symbol eg: BTC_USD_NQ|CDELIVERY|BINANCE
                    today = datetime.today()
                    quarter = (today.month - 1) // 3 + 1
                    tmp_symbol, tmp_quarter = symbol.split("_")
                    symbol_quarter = (
                        (datetime.strptime(str(today.year)[:2] + tmp_quarter, "%Y%m%d")).month - 1
                    ) // 3 + 1
                    if quarter == symbol_quarter:
                        contract_type = "CQ"
                    elif quarter == (symbol_quarter - 1):
                        contract_type = "NQ"
                    else:
                        raise ValueError(f"symbol: {symbol} is invalid with Market type {self._market_type}")
                    std_symbol = tmp_symbol[:-3].upper() + f"_USD_{contract_type}|CDELIVERY|BINANCE"
                else:
                    raise ValueError(f"symbol: {symbol} is invalid with Market type {self._market_type}")
            else:
                std_symbol = ""

            data = await self._load_data_from_kit(name="ticker", key=std_symbol)
            if data:
                return (float(data["apx"]) + float(data["bpx"])) / 2

        # get from exchange
        mkt_cli = self.market_client
        match self._market_type:
            case MarketType.SPOT | MarketType.MARGIN if isinstance(mkt_cli, BinanceSpotRestClient):
                resp = await mkt_cli.get_price(symbol)
            case MarketType.UPERP | MarketType.UDELIVERY if isinstance(mkt_cli, BinanceLinearRestClient):
                resp = await mkt_cli.get_price(symbol)
            case MarketType.CPERP | MarketType.CDELIVERY if isinstance(mkt_cli, BinanceInverseRestClient):
                resp = await mkt_cli.get_price(symbol)
            case _:
                raise ValueError(f"{self._account_type}-{self._market_type} is not supported")

        if isinstance(resp, dict):
            if resp.get("price"):
                return float(resp["price"])
            else:
                raise ValueError(resp.get("msg", resp))
        elif isinstance(resp, list) and isinstance(resp[0], dict) and resp[0].get("price"):
            return float(resp[0]["price"])  # for CPERP,CDELIVERY return data
        else:
            raise ValueError("fail to get price")

    @catch_it
    async def get_prices(self) -> Prices:
        mkt_cli = self.market_client
        match self._market_type:
            case MarketType.SPOT | MarketType.MARGIN if isinstance(mkt_cli, BinanceSpotRestClient):
                resp = await mkt_cli.get_price()
            case MarketType.UPERP | MarketType.UDELIVERY if isinstance(mkt_cli, BinanceLinearRestClient):
                resp = await mkt_cli.get_price()
            case MarketType.CPERP | MarketType.CDELIVERY if isinstance(mkt_cli, BinanceInverseRestClient):
                resp = await mkt_cli.get_price()
            case _:
                raise ValueError(f"{self._account_type}-{self._market_type} is not supported")

        if resp and isinstance(resp, list):
            if MarketType.CPERP == self._market_type:
                return Prices(
                    {item["symbol"]: float(item["price"]) for item in resp if item["symbol"].endswith("PERP")}
                )
            elif MarketType.CDELIVERY == self._market_type:
                return Prices(
                    {item["symbol"]: float(item["price"]) for item in resp if not item["symbol"].endswith("PERP")}
                )
            return Prices({item["symbol"]: float(item["price"]) for item in resp})
        else:
            raise ValueError(resp)

    @catch_it
    async def get_tickers(self) -> Tickers:
        tickers = await self.market_client.get_ticker()
        if tickers is None or isinstance(tickers, dict):
            raise ValueError(f"Failed to get tickers, response: {tickers}")

        update_ts = float(time.time() * 1_000)
        tickers = {
            ticker["symbol"]: Ticker(
                ticker["symbol"],
                float(ticker["bidPrice"]) if ticker["bidPrice"] else np.nan,
                float(ticker["askPrice"]) if ticker["askPrice"] else np.nan,
                ts=ticker.get("time", update_ts),
                update_ts=update_ts,
                bid_qty=float(ticker["bidQty"]) if ticker["bidQty"] else np.nan,
                ask_qty=float(ticker["askQty"]) if ticker["askQty"] else np.nan,
            )
            for ticker in tickers
        }
        if self._market_type == MarketType.UPERP:
            premium_info = await self.market_client.get_linear_swap_premium_index()
            if premium_info is None or isinstance(premium_info, dict):
                raise ValueError(f"Failed to get premium index, response: {premium_info}")
            for premium in premium_info:
                symbol = premium["symbol"]
                if symbol in tickers:
                    tickers[symbol].index_price = float(premium["indexPrice"]) if "indexPrice" in premium else np.nan
                    tickers[symbol].fr = float(premium["lastFundingRate"]) if "lastFundingRate" in premium else np.nan
                    tickers[symbol].fr_ts = (
                        float(premium["nextFundingTime"]) if "nextFundingTime" in premium else np.nan
                    )
        return Tickers(tickers)

    @catch_it
    async def get_quotations(self) -> Quotations:
        mkt_cli = self.market_client
        match self._market_type:
            case MarketType.SPOT | MarketType.MARGIN if isinstance(mkt_cli, BinanceSpotRestClient):
                resp = await mkt_cli.get_ticker()
            case MarketType.UPERP | MarketType.UDELIVERY if isinstance(mkt_cli, BinanceLinearRestClient):
                resp = await mkt_cli.get_ticker()
            case MarketType.CPERP | MarketType.CDELIVERY if isinstance(mkt_cli, BinanceInverseRestClient):
                resp = await mkt_cli.get_ticker()
            case _:
                raise ValueError(f"{self._account_type}-{self._market_type} is not supported")

        if resp is None or isinstance(resp, dict):
            raise ValueError(f"Failed to get quotations, response: {resp}")

        update_ts = float(time.time() * 1_000)
        quotations = {
            t["symbol"]: Quotation(
                t["symbol"],
                float(t["bidPrice"]) if t["bidPrice"] else np.nan,
                float(t["askPrice"]) if t["askPrice"] else np.nan,
                ts=t.get("time", update_ts),
                update_ts=update_ts,
                bid_qty=float(t["bidQty"]) if t["bidQty"] else np.nan,
                ask_qty=float(t["askQty"]) if t["askQty"] else np.nan,
            )
            for t in resp
        }

        return Quotations(quotations)

    @catch_it
    async def get_orderbook_snapshot(self, symbol: str, limit: int = 100) -> OrderBook:
        resp = await self.market_client.get_depth(symbol, limit=limit)
        if not (isinstance(resp, dict) and "bids" in resp):
            raise Exception(f"Get orderbook snapshot failed. err_mgs={resp}")
        orderbook = OrderBook(symbol)
        orderbook.exch_seq = resp["lastUpdateId"]
        orderbook.recv_ts = int(time.time() * 1_000)
        orderbook.exch_ts = int(resp.get("T", time.time() * 1_000))
        for bid in resp["bids"]:
            orderbook.bids.append((Decimal(bid[0]), Decimal(bid[1])))
        for ask in resp["asks"]:
            orderbook.asks.append((Decimal(ask[0]), Decimal(ask[1])))
        return orderbook

    @catch_it
    async def get_fundamentals(self) -> Fundamentals:
        resp = await self.market_client.get_24h_info()
        if resp is None or isinstance(resp, dict):
            raise ValueError(f"Failed to get fundamentals, response: {resp}")
        fundamentals: dict[str, Fundamental] = {}
        _last_price_dic = {}
        for ticker in resp:
            symbol = ticker["symbol"]
            if (symbol not in self._insts) or (self._insts[symbol].status != InstStatus.TRADING):
                continue
            _last_price_dic[symbol] = float(ticker["lastPrice"])
            if self._market_type not in [MarketType.CPERP, MarketType.CDELIVERY]:
                fundamentals[symbol] = Fundamental(
                    symbol, float(ticker["priceChangePercent"]) / 100, float(ticker["quoteVolume"])
                )
            else:
                turnover_24h = Decimal(ticker["volume"]) * self._insts[symbol].quantity_multiplier
                fundamentals[symbol] = Fundamental(
                    symbol,
                    float(ticker["priceChangePercent"]) / 100,
                    float(turnover_24h),
                )
        if self._market_type in [MarketType.UPERP]:
            for symbol, last_price in _last_price_dic.items():
                resp = await self.market_client.get_open_interest(symbol)
                if isinstance(resp, dict) and "code" not in resp:
                    symbol = resp["symbol"]
                    if symbol in fundamentals:
                        fundamentals[symbol].open_interest = (
                            float(resp["openInterest"]) * float(self._insts[symbol].quantity_multiplier) * last_price
                        )
        return fundamentals

    @catch_it
    async def get_discount_rate(self, ccy: str):
        resp = await self.client.get_discount_rate_interest_free_quota(ccy)
        if resp["code"] != "0":
            raise ValueError(resp["msg"])
        else:
            discount_info = resp["data"][0]["discountInfo"]
            discount_rate = [DiscountRate(int(r["minAmt"]), Decimal(r["discountRate"])) for r in discount_info]
            discount_rate = sorted(discount_rate, key=lambda x: x.discount_rate, reverse=True)
            return DiscountRateData(discount_rate)

    @catch_it
    async def get_trade_history(self, start_time: int, end_time: int, symbol_list: list[str]):
        result: dict[str, list[Trade]] = {}
        trade_limit = 1000

        cli = self.client
        match (self._account_type, self._market_type):
            case (AccountType.UNIFIED, MarketType.SPOT | MarketType.MARGIN) if isinstance(
                cli, BinanceUnifiedRestClient
            ):
                trade_func = cli.papi_v1_margin_myTrades
            case (AccountType.UNIFIED, MarketType.UPERP | MarketType.UDELIVERY) if isinstance(
                cli, BinanceUnifiedRestClient
            ):
                trade_func = cli.papi_v1_um_userTrades
            case (AccountType.UNIFIED, MarketType.CPERP | MarketType.CDELIVERY) if isinstance(
                cli, BinanceUnifiedRestClient
            ):
                trade_func = cli.papi_v1_cm_userTrades
            case (AccountType.NORMAL, MarketType.SPOT) if isinstance(cli, BinanceSpotRestClient):
                trade_func = cli.get_trade_history
            case (AccountType.NORMAL, MarketType.MARGIN) if isinstance(cli, BinanceSpotRestClient):
                trade_func = cli.sapi_get_trade_history
            case (AccountType.NORMAL, MarketType.UPERP | MarketType.UDELIVERY) if isinstance(
                cli, BinanceLinearRestClient
            ):
                trade_func = cli.fapi_v1_userTrades
            case (AccountType.NORMAL, MarketType.CPERP | MarketType.CDELIVERY) if isinstance(
                cli, BinanceInverseRestClient
            ):
                trade_func = cli.dapi_v1_userTrades
            case _:
                raise ValueError(f"{self._account_type}-{self._market_type} is not supported")

        trade_data_list = []
        for symbol in symbol_list:
            tmp_start_time = start_time
            if MarketType.CPERP == self._market_type and (not symbol.endswith("_PERP")):
                symbol = symbol + "_PERP"
            while True:
                trade_resp = await trade_func(symbol, startTime=tmp_start_time, endTime=end_time, limit=trade_limit)
                await asyncio.sleep(0.2)
                if trade_resp is None:
                    raise ValueError("Failed to get trade history, response is None")
                elif trade_resp and isinstance(trade_resp, dict) and trade_resp.get("code"):
                    raise ValueError(
                        f"account[{self._account}] {self._market_type} symbol[{symbol}], error: {trade_resp['msg']}"
                    )
                else:
                    trade_data_list.extend(trade_resp)
                    if len(trade_resp) == trade_limit:
                        tmp_start_time = trade_data_list[-1]["time"]
                    else:
                        break
            for data in trade_data_list:
                if self._market_type.is_derivative:
                    side = data["side"]
                else:
                    side = "BUY" if data["isBuyer"] == True else "SELL"
                result.setdefault(data["symbol"], []).append(
                    Trade(
                        create_ts=data["time"],
                        side=getattr(OrderSide, side, OrderSide.UNKNOWN),
                        trade_id=str(data["id"]),
                        order_id=str(data["orderId"]),
                        last_trd_price=Decimal(data["price"]),
                        last_trd_volume=Decimal(data["qty"]),
                        turnover=Decimal(data["price"]) * Decimal(data["qty"]),
                        fill_ts=data["time"],
                        fee=Decimal(data["commission"]),
                        fee_ccy=data["commissionAsset"],
                        is_maker=data.get("isMaker", data.get("maker", -1)),
                    )
                )
        return TradeData(result)

    @catch_it
    async def get_order_history(self, start_time: int, end_time: int, symbol_list: list[str]) -> OrderSnapshotData:
        order_dict: dict[str, list[OrderSnapshot]] = {}
        order_data_list = []

        cli = self.client
        match (self._account_type, self._market_type):
            case (AccountType.NORMAL, MarketType.SPOT) if isinstance(cli, BinanceSpotRestClient):
                order_func = cli.api_v3_allOrders
                order_limit = 500
            case (AccountType.NORMAL, MarketType.MARGIN) if isinstance(cli, BinanceSpotRestClient):
                order_func = cli.sapi_v1_allOrders
                order_limit = 500
            case (AccountType.NORMAL, MarketType.UPERP | MarketType.UDELIVERY) if isinstance(
                cli, BinanceLinearRestClient
            ):
                order_func = cli.fapi_v1_allOrders
                order_limit = 1000
            case (AccountType.NORMAL, MarketType.CPERP | MarketType.CDELIVERY) if isinstance(
                cli, BinanceInverseRestClient
            ):
                order_func = cli.dapi_v1_allOrders
                order_limit = 1000
            case (AccountType.UNIFIED, MarketType.SPOT | MarketType.MARGIN) if isinstance(
                cli, BinanceUnifiedRestClient
            ):
                order_func = cli.papi_v1_margin_allOrders
                order_limit = 500
            case (AccountType.UNIFIED, MarketType.UPERP | MarketType.UDELIVERY) if isinstance(
                cli, BinanceUnifiedRestClient
            ):
                order_func = cli.papi_v1_um_allOrders
                order_limit = 1000
            case (AccountType.UNIFIED, MarketType.CPERP | MarketType.CDELIVERY) if isinstance(
                cli, BinanceUnifiedRestClient
            ):
                order_func = cli.papi_v1_cm_allOrders
                order_limit = 1000
            case _:
                raise ValueError(f"{self._account_type}-{self._market_type} is not supported")

        for symbol in symbol_list:
            tmp_start_time = start_time
            if MarketType.CPERP == self._market_type and (not symbol.endswith("_PERP")):
                symbol = symbol + "_PERP"
            while True:
                resp = await order_func(symbol, startTime=tmp_start_time, endTime=end_time, limit=order_limit)
                await asyncio.sleep(0.2)
                if resp is None or (isinstance(resp, dict) and resp.get("code")):
                    logger.error(
                        f"account[{self._account}] MarketType[{self._market_type}] symbol[{symbol}], error: {resp}"
                    )
                    await asyncio.sleep(0.2)
                    break
                else:
                    order_data_list.extend(resp)
                    if len(resp) == order_limit:
                        tmp_start_time = order_data_list[-1]["time"] + 1
                    else:
                        break

        for od in order_data_list:
            order_type = getattr(OrderType, od["type"], OrderType.UNKNOWN)
            tif = TIF_MAP.get(od["timeInForce"], TimeInForce.UNKNOWN)
            status = STATUS_MAP.get(od["status"], OrderStatus.UNKNOWN)
            side = getattr(OrderSide, od["side"].upper(), OrderSide.UNKNOWN)

            o = OrderSnapshot(
                exch_symbol=od["symbol"],
                order_side=side,
                order_id=str(od["orderId"]),
                client_order_id=od["clientOrderId"],
                qty=Decimal(od["origQty"]),
                price=Decimal(od["price"]),
                filled_qty=Decimal(od.get("executedQty", "0")),
                avg_price=float(od.get("avgPrice", "0")),
                order_type=order_type,
                order_time_in_force=tif,
                order_status=status,
                place_ack_ts=int(od["time"]),
                exch_update_ts=int(od["updateTime"]),
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
        assert self._market_type in [MarketType.CPERP, MarketType.UPERP], f"Invalid Market type {self._market_type}"
        income_list = []
        funding_dict: dict[str, list[FundingFee]] = {}

        cli = self.client
        match (self._account_type, self._market_type):
            case (AccountType.UNIFIED, MarketType.UPERP) if isinstance(cli, BinanceUnifiedRestClient):
                func = cli.papi_v1_um_income
            case (AccountType.UNIFIED, MarketType.CPERP) if isinstance(cli, BinanceUnifiedRestClient):
                func = cli.papi_v1_cm_income
            case (AccountType.NORMAL, MarketType.UPERP) if isinstance(cli, BinanceLinearRestClient):
                func = cli.fapi_v1_income
            case (AccountType.NORMAL, MarketType.CPERP) if isinstance(cli, BinanceInverseRestClient):
                func = cli.dapi_v1_income
            case _:
                raise ValueError(f"{self._account_type}-{self._market_type} is not supported")

        while True:
            resp = await func(
                symbol=None, incomeType="FUNDING_FEE", startTime=start_time, endTime=end_time, limit=1000
            )
            if not isinstance(resp, list):
                raise ValueError(resp)

            income_list.extend(resp)
            if len(resp) != 1000:
                break

            start_time = income_list[-1]["time"]

        for item in income_list:
            if symbol_list and item["symbol"] not in symbol_list:
                continue
            if item["symbol"] not in funding_dict:
                funding_dict[item["symbol"]] = [FundingFee(Decimal(item["income"]), item["time"])]
            else:
                funding_dict[item["symbol"]].append(FundingFee(Decimal(item["income"]), item["time"]))

        return FundingFeeData(funding_dict)

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
        limit = 1000
        data: list[dict[str, Any]] = []

        match self._market_type:
            ## FIXME: corner case? BUG?
            # case MarketType.UPERP if len(symbol_list) != 1:
            #     _start_ts = start_ts
            #     while True:
            #         funding_rate_his = await self.market_client.get_funding_rate(start_time=_start_ts, limit=limit)
            #         await asyncio.sleep(0.2)
            #         if funding_rate_his is None:
            #             break
            #         data.extend(funding_rate_his)
            #         if len(funding_rate_his) < limit:
            #             break
            #         _start_ts = int(funding_rate_his[-1]["fundingTime"])
            case _:
                for symbol in symbol_list:
                    count = 0
                    _start_ts = start_ts
                    for _ in range(1000):
                        count += 1
                        funding_rate_his = await self.market_client.get_funding_rate(
                            symbol=symbol, start_time=_start_ts, limit=limit
                        )
                        await asyncio.sleep(0.5)
                        if funding_rate_his is None:
                            await asyncio.sleep(1)
                            break
                        data.extend(funding_rate_his)
                        if len(funding_rate_his) < limit:
                            break
                        _start_ts = int(funding_rate_his[-1]["fundingTime"])

        frs: dict[str, set[FundingRateSimple]] = {}
        for d in data:
            if d["symbol"] not in symbol_list:
                continue
            symbol = d["symbol"]
            fr = float(d["fundingRate"])
            ts = float(d["fundingTime"])
            frs.setdefault(symbol, set()).add(FundingRateSimple(fr, ts))

        return FundingRatesHis({symbol: sorted(list(fr)) for symbol, fr in frs.items()})

    @catch_it
    async def get_current_funding_rate(self, symbol_list: list[str] | None = None) -> FundingRatesCur:
        assert MarketType.UPERP == self._market_type, "only support get current funding rate for UPERP market"
        funding_rates_ret = await self.market_client.get_linear_swap_premium_index()
        if funding_rates_ret is None:
            raise ValueError("Failed to get current funding rate, response is None")
        if isinstance(funding_rates_ret, dict) and funding_rates_ret.get("code"):
            raise ValueError(funding_rates_ret["msg"])
        funding_rates_dict = {fr["symbol"]: fr for fr in funding_rates_ret}

        if not symbol_list:
            symbol_list = list(funding_rates_dict.keys())

        funding_times_ret = await self.market_client.get_funding_info()
        if funding_times_ret is None:
            raise ValueError("Failed to get funding times, response is None")
        funding_times_dict = {ft["symbol"]: ft for ft in funding_times_ret}
        fr_limit_dict = {}
        if self._account_config.has_credentials():
            leverage_bracket_ret = await self.market_client.get_leverage_bracket()
            if not isinstance(leverage_bracket_ret, list):
                raise ValueError(f"Failed to get leverage bracket, response is not a list: {leverage_bracket_ret}")
            for le in leverage_bracket_ret:
                symbol = le["symbol"]
                for bracket in le["brackets"]:
                    if bracket["bracket"] == 1:
                        fr_limit_dict[symbol] = bracket["maintMarginRatio"] * 0.75
        frs: FundingRatesCur = FundingRatesCur()
        for symbol in symbol_list:
            fr = float(funding_rates_dict.get(symbol, {}).get("lastFundingRate", 0))
            ts = float(funding_rates_dict.get(symbol, {}).get("nextFundingTime", 0))
            interval_hour = int(funding_times_dict.get(symbol, {}).get("fundingIntervalHours", 8))
            fr_cap = float(
                funding_times_dict.get(symbol, {}).get("adjustedFundingRateCap", fr_limit_dict.get(symbol, 0))
            )
            fr_floor = float(
                funding_times_dict.get(symbol, {}).get("adjustedFundingRateFloor", -fr_limit_dict.get(symbol, 0))
            )
            frs[symbol] = FundingRate(fr, ts, interval_hour, fr_cap, fr_floor)
        return frs

    @catch_it
    async def get_current_simple_funding_rate(self, symbol_list: list[str] | None = None) -> FundingRatesSimple:
        assert MarketType.UPERP == self._market_type, "only support get current funding rate for UPERP market"
        funding_rates_ret = await self.market_client.get_linear_swap_premium_index()
        if funding_rates_ret is None:
            raise ValueError("Failed to get current funding rate, response is None")
        if isinstance(funding_rates_ret, dict) and funding_rates_ret.get("code"):
            raise ValueError(funding_rates_ret["msg"])

        funding_rates_dict = {fr["symbol"]: fr for fr in funding_rates_ret}
        if not symbol_list:
            symbol_list = list(funding_rates_dict.keys())

        funding_times_ret = await self.market_client.get_funding_info()
        if funding_times_ret is None:
            raise ValueError("Failed to get funding times, response is None")
        funding_times_dict = {ft["symbol"]: ft for ft in funding_times_ret}

        frs: FundingRatesSimple = FundingRatesSimple()
        for symbol in symbol_list:
            fr = float(funding_rates_dict.get(symbol, {}).get("lastFundingRate", 0))
            ts = float(funding_rates_dict.get(symbol, {}).get("nextFundingTime", 0))
            interval_hour = int(funding_times_dict.get(symbol, {}).get("fundingIntervalHours", 8))
            frs[symbol] = FundingRateSimple(fr, ts, interval_hour)
        return frs

    @catch_it
    async def get_historical_kline(
        self,
        symbol: str,
        interval: Interval,
        start_time: int,
        end_time: Optional[int] = None,
        contract_type: Literal["PERPETUAL", "CURRENT_QUARTER", "NEXT_QUARTER"] = "PERPETUAL",
    ):
        kline_list = []
        result: list[KLine] = []
        if not end_time:
            end_time = int(time.time() * 1000)
        interval_str = interval.name.lstrip("_")
        if MarketType.CDELIVERY == self._market_type or MarketType.CPERP == self._market_type:
            symbol = symbol.split("_")[0]
        max_limit = 1000
        while True:
            if not self._market_type.is_derivative:
                resp = await self.market_client.get_history_kline(
                    symbol, interval_str, start_time, end_time, limit=max_limit
                )
            else:
                resp = await self.market_client.get_history_kline(
                    symbol,
                    interval_str,
                    contract_type=contract_type,
                    start_time=start_time,
                    end_time=end_time,
                    limit=max_limit,
                )
            if resp is None:
                raise ValueError("Failed to get kline data, response is None")
            if isinstance(resp, dict) and resp.get("code"):
                raise ValueError(resp["msg"])
            if self._market_type in [MarketType.SPOT, MarketType.UPERP]:
                kline_list += resp
            else:
                kline_list += resp[::-1]
            if len(resp) == max_limit:
                if self._market_type in [MarketType.SPOT, MarketType.UPERP]:
                    start_time = int(resp[-1][0]) + 1
                else:
                    end_time = int(resp[0][0]) - 1
            else:
                break

        if self._market_type in [MarketType.SPOT, MarketType.UPERP]:
            for lis in kline_list:
                result.append(
                    KLine(
                        start_ts=int(lis[0]),
                        open=Decimal(lis[1]),
                        high=Decimal(lis[2]),
                        low=Decimal(lis[3]),
                        close=Decimal(lis[4]),
                        volume=Decimal(lis[5]),
                        turnover=Decimal(lis[7]),
                    )
                )
            return KLineData(result)
        else:
            for lis in kline_list[::-1]:
                result.append(
                    KLine(
                        start_ts=int(lis[0]),
                        open=Decimal(lis[1]),
                        high=Decimal(lis[2]),
                        low=Decimal(lis[3]),
                        close=Decimal(lis[4]),
                        volume=Decimal(lis[5]),
                        turnover=Decimal(lis[7]),
                    )
                )
            return KLineData(result)

    @catch_it
    async def get_leverage(self, symbol: str, mgnMode: MarginMode):
        assert self._market_type.is_derivative, f"Market type {self._market_type} is not supported for get_leverage"
        if AccountType.UNIFIED == self._account_type:
            resp = await self.client.get_position_risk(symbol=symbol, category=self.get_unified_category())
        elif self._market_type == MarketType.UPERP:
            resp = await self.client.get_linear_swap_position(symbol)
        else:
            raise ValueError(f"Market type {self._market_type} is not supported for get_leverage")
        leverage = Leverage()
        if resp is None:
            raise ValueError(f"Could not get leverage for symbol[{symbol}] mgnMode[{mgnMode}]")
        if isinstance(resp, dict):
            if resp.get("code"):
                raise ValueError(resp["msg"])
        for data in resp:
            if data["symbol"] == symbol and data["marginType"] == mgnMode.name.lower():
                if data["positionSide"] == "LONG":
                    leverage.long = Decimal(data["leverage"])
                elif data["positionSide"] == "SHORT":
                    leverage.short = Decimal(data["leverage"])
                elif data["positionSide"] == "BOTH":
                    leverage.long = Decimal(data["leverage"])
                    leverage.short = Decimal(data["leverage"])
        if leverage.long or leverage.short:
            return leverage
        raise ValueError(f"fail to get leverage for symbol[{symbol}] mgnMode[{mgnMode}] resp[{resp}]")

    @catch_it
    async def get_max_open_notional(self, symbol: str, mgnMode: MarginMode = MarginMode.CROSS):
        # Binance 期货只在有持仓的时候才能拿到max_notional（exchangeInfo里只有min notional）, 现货在exchangeInfo里面获取
        if self._market_type in [MarketType.SPOT, MarketType.MARGIN]:
            resp = await self.client.get_exchange_info(symbol)
            if resp.get("code"):
                raise ValueError(resp["msg"])
            else:
                limit = [i for i in resp["symbols"][0]["filters"] if i.get("filterType") == "NOTIONAL"]
                if limit:
                    max_notional = Decimal(limit[0]["maxNotional"])
                    return MaxOpenNotional(buy=max_notional, sell=max_notional)
        else:
            if AccountType.UNIFIED == self._account_type:
                resp = await self.client.get_position_risk(symbol=symbol, category=self.get_unified_category())
            elif self._market_type == MarketType.UPERP:
                resp = await self.client.get_linear_swap_position(symbol)
            else:
                resp = await self.client.get_inverse_swap_position(symbol)
            buy_notional = Decimal(0)
            sell_notional = Decimal(0)
            if resp is None:
                raise ValueError(f"Could not get maxNotionalValue for symbol[{symbol}] mgnMode[{mgnMode}]")
            if isinstance(resp, dict):
                if resp.get("code"):
                    raise ValueError(resp["msg"])
            for data in resp:
                if data["symbol"] == symbol and data["marginType"] == mgnMode.name.lower():
                    if data["positionSide"] == "LONG":
                        buy_notional = Decimal(data["maxNotionalValue"])
                    elif data["positionSide"] == "SHORT":
                        sell_notional = Decimal(data["maxNotionalValue"])
                    elif data["positionSide"] == "BOTH":
                        buy_notional = Decimal(data["maxNotionalValue"])
                        sell_notional = Decimal(data["maxNotionalValue"])
            if buy_notional or sell_notional:
                return MaxOpenNotional(buy=buy_notional, sell=sell_notional)
        raise ValueError(f"fail to get maxNotionalValue for symbol[{symbol}] mgnMode[{mgnMode}] resp[{resp}]")

    @catch_it
    async def get_commission_rate(self, symbol: str, from_redis: bool = False) -> CommissionRate:
        if from_redis:
            assert self._account, "account is required when from_redis is True"
            data = await self._load_data_from_rmx("trading_fee:binance", key=self._account)
            if not data:
                raise ValueError(f"Could not get current commission rate from redis for symbol[{symbol}]")

            if self._market_type in [MarketType.SPOT, MarketType.MARGIN]:
                makerfee = data["spot_maker"]
                takerfee = data["spot_taker"]
            else:
                makerfee = data["swap_maker"]
                takerfee = data["swap_taker"]
        else:
            match (self._account_type, self._market_type):
                case (AccountType.UNIFIED, MarketType.UPERP | MarketType.UDELIVERY):
                    resp = await self.client.papi_v1_um_commission_rate(symbol)
                    if not isinstance(resp, dict):
                        raise ValueError(f"Could not get current commission rate for symbol[{symbol}]")

                    makerfee = resp["makerCommissionRate"]
                    takerfee = resp["takerCommissionRate"]
                case (AccountType.UNIFIED, MarketType.CPERP | MarketType.CDELIVERY):
                    resp = await self.client.papi_v1_cm_commission_rate(symbol)
                    if not isinstance(resp, dict):
                        raise ValueError(f"Could not get current commission rate for symbol[{symbol}]")

                    makerfee = resp["makerCommissionRate"]
                    takerfee = resp["takerCommissionRate"]
                case (_, MarketType.SPOT | MarketType.MARGIN):
                    resp = await self.market_client.get_spot_account()
                    if not isinstance(resp, dict):
                        raise ValueError(f"Could not get current commission rate for symbol[{symbol}]")

                    if resp.get("code"):
                        raise ValueError(resp["msg"])

                    makerfee = resp["commissionRates"]["maker"]
                    takerfee = resp["commissionRates"]["taker"]
                case (_, MarketType.UPERP | MarketType.UDELIVERY | MarketType.CPERP | MarketType.CDELIVERY):
                    resp = await self.market_client.get_commission_rate(symbol=symbol)
                    if not isinstance(resp, dict):
                        raise ValueError(f"Could not get current commission rate for symbol[{symbol}]")

                    if resp.get("code"):
                        raise ValueError(resp["msg"])

                    makerfee = resp["makerCommissionRate"]
                    takerfee = resp["takerCommissionRate"]
                case _:
                    raise UnsupportedOperationError(
                        f"Account type {self._account_type} and market type {self._market_type} is not supported"
                    )  # should not reach here

        return CommissionRate(maker=Decimal(str(makerfee)), taker=Decimal(str(takerfee)))

    @catch_it
    async def get_account_vip_level(self) -> str | int:
        if self._market_type in [MarketType.SPOT, MarketType.MARGIN]:
            resp = await self.market_client.sapi_v1_account_info()
            if not (isinstance(resp, dict) and "vipLevel" in resp):
                raise Exception(f"查询vipLevel相关信息失败, 返回: {resp}")
            return resp["vipLevel"]
        else:
            raise ValueError(f"Market type {self._market_type} is not supported for get_account_vip_level")

    @catch_it
    async def get_interest_rates_cur(
        self,
        vip_level: int | str | None = None,
        vip_loan: bool = False,
        asset: str = "",
        days: int = -1,
    ) -> InterestRates:
        def _get_daily_rate(data: Optional[dict[str, str]] = None) -> Decimal:
            if data is not None:
                return Decimal(data["annuallyRate"]) / 365
            return Decimal(-1)

        assert self._market_type == MarketType.SPOT, f"Invalid Market type {self._market_type}, only support SPOT"
        assert days == -1, "param days only support -1"
        interest_rates: list[InterestRate] = []
        if vip_loan:
            resp = await self.market_client.bapi_vip_loanable_asset(vip_level)
            for line in resp.get("data", []):
                coin = line["coin"]
                if asset and coin != asset:
                    continue
                interest_rates.append(
                    InterestRate(
                        asset=coin,
                        days=days,
                        ir=_get_daily_rate(line["flexibleRates"][0]),
                        ts=time.time() * 1000,  # ms
                    )
                )
        else:
            resp = await self.market_client.sapi_loan_flexible_loanable(loanCoin=asset)
            if not (isinstance(resp, dict) and "rows" in resp):
                raise ValueError(f"unexpected response[{resp}]")
            for line in resp["rows"]:
                coin = line["loanCoin"]
                if asset and coin != asset:
                    continue
                interest_rates.append(
                    InterestRate(
                        asset=coin,
                        days=days,
                        ir=Decimal(line["flexibleInterestRate"]) / 365,
                        ts=time.time() * 1000,  # ms
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
        assert asset, "param `asset` is empty"
        assert self._market_type == MarketType.SPOT, f"Invalid Market type {self._market_type}, only support SPOT"
        window_size = 180 * 24 * 60 * 60 * 1000 if vip_loan else 90 * 24 * 60 * 60 * 1000
        if not end_time:
            end_time = int(datetime.now().timestamp() * 1000)
        if not start_time:
            start_time = end_time - 30 * 24 * 60 * 60 * 1000

        interest_rates: list[InterestRate] = []
        data_list: list[dict[str, str]] = []
        tmp_e_time = end_time
        tmp_s_time = end_time - window_size if end_time - start_time > window_size else start_time
        while True:
            current = 1
            while True:
                if vip_loan:
                    resp = await self.market_client.sapi_loan_vip_request_interest_history(
                        coin=asset, startTime=tmp_s_time, endTime=tmp_e_time, current=current
                    )
                else:
                    resp = await self.market_client.sapi_loan_interest_history(
                        coin=asset, startTime=tmp_s_time, endTime=tmp_e_time, current=current
                    )
                if not (isinstance(resp, dict) and "rows" in resp):
                    raise ValueError(resp)
                await asyncio.sleep(5)  # 请求权重(IP): 400
                data = resp["rows"]
                if data:
                    data_list.extend(data)
                else:
                    break
                current += 1
            if tmp_s_time <= start_time:
                break
            tmp_e_time = tmp_s_time - 1
            tmp_s_time -= window_size
            if tmp_s_time < start_time:
                tmp_s_time = start_time

        for info in data_list:
            ccy = info["coin"]
            if asset and ccy != asset:
                continue
            interest_rates.append(
                InterestRate(
                    asset=ccy,
                    ir=Decimal(info["annualizedInterestRate"]) / 365,  # rate:出借年利率
                    ts=float(info["time"]),  # ms
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
        resp = await self.client.sapi_margin_interest_rate_cur(asset=asset)
        if not isinstance(resp, list):
            raise ValueError(resp)
        interest_rates: list[InterestRate] = []
        for info in resp:
            coin = info["asset"]
            if coin != asset:
                continue
            interest_rates.append(
                InterestRate(
                    asset=asset,
                    ir=Decimal(info["nextHourlyInterestRate"]) * 24,
                    ts=time.time() * 1000,
                )
            )
        return interest_rates

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
        window_size = 28 * 24 * 60 * 60 * 1000
        if not end_time:
            end_time = int(datetime.now().timestamp() * 1000)
        if not start_time:
            start_time = end_time - 30 * 24 * 60 * 60 * 1000

        interest_rates: list[InterestRate] = []
        data_list: list[dict[str, str]] = []
        tmp_e_time = end_time
        tmp_s_time = end_time - window_size if end_time - start_time > window_size else start_time
        while True:
            resp = await self.client.sapi_margin_interest_rate_his(
                asset=asset, vipLevel=vip_level, startTime=tmp_s_time, endTime=tmp_e_time
            )
            if not isinstance(resp, list):
                raise ValueError(resp)
            await asyncio.sleep(1)
            if resp:
                data_list.extend(resp)
            else:
                break
            if tmp_s_time <= start_time:
                break
            tmp_e_time = tmp_s_time - 1
            tmp_s_time -= window_size
            if tmp_s_time < start_time:
                tmp_s_time = start_time

        for info in data_list:
            ccy = info["asset"]
            if asset and ccy != asset:
                continue
            res_vip_level = info["vipLevel"]
            interest_rates.append(
                InterestRate(
                    asset=ccy,
                    vip_level=str(res_vip_level),
                    ir=Decimal(info["dailyInterestRate"]),
                    ts=float(info["timestamp"]),  # ms
                )
            )
        return interest_rates

    @catch_it
    async def get_p2p_interest_rates_cur(
        self,
        asset: str,
    ) -> InterestRates:
        assert self._market_type == MarketType.SPOT, f"Invalid Market type {self._market_type}, only support SPOT"
        interest_rates: InterestRates = []
        data_list = []
        current = 1
        while True:
            resp = await self.client.sapi_loan_p2p_market(coin=asset, current=current, size=8)
            if not (isinstance(resp, dict) and "rows" in resp):
                raise ValueError(resp)
            await asyncio.sleep(2)
            data = resp.get("rows", [])
            if data:
                data_list.extend(data)
            else:
                break
            current += 1

        for info in data_list:
            coin = info["borrowCoin"]
            if coin != asset:
                continue
            available_qty = Decimal(info["borrowAmount"])
            if available_qty <= 0:
                continue
            interest_rates.append(
                InterestRate(
                    asset=coin,
                    days=int(info["duration"].replace("Days", "")),
                    ir=Decimal(info["interestRate"]) / 365,
                    available_qty=available_qty,
                    minimum_qty=Decimal(info["minBorrowAmount"]),
                    ts=time.time() * 1000,
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
            func = self.client.sapi_staking_sol_rate_history
        elif asset == "ETH":
            func = self.client.sapi_staking_eth_rate_history
        else:
            raise NotImplementedError("asset only support SOL,ETH")

        window_size = 80 * 24 * 60 * 60 * 1000
        data_list: list[dict[str, str]] = []
        tmp_e_time = end_time
        tmp_s_time = end_time - window_size if end_time - start_time > window_size else start_time
        while True:
            current = 1
            while True:
                resp = await func(startTime=tmp_s_time, endTime=tmp_e_time, current=current)
                if not (isinstance(resp, dict) and "rows" in resp):
                    raise ValueError(resp)
                await asyncio.sleep(2)  # 请求权重(IP) 150
                if resp["rows"]:
                    data_list.extend(resp["rows"])
                    current += 1
                else:
                    break
            if tmp_s_time <= start_time:
                break
            tmp_e_time = tmp_s_time - 1
            tmp_s_time -= window_size
            if tmp_s_time < start_time:
                tmp_s_time = start_time

        interest_rates: list[InterestRate] = []
        for info in data_list:
            interest_rates.append(
                InterestRate(
                    asset=asset,
                    ir=Decimal(info["annualPercentageRate"]) / 365,
                    ts=float(info["time"]),
                )
            )
        return interest_rates

    def get_interval(self, interval: Interval) -> str:
        return interval.name.lstrip("_")

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
        interval_str = self.get_interval(interval)
        resp = await self.market_client.get_long_short_ratio(symbol, interval_str, limit)
        if isinstance(resp, list):
            return LongShortRatioData(
                [
                    LongShortRatio(long_short_ratio=Decimal(data["longShortRatio"]), ts=int(data["timestamp"]))
                    for data in resp
                ]
            )
        raise ValueError(f"unexpected response[{resp}]")

    @catch_it
    async def set_account_position_mode(self, mode: PositionMode):
        assert self._market_type in [
            MarketType.UPERP,
            MarketType.UDELIVERY,
        ], f"Market type {self._market_type} is not supported for set_account_position_mode"
        assert (
            self._account_type == AccountType.NORMAL
        ), f"Account type {self._account_type} is not supported for set_account_position_mode"
        if mode == PositionMode.HEDGE:
            logger.error("HEDGE mode is not supported by Binance")
        resp = await self.client.get_position_side_dual()
        if resp and resp.get("dualSidePosition"):
            resp = await self.client.fapi_position_side_dual(dualSidePosition=False)
            logger.info(f"Set account position mode to {mode.name}, response: {resp}")
        else:
            logger.info(f"Account position mode is already set to {mode.name}, no action taken")

    @catch_it
    async def set_account_leverage(self, leverage: int):
        assert self._market_type in [
            MarketType.UPERP,
            MarketType.UDELIVERY,
        ], f"Market type {self._market_type} is not supported for set_account_leverage"
        if leverage <= 0:
            logger.error(f"无效的杠杆倍数: {leverage}", level="warning")
            return
        if self._market_type.is_derivative:
            category: Literal["linear", "inverse"] = self.get_unified_category()  # type: ignore
            if self._account_type == AccountType.UNIFIED:
                positions_info = await self.client.get_position_risk(category)
            elif self._market_type in [MarketType.UPERP, MarketType.UDELIVERY]:
                positions_info = await self.client.get_linear_swap_position()
            else:
                positions_info = await self.client.get_inverse_swap_position()
            positions_leverages = {}
            if positions_info:
                positions_leverages = {position["symbol"]: int(position["leverage"]) for position in positions_info}
                for symbol, current_leverage in positions_leverages.items():
                    if leverage != current_leverage:
                        await self.client.set_leverage(symbol, leverage)
                        await asyncio.sleep(0.1)

    @catch_it
    async def enable_auto_repayment(self):
        if self._account_type not in [AccountType.UNIFIED, AccountType.CLASSIC_UNIFIED]:
            return
        resp = await self.client.query_repay_type()
        if resp is None:
            raise ValueError("Failed to query repay type, response is None")
        if not resp.get("autoRepay"):
            raise ValueError(f"Failed to query repay type, response: {resp}")
        else:
            await self.client.change_repay_type(True)

    @catch_it
    async def collect_balances(self):
        assert self._account_type in [
            AccountType.UNIFIED,
            AccountType.CLASSIC_UNIFIED,
        ], f"Account type {self._account_type} is not supported for collect_balances"
        if self._account_type == AccountType.UNIFIED:
            resp = await self.client.papi_auto_collection()
        else:
            resp = await self.client.sapi_auto_collection()
        if resp is None:
            raise ValueError("Failed to collect balances, response is None")
        elif resp.get("msg") == "success":
            logger.info("Collect balances successfully")
            return True
        return False

    @catch_it
    async def repay_negative_balances(self):
        assert self._account_type in [
            AccountType.UNIFIED,
            AccountType.CLASSIC_UNIFIED,
        ], f"Account type {self._account_type} is not supported for repay_negative_balances"
        if self._account_type == AccountType.UNIFIED:
            resp = await self.client.papi_repay_futures_negative_balance()
        else:
            resp = await self.client.sapi_repay_futures_negative_balance()
        if resp is None:
            raise ValueError("Failed to repay negative balances, response is None")
        elif resp.get("msg") == "success":
            logger.info("Repay negative balances successfully")
            return True
        return False

    @catch_it
    async def get_collateral_ratio(self) -> CollateralRatios:
        crs: list[CollateralRatio] = []
        if self._market_type in [MarketType.SPOT, MarketType.MARGIN]:
            # 借贷质押率
            if self._account_config.extra_params.get("has_loan", False):
                resp = await self.client.sapi_loan_vip_collateral_data()
                if resp and resp.get("rows"):
                    for row in resp["rows"]:
                        asset = row["collateralCoin"]
                        cr: dict[float, float] = {
                            float(i.replace("above ", ">").split("-")[0].split(">")[-1]): float(j.replace("%", ""))
                            / 100
                            for i, j in zip(
                                [i for k1, i in row.items() if "CollateralRange" in k1],
                                [j for k2, j in row.items() if "CollateralRatio" in k2],
                            )
                        }
                        crs.append(CollateralRatio(asset, cr))
            else:
                # TODO: need insts
                pass
        elif self._account_type == AccountType.UNIFIED:
            resp = await self.client.get_collateral_rate()
            if isinstance(resp, dict) and resp.get("code") == "000000":
                for row in resp["data"]:
                    asset = row["asset"]
                    cr: dict[float, float] = {0: float(row["collateralRate"])}
                    crs.append(CollateralRatio(asset, cr))
            else:
                logger.error(f"get collateral-rate error: {resp}")
        elif self._account_type == AccountType.CLASSIC_UNIFIED:
            resp = await self.client.sapi_v2_get_collateral_rate()
            if resp:
                for row in resp:
                    asset = row["asset"]
                    cr: dict[float, float] = {
                        float(tier["tierFloor"]): float(tier["collateralRate"]) for tier in row["collateralInfo"]
                    }
                    crs.append(CollateralRatio(asset, cr))
            else:
                logger.error(f"get collateral-rate error: {resp}")
        else:
            raise ValueError(
                f"Account type [{self._account_type}] MarketType[{self._market_type}] is not supported for get_collateral_ratio"
            )
        return crs

    async def _get_user_id(self):
        ret = await self.client.get_spot_account()
        if ret:
            return ret["uid"]

    @catch_it
    async def get_loan_orders(
        self,
        order_id: int | None = None,
        asset: str | None = None,
        page_limit: int | None = None,
    ):
        if self._account_config.extra_params.get("user_id") is None:
            user_id = await self._get_user_id()
            if user_id is None:
                raise Exception("获取用户ID失败")
            self._account_config.extra_params["user_id"] = user_id
        """
        Args:
            order_id (Optional[int], optional): _description_. Defaults to None.
            loan_coin (Optional[str], optional): _description_. Defaults to None.

        Raises:
            RuntimeError: _description_

        Returns:
            list[dict]: _description_

        Return Example:
            [{'orderId': '942272960149277825',
            'loanCoin': 'USDT',
            'totalDebt': '126544.72514565',
            'residualInterest': '2.76114565',
            'collateralAccountId': '452730159,452730167,452730168,486948844,486948845,486948854',
            'collateralCoin': '1INCH,AAVE,ACH,ADA,AERGO,AGIX,AGLD,ALCX,ALGO,ALPINE,AMP,ANKR,APE,APT,ARB,ARK,ARKM,ARPA,ASTR,ATOM,AVAX,AXS,BAKE,BAND,BCH,BNB,BOND,BTC,BTTC,CAKE,CFX,CHZ,COMP,CRV,CTSI,CVC,DOGE,DOT,DYDX,EDU,EGLD,ENS,EOS,ERN,ETC,ETH,EUR,FARM,FDUSD,FET,FIL,FIS,FLOW,FLUX,FORTH,FTM,FXS,GAL,GALA,GAS,GLM,GMT,GRT,HBAR,HFT,HIGH,HOOK,ICP,ID,IDEX,ILV,IMX,INJ,JASMY,JOE,KSM,LDO,LEVER,LINA,LINK,LOKA,LOOM,LPT,LQTY,LTC,LUNA,LUNC,MAGIC,MANA,MASK,MATIC,MDT,MEME,MINA,MKR,MTL,NEAR,NMR,OCEAN,OG,ONE,OOKI,OP,ORDI,OSMO,PAXG,PEOPLE,PEPE,PHB,PLA,POLS,POLYX,POND,PYR,QI,QNT,QUICK,RAD,RARE,RDNT,REQ,RNDR,ROSE,RSR,RUNE,SAND,SANTOS,SHIB,SNX,SOL,SSV,STG,STX,SUI,SUPER,SUSHI,SXP,SYS,THETA,TIA,TRB,TRU,TRX,TUSD,TVK,TWT,UNFI,UNI,USDC,USDP,USDT,VET,VOXEL,VTHO,WAVES,WBETH,XLM,XMR,XRP,YGG,ZEC,ZEN,ZIL,USTC,GMX,SEI,POWR,JTO,NTRN',
            'collateralValue': '6029225.68610074',
            'lockedCollateralValue': '6029225.68610074',
            'totalCollateralValueAfterHaircut': '7871995.65614603',
            'currentLTV': '0.55161211',
            'expirationTime': '0',
            'loanDate': '1702613780289',
            'loanRate': 'Flexible Rate',
            'loanTerm': 'Open Term',
            'initialLtv': '72%',
            'marginCallLtv': '77%',
            'liquidationLtv': '91%'}]
        """
        # fetch data
        loan_orders = []
        current_page = 1
        limit = 100
        while True:
            params = {
                "collateralAccountId": self._account_config.extra_params["user_id"],
                "loanCoin": asset,
                "current": current_page,
                "limit": limit,
            }

            resp = await self.client.sapi_loan_vip_ongoing_order(**params)

            if resp is None:
                return

            loan_orders += resp["rows"]
            if len(loan_orders) == resp["total"]:
                break
            elif page_limit is not None and current_page >= page_limit:
                # limit pages
                break
            else:
                current_page += 1
                await asyncio.sleep(20)
        return loan_orders

    @catch_it
    async def get_account_info(self) -> AccountInfo:
        if self._account_type == AccountType.UNIFIED:
            pm_account_resp = await self.client.get_account()
            balances = await self._get_unified_assets()
            if pm_account_resp is None:
                raise ValueError("Failed to get account info, response is None")
            equity = float(pm_account_resp["actualEquity"])
            available_balance = float(pm_account_resp["totalAvailableBalance"])
            margin_balance = float(pm_account_resp["accountEquity"])
            usdt = balances.get("USDT", Balance("USDT"))

            if (im := float(pm_account_resp["accountInitialMargin"])) != 0:
                imr = margin_balance / im
            else:
                imr = 999

            mmr = float(pm_account_resp["uniMMR"])
            return AccountInfo(
                account=self._account_meta,
                equity=equity,
                usdt_free=usdt.free,
                imr=imr,
                mmr=mmr,
                available_balance=available_balance,
                margin_balance=margin_balance,
                usdt_borrowed=usdt.borrowed,
            )
        elif self._account_type == AccountType.CLASSIC_UNIFIED:
            cpm_balance_resp = await self.client.sapi_get_balance()
            cpm_account_resp = await self.client.sapi_get_account()
            if cpm_balance_resp is None or cpm_account_resp is None:
                raise ValueError("Failed to get account info, balance or account response is None")
            equity = float(cpm_account_resp["actualEquity"])
            available_balance = float(cpm_account_resp["totalAvailableBalance"])
            margin_balance = float(cpm_account_resp["accountEquity"])
            imr = 999  # PM Pro 不检查IM
            mmr = float(cpm_account_resp["uniMMR"])

            usdt_free = 0
            for entry in cpm_balance_resp:
                if entry["asset"] == "USDT":
                    usdt_free = float(entry["crossMarginFree"])
                    break

            return AccountInfo(
                account=self._account_meta,
                equity=equity,
                usdt_free=usdt_free,
                imr=imr,
                mmr=mmr,
                available_balance=available_balance,
                margin_balance=margin_balance,
            )
        elif self._market_type == MarketType.MARGIN:
            margin_account_resp = await self.client.sapi_margin_account()
            balances = await self._get_margin_assets()
            ticker_resp = await self.get_tickers()
            if ticker_resp["status"] != 0:
                raise ValueError("Failed to get account info(failed to get ticker)")
            ticker = ticker_resp["data"]
            if balances is None or margin_account_resp is None:
                raise ValueError("Failed to get account info, balances response is None")
            equity = float(margin_account_resp["totalNetAssetOfBtc"]) * ticker["BTCUSDT"].mpx

            usdt = balances.get("USDT")
            if usdt is None:
                raise ValueError("fail to get account info: cannot find USDT balance")

            imr = equity / (equity - usdt.balance)

            return AccountInfo(account=self._account_meta, equity=equity, imr=imr, usdt_borrowed=usdt.borrowed)
        elif self._market_type == MarketType.SPOT:
            balances = await self._get_sp_assets()
            ticker_resp = await self.get_tickers()
            if ticker_resp["status"] != 0:
                raise ValueError("Failed to get account info(failed to get ticker)")
            tickers = ticker_resp["data"]
            if balances is None:
                raise ValueError("Failed to get account info, balance response is None")
            usdt = balances.get("USDT", Balance("USDT"))
            total_position_value = 0
            for asset, balance in balances.items():
                if asset in ["USDT"]:
                    continue
                symbol = asset + "_USDT"
                total = balance.balance
                ticker = tickers.get(symbol)
                if ticker is None:
                    continue
                total_position_value += abs(total) * ticker.bid

            equity = total_position_value + usdt.free

            if self._account_config.extra_params.get("has_loan", False):
                loan_orders_resp = await self.get_loan_orders(page_limit=1)

                if loan_orders_resp["status"] != 0:
                    raise Exception("Failed to get account info, loan orders is None")
                loan_orders = loan_orders_resp["data"]
            else:
                loan_orders = []

            if loan_orders is None or len(loan_orders) == 0:
                # No loan at all
                ltv = 999
                margin_balance = equity
            else:
                ltv = 1 - float((loan_orders[0]["currentLTV"]))
                margin_balance = float(loan_orders[0]["totalCollateralValueAfterHaircut"])

            return AccountInfo(
                account=self._account_meta,
                equity=equity,
                usdt_free=usdt.free,
                ltv=ltv,
                margin_balance=margin_balance,
                usdt_borrowed=usdt.borrowed,
            )
        elif self._market_type in [MarketType.UPERP, MarketType.UDELIVERY]:
            balances = await self._get_lps_assets()
            account = await self.client.fapi_v2_account()
            if account is None:
                raise ValueError("Failed to get account info, account response is None")
            equity = float(account["totalMarginBalance"])
            available_balance = float(account["availableBalance"])
            margin_balance = float(account["totalMarginBalance"])
            maintenance_margin = float(account["totalMaintMargin"])
            usdt = balances.get("USDT")
            if usdt is None:
                raise ValueError("fail to get account info: cannot find USDT balance")

            if (im := float(account["totalInitialMargin"])) > 0:
                imr = margin_balance / im
            else:
                imr = 999

            if maintenance_margin > 0:
                mmr = margin_balance / maintenance_margin
            else:
                mmr = 999
            return AccountInfo(
                account=self._account_meta,
                equity=equity,
                usdt_free=usdt.free,
                imr=imr,
                mmr=mmr,
                available_balance=available_balance,
                margin_balance=margin_balance,
            )
        else:
            raise ValueError(f"Market type {self._market_type} is not supported for get_account_info")
