from decimal import Decimal
import time
from typing import Optional, Union, Any, Literal
from datetime import datetime, timedelta
from dateutil import parser
from loguru import logger
import asyncio
import traceback
from ..base_wrapper import BaseRestWrapper, catch_it
from ..enum_type import (
    AccountType,
    Interval,
    TimeInForce,
    OrderSide,
    MarketType,
    MarginMode,
    Event,
    OrderStatus,
    OrderType,
)
from ..get_client import get_rest_client, get_ws_client
from ..data_type import *
from ..common.exceptions import UnsupportedOperationError
import ccxt.async_support as ccxt
from ccxt.base.types import Order as ccxtOrder, ConstructorArgs


class KucoinRestWrapper(BaseRestWrapper):
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
        match self._account_meta.market_type:
            case MarketType.SPOT:
                ccxt_default_type = "spot"
            case MarketType.MARGIN:
                ccxt_default_type = "margin"
            case MarketType.UPERP:
                ccxt_default_type = "swap"

        ccxt_params: ConstructorArgs = {
            "apiKey": self._account_config.api_key,
            "secret": self._account_config.secret_key,
            "password": self._account_config.passphrase,  # Kucoin需要passphrase
            "enableRateLimit": True,
            "options": {
                "defaultType": ccxt_default_type,
            },
        }
        self.ccxt_client = ccxt.kucoin(ccxt_params)

    def _fix_symbol(self, symbol: str):
        symbol = f"{symbol.upper()}"
        if symbol[-1] != "M":
            symbol += "M"
        return symbol

    @catch_it
    async def get_positions(self, from_redis: bool = False):
        if MarketType.UPERP != self._market_type:
            raise ValueError(f"Market type {self._market_type} is not supported(only supported for UPERP)")
        positions = await self.get_lps_positions(from_redis)
        if (data := positions.get("data")) is not None:
            return data
        else:
            raise ValueError(positions.get("msg", "unknown error"))

    @catch_it
    async def get_lps_positions(self, from_redis: bool = False) -> Positions:
        result: dict[str, Position] = {}
        if from_redis:
            suffix = "raw:test"
            key = "swap_position"
            data = await self._load_data_from_rmx_acc(suffix, key)
        else:
            resp = await self.client.fetch_positions()
            if resp["code"] != "200000":
                raise ValueError(resp.get("msg", ""))

            data = resp["data"]

        if not isinstance(data, list):
            raise ValueError()

        for i in data:
            if i["currentQty"] != 0:
                result[i["symbol"]] = Position(
                    exch_symbol=i["symbol"],
                    net_qty=float(i["currentQty"]),
                    entry_price=i["avgEntryPrice"],
                    value=i["markValue"],
                    liq_price=i["liquidationPrice"],
                    unrealized_pnl=i["unrealisedPnl"],
                    ts=int(time.time() * 1000),
                )
        return Positions(result)

    @catch_it
    async def get_equity(self) -> float:
        """
        U合约的totalMarginBalance + 现货free+locked + 杠杆BTC net asset
        """
        if self._market_type in [MarketType.SPOT, MarketType.MARGIN]:
            sp_equity = await self._get_sp_equity()
            lps_account_meta = AccountMeta(
                exch_name=self._exchange,
                market_type=MarketType.UPERP,
                account_type=self._account_type,
            )
            lps_client = get_rest_client(lps_account_meta, self._account_config, self._rest_config)
            lps_equity = await self._get_lps_equity(lps_client)
            return sp_equity + lps_equity
        else:
            raise ValueError(f"Invalid Market type {self._market_type}")

    async def _get_sp_equity(self) -> float:
        types = ["trade_hf", "trade", "margin"]
        asset_total = 0
        for type in types:
            resp = await self.client.get_account(type=type)
            if resp["code"] == "200000":
                data = resp["data"]
            else:
                raise ValueError(resp.get("msg", ""))
            if isinstance(data, list):
                for info in data:
                    if float(info["balance"]) == 0:
                        continue
                    coin = info["currency"]
                    try:
                        if coin != "USDT":
                            asset_price_resp = await self.get_price(coin + "-USDT")
                            if asset_price_resp["status"] != 0:
                                logger.warning(asset_price_resp)
                                continue
                            asset_price = asset_price_resp["data"]
                        else:
                            asset_price = 1
                        asset_value = asset_price * float(info["balance"])
                    except:
                        raise ValueError("Invalid asset ticker: " + coin)
                    asset_total += asset_value
        return float(asset_total)

    async def _get_lps_equity(self, lps_client=None) -> float:
        if lps_client is None:
            lps_client = self.client
        resp = await lps_client.fetch_future_balance()
        if resp["code"] == "200000":
            data = resp["data"]
        else:
            raise ValueError(resp.get("msg", "Unknown error"))
        if isinstance(data, dict):
            return float(data["accountEquity"])
        raise ValueError(f"Failed to parse data {resp}")

    @catch_it
    async def get_assets(self, from_redis: bool = False):
        if MarketType.UPERP == self._market_type:
            assets = await self.get_lps_assets(from_redis)
        elif self._market_type in [MarketType.SPOT, MarketType.MARGIN]:
            assets = await self.get_sp_assets(from_redis)
        else:
            raise ValueError(f"Market type {self._market_type} is not supported")
        if (data := assets.get("data")) is not None:
            return data
        else:
            raise ValueError(assets.get("msg", "unknown error"))

    @catch_it
    async def get_sp_assets(self, from_redis: bool = False):
        result: dict[str, Balance] = {}
        update_time = int(time.time() * 1000)
        trade_acct_type_dict = {
            AccountType.HFT: "trade_hf",
            AccountType.FUND: "main",
        }
        trade_market_type_dict = {
            MarketType.SPOT: "trade",
            MarketType.MARGIN: "margin",
        }
        trade_type = trade_acct_type_dict.get(self._account_type, trade_market_type_dict[self._market_type])
        if from_redis:
            suffix = "raw:test"
            if self._market_type in [MarketType.SPOT, MarketType.MARGIN] and AccountType.HFT != self._account_type:
                key = "spot_balance"
            elif self._market_type in [MarketType.SPOT, MarketType.MARGIN] and AccountType.HFT == self._account_type:
                key = "trade_hf_balance"
            else:
                raise ValueError(f"Market type {self._market_type} is not supported")
            data = await self._load_data_from_rmx_acc(suffix, key)
        else:
            resp = await self.client.get_account(type=trade_type)
            if resp["code"] != "200000":
                raise ValueError(resp.get("msg", ""))

            data = resp["data"]

        if not isinstance(data, list):
            raise ValueError(f"Invalid data {data}")

        for info in data:
            if float(info["balance"]) == 0 or info["type"] != trade_type:
                continue
            result[info["currency"]] = Balance(
                asset=info["currency"],
                balance=float(str(info["balance"])),
                free=float(str(info["available"])),
                locked=float(str(info["holds"])),
                type="full",
                ts=update_time,
            )
        return Balances(result)

    @catch_it
    async def get_lps_assets(self, from_redis: bool = False):
        result: dict[str, Balance] = {}
        if from_redis:
            suffix = "raw:test"
            key = "swap_balance"
            data = await self._load_data_from_rmx_acc(suffix, key)
        else:
            resp = await self.client.fetch_future_balance()
            if resp["code"] != "200000":
                raise ValueError(resp.get("msg", "Unknown error"))

            data = resp["data"]

        update_time = int(time.time() * 1000)
        if not isinstance(data, dict):
            raise ValueError

        result["USDT"] = Balance(
            asset="USDT",
            balance=float(data["accountEquity"]),
            free=float(data["availableBalance"]),
            locked=float(data["positionMargin"]) + float(data["orderMargin"]) + float(data["frozenFunds"]),
            type="full",
            ts=update_time,
        )
        return Balances(result)

    @catch_it
    async def set_swap_risk_limit(self, symbol: str, risk_limit_level: int) -> bool:
        if MarketType.UPERP != self._market_type:
            raise ValueError(f"Market type {self._market_type} is not supported(only supported for UPERP)")
        symbol = self._fix_symbol(symbol)
        logger.debug(f"Change risk limit level of {symbol} to {risk_limit_level}")
        resp = await self.client.change_risk_limit(symbol=symbol, level=risk_limit_level)
        status = resp.get("code", None)
        if status == "200000":
            return True
        else:
            raise ValueError(resp.get("msg", ""))

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
        transfer_from = ""
        transfer_to = ""
        market_type_dict: dict[MarketType, str] = {
            MarketType.SPOT: "trade",
            MarketType.UPERP: "contract",
            MarketType.CPERP: "contract",
            MarketType.MARGIN: "margin",
        }
        if isinstance(from_market_type, str):
            from_market_type = MarketType[from_market_type]
            transfer_from = market_type_dict[from_market_type]
        if isinstance(to_market_type, str):
            to_market_type = MarketType[to_market_type]
            transfer_to = market_type_dict[to_market_type]
        if isinstance(from_account_type, str):
            from_account_type = AccountType[from_account_type]
            if from_account_type == AccountType.FUND:
                transfer_from = "main"
            else:
                raise ValueError(f"Invalid from_account_type {from_account_type} for transfer")
        if isinstance(to_account_type, str):
            to_account_type = AccountType[to_account_type]
            if to_account_type == AccountType.FUND:
                transfer_to = "main"
            else:
                raise ValueError(f"Invalid to_account_type {to_account_type} for transfer")
        if transfer_from == "":
            raise ValueError("transfer_from is empty")
        elif transfer_to == "":
            raise ValueError("transfer_to is empty")
        if transfer_from == transfer_to:
            raise ValueError("transfer_from 和 transfer_to 相同, 无需划转")
        if self._market_type.is_derivative:
            if transfer_from == "contract":
                resp = await self.client.transfer_out(asset.upper(), str(qty), transfer_to)
            elif to_market_type and to_market_type.is_derivative:
                resp = await self.client.transfer_in(asset.upper(), str(qty), transfer_from)
            if resp["code"] == "200000":
                return TransferResponse(apply_id=resp["data"]["applyId"])
            else:
                raise ValueError(resp["msg"])

        elif MarketType.SPOT == self._market_type:
            assert from_market_type != MarketType.UPERP, "Please use UPERP wrapper to transfer UPERP funds"
            clientOid = str(int(time.time()))
            resp = await self.client.inner_transfer(clientOid, asset, str(qty), transfer_from, transfer_to)
            if resp["code"] == "200000":
                return TransferResponse(apply_id=resp["data"]["orderId"])
            else:
                raise ValueError(resp["msg"])
        raise ValueError(f"invalid AccountType{self._account_type} MarketType{self._market_type} for transfer")

    @catch_it
    async def get_prices(self) -> Prices:
        dic = {}
        if self._market_type in [MarketType.SPOT, MarketType.MARGIN]:
            spot_resp = await self.client.get_all_tickers()
            if spot_resp["code"] == "200000" and spot_resp["data"]:
                dic = {ticker["symbol"]: float(ticker["last"] or "NaN") for ticker in spot_resp["data"]["ticker"]}
            else:
                raise ValueError(spot_resp)
        else:
            future_resp = await self.client.get_swap_instrument_info()
            if future_resp["code"] == "200000" and future_resp["data"]:
                dic = {ticker["symbol"]: float(ticker["markPrice"] or "NaN") for ticker in future_resp["data"]}
            else:
                raise ValueError(future_resp)
        return Prices(dic)

    @catch_it
    async def get_price(self, symbol: str, from_redis: bool = False) -> float:
        if from_redis:
            if self._market_type in [MarketType.SPOT, MarketType.MARGIN]:
                std_symbol = symbol.replace("-", "_") + "|SPOT|KUCOIN"
            elif MarketType.UPERP == self._market_type:
                std_symbol = symbol.replace("-", "_") + "|UPERP|KUCOIN"
            else:
                std_symbol = ""
            data = await self._load_data_from_kit("ticker", key=std_symbol)
            return (float(data["apx"]) + float(data["bpx"])) / 2
        else:
            if self._market_type in [MarketType.SPOT, MarketType.MARGIN]:
                spot_resp = await self.client.get_spot_market(symbol)
                if spot_resp["code"] == "200000" and spot_resp["data"]:
                    return float(spot_resp["data"]["price"])
                else:
                    raise ValueError(spot_resp.get("msg", ""))
            elif MarketType.UPERP == self._market_type:
                future_resp = await self.client.get_swap_market(symbol)
                if future_resp["code"] == "200000" and future_resp["data"]:
                    return float(future_resp["data"]["price"])
                else:
                    raise ValueError(future_resp.get("msg", ""))
        raise ValueError(f"Invalid Market type {self._market_type} for get_price")

    @catch_it
    async def get_trade_history(self, start_time: int, end_time: int, symbol_list: list[str]):
        result: dict[str, list[Trade]] = {}
        trade_data_list = []
        if self._market_type in [MarketType.SPOT, MarketType.MARGIN]:
            trade_limit = 500
        elif self._account_type == AccountType.HFT:
            trade_limit = 100
        else:
            trade_limit = 1000
        for symbol in symbol_list:
            if self._account_type != AccountType.HFT:
                total_page = 1
                idx = 1
                while idx <= total_page:
                    trade_resp = await self.client.fetch_fills(
                        start_at=start_time,
                        end_at=end_time,
                        symbol=symbol,
                        trade_type="TRADE",
                        page_size=trade_limit,
                        isHf=False,
                    )
                    await asyncio.sleep(1)
                    if trade_resp["code"] == "200000" and not trade_resp.get("msg"):
                        trade_data_list += trade_resp["data"]["items"]
                        total_page = trade_resp["data"]["totalPage"]
                    else:
                        raise ValueError(trade_resp["msg"])
                    idx += 1
            else:
                last_id = None
                while True:
                    trade_resp = await self.client.fetch_fills(
                        start_at=start_time,
                        end_at=end_time,
                        symbol=symbol,
                        page_size=trade_limit,
                        last_id=last_id,
                        isHf=True,
                    )
                    await asyncio.sleep(1)
                    if trade_resp["code"] == "200000" and not trade_resp.get("msg"):
                        if trade_resp["data"]["items"]:
                            trade_data_list += trade_resp["data"]["items"]
                            last_id = trade_resp["data"]["lastId"]
                        else:
                            break
                    else:
                        raise ValueError(trade_resp["msg"])
        trade_data_list = sorted(trade_data_list, key=lambda trade_data: trade_data["tradeId"])
        for data in trade_data_list:
            result.setdefault(data["symbol"], []).append(
                Trade(
                    create_ts=int(data["createdAt"]),
                    side=getattr(OrderSide, data["side"].upper(), OrderSide.UNKNOWN),
                    trade_id=str(data["tradeId"]),
                    order_id=data["orderId"],
                    last_trd_price=Decimal(data["price"]),
                    last_trd_volume=Decimal(str(data["size"])),
                    turnover=(
                        Decimal(data["funds"]) if self._market_type == MarketType.SPOT else Decimal(data["value"])
                    ),
                    fill_ts=(
                        int(int(data["tradeTime"]) // 1e6)
                        if self._market_type == MarketType.SPOT
                        else int(data["createdAt"])
                    ),
                    fee=Decimal(data["fee"]),
                    fee_ccy=data["feeCurrency"],
                    is_maker=True if data["liquidity"] == "maker" else False,
                )
            )
        return TradeData(result)

    @catch_it
    async def get_funding_fee(
        self,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        look_back: Optional[int] = None,
        symbol_list: Optional[list[str]] = None,
    ):
        assert symbol_list, "kucoin required positional arguments `symbol_list`"
        start_time, end_time = self._parse_start_end_look_back(start_time, end_time, look_back)
        funding_dict = {}
        for symbol in symbol_list:
            resp = await self.client.fetch_funding_history(symbol=symbol, start_at=start_time, end_at=end_time)
            if resp["code"] == "200000":
                for item in resp["data"]["dataList"]:
                    if item["symbol"] not in funding_dict:
                        funding_dict[item["symbol"]] = [
                            FundingFee(Decimal(item["funding"]), int(item["timePoint"] / 1000))
                        ]
                    else:
                        funding_dict[item["symbol"]].append(
                            FundingFee(Decimal(item["funding"]), int(item["timePoint"] / 1000))
                        )
                await asyncio.sleep(3)
            else:
                raise ValueError(resp["msg"])
        return FundingFeeData(funding_dict)

    @catch_it
    async def get_order_history(self, start_time: int, end_time: int, symbol_list: list[str]) -> OrderSnapshotData:
        order_dict: dict[str, list[OrderSnapshot]] = {}
        after = None
        order_data_list = []
        order_limit = 1000
        if self._market_type in [MarketType.SPOT, MarketType.MARGIN]:
            order_limit = 500
        elif self._account_type == AccountType.HFT:
            order_limit = 100
        try:
            for symbol in symbol_list:
                if self._account_type != AccountType.HFT:
                    total_page = 1
                    idx = 1
                    while idx <= total_page:
                        order_resp = await self.client.get_order_history(
                            status="done",
                            start_at=start_time,
                            end_at=end_time,
                            symbol=symbol,
                            page_size=order_limit,
                            current_page=idx,
                        )
                        await asyncio.sleep(0.2)
                        if order_resp["code"] == "200000" and order_resp.get("data"):
                            order_data_list += order_resp["data"]["items"]
                            total_page = order_resp["data"]["totalPage"]
                        else:
                            logger.error(order_resp.get("msg", ""))
                            break
                        idx += 1
                else:
                    last_id = None
                    while True:
                        order_resp = await self.client.get_order_history(
                            status="done",
                            start_at=start_time,
                            end_at=end_time,
                            symbol=symbol,
                            page_size=order_limit,
                            last_id=last_id,
                            isHf=True,
                        )
                        await asyncio.sleep(1)
                        if order_resp["code"] == "200000" and not order_resp.get("msg"):
                            if order_resp["data"] and order_resp["data"]["items"]:
                                order_data_list += order_resp["data"]["items"]
                                last_id = order_resp["data"]["lastId"]
                            else:
                                break
                        else:
                            raise ValueError(order_resp.get("msg", ""))
        except:
            logger.error(traceback.format_exc())
        for data in order_data_list:
            if self._account_type in [MarketType.SPOT, MarketType.MARGIN, AccountType.HFT]:
                tot_trd_val = Decimal(str(data["dealFunds"]))
                tot_trd_volm = Decimal(str(data["dealSize"]))
            else:
                tot_trd_val = Decimal(str(data["filledValue"]))
                tot_trd_volm = Decimal(str(data["filledSize"]))
            orig_volm = Decimal(str(data["size"]))
            if tot_trd_volm != Decimal("0"):
                avg_price = tot_trd_val / tot_trd_volm
            else:
                avg_price = Decimal("0")
            order_type = getattr(OrderType, data["type"].upper(), OrderType.UNKNOWN)

            if data.get("postOnly"):
                tif = TimeInForce.GTX
            else:
                tif = getattr(TimeInForce, data["timeInForce"].upper(), TimeInForce.UNKNOWN)

            if tot_trd_volm == orig_volm:
                status = OrderStatus.FILLED
            else:
                status = OrderStatus.CANCELED

            side = getattr(OrderSide, data["side"].upper(), OrderSide.UNKNOWN)

            snapshot = OrderSnapshot(
                exch_symbol=data["symbol"],
                order_side=side,
                order_id=data["id"],
                client_order_id=data["clientOid"],
                price=Decimal(data["price"]) if data["price"] else Decimal(0),
                qty=orig_volm,
                filled_qty=tot_trd_volm,
                avg_price=float(avg_price),
                order_type=order_type,
                order_time_in_force=tif,
                order_status=status,
                place_ack_ts=int(data["createdAt"]),
                exch_update_ts=int(data["updatedAt"] if "updatedAt" in data else data["lastUpdatedAt"]),
                local_update_ts=int(time.time() * 1000),
            )
            order_dict.setdefault(snapshot.exch_symbol, []).append(snapshot)
        return OrderSnapshotData(order_dict)

    @catch_it
    async def get_historical_funding_rate(
        self,
        symbol_list: list[str],
        start_time: datetime | str | int | None = None,
        days: int = 7,
    ) -> FundingRatesHis:
        assert self._market_type in (MarketType.UPERP, MarketType.CPERP), f"Invalid Market type {self._market_type}"

        if start_time is None:
            start_time = datetime.now() - timedelta(days=days)
        elif isinstance(start_time, str):
            start_time = parser.parse(start_time)
        if isinstance(start_time, int):
            start_ts = start_time
        else:
            start_ts = int(start_time.timestamp() * 1000)
        end_ts = int(time.time() * 1000)
        frs: dict[str, set[FundingRateSimple]] = {}
        for symbol in symbol_list:
            frs[symbol] = set()
            _end_ts = end_ts
            for _ in range(1000):
                resp = await self.client.get_history_funding_rate(symbol, start_ts, _end_ts)
                await asyncio.sleep(0.1)
                if resp["code"] != "200000":
                    raise ValueError(resp.get("msg"))
                data_list = resp["data"] or []
                for item in data_list:
                    ts = int(item["timepoint"])
                    frs[symbol].add(FundingRateSimple(funding_rate=float(item["fundingRate"]), funding_ts=ts))

                if len(data_list) < 100:
                    break

                _end_ts = int(min(fr.funding_ts for fr in frs[symbol]) - 1)

                if _end_ts <= start_ts:
                    break

            await asyncio.sleep(0.5)

        return FundingRatesHis({symbol: sorted(list(fr)) for symbol, fr in frs.items()})

    def get_swap_interval(self, interval):
        interval = interval.lstrip("_")
        if interval[-1] == "h":
            interval_num = int(interval[:-1]) * 60
        elif interval[-1] == "m":
            interval_num = int(interval[:-1])
        elif interval == "1w":
            interval_num = 10080
        elif interval == "1d":
            interval_num = 1440
        else:
            interval_num = -1
        return interval_num

    def get_spot_interval(self, interval):
        interval = interval.lstrip("_")
        rep_dict = {"h": "hour", "m": "min", "d": "day", "w": "week", "M": "month"}
        translation_table = str.maketrans(rep_dict)
        return interval.translate(translation_table)

    @catch_it
    async def get_historical_kline(
        self,
        symbol: str,
        interval: Interval,
        start_time: int,
        end_time: Optional[int] = None,
        contract_type: Literal["PERPETUAL", "CURRENT_QUARTER", "NEXT_QUARTER"] = "PERPETUAL",
    ):
        # kline api will miss some data sometimes
        result: list[KLine] = []

        data_list: list[Any] = []
        b_future = False
        if end_time is None:
            end_time = int(time.time() * 1000)
        while True:
            if self._market_type in [MarketType.SPOT, MarketType.MARGIN]:
                interval_str = self.get_spot_interval(interval.name)
                resp = await self.client.get_spot_kline(
                    symbol, start_time=int(start_time / 1000), end_time=int(end_time / 1000), interval=interval_str
                )
            else:
                interval_str = self.get_swap_interval(interval.name)
                resp = await self.client.get_swap_kline(
                    symbol, start_time=start_time, end_time=end_time, interval=interval_str
                )
                b_future = True
            if resp["code"] == "200000":
                if not resp["data"]:
                    break
                data_list += resp["data"]
                await asyncio.sleep(0.25)
            else:
                raise ValueError(resp["msg"])
            if not b_future and len(resp["data"]) == 1500:
                end_time = int(resp["data"][-1][0]) * 1000 - 1000
            elif b_future and len(resp["data"]) == 200:
                start_time = int(resp["data"][-1][0]) + 1
            else:
                break

        if not b_future:
            # spot
            for lis in data_list[::-1]:
                result.append(
                    KLine(
                        start_ts=int(int(lis[0]) * 1000),
                        open=Decimal(str(lis[1])),
                        high=Decimal(str(lis[2])),
                        low=Decimal(str(lis[3])),
                        close=Decimal(str(lis[4])),
                        volume=Decimal(str(lis[5])),
                        turnover=Decimal(str(lis[6])),
                    )
                )
        else:
            for lis in data_list:
                result.append(
                    KLine(
                        start_ts=int(lis[0]),
                        open=Decimal(str(lis[1])),
                        high=Decimal(str(lis[2])),
                        low=Decimal(str(lis[3])),
                        close=Decimal(str(lis[4])),
                        volume=Decimal(str(lis[5])),
                        turnover=Decimal(str(lis[5] * (sum(lis[1:5]) / 4))),
                    )
                )

        return KLineData(result)

    @catch_it
    async def cancel_order(self, symbol: str, orderId: Optional[str] = None, clientOid: Optional[str] = None) -> bool:
        assert orderId or clientOid, "Either Parameters `orderId` and `clientOid` is Required"
        if self._market_type not in [MarketType.SPOT, MarketType.MARGIN]:
            resp = await self.client.cancel_order(symbol, orderId, clientOid)
        else:
            resp = await self.client.cancel_order(symbol, orderId, clientOid, self._account_type == AccountType.HFT)
        if not resp:
            raise ValueError
        if resp.get("code", "") != "200000":
            raise ValueError(resp["msg"])
        else:
            return True

    @catch_it
    async def cancel_all(self, symbol: str) -> bool:
        if self._market_type not in [MarketType.SPOT, MarketType.MARGIN]:
            resp = await self.client.cancel_all_orders(symbol)
        else:
            resp = await self.client.cancel_all_orders(symbol, self._account_type == AccountType.HFT)
        if not resp:
            raise ValueError
        if resp.get("code", "") != "200000":
            raise ValueError(resp["msg"])
        return True

    @catch_it
    async def get_max_open_notional(self, symbol: str, mgnMode: MarginMode = MarginMode.CROSS):
        if self._market_type not in [MarketType.SPOT, MarketType.MARGIN]:
            resp = await self.client.fetch_position(symbol)
            if not resp:
                raise ValueError("fail to get response")
            if resp.get("code", "") != "200000":
                raise ValueError(resp["msg"])
            max_open_notional = Decimal(resp["data"]["riskLimit"])
            return MaxOpenNotional(buy=max_open_notional, sell=max_open_notional)
        else:
            raise UnsupportedOperationError("Kucoin SPOT cannot get max open notional")

    @catch_it
    async def get_current_funding_rate(self, symbol_list: list[str] | None = None) -> FundingRatesCur:
        assert MarketType.UPERP == self._market_type, "only support get current funding rate for UPERP"
        assert symbol_list, "kucoin required positional arguments `symbol_list`"
        frs: FundingRatesCur = FundingRatesCur()
        for symbol in symbol_list:
            funding_rate_ret = await self.client.get_current_funding_rate(symbol)
            if not isinstance(funding_rate_ret, dict):
                raise ValueError(f"Invalid response type for Symbol[{symbol}] funding rate [{funding_rate_ret}]")

            if funding_rate_ret["code"] != "200000":
                raise ValueError(funding_rate_ret["msg"])
            funding_rate = funding_rate_ret["data"]
            fr_curr = float(funding_rate["value"])
            fr_next = (
                float(funding_rate["predictedValue"]) if "predictedValue" in funding_rate else 0
            )  # 下下一个结算点的费率, 随PI变化
            w_curr = 1
            w_next = (time.time() * 1000 - funding_rate["timePoint"]) / funding_rate["granularity"]
            w_curr, w_next = w_curr / (w_curr + w_next), w_next / (w_curr + w_next)
            fr = w_curr * fr_curr + fr_next * w_next  # 加权和作为fr
            interval = int(funding_rate["granularity"]) // 60 // 60 // 1000
            ts = funding_rate["timePoint"] + funding_rate["granularity"]
            fr_cap = float(funding_rate["fundingRateCap"])
            fr_floor = float(funding_rate["fundingRateFloor"])
            frs[symbol] = FundingRate(
                funding_rate=fr,
                funding_ts=ts,
                fr_cap=fr_cap,
                fr_floor=fr_floor,
                interval_hour=interval,
            )
        return frs

    @catch_it
    async def get_current_simple_funding_rate(self, symbol_list: list[str] | None = None) -> FundingRatesSimple:
        assert MarketType.UPERP == self._market_type, "only support get current funding rate for UPERP"
        assert symbol_list, "kucoin required positional arguments `symbol_list`"
        frs: FundingRatesSimple = FundingRatesSimple()
        for symbol in symbol_list:
            funding_rate_ret = await self.client.get_current_funding_rate(symbol)
            if not isinstance(funding_rate_ret, dict):
                raise ValueError(f"Invalid response type for Symbol[{symbol}] funding rate [{funding_rate_ret}]")

            if funding_rate_ret["code"] != "200000":
                raise ValueError(funding_rate_ret["msg"])
            funding_rate = funding_rate_ret["data"]
            fr_curr = float(funding_rate["value"])
            fr_next = (
                float(funding_rate["predictedValue"]) if "predictedValue" in funding_rate else 0
            )  # 下下一个结算点的费率, 随PI变化
            w_curr = 1
            w_next = (time.time() * 1000 - funding_rate["timePoint"]) / funding_rate["granularity"]
            w_curr, w_next = w_curr / (w_curr + w_next), w_next / (w_curr + w_next)
            fr = w_curr * fr_curr + fr_next * w_next  # 加权和作为fr
            interval = int(funding_rate["granularity"]) // 60 // 60 // 1000
            ts = funding_rate["timePoint"] + funding_rate["granularity"]
            frs[symbol] = FundingRateSimple(
                funding_rate=fr,
                funding_ts=ts,
                interval_hour=interval,
            )
        return frs

    @catch_it
    async def get_commission_rate(self, symbol: str, from_redis: bool = False) -> CommissionRate:
        if from_redis:
            assert self._account, "Kucoin commission rate from redis requires account"
            data = await self._load_data_from_rmx("trading_fee:kucoin", key=self._account)
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
                resp = await self.client.get_commission_rate(symbol=symbol)
                if resp["code"] == "200000":
                    resp = resp["data"][0]
                else:
                    raise ValueError(resp["msg"])
            else:
                resp = await self.client.get_contract_detail(symbol=symbol)
                if resp["code"] == "200000":
                    resp = resp["data"]
                else:
                    raise ValueError(resp["msg"])
            makerfee = resp["makerFeeRate"]
            takerfee = resp["takerFeeRate"]
        return CommissionRate(maker=Decimal(str(makerfee)), taker=Decimal(str(takerfee)))

    @catch_it
    async def place_order(
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
    ):
        leverage = 1
        if self._market_type not in [MarketType.MARGIN, MarketType.SPOT]:
            assert extras and extras["leverage"] is not None, "kucoin futures need parameters `leverage`"
            leverage = extras["leverage"]
        if isinstance(order_time_in_force, str):
            order_time_in_force = TimeInForce[order_time_in_force]
        if isinstance(order_side, str):
            order_side = OrderSide[order_side]
        if isinstance(order_type, str):
            order_type = OrderType[order_type]
        if not client_order_id:
            client_order_id = "xclients" + str(int(time.time() * 1000000))
        send_time_in_force = None
        post_only = None
        if order_time_in_force:
            if TimeInForce.GTX == order_time_in_force:
                post_only = True
            else:
                send_time_in_force = order_time_in_force.name
        send_order_type = "limit" if order_type == OrderType.LIMIT else "market"
        send_order_side = "buy" if order_side == OrderSide.BUY else "sell"
        if self._market_type not in [MarketType.MARGIN, MarketType.SPOT]:
            resp = await self.client.place_order(
                client_order_id,
                send_order_side,
                symbol,
                type=send_order_type,
                leverage=leverage,
                price=str(price) if price else None,
                size=int(qty) if qty else None,
                timeInForce=send_time_in_force,
                postOnly=post_only,
                reduceOnly=reduce_only,
            )
        else:
            resp = await self.client.place_order(
                client_order_id,
                send_order_side,
                symbol,
                type=send_order_type,
                price=str(price) if price else None,
                size=str(qty) if qty else None,
                timeInForce=send_time_in_force,
                postOnly=post_only,
                isHf=True if AccountType.HFT == self._account_type else False,
            )
        snapshot = OrderSnapshot(
            exch_symbol=symbol,
            client_order_id=client_order_id,
            order_side=order_side,
            order_type=order_type,
            order_time_in_force=order_time_in_force or TimeInForce.UNKNOWN,
            price=price or Decimal(0),
            qty=qty,
            local_update_ts=int(time.time() * 1000),
        )
        if resp is None:
            snapshot.order_status = OrderStatus.REJECTED
            snapshot.rejected_message = "Failed to place order, response is None"
        if resp.get("code", "") != "200000":
            snapshot.order_status = OrderStatus.REJECTED
            snapshot.rejected_message = resp.get("msg", "")
        else:
            snapshot.order_id = resp["data"]["orderId"]
            snapshot.order_status = OrderStatus.LIVE
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
            order_resp: ccxtOrder = await self.ccxt_client.cancel_order(order_id or "", symbol, params=params)  # type: ignore
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
    async def set_symbol_margin_mode(self, symbol: str, mode: MarginMode):
        curr_margin_mode = await self.client.get_margin_mode(symbol)
        new_margin_mode = mode.name
        if curr_margin_mode != new_margin_mode:
            await self.client.set_margin_mode(symbol, new_margin_mode)

    @catch_it
    async def set_symbol_leverage(self, symbol: str, leverage: int):
        resp = self.client.get_cross_margin_leverage(symbol)
        if not (isinstance(resp, dict) and resp["code"] == "200000"):
            logger.warning(f"获取全仓杠杠倍数失败, 返回: {resp}")
            return

        current_leverage = int(resp["data"]["leverage"])
        if current_leverage != leverage:
            await self.client.set_cross_margin_leverage(symbol, leverage)

    @catch_it
    async def get_tickers(self) -> Tickers:
        if self._market_type in [MarketType.SPOT, MarketType.MARGIN]:
            tickers = await self.client.get_all_tickers()
            if tickers is None or not isinstance(tickers, dict):
                raise ValueError(f"Failed to get tickers, response: {tickers}")

            update_ts = float(time.time() * 1_000)
            tickers = {
                ticker["symbol"]: Ticker(
                    ticker["symbol"],
                    float(ticker["buy"]) if ticker["buy"] else np.nan,
                    float(ticker["sell"]) if ticker["sell"] else np.nan,
                    np.nan,
                    ts=tickers["data"]["time"],
                    update_ts=update_ts,
                    bid_qty=float(ticker["bestBidSize"]) if ticker["bestBidSize"] else np.nan,
                    ask_qty=float(ticker["bestAskSize"]) if ticker["bestAskSize"] else np.nan,
                )
                for ticker in tickers["data"]["ticker"]
            }
            return tickers
        else:
            tickers_info = await self.client.get_all_tickers()
            if tickers_info is None or not (isinstance(tickers_info, dict) and tickers_info.get("data")):
                raise ValueError(f"Failed to get tickers, response: {tickers_info}")

            update_ts = float(time.time() * 1_000)
            tickers = {
                ticker["symbol"]: Ticker(
                    ticker["symbol"],
                    float(ticker["bestBidPrice"]) if ticker["bestBidPrice"] else np.nan,
                    float(ticker["bestAskPrice"]) if ticker["bestAskPrice"] else np.nan,
                    np.nan,
                    ts=ticker["ts"] / 1_000_000,
                    update_ts=update_ts,
                    bid_qty=float(ticker["bestBidSize"]) if ticker["bestBidSize"] else np.nan,
                    ask_qty=float(ticker["bestAskSize"]) if ticker["bestAskSize"] else np.nan,
                )
                for ticker in tickers_info["data"]
            }
            active_info = await self.client.get_swap_instrument_info()
            if active_info is None or not (isinstance(active_info, dict) and active_info.get("data")):
                logger.error(f"获取tickers index_price失败, 返回: {active_info}")
                return tickers
            for active in active_info["data"]:
                symbol = active["symbol"]
                if symbol in tickers:
                    tickers[symbol].index_price = float(active["indexPrice"])
            return tickers

    @catch_it
    async def get_quotations(self) -> Quotations:
        if self._market_type in [MarketType.SPOT, MarketType.MARGIN]:
            tickers = await self.client.get_all_tickers()
            if tickers is None or not isinstance(tickers, dict):
                raise ValueError(f"Failed to get quotations, response: {tickers}")

            update_ts = float(time.time() * 1_000)
            quotations = {
                t["symbol"]: Quotation(
                    exch_symbol=t["symbol"],
                    bid=float(t["buy"]) if t["buy"] else np.nan,
                    ask=float(t["sell"]) if t["sell"] else np.nan,
                    ts=tickers["data"]["time"],
                    update_ts=update_ts,
                    bid_qty=float(t["bestBidSize"]) if t["bestBidSize"] else np.nan,
                    ask_qty=float(t["bestAskSize"]) if t["bestAskSize"] else np.nan,
                )
                for t in tickers["data"]["ticker"]
            }
            return Quotations(quotations)
        else:
            tickers_info = await self.client.get_all_tickers()
            if tickers_info is None or not (isinstance(tickers_info, dict) and tickers_info.get("data")):
                raise ValueError(f"Failed to get quotations, response: {tickers_info}")

            update_ts = float(time.time() * 1_000)
            quotations = {
                t["symbol"]: Quotation(
                    exch_symbol=t["symbol"],
                    bid=float(t["bestBidPrice"]) if t["bestBidPrice"] else np.nan,
                    ask=float(t["bestAskPrice"]) if t["bestAskPrice"] else np.nan,
                    ts=t["ts"] / 1_000_000,
                    update_ts=update_ts,
                    bid_qty=float(t["bestBidSize"]) if t["bestBidSize"] else np.nan,
                    ask_qty=float(t["bestAskSize"]) if t["bestAskSize"] else np.nan,
                )
                for t in tickers_info["data"]
            }
            return Quotations(quotations)

    @catch_it
    async def get_orderbook_snapshot(self, symbol: str, limit: int = 100) -> OrderBook | None:
        resp = await self.client.get_orderbook(symbol)
        if not resp or not (isinstance(resp, dict) and resp["code"] == "200000"):
            raise Exception(f"Get orderbook snapshot failed. err_mgs={resp}")
        orderbook = OrderBook(symbol)
        orderbook.exch_seq = int(resp["data"]["sequence"])
        orderbook.exch_ts = int(resp["data"]["time"])
        orderbook.recv_ts = int(time.time() * 1_000)
        for bid in resp["data"]["bids"][:limit]:
            orderbook.bids.append((Decimal(bid[0]), Decimal(bid[1])))
        for ask in resp["data"]["asks"][:limit]:
            orderbook.asks.append((Decimal(ask[0]), Decimal(ask[1])))
        return orderbook

    @catch_it
    async def get_fundamentals(self) -> Fundamentals:
        if self._market_type in [MarketType.SPOT, MarketType.MARGIN]:
            tickers = await self.client.get_spot_instrument_info()
            if tickers is None or not (isinstance(tickers, dict) and tickers["code"] == "200000"):
                raise ValueError(f"Failed to get fundamentals, response: {tickers}")
            fundamentals: dict[str, Fundamental] = {}
            for ticker in tickers["data"]["ticker"]:
                symbol = ticker["symbol"]
                if (symbol not in self._insts) or (self._insts[symbol].status != InstStatus.TRADING):
                    continue

                fundamentals[symbol] = Fundamental(symbol, float(ticker["changeRate"]), float(ticker["volValue"]))
            return fundamentals
        else:
            tickers = await self.client.get_swap_instrument_info()
            if tickers is None or not (isinstance(tickers, dict) and tickers["code"] == "200000"):
                raise ValueError(f"Failed to get fundamentals, response: {tickers}")
            fundamentals: dict[str, Fundamental] = {}
            for info in tickers["data"]:
                exch_symbol = info["symbol"]
                symbol = exch_symbol
                if (symbol not in self._insts) or (self._insts[symbol].status != InstStatus.TRADING):
                    continue
                open_interest = float(
                    Decimal(info["openInterest"])
                    * Decimal(self._insts[symbol].quantity_multiplier)
                    * Decimal(info["lastTradePrice"])
                )
                fundamentals[symbol] = Fundamental(
                    symbol,
                    float(info["priceChgPct"]),
                    float(info["turnoverOf24h"]),
                    open_interest,
                )
            return fundamentals

    @catch_it
    async def get_account_info(self) -> AccountInfo:
        if self._market_type in [MarketType.SPOT, MarketType.MARGIN]:
            ticker_resp = await self.client.get_all_tickers()
            if ticker_resp is None or not isinstance(ticker_resp, dict) or ticker_resp.get("code", None) != "200000":
                raise Exception("更新风险指标失败, 原因: 获取ticker失败 {}".format(ticker_resp))
            else:
                tickers = {
                    ticker_data["symbol"]: float(ticker_data["last"]) if ticker_data["last"] else np.nan
                    for ticker_data in ticker_resp["data"]["ticker"]
                }

            account_balance_resp = await self.client.get_account(type="trade")
            if (
                account_balance_resp is None
                or not isinstance(account_balance_resp, dict)
                or account_balance_resp.get("code", None) != "200000"
            ):
                raise Exception("更新风险指标失败, 原因: 获取account_balance失败 {}".format(ticker_resp))

            usdt_free = 0
            total_equity = 0
            total_position_value = 0
            for balance_data in account_balance_resp["data"]:
                if balance_data["currency"] != "USDT":
                    symbol = f"{balance_data["currency"]}-USDT"
                    if symbol in tickers:
                        total_equity += float(balance_data["balance"]) * tickers[symbol]
                        total_position_value += float(balance_data["balance"]) * tickers[symbol]
                    else:
                        raise Exception("无法获取价格, 跳过 {}".format(balance_data["currency"]))
                else:
                    usdt_free = float(balance_data["available"])
                    total_equity += float(balance_data["balance"])

            return AccountInfo(
                account=self._account_meta,
                equity=total_equity,
                usdt_free=usdt_free,
                total_position_value=total_position_value,
            )
        else:
            account_overview_resp = await self.client.fetch_future_balance()
            if not (isinstance(account_overview_resp, dict) and account_overview_resp["code"] == "200000"):
                raise Exception(f"更新风险指标时获取account overview失败, 返回:{account_overview_resp}")

            positions_info = await self.client.fetch_positions()
            if positions_info is None or not isinstance(positions_info, dict) or positions_info["code"] != "200000":
                raise Exception(f"更新风险指标时获取持仓失败, 返回: {positions_info}")

            # Account equity = marginBalance + Unrealised PNL
            equity = float(account_overview_resp["data"]["accountEquity"])
            mmr = float(account_overview_resp["data"]["riskRatio"])
            # Margin balance = positionMargin + orderMargin + frozenFunds + availableBalance - unrealisedPNL
            margin_balance = float(account_overview_resp["data"]["marginBalance"])
            # usdt_free defined as available_balance
            usdt_free = available_balance = float(account_overview_resp["data"]["availableBalance"])

            init_margin = 0
            total_position_value = 0
            for pos in positions_info["data"]:
                # Inital margin Cross = opening value/cross leverage; isolated = accumulation of initial margin for each transaction
                init_margin += float(pos["posInit"])
                total_position_value += abs(float(pos["markValue"]))

            imr = 999 if equity == 0 else init_margin / equity

            return AccountInfo(
                account=self._account_meta,
                equity=equity,
                margin_balance=margin_balance,
                available_balance=available_balance,
                mmr=mmr,
                imr=imr,
                usdt_free=usdt_free,
                total_position_value=total_position_value,
            )
