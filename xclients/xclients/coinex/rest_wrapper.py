from decimal import Decimal
from datetime import datetime, timedelta
from dateutil import parser
from typing import Optional, Union, Any, Literal, Callable
import time
import asyncio
from .rest import CoinexRestClient
from ..base_wrapper import BaseRestWrapper, BaseWssWrapper, catch_it
from ..enum_type import (
    TimeInForce,
    Interval,
    OrderSide,
    MarginMode,
    OrderStatus,
    OrderType,
)
from ..data_type import *
from .constants import INTERVAL_MAP, WITHDRAW_STATUS_MAP, TIF_MAP
import ccxt.async_support as ccxt
from ccxt.base.types import Order as ccxtOrder, ConstructorArgs
from loguru import logger


class CoinexRestWrapper(BaseRestWrapper):
    client: CoinexRestClient

    def __init__(
        self,
        account_meta: AccountMeta,
        account_config: AccountConfig,
        rest_config: RestConfig,
    ) -> None:
        super().__init__(account_meta, account_config, rest_config)
        self.init_ccxt_client()

    def init_ccxt_client(self):
        ccxt_default_type = ""
        ccxt_sub_default_type = ""
        match self._account_meta.market_type:
            case MarketType.SPOT:
                ccxt_default_type = "spot"
            case MarketType.MARGIN:
                ccxt_default_type = "margin"
            case MarketType.UPERP | MarketType.UDELIVERY:
                ccxt_default_type = "swap"
                ccxt_sub_default_type = "linear"
            case MarketType.CPERP | MarketType.CDELIVERY:
                ccxt_default_type = "swap"
                ccxt_sub_default_type = "inverse"

        ccxt_params = {
            "apiKey": self._account_config.api_key,
            "secret": self._account_config.secret_key,
            "enableRateLimit": True,
            "options": {
                "defaultType": ccxt_default_type,
                "defaultSubType": ccxt_sub_default_type,
            },
        }
        self.ccxt_client = ccxt.coinex(ConstructorArgs(ccxt_params))

    @catch_it
    async def subaccount_transfer(
        self,
        ccy: str,
        amount: Decimal,
        from_market_type: Union[str, MarketType],
        to_market_type: Union[str, MarketType],
        from_user_id: Optional[str] = None,
        to_user_id: Optional[str] = None,
    ):
        """
        当子账户转入母账户时, to_user_id不传, 否则会导致Invalid Parameter
        当母账户转入子账户时, from_user_id不传, 否则会导致Invalid Parameter
        """
        if (not to_user_id) and (not from_user_id):
            raise ValueError("Either Parameters `to_user_id` or `from_user_id` is needed")

        if isinstance(from_market_type, str):
            from_market_type = MarketType[from_market_type]
        if isinstance(to_market_type, str):
            to_market_type = MarketType[to_market_type]
        account_type_dict: dict[MarketType, Literal["SPOT", "FUTURES"]] = {
            MarketType.SPOT: "SPOT",
            MarketType.UPERP: "FUTURES",
        }
        resp = await self.client.subaccount_transfer(
            from_account_type=account_type_dict[from_market_type],
            to_account_type=account_type_dict[to_market_type],
            ccy=ccy,
            amount=str(amount),
            from_user_name=from_user_id,
            to_user_name=to_user_id,
        )
        if isinstance(resp, dict) and resp["code"] == 0:
            # 文档中响应示例如下，不会返回对应id, 对应TransferResponse.apply_id=""
            # {
            #     "code": 0,
            #     "message": "OK",
            #     "data": {}
            # }
            return TransferResponse(apply_id="")  # TODO confirm it
        elif resp:
            raise ValueError(resp["message"])
        else:
            raise ValueError("fail to subaccount_transfer from exchange")

    @catch_it
    async def withdraw(
        self,
        transfer_type: Literal["on_chain", "inter_user"],
        address: str,
        ccy: str,
        amount: Decimal,
        chain: Optional[str] = None,
    ):
        # TODO not really tested
        if transfer_type == "on_chain" and not chain:
            raise ValueError("on_chain withdraw `chain` param is needed")

        resp = await self.client.withdraw(
            withdraw_method=transfer_type,
            to_address=address,
            ccy=ccy,
            amount=str(amount),
            chain=chain,
        )
        if isinstance(resp, dict) and resp["code"] == 0:
            return WithdrawResponse(
                order_id=str(resp["data"]["withdraw_id"]),
                status=WITHDRAW_STATUS_MAP.get(resp["data"]["status"], WithdrawStatus.UNKNOWN),
            )
        elif resp:
            raise ValueError(resp["message"])
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
            ccy=ccy,
            withdraw_id=int(order_id),
        )
        if isinstance(resp, dict) and resp["code"] == 0:
            for record in resp["data"]:
                if str(record["withdraw_id"]) == str(order_id):
                    return WithdrawResponse(
                        order_id=str(record["withdraw_id"]),
                        status=WITHDRAW_STATUS_MAP.get(record["status"], WithdrawStatus.UNKNOWN),
                    )
        elif resp:
            raise ValueError(resp["message"])
        else:
            raise ValueError("fail to get withdraw_records from exchange")

    @catch_it
    async def get_positions(self, from_redis: bool = False) -> Positions:
        assert self._market_type in [MarketType.UPERP], f"Market type {self._market_type} is not supported"

        raw_data = []
        result: dict[str, Position] = {}
        if from_redis:
            suffix = "raw:test"
            if MarketType.UPERP == self._market_type:
                key = "future_position"
            else:
                key = ""  # TODO add in redis
            assert key, f"MarketType: {self._market_type} have no redis data"

            data = await self._load_data_from_rmx_acc(suffix, key)
            raw_data.extend(data.values())
        else:
            page = 1
            limit = 1000
            while True:
                resp = await self.client.get_current_position("FUTURES", page=page, limit=limit)
                if resp is None or resp["code"] != 0:
                    raise ValueError("fail to get position from exchange")

                raw_data.extend(resp["data"])
                if not resp["pagination"]["has_next"]:
                    break

                page += 1

        for d in raw_data:
            if float(d["open_interest"]) != 0:
                sign = {"long": 1, "short": -1}.get(d["side"], 1)
                net_qty = float(d["open_interest"]) * sign
                entry_price = float(d["avg_entry_price"])
                result[d["market"]] = Position(
                    exch_symbol=d["market"],
                    net_qty=net_qty,
                    entry_price=entry_price,
                    value=net_qty * entry_price,
                    liq_price=float(d["liq_price"]),
                    unrealized_pnl=float(d["unrealized_pnl"]),
                    ts=int(d["updated_at"]),
                )

        return Positions(result)

    @catch_it
    async def get_assets(self, from_redis: bool = False) -> Balances:
        if self._market_type not in [MarketType.SPOT, MarketType.MARGIN, MarketType.UPERP]:
            raise ValueError(f"Market type {self._market_type} is not supported")

        raw_data = []
        result: dict[str, Balance] = {}
        if from_redis:
            suffix = "raw:test"
            if self._market_type in [MarketType.SPOT, MarketType.MARGIN]:
                key = "spot_balance"
            else:
                key = "futures_balance"

            data = await self._load_data_from_rmx_acc(suffix, key)
            raw_data.extend(data.values())
        else:
            if self._market_type in [MarketType.SPOT, MarketType.MARGIN]:
                fetch_func = self.client.get_spot_balance
            else:
                fetch_func = self.client.get_future_balance

            resp = await fetch_func()
            if resp is None or resp["code"] != 0:
                raise ValueError(f"fail to get balance from exchange: {resp}")
            if resp["data"]:
                raw_data.extend(resp["data"])
        for d in raw_data:
            asset = Balance(
                asset=d["ccy"],
                balance=float(d["available"]) + float(d["frozen"]),
                free=float(d["available"]),
                locked=float(d["frozen"]),
                type="full",
                ts=int(time.time() * 1_000),
            )

            if asset.balance == 0:
                continue

            result[d["ccy"]] = asset

        return Balances(result)

    async def get_subaccount_assets(self, user_id: str):
        if self._market_type in [MarketType.SPOT, MarketType.MARGIN]:
            result: dict[str, Balance] = {}
            resp = await self.client.get_spot_subaccount_balance(sub_user_name=user_id)
            if isinstance(resp, dict) and resp["code"] == 0:
                for sub_info in resp["data"]:
                    balance = float(sub_info["available"]) + float(sub_info["frozen"])
                    if balance == 0:
                        continue
                    result[sub_info["ccy"]] = Balance(
                        asset=sub_info["ccy"],
                        balance=balance,
                        free=float(sub_info["available"]),
                        locked=float(sub_info["frozen"]),
                    )
                return Balances(result)
            elif resp:
                raise ValueError(resp["message"])
            else:
                raise ValueError("fail to get sub assets from exchange")
        else:
            raise ValueError(f"Market type {self._market_type} is not supported")

    @catch_it
    async def get_equity(self) -> float:
        raw_spot_balance, raw_future_balance, raw_spot_ticker = await asyncio.gather(
            self.client.get_spot_balance(), self.client.get_future_balance(), self.client.get_spot_ticker()
        )
        if not (raw_spot_balance and raw_future_balance and raw_spot_ticker):
            raise ValueError("fail to get balance or ticker from exchange")

        spot_balance = raw_spot_balance["data"]
        future_balance = raw_future_balance["data"]
        spot_ticker = raw_spot_ticker["data"]

        tickers = {t["market"]: float(t["last"]) for t in spot_ticker}
        tickers["USDTUSDT"] = 1

        equity = 0
        if spot_balance:
            for b in spot_balance:
                symbol = f"{b['ccy']}USDT"
                amount = float(b["available"]) + float(b["frozen"])
                price = tickers.get(symbol, 0)
                equity += amount * price

        if future_balance:
            for b in future_balance:
                symbol = f"{b['ccy']}USDT"
                amount = float(b["available"]) + float(b["frozen"]) + float(b["margin"]) + float(b["unrealized_pnl"])
                price = tickers.get(symbol, 0)
                equity += amount * price

        return equity

    @catch_it
    async def get_current_funding_rate(self, symbol_list: list[str] | None = None) -> FundingRatesCur:
        assert self._market_type == MarketType.UPERP, "only support get current funding rate for UPERP"
        resp = await self.client.get_funding_rate_current()
        if not (resp and resp["code"] == 0):
            raise ValueError(f"get all market funding rate error, resp: {resp}")

        frs: FundingRatesCur = FundingRatesCur()
        for d in resp["data"]:
            exch_symbol = d["market"]
            if symbol_list and exch_symbol not in symbol_list:
                continue
            fr = float(d["latest_funding_rate"])
            interval = (int(d["next_funding_time"]) - int(d["latest_funding_time"])) // 1000 // 60 // 60
            ts = int(d["latest_funding_time"])
            fr_cap = float(d["max_funding_rate"])
            fr_floor = float(d["min_funding_rate"])
            frs[exch_symbol] = FundingRate(fr, ts, interval_hour=interval, fr_cap=fr_cap, fr_floor=fr_floor)

        return frs

    @catch_it
    async def get_current_simple_funding_rate(self, symbol_list: list[str] | None = None) -> FundingRatesSimple:
        assert self._market_type == MarketType.UPERP, "only support get current funding rate for UPERP"
        resp = await self.client.get_funding_rate_current()
        if not (resp and resp["code"] == 0):
            raise ValueError(f"get all market funding rate error, resp: {resp}")

        frs: FundingRatesSimple = FundingRatesSimple()
        for d in resp["data"]:
            exch_symbol = d["market"]
            if symbol_list and exch_symbol not in symbol_list:
                continue
            fr = float(d["latest_funding_rate"])
            interval = (int(d["next_funding_time"]) - int(d["latest_funding_time"])) // 1000 // 60 // 60
            ts = int(d["latest_funding_time"])
            frs[exch_symbol] = FundingRateSimple(fr, ts, interval_hour=interval)

        return frs

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

        end_ts = int(time.time() * 1000)
        frs: dict[str, set[FundingRateSimple]] = {}
        if not symbol_list:
            symbol_list = []
            resp = await self.client.get_swap_market()
            if not (resp and resp["code"] == 0):
                raise ValueError(resp)

            for info in resp["data"]:
                if info["quote_ccy"] not in ("USDT", "USDC", "USD"):
                    continue

                match self._market_type:
                    case MarketType.UPERP:
                        if info["contract_type"] == "inverse":
                            continue
                    case MarketType.CPERP:
                        if info["contract_type"] == "linear":
                            continue

                symbol_list.append(info["market"])

        for symbol in symbol_list:
            frs[symbol] = set()
            data_list = []
            page = 1
            limit = 1000
            for _ in range(1000):
                resp = await self.client.get_funding_rate_history(
                    symbol, start_time=start_ts, end_time=end_ts, page=page, limit=limit
                )
                if not (resp and resp["code"] == 0):
                    raise ValueError(resp)

                data = resp["data"]
                data_list.extend(data)

                if len(data) < limit:
                    break

                page += 1
                await asyncio.sleep(0.2)

            for item in data_list:
                if not (start_ts <= item["funding_time"] < end_ts):
                    continue

                ts = float(item["funding_time"])
                frs[symbol].add(FundingRateSimple(funding_rate=float(item["actual_funding_rate"]), funding_ts=ts))

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
        kline_list = []
        result: list[KLine] = []
        interval_str = INTERVAL_MAP.get(interval)

        if not interval_str:
            raise Exception(f"unsupported interval: {interval}")

        cur_time = int(time.time() * 1000)
        if not end_time:
            end_time = cur_time

        limit = min(1000, (cur_time - start_time) // (interval.value * 1000) + 1)

        params = {
            "market": symbol,
            "period": interval_str,
            "limit": limit,
        }

        if self._market_type in [MarketType.SPOT, MarketType.MARGIN]:
            fetch_func = self.client.get_spot_kline
        else:
            fetch_func = self.client.get_future_kline

        resp = await fetch_func(**params)
        if not (resp and resp["code"] == 0):
            raise ValueError(resp)

        kline_list = resp["data"]

        for kline in kline_list:
            if not (start_time < int(kline["created_at"]) < end_time):
                continue

            result.append(
                KLine(
                    start_ts=int(kline["created_at"]),
                    open=Decimal(kline["open"]),
                    high=Decimal(kline["high"]),
                    low=Decimal(kline["low"]),
                    close=Decimal(kline["close"]),
                    volume=Decimal(kline["volume"]),
                    turnover=Decimal(kline["value"]),
                )
            )
        return KLineData(result)

    @catch_it
    async def get_price(self, symbol: str) -> float:
        if self._market_type in [MarketType.SPOT, MarketType.MARGIN]:
            resp = await self.client.get_spot_ticker(symbol)
        else:
            resp = await self.client.get_future_ticker(symbol)

        if resp is None or resp["code"] != 0:
            raise ValueError(f"fail to get price - {resp}")

        return float(resp["data"][0]["last"])

    @catch_it
    async def get_prices(self) -> Prices:
        if self._market_type in [MarketType.SPOT, MarketType.MARGIN]:
            resp = await self.client.get_spot_ticker()
        else:
            resp = await self.client.get_future_ticker()

        if resp is None or resp["code"] != 0:
            raise ValueError(f"fail to get prices - {resp}")

        return Prices({d["market"]: float(d["last"]) for d in resp["data"]})

    @catch_it
    async def get_tickers(self) -> Tickers:
        if self._market_type in [MarketType.SPOT, MarketType.MARGIN]:
            tickers = await self.client.get_spot_ticker()
        else:
            tickers = await self.client.get_future_ticker()
        if not (isinstance(tickers, dict) and tickers["code"] == 0):
            raise ValueError(f"fail to get tickers from exchange: {tickers}")

        update_ts = int(time.time() * 1_000)
        tickers = {
            ticker["market"]: Ticker(
                ticker["market"],
                float(ticker["last"]) if ticker["last"] else np.nan,
                float(ticker["last"]) if ticker["last"] else np.nan,
                (float(ticker["index_price"]) if "index_price" in ticker else np.nan),
                ts=update_ts,
                update_ts=update_ts,
                bid_qty=np.nan,
                ask_qty=np.nan,
            )
            for ticker in tickers["data"]
        }
        return Tickers(tickers)

    @catch_it
    async def get_quotations(self) -> Quotations:
        if self._market_type in [MarketType.SPOT, MarketType.MARGIN]:
            tickers = await self.client.get_spot_ticker()
        else:
            tickers = await self.client.get_future_ticker()
        if not (isinstance(tickers, dict) and tickers["code"] == 0):
            raise ValueError(f"fail to get quotations from exchange: {tickers}")

        update_ts = int(time.time() * 1_000)
        quotations = {
            t["market"]: Quotation(
                exch_symbol=t["market"],
                bid=float(t["last"]) if t["last"] else np.nan,
                ask=float(t["last"]) if t["last"] else np.nan,
                ts=update_ts,
                update_ts=update_ts,
                bid_qty=np.nan,
                ask_qty=np.nan,
            )
            for t in tickers["data"]
        }
        return Quotations(quotations)

    @catch_it
    async def get_orderbook_snapshot(self, symbol: str, limit: int = 50) -> OrderBook:
        if self._market_type in [MarketType.SPOT, MarketType.MARGIN]:
            resp = await self.client.get_spot_depth(market=symbol, limit=limit)  # type: ignore[assignment]
        else:
            resp = await self.client.get_future_depth(market=symbol, limit=limit)  # type: ignore[assignment]
        if not resp or not (isinstance(resp, dict) and resp.get("code") == 0):
            raise ValueError(f"fail to get orderbook from exchange: {resp}")
        orderbook = OrderBook(symbol)
        orderbook.exch_seq = int(resp["data"]["depth"]["updated_at"])  # no sequence id
        orderbook.exch_ts = int(resp["data"]["depth"]["updated_at"])
        orderbook.recv_ts = int(time.time() * 1_000)
        for bid in resp["data"]["depth"]["bids"]:
            orderbook.bids.append((Decimal(bid[0]), Decimal(bid[1])))
        for ask in resp["data"]["depth"]["asks"]:
            orderbook.asks.append((Decimal(ask[0]), Decimal(ask[1])))
        return orderbook

    @catch_it
    async def get_fundamentals(self) -> Fundamentals:
        if self._market_type in [MarketType.SPOT, MarketType.MARGIN]:
            tickers = await self.client.get_spot_ticker()
        else:
            tickers = await self.client.get_future_ticker()
        if not (isinstance(tickers, dict) and tickers["code"] == 0):
            raise ValueError(f"fail to get tickers from exchange: {tickers}")
        fundamentals: dict[str, Fundamental] = {}
        _last_price_dic = {}
        for ticker in tickers["data"]:
            symbol = ticker["market"]
            if (symbol not in self._insts) or (self._insts[symbol].status != InstStatus.TRADING):
                continue
            _last_price_dic[symbol] = float(ticker["last"])
            fundamentals[symbol] = Fundamental(
                symbol,
                (
                    float(ticker["last"]) / float(ticker["open"]) - 1 if float(ticker["open"]) != 0 else np.nan
                ),  # 24时涨跌幅 = last/open - 1
                float(ticker["value"]),  # 成交额
            )
        if self._market_type in [MarketType.UPERP]:
            exchange_info = await self.client.get_swap_market()
            if not (isinstance(exchange_info, dict) and exchange_info["code"] == 0):
                raise Exception(f"get open_interest error: {exchange_info}")
            for info in exchange_info["data"]:
                symbol = info["market"]
                if symbol in fundamentals:
                    fundamentals[symbol].open_interest = (
                        float(info["open_interest_volume"])
                        * float(self._insts[symbol].quantity_multiplier)
                        * _last_price_dic[symbol]
                    )
        return fundamentals

    @catch_it
    async def get_commission_rate(self, symbol: str, from_redis: bool = False) -> CommissionRate:
        if from_redis:
            assert self._account, "Account is required to get commission rate from redis"
            data = await self._load_data_from_rmx("trading_fee:coinex", key=self._account)
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
                resp = await self.client.get_commission_rate(symbol=symbol, market_type="SPOT")
            else:
                resp = await self.client.get_commission_rate(symbol=symbol, market_type="FUTURES")
            if not isinstance(resp, dict):
                raise ValueError(f"Could not get current commission rate for symbol[{symbol}]")

            if resp.get("code") != 0:
                raise ValueError(resp["message"])

            makerfee = resp["data"]["maker_rate"]
            takerfee = resp["data"]["taker_rate"]
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

        raw_datas = []
        page = 1
        limit = 1000
        while True:
            resp = await self.client.get_funding_history(
                market_type="FUTURES",
                market=None,
                start_time=start_time,
                end_time=end_time,
                page=page,
                limit=limit,
            )
            if not isinstance(resp, dict):
                raise ValueError(f"Could not get funding fee, resp: {resp}")
            if resp.get("code") != 0:
                raise ValueError(resp["message"])

            data = resp["data"] if resp["data"] else []
            raw_datas.extend(data)

            if len(data) < limit:
                break
            if not resp["pagination"]["has_next"]:
                break
            page += 1

        for funding in raw_datas:
            symbol = funding["market"]
            if symbol_list and symbol not in symbol_list:
                continue
            if symbol not in funding_dict:
                funding_dict[symbol] = [FundingFee(Decimal(funding["funding_value"]), funding["created_at"])]
            else:
                funding_dict[symbol].append(FundingFee(Decimal(funding["funding_value"]), funding["created_at"]))
        return FundingFeeData(funding_dict)

    @catch_it
    async def get_trade_history(self, start_time: int, end_time: int, symbol_list: list[str]):
        result: dict[str, list[Trade]] = {}
        limit = 1000
        if MarketType.SPOT == self._market_type:
            trade_func = self.client.get_spot_trade
            market_type = "SPOT"
        elif MarketType.MARGIN == self._market_type:
            trade_func = self.client.get_spot_trade
            market_type = "MARGIN"
        elif self._market_type in [MarketType.UPERP, MarketType.CPERP]:
            trade_func = self.client.get_future_trade
            market_type = "FUTURES"
        else:
            raise ValueError(f"Market type {self._market_type} is not supported")

        for symbol in symbol_list:
            trade_data_list = []
            page = 1
            while True:
                resp = await trade_func(
                    market=symbol,
                    market_type=market_type,  # type: ignore[assignment]
                    start_time=start_time,
                    end_time=end_time,
                    page=page,
                    limit=limit,
                )
                await asyncio.sleep(0.1)
                if not (isinstance(resp, dict) and resp.get("code") == 0):
                    raise ValueError(f"account[{self._account}] {self._market_type} symbol[{symbol}], error: {resp}")

                data = resp["data"] if resp["data"] else []
                trade_data_list.extend(data)

                if len(data) < limit:
                    break
                if not resp["pagination"]["has_next"]:
                    break
                page += 1
            for data in trade_data_list[::-1]:
                result.setdefault(data["market"], []).append(
                    Trade(
                        create_ts=data["created_at"],
                        side=getattr(OrderSide, data["side"].upper(), OrderSide.UNKNOWN),
                        trade_id=str(data["deal_id"]),
                        order_id=str(data["order_id"]),
                        last_trd_price=Decimal(data["price"]),
                        last_trd_volume=Decimal(data["amount"]),
                        turnover=Decimal(data["price"]) * Decimal(data["amount"]),
                        fill_ts=data["created_at"],
                        fee=Decimal(data["fee"]),
                        fee_ccy=data["fee_ccy"],
                        is_maker=data["role"] == "maker",
                    )
                )
        return TradeData(result)

    @catch_it
    async def get_order_history(self, start_time: int, end_time: int, symbol_list: list[str]) -> OrderSnapshotData:
        order_dict: dict[str, list[OrderSnapshot]] = {}
        order_data_list = []
        limit = 1000

        if MarketType.SPOT == self._market_type:
            fetch_order = self.client.get_spot_finished_orders
            market_type = "SPOT"
        elif MarketType.MARGIN == self._market_type:
            fetch_order = self.client.get_spot_finished_orders
            market_type = "MARGIN"
        elif self._market_type in [MarketType.UPERP, MarketType.CPERP]:
            fetch_order = self.client.get_future_finished_orders
            market_type = "FUTURES"
        else:
            raise ValueError(f"Market type {self._market_type} is not supported")

        for symbol in symbol_list:
            page = 1
            while True:
                resp = await fetch_order(market_type, market=symbol, page=page, limit=limit)  # type: ignore[assignment]
                await asyncio.sleep(0.1)
                if not (isinstance(resp, dict) and resp.get("code") == 0):
                    logger.error(
                        f"account[{self._account}] MarketType[{self._market_type}] symbol[{symbol}], error: {resp}"
                    )
                    await asyncio.sleep(0.2)
                    break

                data = resp["data"] if resp["data"] else []
                order_data_list.extend(data)

                if len(data) < limit:
                    break
                if not resp["pagination"]["has_next"]:
                    break
                if data[-1]["created_at"] < start_time:
                    break

                page += 1

        for od in order_data_list[::-1]:
            if not (start_time <= od["created_at"] < end_time):
                continue

            quantity = Decimal(od["amount"])
            filled_quantity = Decimal(od["filled_amount"])

            if filled_quantity != Decimal(0):
                filled_price = Decimal(od["filled_value"]) / filled_quantity
            else:
                filled_price = Decimal(0)

            if od["type"] == "limit":
                order_type = OrderType.LIMIT
                tif = TimeInForce.GTC
            elif od["type"] == "market":
                order_type = OrderType.MARKET
                tif = TimeInForce.UNKNOWN
            elif od["type"] == "maker_only":
                order_type = OrderType.LIMIT
                tif = TimeInForce.GTX
            elif od["type"] in ("ioc", "fok"):
                order_type = OrderType.LIMIT
                tif = TimeInForce[od["type"].upper()]
            else:
                order_type = OrderType.UNKNOWN
                tif = TimeInForce.UNKNOWN

            if quantity == filled_quantity:
                status = OrderStatus.FILLED
            else:
                status = OrderStatus.CANCELED

            o = OrderSnapshot(
                place_ack_ts=od["created_at"],
                exch_symbol=od["market"],
                order_side=OrderSide[od["side"].upper()],
                order_id=str(od["order_id"]),
                client_order_id=str(od["client_id"]),
                price=Decimal(od["price"]),
                qty=quantity,
                avg_price=float(filled_price),
                filled_qty=filled_quantity,
                order_type=order_type,
                order_time_in_force=tif,
                order_status=status,
                exch_update_ts=od["updated_at"],
                local_update_ts=int(time.time() * 1000),
            )

            order_dict.setdefault(o.exch_symbol, []).append(o)

        return OrderSnapshotData(order_dict)

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

        # 市价单不需要price参数
        if order_type == OrderType.MARKET and price is not None:
            raise ValueError("In market_order parameter price not required")
        # order_type:
        #     limit: 限价单，一直生效，GTC 订单
        #     market: 市价单
        #     maker_only: 只做 maker 单，post_only 订单
        #     ioc: 立即成交或取消
        #     fok: 全部成交或全部取消

        send_order_type: Literal["limit", "market", "maker_only", "ioc", "fok"] = "limit"
        if order_time_in_force:
            send_order_type = TIF_MAP[order_time_in_force]
        elif order_type == OrderType.LIMIT:
            send_order_type = "limit"
        elif order_type == OrderType.MARKET:
            send_order_type = "market"
        send_order_side: Literal["buy", "sell"] = "buy" if order_side == OrderSide.BUY else "sell"

        # 从 extras 中提取 quote_qty
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

        match self._market_type:
            case MarketType.SPOT | MarketType.MARGIN:
                if self._market_type == MarketType.SPOT:
                    market_type = "SPOT"
                else:
                    market_type = "MARGIN"

                client_func = self.client.place_spot_order
                base_ccy, quote_ccy = self.get_ccy(symbol)

                if order_type == OrderType.MARKET:
                    ccy = base_ccy if use_base_qty else quote_ccy
                else:
                    ccy = None

                resp = await client_func(
                    market=symbol,
                    market_type=market_type,  # type: ignore[assignment]
                    side=send_order_side,
                    type=send_order_type,
                    price=str(price) if price else None,
                    amount=str(qty),
                    client_id=client_order_id,
                    ccy=ccy,
                )
            case MarketType.UPERP | MarketType.CPERP:
                market_type = "FUTURES"
                # 只能使用base_qty
                if not use_base_qty:
                    raise ValueError("Only base_qty is allowed")

                client_func = self.client.place_future_order

                resp = await client_func(
                    market=symbol,
                    market_type=market_type,  # type: ignore[assignment]
                    side=send_order_side,
                    type=send_order_type,
                    price=str(price) if price else None,
                    amount=str(qty),
                    client_id=client_order_id,
                )
            case _:
                raise ValueError(f"Market type {self._market_type} is not supported")

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
            snapshot.rejected_message = "No response from exchange"
        elif not (isinstance(resp, dict) and resp.get("code") == 0):
            snapshot.order_status = OrderStatus.REJECTED
            snapshot.rejected_message = resp.get("message", "Unknown error")
        else:
            snapshot.order_id = str(resp["data"]["order_id"])
            snapshot.order_status = OrderStatus.LIVE
            snapshot.place_ack_ts = snapshot.local_update_ts
            snapshot.exch_update_ts = resp["data"]["updated_at"]
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
            if order_snapshot.order_id:
                order_snapshot.order_status = OrderStatus.LIVE
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
        # ccxt sync order 不返回timeInForce
        # 没有任何成交的订单撤销后，服务器不会保存该订单，不能通过任何接口再查询到这个订单的数据。
        # 因此ioc单无法rest获取到订单终止状态
        if not order_id:
            raise ValueError("`order_id` must be provided")

        try:
            params: dict[str, Any] = {}
            order_resp = await self.ccxt_client.fetch_order(order_id, symbol, params=params)
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
    async def set_fee_coin_burn(self, enable: bool) -> None:
        resp = await self.client.enable_cet_discount(enable=enable)
        logger.info(f"Set fee coin burn to {enable}, response: {resp}")

    @catch_it
    async def get_account_info(self) -> AccountInfo:
        if self._market_type not in [MarketType.SPOT, MarketType.MARGIN]:
            balances = await self.get_assets()
            tickers = await self.get_tickers()

            if balances["status"] != 0:
                raise ValueError(f"fail to get account info from exchange: {balances['msg']}")
            if tickers["status"] != 0:
                raise ValueError(f"fail to get account info from exchange: {tickers['msg']}")
            balances = balances["data"]
            tickers = tickers["data"]

            usdt = balances.get("USDT", Balance("USDT")).balance
            total_position_value = 0
            for asset, balance in balances.items():
                if asset in ["USDT"]:
                    continue
                symbol = asset + "_USDT"
                total = balance.balance
                ticker = tickers.get(symbol)
                if ticker is None:
                    logger.warning(f"获取{symbol}行情失败")
                    continue
                total_position_value += abs(total) * ticker.mpx

            equity = total_position_value + usdt

            # No loan at all
            ltv = 999
            margin_balance = equity

            return AccountInfo(
                account=self._account_meta,
                equity=equity,
                usdt_free=usdt,
                ltv=ltv,
                margin_balance=margin_balance,
            )
        else:
            balances = await self.client.get_future_balance()
            if not (isinstance(balances, dict) and balances["code"] == 0):
                raise ValueError(f"fail to get account info from exchange: {balances}")
            account_info = None
            for data in balances["data"]:
                if data["ccy"] == "USDT":
                    account_info = data
                    break
            if account_info is None:
                raise Exception("更新风险指标失败")

            equity = float(account_info["available"]) + float(account_info["frozen"]) + float(account_info["margin"])
            margin_balance = equity
            usdt = float(account_info["available"])

            imr = equity / float(account_info["margin"]) if float(account_info["margin"]) != 0 else 999
            mmr = 999

            return AccountInfo(
                account=self._account_meta,
                equity=equity,
                usdt_free=usdt,
                available_balance=usdt,
                imr=imr,
                mmr=mmr,
                margin_balance=margin_balance,
            )

    def get_ccy(self, symbol: str) -> tuple[str, str]:
        if symbol.endswith("USDT"):
            base_ccy = symbol[:-4]
            quote_ccy = "USDT"
        elif symbol.endswith("USDC"):
            base_ccy = symbol[:-4]
            quote_ccy = "USDC"
        else:
            raise ValueError(f"In market_order symbol {symbol} must end with USDT or USDC")

        return base_ccy, quote_ccy
