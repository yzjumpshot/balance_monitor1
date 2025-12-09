import asyncio
import json
import os
import re
import time
from collections import defaultdict
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any, Self

from loguru import logger
from redis.asyncio import Redis

from xclients.binance.rest import BinanceRestClient

from .base_wrapper import BaseRestWrapper
from .constants import PRICE_MULTIPLIER_MAPS, UNIFIED_SYMBOL_MAPS
from .enum_type import AccountType, ContractType, ExchangeName, InstStatus, MarketType
from .get_client import get_rest_client

"""
exchange
- BINANCE
- OKX
- BYBIT
- GATE

market_type
- SPOT
- CPERP
- UPERP
- CDELIVERY
- UDELIVERY
"""

QUOTE_ASSETS = ["USDT", "USD", "USDC", "FDUSD", "USDE", "BFUSD", "USD1"]


class Singleton(type):
    _instances: dict[type, type] = {}

    def __call__(cls, *args: Any, **kwargs: Any):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class Instrument(object):
    def __init__(
        self,
        exchange_symbol: str,
        exchange: ExchangeName,
        market_type: MarketType,
        base_asset: str,
        quote_asset: str,
        unified_symbol: str,
        tick_size: Decimal = Decimal(0),
        lot_size: Decimal = Decimal(1),
        min_order_size: Decimal = Decimal(0),
        min_order_notional: Decimal = Decimal(5),
        max_market_order_size: Decimal = Decimal("inf"),
        max_market_order_notional: Decimal = Decimal("inf"),
        max_position_size: Decimal = Decimal("inf"),
        max_position_notional: Decimal = Decimal("inf"),
        max_slippage: Decimal = Decimal("inf"),
        quantity_multiplier: Decimal = Decimal(1),
        trade_in_notional: bool = False,
        price_multiplier: int = 1,
        status: InstStatus = InstStatus.TRADING,
        margin_trading: bool = False,
        fu_contract_types: list[ContractType] = [],
    ) -> None:
        self.exchange_symbol = exchange_symbol
        self.exchange = exchange
        self.market_type = market_type
        self.base_asset = base_asset
        self.quote_asset = quote_asset
        self.tick_size = tick_size
        self.lot_size = lot_size
        self.price_precision = self.get_precision(tick_size)
        self.quantity_precision = self.get_precision(lot_size)
        self.min_order_size = min_order_size
        self.min_order_notional = min_order_notional
        self.max_market_order_size = max_market_order_size
        self.max_market_order_notional = max_market_order_notional
        self.max_position_size = max_position_size
        self.max_position_notional = max_position_notional
        self.max_slippage = max_slippage
        self.quantity_multiplier = quantity_multiplier
        self.unified_symbol = unified_symbol
        self.unified_base_asset = self.unified_symbol.split("_")[0]
        self.generic_symbol = self.unified_base_asset + "_" + self.to_generic_asset(self.quote_asset)
        self.price_multiplier = price_multiplier
        self.trade_in_notional = trade_in_notional
        self.status = status
        self.margin_trading = margin_trading
        self.fu_contract_types = fu_contract_types

    def to_generic_asset(self, asset: str) -> str:
        if asset in ["USDT", "USDC", "BUSD", "FDUSD"]:
            return "USD"
        return asset

    def to_generic_symbol(self, symbol: str) -> str:
        return re.sub(r"_(USDT|USDC|BUSD|FDUSD)$", "_USD", symbol)

    def get_precision(self, tick: Decimal):
        tick_str = str(tick)
        tick_str = tick_str if tick_str.find(".") == -1 else tick_str.rstrip("0")
        return -int(Decimal(tick_str).as_tuple().exponent) + 1

    @property
    def is_tradable(self):
        return InstStatus.TRADING == self.status or InstStatus.DELISTING == self.status

    @property
    def is_untradable(self):
        return InstStatus.UNTRADABLE == self.status

    @property
    def is_offline(self):
        return InstStatus.OFFLINE == self.status

    @property
    def symbol(self):
        return "{}|{}|{}".format(self.unified_symbol, self.market_type, self.exchange)

    @property
    def unified_tick_size(self):
        return self.tick_size / Decimal(str(self.price_multiplier))

    @property
    def unified_lot_size(self):
        return self.lot_size * Decimal(str(self.price_multiplier)) * self.quantity_multiplier

    @property
    def unified_min_order_size(self):
        return self.min_order_size * Decimal(str(self.price_multiplier)) * self.quantity_multiplier

    @property
    def unified_max_position_size(self):
        return self.max_position_size * Decimal(str(self.price_multiplier)) * self.quantity_multiplier

    def to_json(self) -> str:
        return json.dumps(
            {
                "unified_symbol": self.unified_symbol,
                "exchange_symbol": self.exchange_symbol,
                "generic_symbol": self.generic_symbol,
                "exchange": self.exchange.name,
                "market_type": self.market_type.name,
                "base_asset": self.base_asset,
                "unified_base_asset": self.unified_base_asset,
                "quote_asset": self.quote_asset,
                "tick_size": str(self.tick_size),
                "lot_size": str(self.lot_size),
                "price_precision": str(self.price_precision),
                "quantity_precision": str(self.quantity_precision),
                "min_order_size": str(self.min_order_size),
                "min_order_notional": str(self.min_order_notional),
                "max_market_order_size": str(self.max_market_order_size),
                "max_market_order_notional": str(self.max_market_order_notional),
                "max_position_size": str(self.max_position_size),
                "max_position_notional": str(self.max_position_notional),
                "max_slippage": str(self.max_slippage),
                "quantity_multiplier": str(self.quantity_multiplier),
                "price_multiplier": str(self.price_multiplier),
                "trade_in_notional": self.trade_in_notional,
                "status": self.status.name,
                "margin_trading": self.margin_trading,
                "fu_contract_types": [fu_contract_type.name for fu_contract_type in self.fu_contract_types],
            },
            indent=4,
        )

    @classmethod
    def from_json(cls, json_str: str) -> Self:
        data = json.loads(json_str)
        return cls(
            exchange_symbol=data["exchange_symbol"],
            exchange=ExchangeName[data["exchange"]],
            market_type=MarketType[data["market_type"]],
            base_asset=data["base_asset"],
            quote_asset=data["quote_asset"],
            unified_symbol=data["unified_symbol"],
            tick_size=Decimal(data["tick_size"]),
            lot_size=Decimal(data["lot_size"]),
            min_order_size=Decimal(data["min_order_size"]),
            min_order_notional=Decimal(data["min_order_notional"]),
            max_market_order_size=Decimal(data["max_market_order_size"]),
            max_market_order_notional=Decimal(data["max_market_order_notional"]),
            max_position_size=Decimal(data["max_position_size"]),
            max_position_notional=Decimal(data["max_position_notional"]),
            max_slippage=Decimal(data["max_slippage"]),
            quantity_multiplier=Decimal(data["quantity_multiplier"]),
            trade_in_notional=data["trade_in_notional"],
            price_multiplier=int(data["price_multiplier"]),
            status=InstStatus[data["status"]],
            margin_trading=data.get("margin_trading", False),
            fu_contract_types=[ContractType[ct] for ct in data.get("fu_contract_types", [])],
        )

    def __repr__(self) -> str:
        return f"{super().__repr__()} - {self.symbol}"

    def __str__(self) -> str:
        return f"Instrument(exchange_symbol={self.exchange_symbol}, exchange={self.exchange}, market_type={self.market_type}, base_asset={self.base_asset}, quote_asset={self.quote_asset}, , unified_symbol={self.unified_symbol}, status={self.status})"

    def __eq__(self, other):
        if not isinstance(other, Instrument):
            return NotImplemented
        return (
            self.unified_symbol == other.unified_symbol
            and self.market_type == other.market_type
            and self.exchange == other.exchange
        )

    def __hash__(self):
        return hash(self.unified_symbol) ^ hash(self.market_type) ^ hash(self.exchange)


class InstrumentManager(metaclass=Singleton):
    def __init__(self, quote_assets: list[str] = ["USDT", "USD", "USDC", "FDUSD"]):
        self._mapping_from_unified_symbol: dict[str, dict[str, Instrument]] = defaultdict(dict)
        self._mapping_from_exch_symbol: dict[str, dict[str, Instrument]] = defaultdict(dict)
        self._mapping_from_generic_symbol: dict[str, dict[str, Instrument]] = defaultdict(dict)
        self.unified_symbol_maps: dict[ExchangeName, dict[MarketType, dict[str, str]]] = {}
        self.unified_price_multiplier_maps: dict[ExchangeName, dict[MarketType, dict[str, int]]] = {}
        self.redis_env_vrb = "REDIS_KIT_URL"
        self._inited_exch_market_type: defaultdict[tuple[ExchangeName, MarketType], asyncio.Event] = defaultdict(
            asyncio.Event
        )
        self._quote_assets = quote_assets

    def add_inst(self, exchange: ExchangeName, market_type: MarketType, inst_list: list[Instrument]):
        key = f"{exchange}-{market_type}"
        for inst in inst_list:
            self._mapping_from_unified_symbol[key][inst.unified_symbol] = inst
            self._mapping_from_exch_symbol[key][inst.exchange_symbol] = inst
            self._mapping_from_generic_symbol[key][inst.generic_symbol] = inst

    def clear(self):
        self._mapping_from_unified_symbol.clear()
        self._mapping_from_exch_symbol.clear()
        self._inited_exch_market_type.clear()

    def _gen_unified_symbol_by_exchange_symbol(
        self, exchange: str | ExchangeName, market_type: str | MarketType, exchange_symbol: str
    ):
        if inst := self.get_inst_by_exchange_symbol(exchange, market_type, exchange_symbol):
            return inst.unified_symbol

    def get_exchange_symbol_by_unified_symbol(
        self,
        exchange: str | ExchangeName,
        market_type: str | MarketType,
        unified_symbol: str,
    ):
        inst = self.get_inst_by_unified_symbol(exchange, market_type, unified_symbol)
        if inst:
            return inst.exchange_symbol

    def get_unified_symbol_by_exchange_symbol(
        self,
        exchange: str | ExchangeName,
        market_type: str | MarketType,
        exchange_symbol: str,
    ):
        if inst := self.get_inst_by_exchange_symbol(exchange, market_type, exchange_symbol):
            return inst.unified_symbol

    def get_unified_asset_by_exchange_asset(
        self, exchange: str | ExchangeName, market_type: str | MarketType, exchange_asset: str
    ):
        if exchange_asset in QUOTE_ASSETS:
            return exchange_asset
        insts = self.get_insts_by_exchange_and_asset(exchange, market_type, exchange_asset)
        if len(insts) == 0:
            return None
        elif len(insts) == 1:
            return insts[list(insts.keys())[0]].unified_base_asset
        else:
            unified_base_assets = {inst.unified_base_asset for inst in insts.values()}
            if len(unified_base_assets) == 1:
                return unified_base_assets.pop()
            else:
                return None

    def get_inst_by_unified_symbol(
        self,
        exchange: str | ExchangeName,
        market_type: str | MarketType,
        unified_symbol: str,
    ):
        exchange_key = f"{exchange}-{market_type}"
        return self._mapping_from_unified_symbol.get(exchange_key, {}).get(unified_symbol, None)

    def get_inst_by_exchange_symbol(
        self, exchange: str | ExchangeName, market_type: str | MarketType, exchange_symbol: str
    ):
        exchange_key = f"{exchange}-{market_type}"
        return self._mapping_from_exch_symbol.get(exchange_key, {}).get(exchange_symbol, None)

    def get_inst_by_generic_symbol(
        self, exchange: str | ExchangeName, market_type: str | MarketType, generic_symbol: str
    ):
        exchange_key = f"{exchange}-{market_type}"
        return self._mapping_from_generic_symbol.get(exchange_key, {}).get(generic_symbol, None)

    def get_insts_by_exchange(
        self, exchange: str | ExchangeName, market_type: str | MarketType
    ) -> dict[str, Instrument]:
        return self._mapping_from_exch_symbol.get(f"{exchange}-{market_type}", {})

    def get_insts_by_exchange_and_asset(
        self, exchange: str | ExchangeName, market_type: str | MarketType, asset: str
    ) -> dict[str, Instrument]:
        return {
            k: v
            for k, v in self._mapping_from_exch_symbol.get(f"{exchange}-{market_type}", {}).items()
            if v.base_asset == asset
        }

    def get_price_multiplier_by_exchange_and_asset(
        self, exchange: str | ExchangeName, market_type: str | MarketType, asset: str
    ) -> int | None:
        if asset in QUOTE_ASSETS:
            return 1
        insts = self.get_insts_by_exchange_and_asset(exchange, market_type, asset)
        if len(insts) == 0:
            return None
        elif len(insts) == 1:
            return list(insts.values())[0].price_multiplier
        else:
            price_multipliers = {inst.price_multiplier for inst in insts.values()}
            if len(price_multipliers) == 1:
                return price_multipliers.pop()
            else:
                raise ValueError(f"Asset {asset} has multiple price multipliers in {exchange}-{market_type}")

    async def init_unified_symbol_maps(self):
        unified_symbol_maps = await self.prepare_maps("unified_symbol_maps")

        self.unified_symbol_maps.clear()
        for exchange, data in unified_symbol_maps.items():
            exchange = ExchangeName.get_by_str(exchange)
            if exchange == ExchangeName.UNKNOWN:
                continue

            for market_type, usm in data.items():
                market_type = MarketType.get_by_str(market_type)
                if market_type == MarketType.UNKNOWN:
                    continue

                self.unified_symbol_maps.setdefault(exchange, {})[market_type] = usm  # type: ignore

    async def init_unified_price_multiplier_maps(self):
        unified_price_multiplier_maps = await self.prepare_maps("unified_price_multiplier_maps")

        self.unified_price_multiplier_maps.clear()
        for exchange, data in unified_price_multiplier_maps.items():
            exchange = ExchangeName.get_by_str(exchange)
            if exchange == ExchangeName.UNKNOWN:
                continue

            for market_type, upmm in data.items():
                market_type = MarketType.get_by_str(market_type)
                if market_type == MarketType.UNKNOWN:
                    continue

                self.unified_price_multiplier_maps.setdefault(exchange, {})[market_type] = upmm  # type: ignore

    def _get_redis_cli(self) -> Redis:
        redis_url = os.getenv(self.redis_env_vrb)
        if not redis_url:
            raise ValueError(f"fail to find {self.redis_env_vrb} from environment")

        return Redis.from_url(redis_url)

    async def prepare_maps(self, map_name: str):
        try:
            rds = self._get_redis_cli()
            data = await rds.get(map_name)
            if not data and map_name == "unified_symbol_maps":
                map_name = "meta_symbol_maps"
                data = await rds.get(map_name)

            if not data:
                raise ValueError(f"fail to get data for {map_name} from redis")

            data = json.loads(data.decode("utf-8"))  # type: ignore
        except:
            logger.exception("prepare_maps failed, use constant data")
            return self.get_data_from_constants(map_name)
        return data

    def get_data_from_constants(self, map_name: str):
        if map_name == "unified_symbol_maps":
            return UNIFIED_SYMBOL_MAPS
        elif map_name == "unified_price_multiplier_maps":
            return PRICE_MULTIPLIER_MAPS
        else:
            raise ValueError("map_name: '{}' not found".format(map_name))

    def set_offline_tag(
        self, exchange: ExchangeName, market_type: MarketType, origin_symbol_set: set[str], curr_symbols_set: set[str]
    ):
        offline_symbol_set = origin_symbol_set - curr_symbols_set
        for exch_symbol in offline_symbol_set:
            inst = self.get_inst_by_exchange_symbol(exchange, market_type, exch_symbol)
            if inst and inst.status is not InstStatus.OFFLINE:
                inst.status = InstStatus.OFFLINE
                logger.info(f"{inst.exchange}-{inst.market_type}-{inst.unified_symbol} is offline")

    async def init_instruments_from_wrapper(self, rest_wrapper: BaseRestWrapper, from_redis: bool = False):
        exchange = rest_wrapper._account_meta.exch_name
        market_type = rest_wrapper._account_meta.market_type
        if rest_wrapper._account_type == AccountType.UNIFIED:
            client = rest_wrapper.market_client
        else:
            client = rest_wrapper.client
        await self.init_instruments(exchange, market_type, from_redis=from_redis, client=client)
        insts = self.get_insts_by_exchange(exchange, market_type)
        await rest_wrapper.set_instruments(insts)

    async def init_instruments(
        self, exchange: str | ExchangeName, market_type: str | MarketType, from_redis: bool = False, client: Any = None
    ):
        if from_redis:
            await self.init_instruments_from_redis(exchange, market_type)
            return
        await asyncio.gather(self.init_unified_symbol_maps(), self.init_unified_price_multiplier_maps())

        if isinstance(exchange, str):
            exchange = ExchangeName[exchange]
        if isinstance(market_type, str):
            market_type = MarketType[market_type]
        if not client:
            client = get_rest_client(exchange, market_type)
        if exchange == ExchangeName.BINANCE:
            await self._init_binance_instruments(market_type, client)
        elif exchange == ExchangeName.GATE:
            await self._init_gate_instruments(market_type, client)
        elif exchange == ExchangeName.BYBIT:
            await self._init_bybit_instruments(market_type, client)
        elif exchange == ExchangeName.OKX:
            await self._init_okx_instruments(market_type, client)
        elif exchange == ExchangeName.KUCOIN:
            await self._init_kucoin_instruments(market_type, client)
        elif exchange == ExchangeName.DERIBIT:
            await self._init_deribit_instruments(market_type, client)
        elif exchange == ExchangeName.BITGET:
            await self._init_bitget_instruments(market_type, client)
        elif exchange == ExchangeName.COINEX:
            await self._init_coinex_instruments(market_type, client)
        else:
            raise ValueError("{} exchange is not supported now".format(exchange))

        self._inited_exch_market_type[(exchange, market_type)].set()

    def check_is_inited(self, exchange: str | ExchangeName, market_type: str | MarketType) -> bool:
        if isinstance(exchange, str):
            exchange = ExchangeName[exchange]
        if isinstance(market_type, str):
            market_type = MarketType[market_type]

        return self._inited_exch_market_type[(exchange, market_type)].is_set()

    @staticmethod
    def _gen_quarter_delivery_timestamp() -> list[str]:
        """
        计算最近2个年度 每季度最后一个周五08:00:00的timestamp(UTC ms)
        """
        today = datetime.today()
        nature_quarters = [
            datetime(year, month, day, 8, 0, 0)
            for year in [today.year, today.year + 1]
            for month, day in [(3, 31), (6, 30), (9, 30), (12, 31)]
        ]
        delivery_dates = [
            quarter_date - timedelta(days=(7 + quarter_date.weekday() - 4) % 7) for quarter_date in nature_quarters
        ]
        delivery_dates = [date for date in delivery_dates if date > today]
        delivery_dates.sort()
        delivery_timestamps = [str(int(delivery_quarter.timestamp() * 1000)) for delivery_quarter in delivery_dates]
        return delivery_timestamps

    @staticmethod
    def _gen_week_delivery_timestamp() -> list[str]:
        """
        计算最近2周周五08:00:00的timestamp(UTC ms) CW, NW
        """
        today = datetime.today()
        week_dates = [
            datetime(today.year, today.month, today.day, 8, 0, 0) + timedelta(week_num * 7 + 4 - today.weekday())
            for week_num in range(0, 3)
        ]
        week_dates = [i for i in week_dates if i > today]
        week_dates.sort()
        delivery_timestamps = [str(int(delivery_week.timestamp() * 1000)) for delivery_week in week_dates]
        return delivery_timestamps

    @staticmethod
    def _gen_month_delivery_timestamp() -> list[str]:
        """
        计算最近2个年度 每月最后一个周五08:00:00的timestamp(UTC ms)
        """
        today = datetime.today()
        month_dates = [
            datetime(year, month, 1, 8, 0, 0) - timedelta(days=1)
            for year in [today.year, today.year + 1]
            for month in range(1, 13)
        ]
        month_dates = [dt - timedelta((7 + dt.weekday() - 4) % 7) for dt in month_dates]
        month_dates = [dt for dt in month_dates if dt > today]
        month_dates.sort()
        delivery_timestamps = [str(int(delivery_month.timestamp() * 1000)) for delivery_month in month_dates]
        return delivery_timestamps

    def _gen_unified_symbol(
        self, exchange: ExchangeName, market_type: MarketType, unified_symbol_origin: str, delivery_time: str = ""
    ) -> str:
        unified_symbol = (
            self.unified_symbol_maps[exchange].get(market_type, {}).get(unified_symbol_origin, unified_symbol_origin)
        )
        if not delivery_time:
            return unified_symbol
        else:
            return f"{unified_symbol}_{delivery_time}"

    async def _init_kucoin_instruments(self, market_type: MarketType, client: Any):
        exchange = ExchangeName.KUCOIN
        orig_exch_symbols_set = set(self.get_insts_by_exchange(exchange, market_type).keys())
        curr_exch_symbols_set = set()
        inst_list = []

        match market_type:
            case MarketType.SPOT | MarketType.MARGIN:
                cared_type = []
                resp = await client.get_spot_instrument_info()
            case MarketType.UPERP | MarketType.CPERP:
                cared_type = ["FFWCSX"]
                resp = await client.get_swap_instrument_info()
            case MarketType.UDELIVERY | MarketType.CDELIVERY:
                cared_type = ["FFICSX"]
                resp = await client.get_swap_instrument_info()
            case _:
                raise NotImplementedError(f"Unsupported market type: {market_type}")

        if resp is None or resp["code"] != "200000":
            raise ValueError(resp)

        for info in resp["data"]:
            status = InstStatus.TRADING
            fu_contract_types = []
            if len(cared_type) != 0 and info.get("type") not in cared_type:
                continue

            if self._quote_assets and info["quoteCurrency"] not in self._quote_assets:
                continue

            match market_type:
                case MarketType.UPERP | MarketType.UDELIVERY:
                    if info["isInverse"]:
                        continue
                case MarketType.CPERP | MarketType.CDELIVERY:
                    if not info["isInverse"]:
                        continue

            match market_type:
                case MarketType.UDELIVERY | MarketType.CDELIVERY:
                    delivery_timestamps = self._gen_quarter_delivery_timestamp()
                    if str(int(info["expireDate"])) == delivery_timestamps[0]:
                        fu_contract_types.append(ContractType.CQ)
                    elif str(int(info["expireDate"])) == delivery_timestamps[1]:
                        fu_contract_types.append(ContractType.NQ)
                    else:
                        # logger.info(f"have other delivery time, check it!")
                        continue
                    unified_symbol_origin = "{}_{}".format(info["baseCurrency"], info["quoteCurrency"])
                    delivery_time = datetime.fromtimestamp(int(info["expireDate"]) / 1000).strftime("%y%m%d")
                    unified_symbol = self._gen_unified_symbol(
                        exchange, market_type, unified_symbol_origin, delivery_time
                    )
                case _:
                    unified_symbol_origin = "{}_{}".format(info["baseCurrency"], info["quoteCurrency"])
                    unified_symbol = self._gen_unified_symbol(exchange, market_type, unified_symbol_origin)

            exchange_symbol = info["symbol"]

            match market_type:
                case MarketType.SPOT | MarketType.MARGIN:
                    quantity_multiplier = Decimal(1)
                    tick_size = Decimal(str(info["priceIncrement"]))
                    lot_size = Decimal(str(info["baseIncrement"]))
                    min_order_size = Decimal(str(info["baseMinSize"]))
                    min_order_notional = Decimal(str(info["minFunds"])) if info["minFunds"] else Decimal(0)
                    if info["enableTrading"] == False:
                        status = InstStatus.UNTRADABLE
                case MarketType.UPERP | MarketType.CPERP | MarketType.UDELIVERY | MarketType.CDELIVERY:
                    if market_type in (MarketType.UPERP, MarketType.UDELIVERY):
                        quantity_multiplier = Decimal(str(info["multiplier"]))
                    else:
                        quantity_multiplier = Decimal(1)
                    min_order_size = Decimal(str(info["lotSize"]))
                    min_order_notional = Decimal("0")
                    tick_size = Decimal(str(info["tickSize"]))
                    lot_size = Decimal(str(info["lotSize"]))
                    if info["status"] != "Open":
                        status = InstStatus.UNTRADABLE

            price_multiplier = (
                self.unified_price_multiplier_maps.get(exchange, {}).get(market_type, {}).get(unified_symbol_origin, 1)
            )

            inst = Instrument(
                exchange_symbol=exchange_symbol,
                unified_symbol=unified_symbol,
                exchange=exchange,
                market_type=market_type,
                base_asset=info["baseCurrency"].upper(),
                quote_asset=info["quoteCurrency"].upper(),
                tick_size=tick_size,
                lot_size=lot_size,
                min_order_size=min_order_size,
                min_order_notional=min_order_notional,
                quantity_multiplier=quantity_multiplier,
                trade_in_notional=False if market_type not in [MarketType.CPERP, MarketType.CDELIVERY] else True,
                price_multiplier=price_multiplier,
                status=status,
                fu_contract_types=fu_contract_types,
            )
            curr_exch_symbols_set.add(exchange_symbol)
            inst_list.append(inst)
        self.add_inst(ExchangeName.KUCOIN, market_type, inst_list)
        self.set_offline_tag(exchange, market_type, orig_exch_symbols_set, curr_exch_symbols_set)

    async def _init_okx_instruments(self, market_type: MarketType, client: Any):
        exchange = ExchangeName.OKX
        orig_exch_symbols_set = set(self.get_insts_by_exchange(exchange, market_type).keys())
        curr_exch_symbols_set = set()
        inst_list = []

        contract_type_dict = {
            "quarter": ContractType.CQ,
            "next_week": ContractType.NW,
            "next_quarter": ContractType.NQ,
            "this_week": ContractType.CW,
            "this_month": ContractType.CM,
            "next_month": ContractType.NM,
        }
        cared_type = []
        if market_type in [MarketType.SPOT, MarketType.MARGIN]:
            resp = await client.get_instrument_info("SPOT")
            filter_type = "quoteCcy"
        elif market_type == MarketType.UPERP:
            cared_type = ["linear"]
            filter_type = "settleCcy"
            resp = await client.get_instrument_info("SWAP")
        elif market_type == MarketType.UDELIVERY:
            cared_type = ["linear"]
            filter_type = "settleCcy"
            resp = await client.get_instrument_info("FUTURES")
        elif market_type == MarketType.CPERP:
            cared_type = ["inverse"]
            filter_type = "ctValCcy"
            resp = await client.get_instrument_info("SWAP")
        elif market_type == MarketType.CDELIVERY:
            # tips: 以后要做线性交割合约再拆出来搞
            filter_type = "ctValCcy"
            cared_type = ["inverse"]
            resp = await client.get_instrument_info("FUTURES")
        else:
            return
        if not resp.get("data"):
            raise ValueError(resp)
        for info in resp["data"]:
            status = InstStatus.TRADING
            fu_contract_types = []
            if len(cared_type) != 0:
                if "ctType" in info and info["ctType"] not in cared_type:
                    continue
            if self._quote_assets:
                if info[filter_type] not in self._quote_assets:
                    continue
            if market_type in [MarketType.CDELIVERY, MarketType.UDELIVERY]:
                fu_contract_types = [contract_type_dict.get(info["alias"], ContractType.UNKNOWN)]
                unified_symbol_origin = info["instFamily"].replace("-", "_")
                delivery_time = datetime.fromtimestamp(int(info["expTime"]) / 1000).strftime("%y%m%d")
                unified_symbol = self._gen_unified_symbol(exchange, market_type, unified_symbol_origin, delivery_time)
            else:
                unified_symbol_origin = info["instId"].replace("-", "_").replace("_SWAP", "")
                unified_symbol = self._gen_unified_symbol(exchange, market_type, unified_symbol_origin)

            if info["state"] in ["suspend", "preopen", "test"]:
                status = InstStatus.UNTRADABLE
            exchange_symbol = info["instId"]
            quantity_multiplier = (
                Decimal(1)
                if market_type in [MarketType.SPOT, MarketType.MARGIN]
                else Decimal(info["ctVal"]) * Decimal(info["ctMult"])
            )
            tick_size = Decimal(info["tickSz"])
            lot_size = Decimal(info["lotSz"])
            price_multiplier = (
                self.unified_price_multiplier_maps[exchange].get(market_type, {}).get(unified_symbol_origin, 1)
            )
            inst = Instrument(
                exchange_symbol=exchange_symbol,
                exchange=exchange,
                market_type=market_type,
                base_asset=info["instId"].split("-")[0].upper(),
                quote_asset=info["instId"].split("-")[1].upper(),
                tick_size=tick_size,
                lot_size=lot_size,
                min_order_size=Decimal(str(info["minSz"])),
                min_order_notional=Decimal(0),
                quantity_multiplier=quantity_multiplier,
                trade_in_notional=False if market_type not in [MarketType.CPERP, MarketType.CDELIVERY] else True,
                unified_symbol=unified_symbol,
                price_multiplier=price_multiplier,
                status=status,
                fu_contract_types=fu_contract_types,
            )
            curr_exch_symbols_set.add(exchange_symbol)
            inst_list.append(inst)
        self.add_inst(ExchangeName.OKX, market_type, inst_list)
        self.set_offline_tag(exchange, market_type, orig_exch_symbols_set, curr_exch_symbols_set)

    async def _init_gate_instruments(self, market_type: MarketType, client: Any):
        exchange = ExchangeName.GATE
        fu_contract_types = []
        orig_exch_symbols_set = set(self.get_insts_by_exchange(exchange, market_type).keys())
        curr_exch_symbols_set = set()
        inst_list = []

        if market_type in [MarketType.SPOT, MarketType.MARGIN]:
            resp = await client.get_exchange_info()
            if not isinstance(resp, list):
                raise ValueError(resp)
            for info in resp:
                if self._quote_assets and info["quote"] not in self._quote_assets:
                    continue
                if info["trade_status"] != "tradable":
                    status = InstStatus.UNTRADABLE
                else:
                    status = InstStatus.TRADING

                symbol = "{}_{}|{}|{}".format(
                    info["base"].upper(), info["quote"].upper(), market_type.name, exchange.name
                )
                exchange_symbol = info["id"]
                tick_size = Decimal(1) / Decimal(10) ** Decimal(info["precision"])
                lot_size = Decimal(1) / Decimal(10) ** Decimal(info["amount_precision"])
                min_order_size = Decimal(str(info["min_base_amount"])) if "min_base_amount" in info else Decimal(0)
                min_order_notional = (
                    Decimal(str(info["min_quote_amount"])) if "min_quote_amount" in info else Decimal(0)
                )

                unified_symbol_origin = symbol.split("|")[0]
                unified_symbol = self._gen_unified_symbol(exchange, market_type, unified_symbol_origin)
                price_multiplier = (
                    self.unified_price_multiplier_maps[exchange].get(market_type, {}).get(unified_symbol_origin, 1)
                )

                inst = Instrument(
                    exchange_symbol=exchange_symbol,
                    exchange=exchange,
                    market_type=market_type,
                    base_asset=info["base"].upper(),
                    quote_asset=info["quote"].upper(),
                    tick_size=tick_size,
                    lot_size=lot_size,
                    min_order_size=min_order_size,
                    min_order_notional=min_order_notional,
                    quantity_multiplier=Decimal(1),
                    trade_in_notional=False,
                    unified_symbol=unified_symbol,
                    price_multiplier=price_multiplier,
                    status=status,
                    fu_contract_types=fu_contract_types,
                )
                inst_list.append(inst)
                curr_exch_symbols_set.add(exchange_symbol)
        elif market_type == MarketType.UPERP:
            quote = "USDT"  # gate only support USDT/BTC as quote
            cared_types = ["direct"]  # direct contract only
            resp = await client.get_exchange_info(settle=quote.lower())  # type: ignore
            if not isinstance(resp, list):
                raise ValueError(resp)
            for info in resp:
                status = InstStatus.TRADING
                if len(cared_types) != 0:
                    if "type" in info and info["type"] not in cared_types:
                        continue

                base_asset, quote_asset = info["name"].split("_")
                exchange_symbol = info["name"]

                # https://www.gate.io/docs/developers/apiv4/zh_CN/#contract
                # TODO recheck the meaning
                quantity_multiplier = Decimal(info["quanto_multiplier"])
                # or mark_price_round?
                tick_size = Decimal(info["order_price_round"])
                lot_size = Decimal(info["order_size_min"])
                min_order_size = Decimal(info["order_size_min"])
                min_order_notional = (
                    Decimal(str(info["min_quote_amount"])) if "min_quote_amount" in info else Decimal(0)
                )

                unified_symbol_origin = "{}_{}".format(base_asset.upper(), quote_asset.upper())
                unified_symbol = self._gen_unified_symbol(exchange, market_type, unified_symbol_origin)
                price_multiplier = (
                    self.unified_price_multiplier_maps[exchange].get(market_type, {}).get(unified_symbol_origin, 1)
                )

                if info["in_delisting"]:
                    status = InstStatus.DELISTING

                inst = Instrument(
                    exchange_symbol=exchange_symbol,
                    exchange=exchange,
                    market_type=market_type,
                    base_asset=base_asset.upper(),
                    quote_asset=quote_asset.upper(),
                    tick_size=tick_size,
                    lot_size=lot_size,
                    min_order_size=min_order_size,
                    min_order_notional=min_order_notional,
                    quantity_multiplier=quantity_multiplier,
                    trade_in_notional=False,
                    unified_symbol=unified_symbol,
                    price_multiplier=price_multiplier,
                    status=status,
                    fu_contract_types=fu_contract_types,
                )
                inst_list.append(inst)
                curr_exch_symbols_set.add(exchange_symbol)
        elif market_type == MarketType.CPERP:
            quote = "BTC"
            cared_types = ["direct", "inverse"]  # BTC_USD type is "inverse", ETH_USD type is "direct"
            resp = await client.get_exchange_info(settle=quote.lower())  # type: ignore
            if not isinstance(resp, list):
                raise ValueError(resp)
            for info in resp:
                status = InstStatus.TRADING
                if len(cared_types) != 0:
                    if "type" in info and info["type"] not in cared_types:
                        continue

                base_asset, quote_asset = info["name"].split("_")
                exchange_symbol = info["name"]
                # quantity_multiplier = Decimal(info["quanto_multiplier"])  # CPERP quanto_multiplier为0
                quantity_multiplier = Decimal(1)
                tick_size = Decimal(info["order_price_round"])
                lot_size = Decimal(info["order_size_min"])
                min_order_size = Decimal(info["order_size_min"])
                min_order_notional = (
                    Decimal(str(info["min_quote_amount"])) if "min_quote_amount" in info else Decimal(0)
                )
                unified_symbol_origin = "{}_{}".format(base_asset.upper(), quote_asset.upper())
                unified_symbol = self._gen_unified_symbol(exchange, market_type, unified_symbol_origin)
                price_multiplier = (
                    self.unified_price_multiplier_maps[exchange].get(market_type, {}).get(unified_symbol_origin, 1)
                )

                if info["in_delisting"]:
                    status = InstStatus.DELISTING

                inst = Instrument(
                    exchange_symbol=exchange_symbol,
                    exchange=exchange,
                    market_type=market_type,
                    base_asset=base_asset.upper(),
                    quote_asset=quote_asset.upper(),
                    tick_size=tick_size,
                    lot_size=lot_size,
                    min_order_size=min_order_size,
                    min_order_notional=min_order_notional,
                    quantity_multiplier=quantity_multiplier,
                    trade_in_notional=False,
                    unified_symbol=unified_symbol,
                    price_multiplier=price_multiplier,
                    status=status,
                    fu_contract_types=fu_contract_types,
                )
                inst_list.append(inst)
                curr_exch_symbols_set.add(exchange_symbol)
        elif market_type == MarketType.UDELIVERY:
            client = get_rest_client(exchange, market_type)
            quote = "USDT"
            cared_types = ["direct"]  # BTC_USD type is "inverse", ETH_USD type is "direct"
            resp = await client.get_exchange_info(settle=quote.lower())  # type: ignore
            if not isinstance(resp, list):
                raise ValueError(resp)
            for info in resp:
                status = InstStatus.TRADING
                if len(cared_types) != 0:
                    if "type" in info and info["type"] not in cared_types:
                        continue

                base_asset, quote_asset = info["underlying"].split("_")
                exchange_symbol = info["name"]
                quantity_multiplier = Decimal(info["quanto_multiplier"])
                tick_size = Decimal(info["order_price_round"])
                lot_size = Decimal(info["order_size_min"])
                min_order_size = Decimal(info["order_size_min"])
                min_order_notional = (
                    Decimal(str(info["min_quote_amount"])) if "min_quote_amount" in info else Decimal(0)
                )
                unified_symbol_origin = "{}_{}".format(base_asset.upper(), quote_asset.upper())
                delivery_time = datetime.fromtimestamp(int(info["expire_time"])).strftime("%y%m%d")
                unified_symbol = self._gen_unified_symbol(exchange, market_type, unified_symbol_origin, delivery_time)
                price_multiplier = (
                    self.unified_price_multiplier_maps[exchange].get(market_type, {}).get(unified_symbol_origin, 1)
                )

                if info["in_delisting"]:
                    status = InstStatus.DELISTING

                inst = Instrument(
                    exchange_symbol=exchange_symbol,
                    exchange=exchange,
                    market_type=market_type,
                    base_asset=base_asset.upper(),
                    quote_asset=quote_asset.upper(),
                    tick_size=tick_size,
                    lot_size=lot_size,
                    min_order_size=min_order_size,
                    min_order_notional=min_order_notional,
                    quantity_multiplier=quantity_multiplier,
                    trade_in_notional=False,
                    unified_symbol=unified_symbol,
                    price_multiplier=price_multiplier,
                    status=status,
                    fu_contract_types=fu_contract_types,
                )
                inst_list.append(inst)
                curr_exch_symbols_set.add(exchange_symbol)

        self.add_inst(ExchangeName.GATE, market_type, inst_list)
        self.set_offline_tag(exchange, market_type, orig_exch_symbols_set, curr_exch_symbols_set)

    async def _init_binance_instruments(self, market_type: MarketType, client: BinanceRestClient):
        exchange = ExchangeName.BINANCE
        orig_exch_symbols_set = set(self.get_insts_by_exchange(exchange, market_type).keys())
        curr_exch_symbols_set: set[str] = set()
        inst_list = []

        contract_type_dict = {"NEXT_QUARTER": ContractType.NQ, "CURRENT_QUARTER": ContractType.CQ}

        if market_type in [MarketType.SPOT, MarketType.MARGIN]:
            cared_types = []
        elif market_type == MarketType.UPERP:
            cared_types = ["PERPETUAL"]
        elif market_type == MarketType.UDELIVERY:
            cared_types = ["NEXT_QUARTER", "CURRENT_QUARTER"]
        elif market_type == MarketType.CPERP:
            cared_types = ["PERPETUAL"]
        elif market_type == MarketType.CDELIVERY:
            cared_types = ["NEXT_QUARTER", "CURRENT_QUARTER"]
        else:
            return
        resp = await client.get_exchange_info()  # type: ignore
        if resp is None or not resp.get("symbols"):
            raise ValueError(resp)
        for info in resp["symbols"]:
            fu_contract_types = []
            if len(cared_types) != 0:
                if "contractType" in info and info["contractType"] not in cared_types:
                    continue
            if self._quote_assets and info["quoteAsset"] not in self._quote_assets:
                continue

            match market_type:
                case MarketType.SPOT | MarketType.MARGIN:
                    if info["status"] == "TRADING":
                        status = InstStatus.TRADING
                    else:
                        status = InstStatus.UNTRADABLE
                case MarketType.UPERP | MarketType.UDELIVERY:
                    if info["status"] == "SETTLING" and info["deliveryDate"] < int(time.time() * 1000):
                        continue
                    elif info["status"] == "TRADING":
                        status = InstStatus.TRADING
                    else:
                        status = InstStatus.UNTRADABLE
                case MarketType.CPERP | MarketType.CDELIVERY:
                    if info["contractStatus"] == "TRADING":
                        status = InstStatus.TRADING
                    else:
                        status = InstStatus.UNTRADABLE
                case _:
                    status = InstStatus.UNKNOWN  # never reach here

            if market_type in [MarketType.CDELIVERY, MarketType.UDELIVERY]:
                delivery_time = datetime.fromtimestamp(int(info["deliveryDate"]) / 1000).strftime("%y%m%d")
                fu_contract_types = [contract_type_dict[info["contractType"]]]
            else:
                delivery_time = ""

            exchange_symbol = info["symbol"]
            quantity_multiplier = Decimal(1)
            min_order_notional = Decimal(0)
            min_order_size = Decimal(0)
            tick_size = Decimal(1)
            lot_size = Decimal(0)
            for config in info["filters"]:
                filter_type = config["filterType"]
                if filter_type == "PRICE_FILTER":
                    tick_size = Decimal(str(config["tickSize"]))
                elif filter_type == "LOT_SIZE":
                    lot_size = Decimal(str(config["stepSize"]))
                    min_order_size = Decimal(str(config["minQty"]))
                elif filter_type == "MIN_NOTIONAL":
                    min_order_notional = Decimal(str(config["notional"]))
                elif filter_type == "NOTIONAL":
                    min_order_notional = Decimal(str(config["minNotional"]))

            # unified_symbol_origin = symbol.split("|")[0]
            unified_symbol_origin = "{}_{}".format(info["baseAsset"].upper(), info["quoteAsset"].upper())
            unified_symbol = self._gen_unified_symbol(exchange, market_type, unified_symbol_origin, delivery_time)
            price_multiplier = (
                self.unified_price_multiplier_maps[exchange].get(market_type, {}).get(unified_symbol_origin, 1)
            )

            inst = Instrument(
                exchange_symbol=exchange_symbol,
                exchange=exchange,
                market_type=market_type,
                base_asset=info["baseAsset"].upper(),
                quote_asset=info["quoteAsset"].upper(),
                tick_size=tick_size,
                lot_size=lot_size,
                quantity_multiplier=(
                    quantity_multiplier
                    if market_type not in [MarketType.CPERP, MarketType.CDELIVERY]
                    else Decimal(info["contractSize"])
                ),
                min_order_size=min_order_size,
                min_order_notional=min_order_notional,
                trade_in_notional=False if market_type not in [MarketType.CPERP, MarketType.CDELIVERY] else True,
                unified_symbol=unified_symbol,
                price_multiplier=price_multiplier,
                status=status,
                fu_contract_types=fu_contract_types,
            )
            inst_list.append(inst)
            curr_exch_symbols_set.add(exchange_symbol)
        self.add_inst(ExchangeName.BINANCE, market_type, inst_list)
        self.set_offline_tag(exchange, market_type, orig_exch_symbols_set, curr_exch_symbols_set)
        if client.is_auth() and market_type == MarketType.MARGIN:
            await self._patch_binance_margin_trading(client)

    async def _patch_binance_margin_trading(self, auth_client):
        inst_dict: dict[str, Instrument] = self.get_insts_by_exchange(ExchangeName.BINANCE, MarketType.MARGIN)
        if not inst_dict:
            await self._init_binance_instruments(MarketType.MARGIN, auth_client)
        ret = await auth_client.get_cross_margin_pair()
        if ret:
            margin_symbols = []
            for inst in ret:
                margin_symbols.append(inst["symbol"])
            for symbol in list(inst_dict.keys()):
                if inst_dict[symbol].exchange_symbol not in margin_symbols:
                    # TODO 策略里面是del操作
                    inst_dict[symbol].margin_trading = False
                else:
                    inst_dict[symbol].margin_trading = True

    async def _init_bybit_instruments(self, market_type: MarketType, client: Any):
        """
        Bybit的USDC永续合约quoteCoin是USD, settleCoin是USDC, 用settleCoin更准确些
        :param market_type:
        :return:
        """
        exchange = ExchangeName.BYBIT
        orig_exch_symbols_set = set(self.get_insts_by_exchange(exchange, market_type).keys())
        curr_exch_symbols_set = set()
        inst_list = []

        quote_type = "settleCoin"
        lot_type = "qtyStep"
        if market_type in [MarketType.SPOT, MarketType.MARGIN]:
            category = "spot"
            cared_types = []
            quote_type = "quoteCoin"
            lot_type = "basePrecision"
        elif market_type == MarketType.UPERP:
            category = "linear"
            cared_types = ["LinearPerpetual"]
        elif market_type == MarketType.CPERP:
            cared_types = ["InversePerpetual"]
            category = "inverse"
            quote_type = "quoteCoin"  # settleCoin == baseCoin, eg: ['ADA', 'BTC'], quote_type use `quoteCoin` to get value 'USD'
        elif market_type == MarketType.CDELIVERY:
            cared_types = ["InverseFutures"]
            category = "inverse"
            quote_type = "quoteCoin"
        elif market_type == MarketType.UDELIVERY:
            category = "linear"
            cared_types = ["LinearFutures"]
        else:
            return

        resp = await client.get_instrument_info(category=category)
        if resp is None or resp["retCode"] != 0:
            raise ValueError(resp)
        for info in resp["result"]["list"]:
            status = InstStatus.TRADING
            fu_contract_types = []
            if len(cared_types) != 0:
                if "contractType" in info and info["contractType"] not in cared_types:
                    continue
            if self._quote_assets and info[quote_type] not in self._quote_assets:
                continue
            exchange_symbol = info["symbol"]
            base_coin = info["baseCoin"]
            for suffix in ["2S", "2L", "3S", "3L"]:
                if base_coin.endswith(suffix):
                    continue
            if market_type in [MarketType.CDELIVERY, MarketType.UDELIVERY]:
                # BTCUSDU24 --> BTC_USD_240927|CDELIVERY|BYBIT; BTC-27SEP24 --> BTC_USDC_240927|UDELIVERY|BYBIT;
                delivery_timestamps = self._gen_quarter_delivery_timestamp()
                if str(int(info["deliveryTime"])) == delivery_timestamps[0]:
                    fu_contract_types.append(ContractType.CQ)
                elif str(int(info["deliveryTime"])) == delivery_timestamps[1]:
                    fu_contract_types.append(ContractType.NQ)
                else:
                    # only have CQ, NQ symbol
                    continue
                delivery_time = datetime.fromtimestamp(int(info["deliveryTime"]) / 1000).strftime("%y%m%d")
                # print("stander symbol: ", exchange_symbol, '--->', symbol, " deliver date: ", datetime.fromtimestamp(int(info["deliveryTime"]) / 1000))
            else:
                delivery_time = ""
            if info["status"] == "Closed":
                status = InstStatus.OFFLINE
            elif info["status"] in ["Settling", "Delivering", "PreLaunch"]:
                status = InstStatus.UNTRADABLE

            unified_symbol_origin = "{}_{}".format(base_coin.upper(), info[quote_type].upper())
            unified_symbol = self._gen_unified_symbol(exchange, market_type, unified_symbol_origin, delivery_time)
            price_multiplier = (
                self.unified_price_multiplier_maps[exchange].get(market_type, {}).get(unified_symbol_origin, 1)
            )
            quantity_multiplier = Decimal(1)

            tick_size = Decimal(info["priceFilter"]["tickSize"])
            lot_size = Decimal(info["lotSizeFilter"][lot_type])
            if market_type in [MarketType.UPERP, MarketType.CPERP, MarketType.CDELIVERY, MarketType.UDELIVERY]:
                min_order_size = Decimal(str(info["lotSizeFilter"]["minOrderQty"]))
                min_order_notional = Decimal(0)
            elif market_type in [MarketType.SPOT, MarketType.MARGIN]:
                min_order_size = Decimal(str(info["lotSizeFilter"]["minOrderQty"]))
                min_order_notional = Decimal(str(info["lotSizeFilter"]["minOrderAmt"]))
            else:
                raise NotImplementedError

            inst = Instrument(
                exchange_symbol=exchange_symbol,
                exchange=exchange,
                market_type=market_type,
                base_asset=info["baseCoin"].upper(),
                quote_asset=info[quote_type].upper(),
                tick_size=tick_size,
                lot_size=lot_size,
                min_order_size=min_order_size,
                min_order_notional=min_order_notional,
                quantity_multiplier=quantity_multiplier,
                trade_in_notional=False if market_type not in [MarketType.CPERP, MarketType.CDELIVERY] else True,
                unified_symbol=unified_symbol,
                price_multiplier=price_multiplier,
                status=status,
                fu_contract_types=fu_contract_types,
            )
            inst_list.append(inst)
            curr_exch_symbols_set.add(exchange_symbol)
        self.add_inst(ExchangeName.BYBIT, market_type, inst_list)
        self.set_offline_tag(exchange, market_type, orig_exch_symbols_set, curr_exch_symbols_set)
        if client.is_auth() and market_type == MarketType.SPOT:
            await self._patch_bybit_margin_trading(client)

    async def _patch_bybit_margin_trading(self, auth_client):
        inst_dict: dict[str, Instrument] = self.get_insts_by_exchange(ExchangeName.BYBIT, MarketType.SPOT)
        if not inst_dict:
            await self._init_bybit_instruments(MarketType.SPOT, auth_client)
        ret = await auth_client.get_collateral_info()
        if ret:
            margin_assets = [i["currency"] for i in ret["result"]["list"] if i["currency"] not in ["USDT", "RUNE"]]
            for inst in list(inst_dict.values()):
                if inst.base_asset not in margin_assets:
                    inst.margin_trading = False
                else:
                    inst.margin_trading = True

    async def _init_deribit_instruments(self, market_type: MarketType, client: Any):
        """
        :param market_type:
        :return:
        """
        exchange = ExchangeName.DERIBIT
        orig_exch_symbols_set = set(self.get_insts_by_exchange(exchange, market_type).keys())
        curr_exch_symbols_set = set()
        inst_list = []

        if market_type in [MarketType.SPOT, MarketType.MARGIN]:
            currency = "USDT"
            kind = "spot"
            instrument_type = "linear"
            settlement_periods = []
        elif market_type in [MarketType.UDELIVERY]:
            currency = "any"
            kind = "future"
            instrument_type = "linear"
            settlement_periods = ["week", "month"]
        elif market_type in [MarketType.CDELIVERY]:
            currency = "any"
            kind = "future"
            instrument_type = "reversed"
            settlement_periods = ["week", "month"]
        elif market_type in [MarketType.UPERP]:
            currency = "any"
            kind = "future"
            instrument_type = "linear"
            settlement_periods = ["perpetual"]
        elif market_type in [MarketType.CPERP]:
            currency = "any"
            kind = "future"
            instrument_type = "reversed"
            settlement_periods = ["perpetual"]
        else:
            return

        resp = await client.get_instrument_info(currency=currency, kind=kind)
        if resp is None or "result" not in resp:
            raise ValueError(resp)
        for info in resp["result"]:
            status = InstStatus.TRADING
            fu_contract_types = []
            if info["instrument_type"] != instrument_type:
                continue
            if self._quote_assets and info["quote_currency"] not in self._quote_assets:
                continue
            if settlement_periods:
                if "settlement_period" in info and info["settlement_period"] not in settlement_periods:
                    continue

            if market_type in [MarketType.CDELIVERY, MarketType.UDELIVERY]:
                exchange_symbol = info["instrument_name"]
                # deribit目前只有FU，没有LFU对应合约
                # BTC-31MAY24 --> BTC_USD_240531|CDELIVERY|DERIBIT;
                week_delivery_timestamps = self._gen_week_delivery_timestamp()
                month_delivery_timestamps = self._gen_month_delivery_timestamp()
                quoter_delivery_timestamps = self._gen_quarter_delivery_timestamp()

                if info["settlement_period"] == "week":
                    if str(int(info["expiration_timestamp"])) == week_delivery_timestamps[0]:
                        fu_contract_types.append(ContractType.CW)
                    elif str(int(info["expiration_timestamp"])) == week_delivery_timestamps[1]:
                        fu_contract_types.append(ContractType.NW)
                elif info["settlement_period"] == "month":
                    if str(int(info["expiration_timestamp"])) == month_delivery_timestamps[0]:
                        fu_contract_types.append(ContractType.CM)
                    elif str(int(info["expiration_timestamp"])) == month_delivery_timestamps[1]:
                        fu_contract_types.append(ContractType.NM)
                    if str(int(info["expiration_timestamp"])) == quoter_delivery_timestamps[0]:
                        fu_contract_types.append(ContractType.CQ)
                    elif str(int(info["expiration_timestamp"])) == quoter_delivery_timestamps[1]:
                        fu_contract_types.append(ContractType.NQ)
                else:
                    # logger.error(
                    #     f'fu deliver: {info["instrument_name"]} {info["instrument_type"]} --> {info["expiration_timestamp"]}, {datetime.fromtimestamp(int(info["expiration_timestamp"])/ 1000)}'
                    # )
                    continue
                delivery_time = datetime.fromtimestamp(int(info["expiration_timestamp"]) / 1000).strftime("%y%m%d")
                # logger.info(f"stander:{exchange_symbol}->{symbol}, deliver date: {datetime.fromtimestamp(int(info['expiration_timestamp']) / 1000)}")

            else:
                delivery_time = ""

            if not info["is_active"]:
                status = InstStatus.OFFLINE
            exchange_symbol = info["instrument_name"]
            quantity_multiplier = Decimal(1)
            tick_size = Decimal(str(info["tick_size"]))
            lot_size = Decimal(str(info["contract_size"]))
            min_order_size = Decimal(0)
            min_order_notional = Decimal(str(info["min_trade_amount"]))

            unified_symbol_origin = "{}_{}".format(info["base_currency"].upper(), info["quote_currency"].upper())
            unified_symbol = self._gen_unified_symbol(exchange, market_type, unified_symbol_origin, delivery_time)

            price_multiplier = (
                self.unified_price_multiplier_maps[exchange].get(market_type, {}).get(unified_symbol_origin, 1)
            )
            inst = Instrument(
                exchange_symbol=exchange_symbol,
                exchange=exchange,
                market_type=market_type,
                base_asset=info["base_currency"].upper(),
                quote_asset=info["quote_currency"].upper(),
                tick_size=tick_size,
                lot_size=lot_size,
                min_order_size=min_order_size,
                min_order_notional=min_order_notional,
                quantity_multiplier=quantity_multiplier,
                trade_in_notional=False if market_type not in [MarketType.CPERP, MarketType.CDELIVERY] else True,
                unified_symbol=unified_symbol,
                price_multiplier=price_multiplier,
                status=status,
                fu_contract_types=fu_contract_types,
            )
            inst_list.append(inst)
            curr_exch_symbols_set.add(exchange_symbol)
        self.add_inst(exchange, market_type, inst_list)
        self.set_offline_tag(exchange, market_type, orig_exch_symbols_set, curr_exch_symbols_set)

    async def _init_bitget_instruments(self, market_type: MarketType, client: Any):
        exchange = ExchangeName.BITGET
        orig_exch_symbols_set = set(self.get_insts_by_exchange(exchange, market_type).keys())
        curr_exch_symbols_set = set()
        inst_list = []

        if market_type in [MarketType.SPOT, MarketType.MARGIN]:
            resp = await client.get_symbols()
        elif market_type in [MarketType.UPERP, MarketType.UDELIVERY]:
            resp = await client.get_contracts("USDT-FUTURES")
        elif market_type in [MarketType.CPERP, MarketType.CDELIVERY]:
            resp = await client.get_contracts("COIN-FUTURES")
        else:
            raise NotImplementedError(f"not support market type: {market_type}")

        if isinstance(resp, dict) and resp.get("code") == "00000":
            for info in resp["data"]:
                if market_type in [MarketType.UPERP, MarketType.CPERP] and info["symbolType"] == "delivery":
                    continue
                if market_type in [MarketType.UDELIVERY, MarketType.CDELIVERY] and info["symbolType"] == "perpetual":
                    continue
                trade_status = InstStatus.TRADING
                if market_type in [MarketType.SPOT, MarketType.MARGIN]:
                    if info["status"] != "online":
                        trade_status = InstStatus.UNTRADABLE
                elif info["symbolStatus"] != "normal":
                    trade_status = InstStatus.UNTRADABLE
                unified_symbol_origin = f"{info['baseCoin'].upper()}_{info['quoteCoin'].upper()}"
                price_multiplier = (
                    self.unified_price_multiplier_maps[exchange].get(market_type, {}).get(unified_symbol_origin, 1)
                )

                fu_contract_types = []
                if market_type in [MarketType.UDELIVERY, MarketType.CDELIVERY]:
                    contract_type_dict = {"next_quarter": ContractType.NQ, "this_quarter": ContractType.CQ}
                    fu_contract_types.append(contract_type_dict[info["deliveryPeriod"]])
                    delivery_time = datetime.fromtimestamp(int(info["deliveryTime"]) / 1000).strftime("%y%m%d")
                else:
                    delivery_time = ""
                unified_symbol = self._gen_unified_symbol(exchange, market_type, unified_symbol_origin, delivery_time)

                if market_type in [MarketType.SPOT, MarketType.MARGIN]:
                    tick_size = Decimal(1) / Decimal(10) ** Decimal(info["pricePrecision"])
                    lot_size = Decimal(1) / Decimal(10) ** Decimal(info["quantityPrecision"])
                    min_order_size = Decimal(info["minTradeAmount"])
                else:
                    tick_size = Decimal(1) / Decimal(10) ** Decimal(info["pricePlace"])
                    lot_size = Decimal(1) / Decimal(10) ** Decimal(info["volumePlace"])
                    min_order_size = Decimal(info["minTradeNum"])

                inst = Instrument(
                    exchange_symbol=info["symbol"],
                    exchange=exchange,
                    market_type=market_type,
                    base_asset=info["baseCoin"].upper(),
                    quote_asset=info["quoteCoin"].upper(),
                    tick_size=tick_size,
                    lot_size=lot_size,
                    quantity_multiplier=Decimal(1),
                    min_order_size=min_order_size,
                    min_order_notional=Decimal(info["minTradeUSDT"]),
                    trade_in_notional=True,
                    unified_symbol=unified_symbol,
                    price_multiplier=price_multiplier,
                    status=trade_status,
                    fu_contract_types=fu_contract_types,
                )
                inst_list.append(inst)
                curr_exch_symbols_set.add(info["symbol"])
            self.add_inst(exchange, market_type, inst_list)
            self.set_offline_tag(exchange, market_type, orig_exch_symbols_set, curr_exch_symbols_set)
            return
        else:
            raise ValueError(resp.get("msg", resp))

    async def _init_coinex_instruments(self, market_type: MarketType, client: Any):
        exchange = ExchangeName.COINEX
        orig_exch_symbols_set = set(self.get_insts_by_exchange(exchange, market_type).keys())
        curr_exch_symbols_set = set()
        inst_list = []

        if market_type in [MarketType.SPOT, MarketType.MARGIN]:
            resp = await client.get_spot_market()
        elif market_type in [MarketType.UPERP, MarketType.CPERP]:
            resp = await client.get_swap_market()
        else:
            raise NotImplementedError(f"not support market type: {market_type}")

        if isinstance(resp, dict) and resp.get("code") == 0:
            for info in resp["data"]:

                if self._quote_assets and info["quote_ccy"] not in self._quote_assets:
                    continue

                match market_type:
                    case MarketType.UPERP:
                        if info["contract_type"] == "inverse":
                            continue
                    case MarketType.CPERP:
                        if info["contract_type"] == "linear":
                            continue

                if info["status"] == "online":
                    trade_status = InstStatus.TRADING
                else:
                    trade_status = InstStatus.UNTRADABLE

                unified_symbol_origin = f"{info['base_ccy'].upper()}_{info['quote_ccy'].upper()}"
                unified_symbol = self._gen_unified_symbol(exchange, market_type, unified_symbol_origin)
                price_multiplier = (
                    self.unified_price_multiplier_maps[exchange].get(market_type, {}).get(unified_symbol_origin, 1)
                )
                if market_type == MarketType.CPERP:
                    tick_size = Decimal(info["tick_size"])
                    lot_size = Decimal("0.1") ** Decimal(info["base_ccy_precision"])
                    min_order_size = Decimal(info["min_amount"])
                else:
                    tick_size = Decimal("0.1") ** Decimal(info["quote_ccy_precision"])
                    lot_size = Decimal("0.1") ** Decimal(info["base_ccy_precision"])
                    min_order_size = Decimal(info["min_amount"])

                inst = Instrument(
                    exchange_symbol=info["market"],
                    exchange=exchange,
                    market_type=market_type,
                    base_asset=info["base_ccy"].upper(),
                    quote_asset=info["quote_ccy"].upper(),
                    tick_size=tick_size,
                    lot_size=lot_size,
                    quantity_multiplier=Decimal(1),
                    min_order_size=min_order_size,
                    trade_in_notional=True,
                    unified_symbol=unified_symbol,
                    price_multiplier=price_multiplier,
                    status=trade_status,
                    fu_contract_types=[],
                )
                inst_list.append(inst)
                curr_exch_symbols_set.add(info["market"])
            self.add_inst(exchange, market_type, inst_list)
            self.set_offline_tag(exchange, market_type, orig_exch_symbols_set, curr_exch_symbols_set)
            return
        else:
            raise ValueError(resp.get("msg", resp))

    async def save_instruments_to_redis(self):
        rds = self._get_redis_cli()
        update_ts = time.time()
        for exchange_inst_type, symbol_dict in self._mapping_from_exch_symbol.items():
            exch, market_type = exchange_inst_type.split("-", 1)
            insts_info = {inst.symbol.encode(): inst.to_json() for inst in symbol_dict.values()}
            ts_info = {inst.symbol.encode(): update_ts for inst in symbol_dict.values()}
            inst_key = f"instruments:{exch}:{market_type}"
            ts_key = f"instruments_update_ts:{exch}:{market_type}"

            if insts_info:
                await rds.hset(name=inst_key, mapping=insts_info)  # type: ignore
                await rds.zadd(ts_key, mapping=ts_info)  # type: ignore

            orig_keys = await rds.hkeys(inst_key)  # type: ignore
            offline_keys: set[str] = set(orig_keys) - set(insts_info.keys())  # type: ignore
            if offline_keys:
                offline_values = await rds.hmget(inst_key, list(offline_keys))  # type: ignore
                offline_values = [value.replace(b"TRADING", b"OFFLINE") for value in offline_values]  # type: ignore
                await rds.hset(inst_key, mapping=dict(zip(offline_keys, offline_values)))  # type: ignore

    async def init_instruments_from_redis(self, exchange: str | ExchangeName, market_type: str | MarketType):
        if isinstance(exchange, str):
            exchange = ExchangeName[exchange]
        if isinstance(market_type, str):
            market_type = MarketType[market_type]

        rds = self._get_redis_cli()
        rds_data: dict = await rds.hgetall(name=f"instruments:{exchange}:{market_type}")  # type: ignore
        inst_list = []
        orig_exch_symbols_set = set(self.get_insts_by_exchange(exchange, market_type).keys())
        curr_exch_symbols_set = set()
        for inst_json in rds_data.values():
            inst = Instrument.from_json(inst_json.decode("utf-8"))
            inst_list.append(inst)
            curr_exch_symbols_set.add(inst.exchange_symbol)

        self.add_inst(exchange, market_type, inst_list)
        self.set_offline_tag(exchange, market_type, orig_exch_symbols_set, curr_exch_symbols_set)
        self._inited_exch_market_type[(exchange, market_type)].set()
