import importlib
import os
import time
from abc import abstractmethod
from datetime import datetime
from decimal import Decimal
from functools import wraps
from types import TracebackType
from typing import Any, Awaitable, Callable, Coroutine, Generic, Literal, Optional, TypedDict, TypeVar, Union, Type

import orjson
from loguru import logger
from redis.asyncio import Redis
from typing_extensions import ParamSpec
from ccxt.async_support.base.exchange import Exchange as CcxtExchange

from .base_client import BaseWsClient, BaseRestClient
from .common.exceptions import UnsupportedOperationError
from .data_type import *
from .enum_type import AccountType, Event, ExchangeName, Interval, MarginMode, OrderSide, TimeInForce
from .get_client import get_rest_client, get_ws_client
from .setting import IS_DEBUG, XCLIENTS_CONFIG_LOADER
from .utils import EventBus

P = ParamSpec("P")
T = TypeVar("T")


class SuccessResp(TypedDict, Generic[T]):
    status: Literal[0]
    data: T


class ErrorResp(TypedDict):
    status: Literal[-1]
    msg: str


class UnsupportedResp(TypedDict):
    status: Literal[-2]
    msg: str


Resp = Union[SuccessResp[T], ErrorResp, UnsupportedResp]


def catch_it(func: Callable[P, Coroutine[Any, Any, T]]) -> Callable[P, Coroutine[Any, Any, Resp[T]]]:
    @wraps(func)
    async def wrapper_func(*args: P.args, **kwargs: P.kwargs) -> Resp[T]:
        resp: Resp[T]
        try:
            ret = await func(*args, **kwargs)
            resp = {"status": 0, "data": ret}
        except UnsupportedOperationError as ex:
            resp = {"status": -2, "msg": str(ex)}
        except Exception as ex:
            if IS_DEBUG:
                logger.exception(f"wrapper exception in {func.__name__} with args={args} kwargs={kwargs}: {ex}")
            else:
                logger.error(f"wrapper exception in {func.__name__} with args={args} kwargs={kwargs}: {ex}")
            resp = {"status": -1, "msg": str(ex)}

        return resp

    return wrapper_func


class BaseWrapper(object):
    def __init__(self, market_meta: MarketMeta) -> None:
        self._market_type = market_meta.market_type
        self._exchange = market_meta.exch_name
        self._event_bus = EventBus()

    def subscribe_callback(self, event: Event, callback: Callable[..., Awaitable[None]]):
        self._event_bus.subscribe(event, callback)

    async def close(self):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        await self.close()


class BaseRestWrapper(BaseWrapper):
    def __init__(self, account_meta: AccountMeta, account_config: AccountConfig, rest_config: RestConfig) -> None:
        super().__init__(account_meta.market)
        self.client = get_rest_client(account_meta, account_config, rest_config)
        self.market_client = self.client
        self._rest_config = rest_config
        self._account_meta = account_meta
        self._account_config = account_config
        self._account = account_meta.account_name
        self._account_type = account_meta.account_type
        self._insts: dict[str, "Instrument"] = {}
        self.ccxt_client: CcxtExchange = None  # type: ignore

        if "REDIS_RMX_URL" in os.environ:
            self._rmx_redis_cli = Redis.from_url(os.environ["REDIS_RMX_URL"])
        else:
            self._rmx_redis_cli = None

        if "REDIS_KIT_URL" in os.environ:
            self._kit_redis_cli = Redis.from_url(os.environ["REDIS_KIT_URL"])
        else:
            self._kit_redis_cli = None

        self._rmx_acc_prefix = XCLIENTS_CONFIG_LOADER.load_config().get("rmx_acc_prefix")

    def init_ccxt_client(self):
        pass

    async def close(self):
        await super().close()
        if self.ccxt_client:
            await self.ccxt_client.close()
            self.ccxt_client = None  # type: ignore

        if self.client:
            cli: BaseRestClient = self.client
            await cli.close()

        if self.market_client:
            cli: BaseRestClient = self.market_client
            await cli.close()

    def get_account_config(self) -> AccountConfig:
        return self._account_config

    async def set_instruments(self, insts: dict[str, "Instrument"]) -> None:
        self._insts = insts

    async def _load_data_from_rmx(self, name: str, key: str):
        raw_data = await self._rmx_redis_cli.hget(name=name, key=key)  # type: ignore
        if not raw_data:
            raise ValueError(f"fail to get result from rmx redis - name={name} key={key}")

        data = orjson.loads(raw_data)
        return data

    async def _load_data_from_rmx_acc(self, suffix: str, key: str):
        name = ":".join(s for s in [self._rmx_acc_prefix, self._account, suffix] if s)
        raw_data = await self._rmx_redis_cli.hget(name=name, key=key)  # type: ignore
        if not raw_data:
            raise ValueError(f"fail to get acc result from rmx redis - name={name} key={key}")

        data = orjson.loads(raw_data)["data"]
        return data

    async def _load_data_from_kit(self, name: str, key: str):
        raw_data = await self._kit_redis_cli.hget(name=name, key=key)  # type: ignore
        if not raw_data:
            raise ValueError(f"fail to get result for from kit redis - name={name} key={key}")

        data = orjson.loads(raw_data)
        return data

    def _parse_start_end_look_back(
        self, start_time: Optional[int] = None, end_time: Optional[int] = None, look_back: Optional[int] = None
    ) -> tuple[int, int]:
        assert not (start_time is None and look_back is None), "start_time/look_back cannot be None at the same time"
        if not end_time:
            end_time = int(time.time()) * 1000
        if not start_time:
            assert look_back is not None
            start_time = end_time - look_back * 8 * 60 * 60 * 1000
        return start_time, end_time

    @catch_it
    async def set_swap_risk_limit(self, symbol: str, risk_limit_level: int) -> bool:
        # bybit, kucoin
        raise UnsupportedOperationError(f"operation not permitted in exchange {self._exchange}")

    @catch_it
    async def get_discount_rate(self, ccy: str) -> DiscountRateData:
        # okx, bybit
        raise UnsupportedOperationError(f"operation not permitted in exchange {self._exchange}")

    @catch_it
    async def get_position(self, symbol: str, from_redis: bool = False) -> Position:
        # binance, okx, kucoin, gate, bybit
        rsp = await self.get_positions(from_redis=from_redis)
        if rsp["status"] == 0 and (data := rsp.get("data")) is not None:
            return data.get(symbol, Position(exch_symbol=symbol, net_qty=0, entry_price=0, value=0, unrealized_pnl=0))
        else:
            raise ValueError(rsp.get("msg", "unknown error"))

    @catch_it
    async def get_positions(self, from_redis: bool = False) -> Positions:
        # binance, okx, kucoin, gate, bybit
        raise UnsupportedOperationError(f"operation not permitted in exchange {self._exchange}")

    @catch_it
    async def get_asset(self, ccy: str, from_redis: bool = False) -> Balance:
        # binance, okx, kucoin, gate, bybit
        rsp = await self.get_assets(from_redis=from_redis)
        if rsp["status"] == 0 and (data := rsp.get("data")) is not None:
            return data.get(ccy, Balance(asset=ccy))
        else:
            raise ValueError(rsp.get("msg", "unknown error"))

    @catch_it
    async def get_assets(self, from_redis: bool = False) -> Balances:
        # binance, okx, kucoin, gate, bybit
        raise UnsupportedOperationError(f"operation not permitted in exchange {self._exchange}")

    @catch_it
    async def get_account_info(self) -> AccountInfo:
        # binance, bybit, okx, kucoin, gate, bybit, coinex
        raise UnsupportedOperationError(f"operation not permitted in exchange {self._exchange}")

    @catch_it
    async def get_tickers(self) -> Tickers:
        # binance, kucoin, okx, gate, bybit, coinex
        raise UnsupportedOperationError(f"operation not permitted in exchange {self._exchange}")

    @catch_it
    async def get_quotations(self) -> Quotations:
        # binance, kucoin, okx, gate, bybit, coinex
        raise UnsupportedOperationError(f"operation not permitted in exchange {self._exchange}")

    @catch_it
    async def get_orderbook_snapshot(self, symbol: str, limit: int = 100) -> OrderBook:
        # binance, kucoin, okx, gate, bybit
        raise UnsupportedOperationError(f"operation not permitted in exchange {self._exchange}")

    @catch_it
    async def get_fundamentals(self) -> Fundamentals:
        # binance, kucoin, okx, gate, bybit
        raise UnsupportedOperationError(f"operation not permitted in exchange {self._exchange}")

    @catch_it
    async def get_loans(self) -> LoanData:
        # okx, binance
        raise UnsupportedOperationError(f"operation not permitted in exchange {self._exchange}")

    @catch_it
    async def get_loan(self, ccy: str) -> Loan:
        # okx, binance
        resp = await self.get_loans()
        if loans := resp.get("data"):
            return loans.get(ccy, Loan())
        else:
            raise ValueError(resp.get("msg", "unknown error"))

    @catch_it
    async def get_collateral_ratio(self) -> CollateralRatios:
        # binance, bybit, gate
        raise UnsupportedOperationError(f"operation not permitted in exchange {self._exchange}")

    @catch_it
    async def get_account_vip_level(self) -> str | int:
        # binance, kucoin, gate, bybit
        raise UnsupportedOperationError(f"operation not permitted in exchange {self._exchange}")

    @catch_it
    async def get_symbol_leverage_and_margin_mode(self, symbol: str) -> tuple[int, MarginMode]:
        # bitget
        raise UnsupportedOperationError(f"operation not permitted in exchange {self._exchange}")

    @catch_it
    async def get_price(self, symbol: str) -> float:
        # binance, kucoin, okx, gate, bybit
        raise UnsupportedOperationError(f"operation not permitted in exchange {self._exchange}")

    @catch_it
    async def get_prices(self) -> Prices:
        # binance, bybit
        raise UnsupportedOperationError(f"operation not permitted in exchange {self._exchange}")

    @catch_it
    async def get_trade_history(self, start_time: int, end_time: int, symbol_list: list[str]) -> TradeData:
        # binance, kucoin, okx, gate, bybit
        raise UnsupportedOperationError(f"operation not permitted in exchange {self._exchange}")

    @catch_it
    async def get_order_history(self, start_time: int, end_time: int, symbol_list: list[str]) -> OrderSnapshotData:
        # binance, kucoin, okx, bybit
        raise UnsupportedOperationError(f"operation not permitted in exchange {self._exchange}")

    @catch_it
    async def get_funding_fee(
        self,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        look_back: Optional[int] = None,
        symbol_list: Optional[list[str]] = None,
    ) -> FundingFeeData:
        # binance, kucoin, okx, gate, bybit
        raise UnsupportedOperationError(f"operation not permitted in exchange {self._exchange}")

    @catch_it
    async def get_historical_funding_rate(
        self,
        symbol_list: list[str],
        start_time: datetime | str | None = None,
        days: int = 7,
    ) -> FundingRatesHis:
        # binance, kucoin, okx, gate, bybit
        raise UnsupportedOperationError(f"operation not permitted in exchange {self._exchange}")

    @catch_it
    async def get_current_funding_rate(self, symbol_list: list[str] | None = None) -> FundingRatesCur:
        # binance, kucoin, okx, gate, bybit
        raise UnsupportedOperationError(f"operation not permitted in exchange {self._exchange}")

    @catch_it
    async def get_current_simple_funding_rate(self, symbol_list: list[str] | None = None) -> FundingRatesSimple:
        # binance, kucoin, okx, gate, bybit
        raise UnsupportedOperationError(f"operation not permitted in exchange {self._exchange}")

    @catch_it
    async def get_historical_kline(
        self,
        symbol: str,
        interval: Interval,
        start_time: int,
        end_time: Optional[int] = None,
        contract_type: Literal["PERPETUAL", "CURRENT_QUARTER", "NEXT_QUARTER"] = "PERPETUAL",
    ) -> KLineData:
        # binance, okx
        raise UnsupportedOperationError(f"operation not permitted in exchange {self._exchange}")

    @catch_it
    async def get_leverage(self, symbol: str, mgnMode: MarginMode) -> Leverage:
        # binance, okx, bybit, gate
        raise UnsupportedOperationError(f"operation not permitted in exchange {self._exchange}")

    @catch_it
    async def get_max_open_quantity(self, symbol: str, mgnMode: MarginMode = MarginMode.CROSS) -> MaxOpenQty:
        # okx
        raise UnsupportedOperationError(f"operation not permitted in exchange {self._exchange}")

    @catch_it
    async def get_max_open_notional(self, symbol: str, mgnMode: MarginMode = MarginMode.CROSS) -> MaxOpenNotional:
        # binance, bybit, kucoin, gate
        raise UnsupportedOperationError(f"operation not permitted in exchange {self._exchange}")

    @catch_it
    async def get_commission_rate(self, symbol: str, from_redis: bool = False) -> CommissionRate:
        # binance, bybit, kucoin, gate, okx
        raise UnsupportedOperationError(f"operation not permitted in exchange {self._exchange}")

    @catch_it
    async def get_interest_rates_cur(
        self, vip_level: int | str | None = None, vip_loan: bool = False, asset: str = "", days: int = -1
    ) -> InterestRates:
        # binance, bybit, bitget, okx
        raise UnsupportedOperationError(f"operation not permitted in exchange {self._exchange}")

    @catch_it
    async def get_interest_rates_his(
        self,
        vip_level: int | str | None = None,
        vip_loan: bool = False,
        asset: str = "",
        start_time: int | None = None,
        end_time: int | None = None,
    ) -> InterestRates:
        # binance, bybit, bitget, okx
        raise UnsupportedOperationError(f"operation not permitted in exchange {self._exchange}")

    @catch_it
    async def get_margin_interest_rates_cur(self, vip_level: int | None = None, asset: str = "") -> InterestRates:
        # binance, bybit, bitget, okx
        raise UnsupportedOperationError(f"operation not permitted in exchange {self._exchange}")

    @catch_it
    async def get_margin_interest_rates_his(
        self, vip_level: int | None = None, asset: str = "", start_time: int | None = None, end_time: int | None = None
    ) -> InterestRates:
        # binance, bybit, bitget, okx
        raise UnsupportedOperationError(f"operation not permitted in exchange {self._exchange}")

    @catch_it
    async def get_p2p_interest_rates_cur(self, asset: str) -> InterestRates:
        # only binance（存贷易）
        raise UnsupportedOperationError(f"operation not permitted in exchange {self._exchange}")

    @catch_it
    async def get_staking_interest_rates_his(
        self, asset: Literal["SOL", "ETH"], start_time: int | None = None, end_time: int | None = None
    ) -> InterestRates:
        # binance, okx
        raise UnsupportedOperationError(f"operation not permitted in exchange {self._exchange}")

    @catch_it
    async def get_long_short_ratio(self, symbol: str, limit: int, interval: Interval) -> LongShortRatioData:
        raise UnsupportedOperationError(f"operation not permitted in exchange {self._exchange}")

    @catch_it
    async def get_equity(self) -> float:
        raise UnsupportedOperationError(f"operation not permitted in exchange {self._exchange}")

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
        # binance, kucoin, gate
        raise UnsupportedOperationError(f"operation not permitted in exchange {self._exchange}")

    #############################
    ##     交易前的参数设置     ##
    #############################
    # 以下接口均只有部分交易所支持
    @catch_it
    async def set_account_position_mode(self, mode: PositionMode):
        # binance, bitget
        pass

    @catch_it
    async def set_account_margin_mode(self, mode: MarginMode):
        # bybit
        pass

    @catch_it
    async def set_symbol_margin_mode(self, symbol: str, mode: MarginMode):
        # kucoin, bitget
        pass

    @catch_it
    async def set_fee_coin_burn(self, enable: bool):
        # bitget, coinex
        pass

    @catch_it
    async def set_account_leverage(self, leverage: int):
        # bybit, binance
        pass

    @catch_it
    async def set_symbol_leverage(self, symbol: str, leverage: int, **kwargs) -> bool:
        # binance, bybit, okx, gate, kucoin
        raise UnsupportedOperationError(f"operation not permitted in exchange {self._exchange}")

    @catch_it
    async def set_uta_mode(self):
        # bybit
        pass

    @catch_it
    async def enable_auto_repayment(self):
        # binance
        pass

    @catch_it
    async def enable_margin_trading(self):
        # bybit
        pass

    @catch_it
    async def enable_union_asset_mode(self):
        # bitget
        pass

    @catch_it
    async def enable_account_collaterals(self):
        # bybit
        pass

    @catch_it
    async def collect_balances(self) -> bool:
        # binance
        return False

    @catch_it
    async def repay_negative_balances(self) -> bool:
        # binance
        return False

    @catch_it
    async def adjust_risk_limits(self):
        # gate
        pass

    # 发单
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
        raise UnsupportedOperationError(f"operation not permitted in exchange {self._exchange}")

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
        raise UnsupportedOperationError(f"operation not permitted in exchange {self._exchange}")

    async def submit_place_order(
        self,
        instruction: PlaceOrderInstruction,
    ) -> None:
        params = instruction.extras if instruction.extras else {}
        if instruction.take_profit_price:
            params["take_profit_price"] = instruction.take_profit_price
        if instruction.stop_loss_price:
            params["stop_loss_price"] = instruction.stop_loss_price
        order_snapshot: Resp = await self.place_order(
            instruction.exch_symbol,
            instruction.order_side,
            instruction.qty,
            instruction.price,
            instruction.order_type,
            instruction.order_time_in_force,
            instruction.client_order_id,
            instruction.reduce_only,
            params,
        )
        if order_snapshot["status"] == 0:
            await self._event_bus.publish(Event.ORDER, self._account_meta, order_snapshot["data"])

    # 撤单
    @catch_it
    async def cancel_order(self, symbol: str, orderId: Optional[str] = None, clientOid: Optional[str] = None) -> bool:
        raise UnsupportedOperationError(f"operation not permitted in exchange {self._exchange}")

    @catch_it
    async def ccxt_cancel_order(
        self, symbol: str, order_id: str | None = None, client_order_id: str | None = None
    ) -> OrderSnapshot | None:
        raise UnsupportedOperationError(f"operation not permitted in exchange {self._exchange}")

    async def submit_cancel_order(
        self,
        instruction: CancelOrderInstruction,
    ) -> None:
        order_snapshot: Resp = await self.ccxt_cancel_order(
            instruction.exch_symbol, instruction.order_id, instruction.client_order_id
        )
        if order_snapshot["status"] == -2:
            order_snapshot: Resp = await self.cancel_order(
                instruction.exch_symbol, instruction.order_id, instruction.client_order_id
            )
        if order_snapshot["status"] == 0 and order_snapshot["data"]:
            await self._event_bus.publish(Event.ORDER, self._account_meta, order_snapshot["data"])
        else:
            raise ValueError(order_snapshot.get("msg", "unknown error"))

    # 批量撤单
    @catch_it
    async def cancel_all(self, symbol: str) -> bool:
        raise UnsupportedOperationError(f"operation not permitted in exchange {self._exchange}")

    @catch_it
    async def ccxt_cancel_all(self, symbol: str) -> bool:
        raise UnsupportedOperationError(f"operation not permitted in exchange {self._exchange}")

    async def submit_cancel_all(self, instruction: CancelOrderInstruction) -> None:
        try:
            await self.ccxt_cancel_all(instruction.exch_symbol)
        except:
            await self.cancel_all(instruction.exch_symbol)

    # 批量查单
    @catch_it
    async def sync_open_orders(self, symbol: str) -> list[OrderSnapshot]:
        raise UnsupportedOperationError(f"operation not permitted in exchange {self._exchange}")

    @catch_it
    async def ccxt_sync_open_orders(self, symbol: str) -> list[OrderSnapshot]:
        raise UnsupportedOperationError(f"operation not permitted in exchange {self._exchange}")

    async def submit_sync_open_orders(self, instruction: SyncOrderInstruction) -> None:
        orders: Resp = await self.ccxt_sync_open_orders(instruction.exch_symbol)
        if orders["status"] == -2:
            orders: Resp = await self.sync_open_orders(instruction.exch_symbol)
        if orders["status"] == 0:
            for order_snapshot in orders["data"]:
                await self._event_bus.publish(Event.ORDER, self._account_meta, order_snapshot["data"])
        else:
            raise ValueError(orders.get("msg", "unknown error"))

    # 查单
    @catch_it
    async def sync_order(
        self, symbol: str, order_id: str | None = None, client_order_id: str | None = None
    ) -> OrderSnapshot:
        raise UnsupportedOperationError(f"operation not permitted in exchange {self._exchange}")

    @catch_it
    async def ccxt_sync_order(
        self, symbol: str, order_id: str | None = None, client_order_id: str | None = None
    ) -> OrderSnapshot:
        raise UnsupportedOperationError(f"operation not permitted in exchange {self._exchange}")

    async def submit_sync_order(self, instruction: SyncOrderInstruction) -> None:
        order_snapshot: Resp = await self.ccxt_sync_order(
            instruction.exch_symbol, instruction.order_id, instruction.client_order_id
        )
        if order_snapshot["status"] == -2:
            order_snapshot: Resp = await self.sync_order(
                instruction.exch_symbol, instruction.order_id, instruction.client_order_id
            )
        if order_snapshot["status"] == 0:
            await self._event_bus.publish(Event.ORDER, self._account_meta, order_snapshot["data"])
        else:
            raise ValueError(order_snapshot.get("msg", "unknown error"))

    @catch_it
    async def repay(self, asset: str, amount: Decimal, isolated_symbol: Optional[str] = None) -> bool:
        # binance
        raise UnsupportedOperationError(f"operation not permitted in exchange {self._exchange}")

    @staticmethod
    def get_wrapper(
        account_meta: AccountMeta,
        account_config: AccountConfig,
        rest_config: RestConfig = RestConfig(),
    ):
        _rest_cls_mapping = {
            ExchangeName.BINANCE: (".binance.rest_wrapper", "BinanceRestWrapper"),
            ExchangeName.BYBIT: (".bybit.rest_wrapper", "BybitRestWrapper"),
            ExchangeName.BITGET: (".bitget.rest_wrapper", "BitgetRestWrapper"),
            ExchangeName.COINEX: (".coinex.rest_wrapper", "CoinexRestWrapper"),
            ExchangeName.KUCOIN: (".kucoin.rest_wrapper", "KucoinRestWrapper"),
            ExchangeName.GATE: (".gate.rest_wrapper", "GateRestWrapper"),
            ExchangeName.OKX: (".okx.rest_wrapper", "OKXRestWrapper"),
            ExchangeName.DERIBIT: (".deribit.rest_wrapper", "DeribitRestWrapper"),
        }
        cls_info = _rest_cls_mapping.get(account_meta.exch_name)
        if not cls_info:
            raise UnsupportedOperationError(f"{account_meta.exch_name} not supported")

        m = importlib.import_module(cls_info[0], package=__package__)
        cls = getattr(m, cls_info[1])

        return cls(account_meta, account_config, rest_config)


class BaseWssWrapper(BaseWrapper):
    def __init__(self, account_meta: AccountMeta, account_config: AccountConfig, wss_config: WssConfig) -> None:
        super().__init__(account_meta.market)
        self._wss_config = wss_config
        self._account_meta = account_meta
        self._market_meta = account_meta.market
        self._account_config = account_config
        self._subscribed_symbols: set[str] = set()
        self._ws_client: BaseWsClient = self._init_stream(account_meta, account_config, wss_config)

    def get_account_config(self) -> AccountConfig:
        return self._account_config

    @property
    def subscribed_symbols(self) -> set[str]:
        """Get the set of subscribed symbols."""
        return self._subscribed_symbols

    @property
    def registered_events(self) -> list[Event]:
        """Get the registered events for this wrapper."""
        return self._event_bus.get_registered_events()

    def _init_stream(
        self, account_meta: AccountMeta, account_config: AccountConfig, wss_config: WssConfig
    ) -> BaseWsClient:
        """Initialize the WebSocket client for the wrapper."""
        # TODO: is_margin
        ws_client = get_ws_client(account_meta, account_config, wss_config)
        ws_client.register_msg_callback(self._process_message)
        ws_client.register_connected_callback(self._on_connected)
        ws_client.register_disconnected_callback(self._on_disconnected)
        return ws_client

    async def run(self):
        if self._ws_client:
            await self._ws_client.run()

    async def close(self):
        await super().close()
        if self._ws_client:
            await self._ws_client.close()

    @abstractmethod
    async def subscribe_stream(self, symbols: list[str] = []):
        pass

    @abstractmethod
    async def unsubscribe_stream(self, symbols: list[str] = []):
        pass

    def subscribe_symbol(self, symbol: str) -> None:
        if symbol not in self._subscribed_symbols:
            self._subscribed_symbols.add(symbol)

    async def modify_subscribed_symbols(self, symbols: list[str]):
        need_add = list(set(symbols) - self._subscribed_symbols)
        need_remove = list(set(self._subscribed_symbols) - set(symbols))
        if need_add:
            # TODO only public ws need to subscribe symbols
            await self.subscribe_stream(need_add)
        if need_remove:
            await self.unsubscribe_stream(need_remove)
        self._subscribed_symbols = set(symbols)

    @abstractmethod
    async def _process_message(self, message: Any):
        raise NotImplementedError("This method should be implemented in subclasses")

    @abstractmethod
    async def _on_connected(self, ws_client_name: str):
        """Callback when the WebSocket client is connected."""
        raise NotImplementedError("This method should be implemented in subclasses")

    @abstractmethod
    async def _on_disconnected(self, ws_client_name: str):
        """Callback when the WebSocket client is disconnected."""
        raise NotImplementedError("This method should be implemented in subclasses")


class BaseAccountWssWrapper(BaseWssWrapper):
    def __init__(self, account_meta: AccountMeta, account_config: AccountConfig, wss_config: WssConfig) -> None:
        super().__init__(account_meta, account_config, wss_config)

    async def _on_connected(self, ws_client_name: str):
        await self.subscribe_stream()
        await self._event_bus.publish(Event.CONNECTED, self._account_meta)

    async def _on_disconnected(self, ws_client_name: str):
        await self._event_bus.publish(Event.DISCONNECTED, self._account_meta)

    async def _process_message(self, message: dict[str, Any]) -> None:
        for event in self.registered_events:
            if Event.BALANCE == event and self._is_balance_message(message):
                if resp := self._balance_handler(message):
                    for balance in resp.values():
                        await self._event_bus.publish(Event.BALANCE, self._account_meta, balance)
            elif Event.POSITION == event and self._is_position_message(message):
                if resp := self._position_handler(message):
                    for position in resp.values():
                        await self._event_bus.publish(Event.POSITION, self._account_meta, position)
            elif Event.ORDER == event and self._is_order_message(message):
                if resp := self._order_handler(message):
                    for order in resp:
                        await self._event_bus.publish(Event.ORDER, self._account_meta, order)
        await self._process_extra_message(message)

    def _is_balance_message(self, msg: dict[str, Any]) -> bool:
        raise NotImplementedError("Subclasses should implement this method")

    def _balance_handler(self, msg: dict[str, Any]) -> Optional[Balances]:
        raise NotImplementedError("Subclasses should implement this method")

    def _is_order_message(self, msg: dict[str, Any]) -> bool:
        raise NotImplementedError("Subclasses should implement this method")

    def _order_handler(self, msg: dict[str, Any]) -> list[OrderSnapshot]:
        raise NotImplementedError("Subclasses should implement this method")

    def _is_position_message(self, msg: dict[str, Any]) -> bool:
        raise NotImplementedError("Subclasses should implement this method")

    def _position_handler(self, msg: dict[str, Any]) -> Optional[Positions]:
        raise NotImplementedError("Subclasses should implement this method")

    def _is_user_trade_message(self, msg: dict[str, Any]) -> bool:
        raise NotImplementedError("Subclasses should implement this method")

    def _user_trade_handler(self, msg: dict[str, Any]) -> Optional[TradeData]:
        raise NotImplementedError("Subclasses should implement this method")

    async def _process_extra_message(self, msg: dict[str, Any]) -> None:
        """Process any extra message types that are not handled by default."""
        pass


class BaseMarketWssWrapper(BaseWssWrapper):
    def __init__(
        self,
        account_meta: AccountMeta,
        account_config: AccountConfig = AccountConfig(),
        wss_config: WssConfig = WssConfig(),
    ) -> None:
        super().__init__(account_meta, account_config, wss_config)

    async def _on_connected(self, ws_client_name: str):
        await self.subscribe_stream()
        await self._event_bus.publish(Event.CONNECTED, self._market_meta)

    async def _on_disconnected(self, ws_client_name: str):
        await self._event_bus.publish(Event.DISCONNECTED, self._market_meta)

    async def _process_message(self, message: Any):
        for event in self.registered_events:
            if Event.BOOK == event and self._is_orderbook_message(message):
                if resp := self._orderbook_handler(message):
                    await self._event_bus.publish(Event.BOOK, self._market_meta, resp)
            elif Event.TICKER == event and self._is_ticker_message(message):
                if resp := self._ticker_handler(message):
                    await self._event_bus.publish(Event.TICKER, self._market_meta, resp)
            elif Event.FUNDING_RATE == event and self._is_funding_rate_message(message):
                if resp := self._funding_rate_handler(message):
                    await self._event_bus.publish(Event.FUNDING_RATE, self._market_meta, resp)
            elif Event.PREMIUM_INDEX == event and self._is_premium_index_message(message):
                if resp := self._premium_index_handler(message):
                    await self._event_bus.publish(Event.PREMIUM_INDEX, self._market_meta, resp)
            elif Event.KLINE and self._is_kline_message(message):
                if resp := self._kline_handler(message):
                    await self._event_bus.publish(event, self._market_meta, resp)

    def _is_orderbook_message(self, message: Any) -> bool:
        return False

    def _is_ticker_message(self, message: Any) -> bool:
        return False

    def _is_funding_rate_message(self, message: Any) -> bool:
        return False

    def _is_premium_index_message(self, message: Any) -> bool:
        return False

    def _is_kline_message(self, message: Any) -> bool:
        return False

    @abstractmethod
    def _orderbook_handler(self, message: Any) -> OrderBook | None:
        pass

    @abstractmethod
    def _ticker_handler(self, message: Any) -> Tickers | None:
        pass

    @abstractmethod
    def _funding_rate_handler(self, message: Any) -> FundingRatesCur:
        pass

    @abstractmethod
    def _premium_index_handler(self, message: Any) -> Any:
        pass

    @abstractmethod
    def _kline_handler(self, message: Any) -> list[Kline]:
        pass
