import importlib
from typing import Any, overload

from .base_wrapper import BaseAccountWssWrapper, BaseMarketWssWrapper, BaseRestWrapper
from .common.exceptions import UnsupportedOperationError
from .enum_type import ExchangeName, MarketType, AccountType
from .data_type import AccountConfig, AccountMeta, MarketMeta, WssConfig, RestConfig
from .utils import gen_account_meta_and_config


@overload
def get_rest_wrapper(market: MarketMeta, rest_config: RestConfig = RestConfig()) -> BaseRestWrapper: ...


@overload
def get_rest_wrapper(
    account: AccountMeta, account_config: AccountConfig, rest_config: RestConfig = RestConfig()
) -> BaseRestWrapper: ...


# 如果提供 account_name，则从 CredentialManager 获取完整配置
@overload
def get_rest_wrapper(
    exch_name: ExchangeName | str,
    market_type: MarketType | str = MarketType.SPOT,
    account_type: AccountType | str = AccountType.NORMAL,
    account_name: str | None = None,
    api_key: str | None = None,
    secret_key: str | None = None,
    passphrase: str | None = None,
    uid: str | None = None,
    rest_config: RestConfig = RestConfig(),
) -> BaseRestWrapper: ...


def get_rest_wrapper(*args: Any, **kwargs: Any) -> BaseRestWrapper:
    if args and isinstance(args[0], AccountMeta):
        account_meta: AccountMeta = args[0]
        account_config: AccountConfig = args[1]
        rest_config = kwargs.get("rest_config") or (args[2] if len(args) > 2 else RestConfig())
    elif args and isinstance(args[0], MarketMeta):
        market_meta: MarketMeta = args[0]
        account_meta = market_meta.account
        account_config = AccountConfig()
        rest_config = kwargs.get("rest_config") or (args[1] if len(args) > 1 else RestConfig())
    else:
        account_meta, account_config = gen_account_meta_and_config(*args, **kwargs)
        rest_config = kwargs.get("rest_config") or (args[8] if len(args) > 8 else RestConfig())

    match account_meta:
        case AccountMeta(exch_name=ExchangeName.BINANCE):
            module_name = ".binance.rest_wrapper"
            cls_name = "BinanceRestWrapper"
        case AccountMeta(exch_name=ExchangeName.BYBIT):
            module_name = ".bybit.rest_wrapper"
            cls_name = "BybitRestWrapper"
        case AccountMeta(exch_name=ExchangeName.BITGET):
            module_name = ".bitget.rest_wrapper"
            cls_name = "BitgetRestWrapper"
        case AccountMeta(exch_name=ExchangeName.COINEX):
            module_name = ".coinex.rest_wrapper"
            cls_name = "CoinexRestWrapper"
        case AccountMeta(exch_name=ExchangeName.KUCOIN):
            module_name = ".kucoin.rest_wrapper"
            cls_name = "KucoinRestWrapper"
        case AccountMeta(exch_name=ExchangeName.GATE):
            module_name = ".gate.rest_wrapper"
            cls_name = "GateRestWrapper"
        case AccountMeta(exch_name=ExchangeName.OKX):
            module_name = ".okx.rest_wrapper"
            cls_name = "OKXRestWrapper"
        case AccountMeta(exch_name=ExchangeName.DERIBIT):
            module_name = ".deribit.rest_wrapper"
            cls_name = "DeribitRestWrapper"
        case _:
            raise UnsupportedOperationError(f"{account_meta.exch_name} not supported")

    m = importlib.import_module(module_name, package=__package__)
    cls = getattr(m, cls_name)

    return cls(account_meta, account_config, rest_config)


@overload
def get_account_ws_wrapper(
    account: AccountMeta, account_config: AccountConfig, wss_config: WssConfig = WssConfig()
) -> BaseAccountWssWrapper: ...


@overload
def get_account_ws_wrapper(
    exch_name: ExchangeName | str,
    market_type: MarketType | str = MarketType.SPOT,
    account_type: AccountType | str = AccountType.NORMAL,
    account_name: str | None = None,
    api_key: str | None = None,
    secret_key: str | None = None,
    passphrase: str | None = None,
    uid: str | None = None,
    wss_config: WssConfig = WssConfig(),
    **kwargs: Any,
) -> BaseAccountWssWrapper: ...


def get_account_ws_wrapper(*args, **kwargs) -> BaseAccountWssWrapper:
    if args and isinstance(args[0], AccountMeta):
        account_meta: AccountMeta = args[0]
        account_config: AccountConfig = args[1]
        wss_config = kwargs.get("wss_config") or (args[2] if len(args) > 2 else WssConfig())
    else:
        account_meta, account_config = gen_account_meta_and_config(*args, **kwargs)
        wss_config = kwargs.get("wss_config") or (args[8] if len(args) > 8 else WssConfig())

    if not account_config.has_credentials():
        raise UnsupportedOperationError(
            f"{account_meta.exch_name} private wrapper not supported, no credentials provided"
        )

    match account_meta:
        case AccountMeta(exch_name=ExchangeName.BINANCE):
            module_name = ".binance.account_ws_wrapper"
            cls_name = "BinanceAccountWssWrapper"
        case AccountMeta(exch_name=ExchangeName.BYBIT):
            module_name = ".bybit.account_ws_wrapper"
            cls_name = "BybitAccountWssWrapper"
        case AccountMeta(exch_name=ExchangeName.BITGET):
            module_name = ".bitget.account_ws_wrapper"
            cls_name = "BitgetAccountWssWrapper"
        case AccountMeta(exch_name=ExchangeName.COINEX):
            module_name = ".coinex.account_ws_wrapper"
            cls_name = "CoinexAccountWssWrapper"
        case AccountMeta(exch_name=ExchangeName.KUCOIN):
            module_name = ".kucoin.account_ws_wrapper"
            cls_name = "KucoinAccountWssWrapper"
        case AccountMeta(exch_name=ExchangeName.GATE):
            module_name = ".gate.account_ws_wrapper"
            cls_name = "GateAccountWssWrapper"
        case AccountMeta(exch_name=ExchangeName.OKX):
            module_name = ".okx.account_ws_wrapper"
            cls_name = "OKXAccountWssWrapper"
        case AccountMeta(exch_name=ExchangeName.DERIBIT):
            module_name = ".deribit.account_ws_wrapper"
            cls_name = "DeribitAccountWssWrapper"
        case _:
            raise UnsupportedOperationError(f"{account_meta} private wrapper not supported")

    m = importlib.import_module(module_name, package=__package__)
    cls = getattr(m, cls_name)
    return cls(account_meta, account_config, wss_config)


@overload
def get_market_ws_wrapper(market_meta: MarketMeta, wss_config: WssConfig = WssConfig()) -> BaseMarketWssWrapper: ...


@overload
def get_market_ws_wrapper(account_meta: AccountMeta, wss_config: WssConfig = WssConfig()) -> BaseMarketWssWrapper: ...


@overload
def get_market_ws_wrapper(
    exch_name: ExchangeName | str,
    market_type: MarketType | str = MarketType.SPOT,
    account_type: AccountType | str = AccountType.NORMAL,
    account_name: str | None = None,
    api_key: str | None = None,
    secret_key: str | None = None,
    passphrase: str | None = None,
    uid: str | None = None,
    wss_config: WssConfig = WssConfig(),
) -> BaseMarketWssWrapper: ...


def get_market_ws_wrapper(*args, **kwargs) -> BaseMarketWssWrapper:
    if args and isinstance(args[0], AccountMeta):
        account_meta: AccountMeta = args[0]
        account_config: AccountConfig = args[1]
        wss_config = kwargs.get("wss_config") or (args[2] if len(args) > 2 else WssConfig())
    elif args and isinstance(args[0], MarketMeta):
        market_meta: MarketMeta = args[0]
        account_meta = market_meta.account
        account_config = AccountConfig()
        wss_config = kwargs.get("wss_config") or (args[1] if len(args) > 1 else WssConfig())
    else:
        account_meta, account_config = gen_account_meta_and_config(*args, **kwargs)
        wss_config = kwargs.get("wss_config") or (args[8] if len(args) > 8 else WssConfig())

    match account_meta:
        case AccountMeta(exch_name=ExchangeName.BINANCE):
            module_name = ".binance.market_ws_wrapper"
            cls_name = "BinanceMarketWssWrapper"
        case AccountMeta(exch_name=ExchangeName.BYBIT):
            module_name = ".bybit.market_ws_wrapper"
            cls_name = "BybitMarketWssWrapper"
        case AccountMeta(exch_name=ExchangeName.BITGET):
            module_name = ".bitget.market_ws_wrapper"
            cls_name = "BitgetMarketWssWrapper"
        case AccountMeta(exch_name=ExchangeName.COINEX):
            module_name = ".coinex.market_ws_wrapper"
            cls_name = "CoinexMarketWssWrapper"
        case AccountMeta(exch_name=ExchangeName.KUCOIN):
            module_name = ".kucoin.market_ws_wrapper"
            cls_name = "KucoinMarketWssWrapper"
        case AccountMeta(exch_name=ExchangeName.GATE):
            module_name = ".gate.market_ws_wrapper"
            cls_name = "GateMarketWssWrapper"
        case AccountMeta(exch_name=ExchangeName.OKX):
            module_name = ".okx.market_ws_wrapper"
            cls_name = "OKXMarketWssWrapper"
        case AccountMeta(exch_name=ExchangeName.DERIBIT):
            module_name = ".deribit.market_ws_wrapper"
            cls_name = "DeribitMarketWssWrapper"
        case _:
            raise UnsupportedOperationError(f"{account_meta} market wrapper not supported")

    m = importlib.import_module(module_name, package=__package__)
    cls = getattr(m, cls_name)

    return cls(account_meta, account_config, wss_config)
