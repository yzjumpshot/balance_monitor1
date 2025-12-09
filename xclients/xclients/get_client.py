import importlib
from typing import Any, overload, TypeVar, Type

from .base_client import BaseRestClient, BaseWsClient
from .common.exceptions import UnsupportedOperationError
from .enum_type import ExchangeName, MarketType, AccountType
from .data_type import AccountConfig, AccountMeta, MarketMeta, WssConfig, RestConfig
from .utils import gen_account_meta_and_config

RC_T = TypeVar("RC_T", bound=BaseRestClient)
WC_T = TypeVar("WC_T", bound=BaseWsClient)


@overload
def get_rest_client(market_meta: MarketMeta, rest_config: RestConfig = RestConfig()) -> RC_T: ...


@overload
def get_rest_client(
    account_meta: AccountMeta,
    account_config: AccountConfig,
    rest_config: RestConfig = RestConfig(),
) -> RC_T: ...


@overload
def get_rest_client(
    exch_name: ExchangeName | str,
    market_type: MarketType | str = MarketType.SPOT,
    account_type: AccountType | str = AccountType.NORMAL,
    account_name: str = "",
    api_key: str = "",
    secret_key: str = "",
    passphrase: str = "",
    uid: str = "",
    rest_config: RestConfig = RestConfig(),
) -> RC_T: ...


def get_rest_client(*args: Any, **kwargs: Any) -> RC_T:
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
        case AccountMeta(
            exch_name=ExchangeName.BINANCE,
            market_type=MarketType.SPOT | MarketType.MARGIN,
            account_type=at,
        ):
            module_name = ".binance.rest"
            if at != AccountType.UNIFIED:
                cls_name = "BinanceSpotRestClient"
            else:
                cls_name = "BinanceUnifiedRestClient"
        case AccountMeta(
            exch_name=ExchangeName.BINANCE,
            market_type=MarketType.UPERP | MarketType.UDELIVERY,
            account_type=at,
        ):
            module_name = ".binance.rest"
            if at != AccountType.UNIFIED:
                cls_name = "BinanceLinearRestClient"
            else:
                cls_name = "BinanceUnifiedRestClient"
        case AccountMeta(
            exch_name=ExchangeName.BINANCE,
            market_type=MarketType.CPERP | MarketType.CDELIVERY,
            account_type=at,
        ):
            module_name = ".binance.rest"
            if at != AccountType.UNIFIED:
                cls_name = "BinanceInverseRestClient"
            else:
                cls_name = "BinanceUnifiedRestClient"
        case AccountMeta(exch_name=ExchangeName.BYBIT):
            module_name = ".bybit.rest"
            cls_name = "BybitRestClient"
        case AccountMeta(
            exch_name=ExchangeName.BITGET,
            market_type=MarketType.SPOT | MarketType.MARGIN,
        ):
            module_name = ".bitget.rest"
            cls_name = "BitgetSpotRestClient"
        case AccountMeta(
            exch_name=ExchangeName.BITGET,
            market_type=MarketType.UPERP | MarketType.CPERP | MarketType.UDELIVERY | MarketType.CDELIVERY,
        ):
            module_name = ".bitget.rest"
            cls_name = "BitgetFutureRestClient"
        case AccountMeta(exch_name=ExchangeName.COINEX):
            module_name = ".coinex.rest"
            cls_name = "CoinexRestClient"
        case AccountMeta(
            exch_name=ExchangeName.KUCOIN,
            market_type=MarketType.SPOT | MarketType.MARGIN,
        ):
            module_name = ".kucoin.rest"
            cls_name = "KucoinSpotRestClient"
        case AccountMeta(
            exch_name=ExchangeName.KUCOIN,
            market_type=MarketType.UPERP | MarketType.CPERP | MarketType.UDELIVERY | MarketType.CDELIVERY,
        ):
            module_name = ".kucoin.rest"
            cls_name = "KucoinFutureRestClient"
        case AccountMeta(
            exch_name=ExchangeName.GATE,
            market_type=MarketType.SPOT | MarketType.MARGIN,
            account_type=at,
        ):
            module_name = ".gate.rest"
            if at != AccountType.UNIFIED:
                cls_name = "GateSpotRestClient"
            else:
                cls_name = "GateUnifiedSpotRestClient"
        case AccountMeta(
            exch_name=ExchangeName.GATE,
            market_type=MarketType.UPERP | MarketType.CPERP,
            account_type=at,
        ):
            module_name = ".gate.rest"
            if at != AccountType.UNIFIED:
                cls_name = "GateFutureRestClient"
            else:
                cls_name = "GateUnifiedFutureRestClient"
        case AccountMeta(
            exch_name=ExchangeName.GATE,
            market_type=MarketType.UDELIVERY,
            account_type=at,
        ):
            module_name = ".gate.rest"
            if at != AccountType.UNIFIED:
                cls_name = "GateDeliveryRestClient"
            else:
                cls_name = "GateUnifiedDeliveryRestClient"
        case AccountMeta(exch_name=ExchangeName.OKX):
            module_name = ".okx.rest"
            cls_name = "OKXRestClient"
        case AccountMeta(exch_name=ExchangeName.DERIBIT):
            module_name = ".deribit.rest"
            cls_name = "DeribitRestClient"
        case _:
            raise UnsupportedOperationError(f"{account_meta} not supported")

    m = importlib.import_module(module_name, package=__package__)
    cls: Type[RC_T] = getattr(m, cls_name)
    return cls(account_config, rest_config)


@overload
def get_ws_client(market: MarketMeta, wss_config: WssConfig = WssConfig()) -> BaseWsClient: ...


@overload
def get_ws_client(
    account: AccountMeta,
    account_config: AccountConfig,
    wss_config: WssConfig = WssConfig(),
) -> BaseWsClient: ...


@overload
def get_ws_client(
    exch_name: ExchangeName | str,
    market_type: MarketType | str = MarketType.SPOT,
    account_type: AccountType | str = AccountType.NORMAL,
    account_name: str = "",
    api_key: str = "",
    secret_key: str = "",
    passphrase: str = "",
    uid: str = "",
    wss_config: WssConfig = WssConfig(),
) -> BaseWsClient: ...


def get_ws_client(*args: Any, **kwargs: Any) -> BaseWsClient:
    if args and isinstance(args[0], AccountMeta):
        account_meta: AccountMeta = args[0]
        account_config: AccountConfig = args[1]
        wss_config = kwargs.get("wss_config", WssConfig()) or args[2]
    elif args and isinstance(args[0], MarketMeta):
        market_meta: MarketMeta = args[0]
        account_meta = market_meta.account
        account_config = AccountConfig()
        wss_config = kwargs.get("wss_config", WssConfig()) or args[1]
    else:
        account_meta, account_config = gen_account_meta_and_config(*args, **kwargs)
        wss_config = kwargs.get("wss_config") or (args[8] if len(args) > 8 else WssConfig())

    if account_config.has_credentials():
        match account_meta:
            case AccountMeta(exch_name=ExchangeName.BINANCE, market_type=mt, account_type=at):
                module_name = ".binance.websocket"
                if at != AccountType.UNIFIED:
                    cls_name = "BinancePrivateWsClient"
                    default_url = {
                        MarketType.SPOT: "wss://stream.binance.com:9443/ws",
                        MarketType.UPERP: "wss://fstream.binance.com/ws",
                        MarketType.UDELIVERY: "wss://fstream.binance.com/ws",
                        MarketType.CPERP: "wss://dstream.binance.com/ws",
                        MarketType.CDELIVERY: "wss://dstream.binance.com/ws",
                    }[mt]
                else:
                    cls_name = "BinanceUnifiedPrivateWsClient"
                    default_url = "wss://fstream.binance.com/pm"
            case AccountMeta(exch_name=ExchangeName.BYBIT):
                module_name = ".bybit.websocket"
                cls_name = "BybitPrivateWsClient"
                default_url = "wss://stream.bybit.com/v5/private"
            case AccountMeta(exch_name=ExchangeName.BITGET):
                module_name = ".bitget.websocket"
                cls_name = "BitgetPrivateWsClient"
                default_url = "wss://ws.bitget.com/v2/ws/private"
            case AccountMeta(exch_name=ExchangeName.COINEX, market_type=mt):
                module_name = ".coinex.websocket"
                cls_name = "CoinexPrivateWsClient"
                default_url = {
                    MarketType.SPOT: "wss://socket.coinex.com/v2/spot",
                    MarketType.UPERP: "wss://socket.coinex.com/v2/futures",
                }[mt]
            case AccountMeta(exch_name=ExchangeName.KUCOIN):
                module_name = ".kucoin.websocket"
                cls_name = "KucoinPrivateWsClient"
                default_url = ""
            case AccountMeta(exch_name=ExchangeName.GATE, market_type=mt):
                module_name = ".gate.websocket"
                cls_name = "GatePrivateWsClient"
                default_url = {
                    MarketType.SPOT: "wss://api.gateio.ws/ws/v4/",
                    MarketType.UPERP: "wss://fx-ws.gateio.ws/v4/ws/usdt",
                    MarketType.CPERP: "wss://fx-ws.gateio.ws/v4/ws/usdt",
                }[mt]
            case AccountMeta(exch_name=ExchangeName.OKX):
                module_name = ".okx.websocket"
                cls_name = "OKXPrivateWsClient"
                default_url = "wss://ws.okx.com:8443/ws/v5/private"
            case AccountMeta(exch_name=ExchangeName.DERIBIT):
                module_name = ".deribit.websocket"
                cls_name = "DeribitPrivateWsClient"
                default_url = "wss://www.deribit.com/ws/api/v2"
            case _:
                raise UnsupportedOperationError(f"{account_meta} not supported")

        m = importlib.import_module(module_name, package=__package__)
        cls = getattr(m, cls_name)

        wss_config.url = wss_config.url or default_url
        cli = cls(account_meta, account_config, wss_config)
    else:
        match account_meta:
            case AccountMeta(exch_name=ExchangeName.BINANCE, market_type=mt):
                module_name = ".binance.websocket"
                cls_name = "BinanceWsClient"
                default_url = {
                    MarketType.SPOT: "wss://stream.binance.com:9443/stream",
                    MarketType.UPERP: "wss://fstream.binance.com/stream",
                    MarketType.UDELIVERY: "wss://fstream.binance.com/stream",
                    MarketType.CPERP: "wss://dstream.binance.com/stream",
                    MarketType.CDELIVERY: "wss://dstream.binance.com/stream",
                }[mt]
            case AccountMeta(exch_name=ExchangeName.BYBIT, market_type=mt):
                module_name = ".bybit.websocket"
                cls_name = "BybitWsClient"
                default_url = {
                    MarketType.SPOT: "wss://stream.bybit.com/v5/public/spot",
                    MarketType.UPERP: "wss://stream.bybit.com/v5/public/linear",
                    MarketType.CPERP: "wss://stream.bybit.com/v5/public/inverse",
                    MarketType.UDELIVERY: "wss://stream.bybit.com/v5/public/linear",
                    MarketType.CDELIVERY: "wss://stream.bybit.com/v5/public/inverse",
                }[mt]
            case AccountMeta(exch_name=ExchangeName.BITGET):
                module_name = ".bitget.websocket"
                cls_name = "BitgetWsClient"
                default_url = "wss://ws.bitget.com/v2/ws/public"
            case AccountMeta(exch_name=ExchangeName.COINEX, market_type=mt):
                module_name = ".coinex.websocket"
                cls_name = "CoinexWsClient"
                default_url = {
                    MarketType.SPOT: "wss://socket.coinex.com/v2/spot",
                    MarketType.UPERP: "wss://socket.coinex.com/v2/futures",
                    MarketType.CPERP: "wss://socket.coinex.com/v2/futures",
                }[mt]
            case AccountMeta(exch_name=ExchangeName.KUCOIN):
                module_name = ".kucoin.websocket"
                cls_name = "KucoinWsClient"
                default_url = ""
            case AccountMeta(exch_name=ExchangeName.GATE, market_type=mt):
                module_name = ".gate.websocket"
                cls_name = "GateWsClient"
                default_url = {
                    MarketType.SPOT: "wss://api.gateio.ws/ws/v4/",
                    MarketType.UPERP: "wss://fx-ws.gateio.ws/v4/ws/usdt",
                    MarketType.CPERP: "wss://fx-ws.gateio.ws/v4/ws/btc",
                    MarketType.UDELIVERY: "wss://fx-ws.gateio.ws/v4/ws/delivery/usdt",
                }[mt]
            case AccountMeta(exch_name=ExchangeName.OKX):
                module_name = ".okx.websocket"
                cls_name = "OKXWsClient"
                default_url = "wss://ws.okx.com:8443/ws/v5/public"
            case AccountMeta(exch_name=ExchangeName.DERIBIT):
                module_name = ".deribit.websocket"
                cls_name = "DeribitWsClient"
                default_url = "wss://www.deribit.com/ws/api/v2"
            case _:
                raise UnsupportedOperationError(f"{account_meta} not supported")

        m = importlib.import_module(module_name, package=__package__)
        cls = getattr(m, cls_name)

        wss_config.url = wss_config.url or default_url

        cli = cls(account_meta.market, wss_config)
    return cli
