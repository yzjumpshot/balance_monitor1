from xclients.get_wrapper import get_rest_wrapper
from xclients.enum_type import MarketType, ExchangeName, AccountType, OrderStatus
import pytest
from decimal import Decimal


def print_section_header(title: str, level: int = 1):
    """打印分区标题"""
    if level == 1:
        print(f"\n{'='*60}")
        print(f"  {title}")
        print(f"{'='*60}")
    elif level == 2:
        print(f"\n{'-'*40}")
        print(f"  {title}")
        print(f"{'-'*40}")
    else:
        print(f"\n>>> {title}")


def print_test_info(exch: ExchangeName, market_type: MarketType, symbol: str, price: Decimal | float):
    """打印测试基本信息"""
    print(f"📈 Exchange: {exch.name}")
    print(f"💼 Market Type: {market_type.name}")
    print(f"🎯 Symbol: {symbol}")
    print(f"💰 Current Price: ${price}")


def get_symbol(ccy: str, exchange: ExchangeName, market_type: MarketType) -> str:
    ccy = ccy.upper()
    if ExchangeName.BINANCE == exchange:
        if market_type in [MarketType.UPERP, MarketType.SPOT, MarketType.MARGIN]:
            return ccy + "USDT"
        elif market_type == MarketType.CPERP:
            return ccy + "USD_PERP"
        elif market_type == MarketType.CDELIVERY:
            return ccy + "USD_250926"
    elif ExchangeName.OKX == exchange:
        if MarketType.SPOT == market_type:
            return ccy + "-USDT"
        elif MarketType.UPERP == market_type:
            return ccy + "-USDT-SWAP"
        elif MarketType.CPERP == market_type:
            return ccy + "-USD-SWAP"
        else:
            return ccy + "-USD"
    elif ExchangeName.BYBIT == exchange:
        if market_type in [MarketType.SPOT, MarketType.UPERP, MarketType.MARGIN]:
            return ccy + "USDT"
        elif market_type == MarketType.CPERP:
            return ccy + "USD"
        elif market_type == MarketType.CDELIVERY:
            return ccy + "USD1226"
    elif ExchangeName.KUCOIN == exchange:
        if MarketType.SPOT == market_type:
            return ccy + "-USDT"
        elif MarketType.UPERP == market_type:
            return ccy + "USDTM"
        elif MarketType.CPERP == market_type:
            return ccy + "USDM"
    elif ExchangeName.GATE == exchange:
        if market_type == MarketType.CPERP:
            return ccy + "_USD"
        else:
            return ccy + "_USDT"
    elif ExchangeName.BITGET == exchange:
        return ccy + "USDT"
    elif ExchangeName.DERIBIT == exchange:
        if market_type == MarketType.SPOT:
            return ccy + "_USDC"
        elif market_type == MarketType.UPERP:
            return ccy + "_USDC-PERPETUAL"
        elif market_type == MarketType.CPERP:
            return ccy + "-PERPETUAL"
    elif ExchangeName.COINEX == exchange:
        if market_type == MarketType.SPOT:
            return ccy + "USDT"
        elif market_type == MarketType.UPERP:
            return ccy + "USDT"
        elif market_type == MarketType.CPERP:
            return ccy + "USD"
    return ""


def get_account(exch: ExchangeName | str, acct_type: AccountType | str) -> str:
    if isinstance(exch, str):
        exch = ExchangeName[exch.upper()]
    if isinstance(acct_type, str):
        acct_type = AccountType[acct_type.upper()]
    exch_account = {
        (ExchangeName.BINANCE, AccountType.NORMAL): "mpbntest01",
        (ExchangeName.BINANCE, AccountType.UNIFIED): "mpbnpmtest153",
        (ExchangeName.BYBIT, AccountType.UNIFIED): "mpbybittest01",
        (ExchangeName.BITGET, AccountType.NORMAL): "bitgetcjtest01",
        (ExchangeName.OKX, AccountType.NORMAL): "mpokextest01",
        (ExchangeName.DERIBIT, AccountType.UNIFIED): "mpderibittest01",
        (ExchangeName.GATE, AccountType.UNIFIED): "gatecjtest01",
        (ExchangeName.COINEX, AccountType.NORMAL): "coinexcjtest01",
    }
    if (exch, acct_type) in exch_account:
        return exch_account[(exch, acct_type)]
    else:
        raise ValueError(f"Account not found for exchange {exch} and account type {acct_type}")


@pytest.fixture
def exch_account():
    return {
        (ExchangeName.BINANCE, AccountType.NORMAL): "mpbntest01",
        (ExchangeName.BINANCE, AccountType.UNIFIED): "mpbnpmtest153",
        (ExchangeName.BYBIT, AccountType.UNIFIED): "mpbybittest01",
        (ExchangeName.BITGET, AccountType.NORMAL): "bitgetcjtest01",
        (ExchangeName.OKX, AccountType.NORMAL): "mpokextest01",
        (ExchangeName.DERIBIT, AccountType.UNIFIED): "mpderibittest01",
        (ExchangeName.GATE, AccountType.UNIFIED): "gatecjtest01",
        (ExchangeName.COINEX, AccountType.NORMAL): "coinexcjtest01",
    }


@pytest.fixture
def supported_exchanges():
    return [
        ExchangeName.BINANCE,
        ExchangeName.OKX,
        ExchangeName.KUCOIN,
        ExchangeName.BYBIT,
        ExchangeName.GATE,
        ExchangeName.BITGET,
        ExchangeName.DERIBIT,
        ExchangeName.COINEX,
    ]


def get_supoorted_markets(exch):
    markets = {
        ExchangeName.BINANCE: [
            MarketType.SPOT,
            MarketType.MARGIN,
            MarketType.UPERP,
            MarketType.CPERP,
            MarketType.CDELIVERY,
        ],
        ExchangeName.BYBIT: [MarketType.SPOT, MarketType.UPERP, MarketType.CPERP, MarketType.CDELIVERY],
        ExchangeName.OKX: [
            MarketType.SPOT,
            MarketType.MARGIN,
            MarketType.UPERP,
            MarketType.CPERP,
            MarketType.UDELIVERY,
            MarketType.CDELIVERY,
        ],
        ExchangeName.BITGET: [MarketType.SPOT, MarketType.UPERP],
        ExchangeName.GATE: [MarketType.SPOT, MarketType.UPERP, MarketType.CPERP],
        ExchangeName.KUCOIN: [MarketType.SPOT, MarketType.UPERP],
        ExchangeName.DERIBIT: [MarketType.SPOT, MarketType.UPERP, MarketType.CPERP],
        ExchangeName.COINEX: [MarketType.SPOT, MarketType.UPERP],
    }
    return markets.get(exch, [])


def is_ms_ts(ts: float) -> bool:
    """判断时间戳是否为毫秒级"""
    return ts > 1e12 and ts < 1e13
