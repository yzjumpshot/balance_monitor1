from enum import Enum, Flag, auto
from typing import TYPE_CHECKING, Optional, Literal

if TYPE_CHECKING:
    from .inst_mngr import Instrument


class AccountType(Enum):
    NORMAL = "NORMAL"  # 普通账户
    UNIFIED = "UNIFIED"  # 统一账户
    CLASSIC_UNIFIED = "CLASSIC_UNIFIED"  # 币安经典统一账户
    FUND = "FUND"  # 资金账户
    HFT = "HFT"  # 高频交易账户
    UNKNOWN = "UNKNOWN"

    def __str__(self):
        return self.name


class MarketType(Enum):
    UNKNOWN = "UNKNOWN"
    SPOT = "SPOT"
    MARGIN = "MARGIN"
    UPERP = "UPERP"
    CPERP = "CPERP"
    UDELIVERY = "UDELIVERY"
    CDELIVERY = "CDELIVERY"
    OPTIONS = "OPTIONS"

    @staticmethod
    def get_by_str(type_str: str) -> "MarketType":
        match type_str:
            case "SPOT" | "SP":
                return MarketType.SPOT
            case "MARGIN":
                return MarketType.MARGIN
            case "UPERP" | "UFUTURES" | "LPS":
                return MarketType.UPERP
            case "CPERP" | "CFUTURES" | "PS":
                return MarketType.CPERP
            case "UDELIVERY" | "LFU":
                return MarketType.UDELIVERY
            case "CDELIVERY" | "FU":
                return MarketType.CDELIVERY
            case _:
                return MarketType.UNKNOWN

    @property
    def quote_market_type(self):
        if self == MarketType.MARGIN:
            return MarketType.SPOT
        return self

    @property
    def is_derivative(self):
        return self in [
            MarketType.UPERP,
            MarketType.CPERP,
            MarketType.UDELIVERY,
            MarketType.CDELIVERY,
            MarketType.OPTIONS,
        ]

    def __str__(self):
        return self.name

    @property
    def orig_name(self):
        match self.name:
            case "UPERP":
                return "UFUTURES"
            case "CPERP":
                return "CFUTURES"
            case _:
                return self.name

    @property
    def ex_name(self):
        match self.name:
            case "UPERP":
                return "LPS"
            case "CPERP":
                return "PS"
            case "UDELIVERY":
                return "LFU"
            case "CDELIVERY":
                return "CFU"
            case "SPOT":
                return "SP"
            case _:
                return self.name


class ExchangeName(Enum):
    UNKNOWN = "UNKNOWN"
    BINANCE = "BINANCE"
    BYBIT = "BYBIT"
    COINBASE = "COINBASE"
    UPBIT = "UPBIT"
    OKX = "OKX"
    BITGET = "BITGET"
    MEXC = "MEXC"
    GATE = "GATE"
    KUCOIN = "KUCOIN"
    BINGX = "BINGX"
    HTX = "HTX"
    KRAKEN = "KRAKEN"
    BITMART = "BITMART"
    LBANK = "LBANK"
    BITSTAMP = "BITSTAMP"
    BITHUMB = "BITHUMB"
    APEX = "APEX"
    DERIBIT = "DERIBIT"
    COINEX = "COINEX"
    HYPERLIQUID = "HYPERLIQUID"
    DRIFT = "DRIFT"
    DYDX = "DYDX"
    MATRIXPORT = "MATRIXPORT"

    @staticmethod
    def get_by_str(type_str: str) -> "ExchangeName":
        return getattr(ExchangeName, type_str, ExchangeName.UNKNOWN)

    @property
    def is_universal(self):
        return self in [ExchangeName.OKX, ExchangeName.BYBIT]  # NOTE: hard coding

    @property
    def auto_borrow_usdt(self):
        if self in [ExchangeName.OKX]:
            return True
        else:
            return False

    @property
    def short_by_quota(self):
        if self in [ExchangeName.OKX]:
            return True
        else:
            return False

    @property
    def need_order_leverage(self):
        return self in [ExchangeName.KUCOIN]

    def __str__(self):
        return self.name


class OrderSide(Enum):
    UNKNOWN = "UNKNOWN"
    BUY = "BUY"
    SELL = "SELL"

    @property
    def offset(self):
        if OrderSide.BUY == self:
            return 1
        elif OrderSide.SELL == self:
            return -1
        else:
            return 0

    @property
    def opposite(self):
        if self == OrderSide.UNKNOWN:
            return OrderSide.UNKNOWN
        return OrderSide.BUY if self == OrderSide.SELL else OrderSide.SELL

    @classmethod
    def valid_sides(cls):
        return [OrderSide.BUY, OrderSide.SELL]

    @property
    def ccxt(self) -> Literal["buy", "sell"]:
        if self == OrderSide.BUY:
            return "buy"
        elif self == OrderSide.SELL:
            return "sell"
        else:
            raise ValueError(f"Unsupported OrderSide: {self}")

    @staticmethod
    def from_ccxt(side_str: str | None):
        if side_str == "buy":
            return OrderSide.BUY
        elif side_str == "sell":
            return OrderSide.SELL
        else:
            return OrderSide.UNKNOWN


class OrderStatus(Enum):
    UNKNOWN = "Unknown"
    PENDING = "Pending"
    LIVE = "Live"
    PARTIALLY_FILLED = "PartiallyFilled"
    CANCELLING = "Cancelling"
    REJECTED = "Rejected"
    FILLED = "Filled"
    CANCELED = "Canceled"
    ORDER_NOT_FOUND = "OrderNotFound"

    def is_open(self):
        return self in {OrderStatus.LIVE, OrderStatus.PARTIALLY_FILLED}

    def is_completed(self):
        return self in {OrderStatus.REJECTED, OrderStatus.FILLED, OrderStatus.CANCELED}

    @staticmethod
    def from_ccxt(status_str: str | None):
        if status_str == "open":
            return OrderStatus.LIVE
        elif status_str == "closed" or status_str == "filled":
            return OrderStatus.FILLED
        elif status_str == "canceled":
            return OrderStatus.CANCELED
        elif status_str == "expired":
            return OrderStatus.CANCELED
        elif status_str == "rejected":
            return OrderStatus.REJECTED
        elif status_str == "pending":
            return OrderStatus.PENDING
        else:
            return OrderStatus.UNKNOWN


class OrderType(Enum):
    UNKNOWN = "Unknown"
    LIMIT = "Limit"
    MARKET = "Market"

    @property
    def ccxt(self) -> Literal["limit", "market"]:
        if self == OrderType.LIMIT:
            return "limit"
        elif self == OrderType.MARKET:
            return "market"
        else:
            raise ValueError(f"Unsupported OrderType: {self}")

    @staticmethod
    def from_ccxt(type_str: str | None):
        if type_str == "limit":
            return OrderType.LIMIT
        elif type_str == "market":
            return OrderType.MARKET
        else:
            return OrderType.UNKNOWN


class TimeInForce(Enum):
    UNKNOWN = "Unknown"
    GTC = "GTC"
    IOC = "IOC"
    FOK = "FOK"
    GTX = "GTX"
    GTD = "GTD"

    @staticmethod
    def get_by_str(type_str: str):
        return getattr(TimeInForce, type_str, None)

    @staticmethod
    def from_ccxt(type_str: str | None):
        if type_str == "GTC":
            return TimeInForce.GTC
        elif type_str == "IOC":
            return TimeInForce.IOC
        elif type_str == "FOK":
            return TimeInForce.FOK
        elif type_str == "PO" or type_str == "post_only":
            return TimeInForce.GTX
        else:
            return TimeInForce.UNKNOWN

    @property
    def ccxt(self) -> Literal["GTC", "IOC", "FOK", "PO", "GTD"]:
        if self == TimeInForce.GTC:
            return "GTC"
        elif self == TimeInForce.IOC:
            return "IOC"
        elif self == TimeInForce.FOK:
            return "FOK"
        elif self == TimeInForce.GTX:
            return "PO"
        elif self == TimeInForce.GTD:
            return "GTD"
        else:
            raise ValueError(f"Unsupported TimeInForce: {self}")


class MarginMode(Enum):
    CROSS = "CROSS"
    ISOLATED = "ISOLATED"


class Event(Enum):
    BOOK = "BOOK"
    FUNDING_RATE = "FUNDING_RATE"
    OPEN_INTEREST = "OPEN_INTEREST"
    TICKER = "TICKER"
    BALANCE = "BALANCE"
    POSITION = "POSITION"
    USER_TRADE = "USER_TRADE"
    ORDER = "ORDER"
    ORDER_SNAPSHOT = "ORDER_SNAPSHOT"
    EXECUTION = "EXECUTION"
    FUNDING_FEE = "FUNDING_FEE"
    LIQUIDATION = "LIQUIDATION"
    PREMIUM_INDEX = "PREMIUM_INDEX"
    KLINE = "KLINE"
    CONNECTED = "CONNECTED"
    DISCONNECTED = "DISCONNECTED"
    INSTRUMENT_UPDATE = "INSTRUMENT_UPDATE"

    def __str__(self):
        return self.name

    def __repr__(self):
        return str(self)


class Interval(Enum):
    _1s = 1
    _1m = 60
    _3m = 180
    _5m = 300
    _15m = 750
    _30m = 1800
    _1h = 3600
    _2h = 7200
    _4h = 14400
    _6h = 21600
    _8h = 28800
    _12h = 43200
    _1d = 86400
    _3d = 259200
    _1w = 604800
    _1M = 2592000


class InstStatus(Enum):
    UNKNOWN = "UNKNOWN"
    TRADING = "TRADING"
    DELISTING = "DELISTING"
    UNTRADABLE = "UNTRADABLE"
    OFFLINE = "OFFLINE"


class ContractType(Enum):
    UNKNOWN = 0
    CQ = 1
    NQ = 2
    CW = 3
    NW = 4
    CM = 5
    NM = 6

    def __str__(self):
        return self.name


class WithdrawStatus(Enum):
    UNKNOWN = 0
    PENDING = 1
    FAIL = 2
    SUCCESS = 3
    CANCELED = 4
    REJECT = 5


# todo: now no use


class RejectedReason(Enum):
    UNKNOWN = "UNKNOWN"
    INSUFFICIENT_BALANCE = "INSUFFICIENT_BALANCE"
    # 交易所特殊限制下单，例如bitget风控限制下单
    EXCHANGE_RESTRICTED = "EXCHANGE_RESTRICTED"
    RATE_LIMIT = "RATE_LIMIT"
    # PostOnly cross
    POC = "POC"
    REDUCE_ONLY = "REDUCE_ONLY"

    @property
    def log_level(self) -> str:
        match self:
            case RejectedReason.UNKNOWN:
                return "warning"
            case RejectedReason.INSUFFICIENT_BALANCE:
                return "warning"
            case RejectedReason.EXCHANGE_RESTRICTED:
                return "warning"
            case RejectedReason.RATE_LIMIT:
                return "warning"
            case RejectedReason.POC:
                return "ignore"
            case RejectedReason.REDUCE_ONLY:
                return "warning"

    def __str__(self) -> str:
        return self.name


class BookType(Enum):
    FIXED = "FIXED"
    DIFF = "DIFF"


class BookUpdateType(Enum):
    SNAPSHOT = "SNAPSHOT"
    DELTA = "DELTA"


class PositionMode(Enum):
    ONE_WAY = "ONE_WAY"
    HEDGE = "HEDGE"


class TransferStatus(Enum):
    UNKNOWN = "UNKNOWN"
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    PROCESSED = "PROCESSED"
    FAILED = "FAILED"


class FlashConvertStatus(Enum):
    UNKNOWN = "UNKNOWN"
    PROCESS = "PROCESS"
    ACCEPT = "ACCEPT"
    SUCCESS = "SUCCESS"
    FAIL = "FAIL"
