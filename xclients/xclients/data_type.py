import time
from collections import deque
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Literal

import numpy as np
from ccxt.base.types import Order as ccxtOrder
from pydantic import BaseModel, Field, model_serializer, model_validator
import time
from .enum_type import *


class AccountConfig(BaseModel):
    api_key: str = ""
    secret_key: str = ""
    passphrase: str = ""
    uid: str = ""
    extra_params: dict[str, Any] = {}

    def has_credentials(self) -> bool:
        return self.api_key != "" and self.secret_key != ""


class WssConfig(BaseModel):
    name: str = ""
    url: str = ""
    bind_ip: str = ""
    topics: list[str] = []
    heartbeat_interval: int = 10  # seconds
    heartbeat_timeout: int = 60  # seconds
    reconnect_interval: int = 1  # seconds
    request_timeout: int = 10  # seconds
    timeout: int = 1 << 60  # inf
    extra_params: dict[str, Any] = {}


class RestConfig(BaseModel):
    name: str = ""
    url: str = ""
    bind_ips: list[str] = []
    timeout: int = 10  # seconds
    tracing: bool = False
    proxy: str | None = None
    extra_params: dict[str, Any] = {}


class AccountMeta(BaseModel):
    exch_name: ExchangeName = ExchangeName.UNKNOWN
    account_type: AccountType = AccountType.UNKNOWN
    market_type: MarketType = MarketType.UNKNOWN
    account_name: str = ""

    def __str__(self):
        if self.account_name:
            return f"{self.exch_name.name}-{self.account_type.name}-{self.market_type.name}-{self.account_name}"
        else:
            return f"{self.exch_name.name}-{self.account_type.name}-{self.market_type.name}"

    def __hash__(self) -> int:
        return hash((self.exch_name, self.account_type, self.market_type, self.account_name))

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, AccountMeta):
            return False
        return (
            self.exch_name == other.exch_name
            and self.account_type == other.account_type
            and self.market_type == other.market_type
            and (self.account_name == other.account_name or not self.account_name or not other.account_name)
        )

    @model_validator(mode="before")
    @classmethod
    def validate_leg_string(cls, value: Any) -> Any:
        if isinstance(value, str):
            splits = value.split("-")
            if len(splits) == 4:
                exch_type_str, account_type_str, market_type_str, account_name_str = splits
            else:
                exch_type_str, account_type_str, market_type_str = splits
                account_name_str = ""
            return {
                "exch_name": exch_type_str,
                "account_type": account_type_str,
                "market_type": market_type_str,
                "account_name": account_name_str,
            }
        return value

    @model_serializer
    def ser_model(self) -> str:
        return str(self)

    @property
    def market(self) -> "MarketMeta":
        return MarketMeta(exch_name=self.exch_name, market_type=self.market_type)


class MarketMeta(BaseModel):
    exch_name: ExchangeName = ExchangeName.UNKNOWN
    market_type: MarketType = MarketType.UNKNOWN

    def __str__(self):
        return f"{self.exch_name.name}-{self.market_type.name}"

    def __hash__(self):
        return hash((self.exch_name, self.market_type))

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, MarketMeta):
            return False
        return self.exch_name == other.exch_name and self.market_type == other.market_type

    @model_validator(mode="before")
    @classmethod
    def validate_string(cls, value: Any) -> Any:
        if isinstance(value, str):
            if "-" in value:
                exch_name_str, market_type_str = value.split("-")
                return {"exch_name": exch_name_str, "market_type": market_type_str}
        return value

    @model_serializer
    def ser_model(self) -> str:
        return str(self)

    @property
    def account(self) -> AccountMeta:
        return AccountMeta(exch_name=self.exch_name, market_type=self.market_type)


@dataclass
class Balance:
    asset: str
    balance: float = 0
    free: float = 0
    borrowed: float = 0
    locked: float = 0
    type: Literal["full", "delta"] = "full"
    ts: int = 0


Balances = dict[str, Balance]


@dataclass
class Position:
    exch_symbol: str
    net_qty: float
    entry_price: float
    value: float
    unrealized_pnl: float
    liq_price: float = 0
    ts: int = 0


Positions = dict[str, Position]


@dataclass
class AccountInfo:
    account: AccountMeta
    equity: float = 0
    margin_balance: float = 0
    available_balance: float = 0
    mmr: float = 999
    imr: float = 999
    ltv: float = 999
    usdt_free: float = 0
    usdt_borrowed: float = 0
    total_position_value: float = 0

    @property
    def mr(self) -> float:
        if self.account.market_type in [MarketType.SPOT, MarketType.MARGIN]:
            return 999
        if self.total_position_value == 0:
            return 999
        return self.margin_balance / self.total_position_value

    @property
    def ur(self) -> float:
        if self.equity == 0:
            return 1
        return self.usdt_free / self.equity

    @property
    def loan_ratio(self) -> float:
        if self.equity == 0:
            return 999
        return self.usdt_borrowed / self.equity

    @property
    def risk_rate(self) -> float:
        if self.usdt_borrowed <= 0:
            return 999
        return self.margin_balance / self.usdt_borrowed

    def __str__(self) -> str:
        return f"USDT Ratio: {self.ur:.4f} MR: {self.mr:.4f} IMR: {self.imr:.4f} MMR: {self.mmr:.4f} LTV: {self.ltv:.4f} Equity: {self.equity:.2f} Margin Balance: {self.margin_balance:.2f} USDT: {self.usdt_free:.2f} Position Value: {self.total_position_value:.2f} Loan Ratio: {self.loan_ratio:.4f}"

    def to_dict(self) -> dict[str, Any]:
        return {
            "USDT Ratio": self.ur,
            "Margin Ratio": self.mr,
            "IMR": self.imr,
            "MMR": self.mmr,
            "LTV": self.ltv,
            "Equity": self.equity,
            "Margin Balance": self.margin_balance,
            "USDT": self.usdt_free,
            "USDT Borrowed": self.usdt_borrowed,
            "Total Position Value": self.total_position_value,
            "Loan Ratio": self.loan_ratio,
            "Risk Rate": self.risk_rate,
        }


@dataclass
class Loan:
    quantity: Decimal = Decimal(0)
    interest: Decimal = Decimal(0)


class LoanData(dict[str, Loan]):
    def __getitem__(self, __key: str) -> Loan:
        if __key in self:
            return super().__getitem__(__key)
        else:
            return Loan()


@dataclass
class FundingRate:
    funding_rate: float
    funding_ts: float
    interval_hour: int = 8
    fr_cap: float = np.nan
    fr_floor: float = np.nan

    def to_dict(self):
        return {
            "funding_rate": self.funding_rate,
            "funding_ts": self.funding_ts,
            "interval_hour": self.interval_hour,
            "fr_cap": self.fr_cap,
            "fr_floor": self.fr_floor,
        }

    def __hash__(self):
        return hash(self.funding_ts)

    def __eq__(self, other):
        if isinstance(other, FundingRate):
            return self.funding_ts == other.funding_ts
        return False

    def __lt__(self, other):
        if isinstance(other, FundingRate):
            return self.funding_ts < other.funding_ts
        return NotImplemented


FundingRatesCur = dict[str, FundingRate]


@dataclass
class FundingRateSimple:
    funding_rate: float
    funding_ts: float
    interval_hour: int = 0

    def to_dict(self):
        return {"funding_rate": self.funding_rate, "funding_ts": self.funding_ts, "interval_hour": self.interval_hour}

    def __hash__(self):
        return hash(self.funding_ts)

    def __eq__(self, other):
        if isinstance(other, FundingRateSimple):
            return self.funding_ts == other.funding_ts
        return False

    def __lt__(self, other):
        if isinstance(other, FundingRateSimple):
            return self.funding_ts < other.funding_ts
        return NotImplemented


FundingRatesSimple = dict[str, FundingRateSimple]
FundingRatesHis = dict[str, list[FundingRateSimple]]


@dataclass
class CommissionRate:
    maker: Decimal = Decimal(0)
    taker: Decimal = Decimal(0)


@dataclass
class Trade:
    create_ts: int
    fill_ts: int
    side: OrderSide
    trade_id: str
    order_id: str
    last_trd_price: Decimal
    last_trd_volume: Decimal
    turnover: Decimal
    fee: Decimal
    fee_ccy: str
    is_maker: bool


class TradeData(dict[str, list[Trade]]):
    pass


class OrderSnapshot(BaseModel):
    exch_symbol: str = ""

    price: Decimal = Decimal(0)
    qty: Decimal = Decimal(0)
    avg_price: float = 0
    filled_qty: Decimal = Decimal(0)
    left_qty: Decimal = Decimal(0)

    fee: float = 0
    fee_ccy: str = ""

    order_side: OrderSide = OrderSide.UNKNOWN
    order_time_in_force: TimeInForce = TimeInForce.UNKNOWN
    order_type: OrderType = OrderType.LIMIT
    order_status: OrderStatus = OrderStatus.PENDING

    reduce_only: bool = False

    order_id: str = ""
    client_order_id: str = ""

    rejected_reason: RejectedReason = RejectedReason.UNKNOWN
    rejected_message: str = ""
    exch_update_ts: float = 0
    local_update_ts: float = 0
    place_ack_ts: float = 0

    @staticmethod
    def from_ccxt_order(order: ccxtOrder, exch_symbol: str = "") -> "OrderSnapshot":
        order_snapshot = OrderSnapshot(
            exch_symbol=exch_symbol or order["symbol"] or "",
            price=Decimal(str(order["price"])) if order["price"] else Decimal(0),
            qty=Decimal(str(order["amount"])) if order["amount"] else Decimal(0),
            avg_price=order["average"] if isinstance(order["average"], float) else 0,  # TODO: check if this is correct
            filled_qty=Decimal(str(order.get("filled", 0))) if order.get("filled", 0) else Decimal(0),
            order_side=OrderSide.from_ccxt(order["side"]),
            order_time_in_force=TimeInForce.from_ccxt(order["timeInForce"]),
            order_type=OrderType.from_ccxt(order["type"]),
            order_status=OrderStatus.from_ccxt(order["status"]),
            reduce_only=order["reduceOnly"] or False,
            order_id=order["id"] or "",
            client_order_id=order["clientOrderId"] or "",
            exch_update_ts=order["lastTradeTimestamp"] if order["lastTradeTimestamp"] else 0,
            local_update_ts=int(time.time() * 1000),
            place_ack_ts=order["timestamp"] if order["timestamp"] else 0,
        )
        if order["fee"] and order["fee"]["cost"]:
            order_snapshot.fee = float(order["fee"]["cost"])
            if order["fee"]["currency"]:
                order_snapshot.fee_ccy = order["fee"]["currency"]
        return order_snapshot


class PlaceOrderInstruction(BaseModel):
    exch_symbol: str = ""

    order_type: OrderType = OrderType.LIMIT
    order_side: OrderSide = OrderSide.UNKNOWN
    order_time_in_force: TimeInForce = TimeInForce.UNKNOWN

    price: Decimal = Decimal(0)
    qty: Decimal = Decimal(0)

    take_profit_price: Decimal = Decimal(0)
    stop_loss_price: Decimal = Decimal(0)

    reduce_only: bool = False

    # auto fields
    client_order_id: str = ""

    extras: dict[str, Any] = Field(default_factory=dict)


class CancelOrderInstruction(BaseModel):
    exch_symbol: str = ""

    client_order_id: str = ""
    order_id: str = ""


class SyncOrderInstruction(BaseModel):
    exch_symbol: str = ""

    client_order_id: str = ""
    order_id: str = ""


# class OrderDetailData(dict[str, list[OrderDetail]]):
#     pass


class OrderSnapshotData(dict[str, list[OrderSnapshot]]):
    pass


@dataclass
class OrderResponse:
    order_id: str


@dataclass
class TransferResponse:
    apply_id: str


@dataclass
class WithdrawResponse:
    order_id: str
    status: WithdrawStatus


@dataclass
class FundingFee:
    pnl: Decimal
    ts: int


class FundingFeeData(dict[str, list[FundingFee]]):
    pass


@dataclass
class DiscountRate:
    min_amt: int
    discount_rate: Decimal


class DiscountRateData(list[DiscountRate]):
    pass


@dataclass
class KLine:
    start_ts: int
    open: Decimal
    close: Decimal
    high: Decimal
    low: Decimal
    volume: Decimal = Decimal(0)  # 成交量
    turnover: Decimal = Decimal(0)  # 成交额


class KLineData(list[KLine]):
    pass


@dataclass
class Leverage:
    long: Decimal = Decimal(0)
    short: Decimal = Decimal(0)


@dataclass
class MaxOpenQty:
    buy: Decimal = Decimal(0)
    sell: Decimal = Decimal(0)


@dataclass
class MaxOpenNotional:
    buy: Decimal = Decimal(0)
    sell: Decimal = Decimal(0)


@dataclass
class InterestRate:
    asset: str
    days: int = -1  # -1 is flex
    vip_level: str = "UNKNOWN"
    ir: Decimal = Decimal(-1)  # daily rate
    available_qty: Decimal = Decimal(-1)
    minimum_qty: Decimal = Decimal(-1)
    ts: float = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "asset": self.asset,
            "days": self.days,
            "vip_level": self.vip_level,
            "ir": self.ir,
            "available_qty": self.available_qty,
            "minimum_qty": self.minimum_qty,
            "ts": self.ts,
        }


InterestRates = list[InterestRate]


@dataclass
class LongShortRatio:
    long_short_ratio: Decimal
    ts: int = 0


class LongShortRatioData(list[LongShortRatio]):
    pass


Prices = dict[str, float]  # key: exch_symbol, value: price


@dataclass
class CollateralRatio:
    asset: str
    cr: dict[float, float]


CollateralRatios = list[CollateralRatio]


class OrderBook:
    def __init__(
        self,
        exch_symbol: str,
    ):
        self.exch_symbol = exch_symbol
        self.bids: deque[tuple[Decimal, Decimal]] = deque()  # 从大到小
        self.asks: deque[tuple[Decimal, Decimal]] = deque()  # 从小到大
        self.exch_seq = 0
        self.exch_ts = 0  # ms
        self.recv_ts = 0  # ms
        self.stream = ""
        self.book_type: BookType = BookType.FIXED
        self.book_update_type: BookUpdateType = BookUpdateType.SNAPSHOT

    def get_depth(self, side: OrderSide, price_multiplier: int = 1, qty_multiplier: Decimal = Decimal(1)):
        if side is OrderSide.BUY:
            depth = self.bids
        else:
            depth = self.asks
        if price_multiplier != 1 or qty_multiplier != Decimal(1):
            depth = deque(
                [
                    (
                        i[0] / price_multiplier,
                        i[1] * price_multiplier * qty_multiplier,
                    )
                    for i in depth
                ]
            )
        return depth

    def get_price(self, side: OrderSide, price_multiplier: int = 1):
        try:
            if side is OrderSide.BUY:
                return self.bids[0][0] * price_multiplier
            else:
                return self.asks[0][0] * price_multiplier
        except Exception:
            return Decimal("nan")

    def get_price_qty(self, side: OrderSide, price_multiplier: int = 1, qty_multiplier: Decimal = Decimal(1)):
        try:
            if side is OrderSide.BUY:
                price_qty = self.bids[0]
            else:
                price_qty = self.asks[0]
        except Exception:
            price_qty = (Decimal("nan"), Decimal(0))

        if price_multiplier != 1 or qty_multiplier != Decimal(1):
            price_qty = (
                price_qty[0] / price_multiplier,
                price_qty[1] * price_multiplier * qty_multiplier,
            )

        return price_qty

    def __str__(self):
        return f"OrderBook(exch_symbol={self.exch_symbol}, bids={list(self.bids)}, asks={list(self.asks)}, exch_seq={self.exch_seq}, exch_ts={self.exch_ts}, recv_ts={self.recv_ts}, stream={self.stream}, book_type={self.book_type.name}, book_update_type={self.book_update_type.name})"


@dataclass
class Ticker:
    exch_symbol: str
    bid: float = np.nan
    ask: float = np.nan
    index_price: float = np.nan
    ts: float = 0
    update_ts: float = 0
    fr: float = np.nan
    fr_ts: float = 0
    bid_qty: float = np.nan
    ask_qty: float = np.nan

    @property
    def mpx(self) -> float:
        return (self.bid + self.ask) / 2

    def to_dict(self) -> dict[str, Any]:
        return {
            "exch_symbol": self.exch_symbol,
            "bpx": self.bid,
            "apx": self.ask,
            "bqty": self.bid_qty,
            "aqty": self.ask_qty,
            "ipx": self.index_price,
            "ts": self.ts,
            "update_ts": self.update_ts,
            "fr": self.fr,
            "fr_ts": self.fr_ts,
        }

    def __str__(self):
        return f"Ticker(exch_symbol={self.exch_symbol}, bid={self.bid}, ask={self.ask}, index_price={self.index_price}, ts={self.ts}, update_ts={self.update_ts}, fr={self.fr}, fr_ts={self.fr_ts}, bid_qty={self.bid_qty}, ask_qty={self.ask_qty})"


Tickers = dict[str, Ticker]


@dataclass
class Quotation:
    exch_symbol: str
    bid: float = np.nan
    ask: float = np.nan
    ts: float = 0
    update_ts: float = 0
    bid_qty: float = np.nan
    ask_qty: float = np.nan

    @property
    def mpx(self) -> float:
        return (self.bid + self.ask) / 2

    def to_dict(self) -> dict[str, Any]:
        return {
            "exch_symbol": self.exch_symbol,
            "bpx": self.bid,
            "apx": self.ask,
            "bqty": self.bid_qty,
            "aqty": self.ask_qty,
            "ts": self.ts,
            "update_ts": self.update_ts,
        }

    def __str__(self):
        return f"Quotation(exch_symbol={self.exch_symbol}, bid={self.bid}, ask={self.ask}, ts={self.ts}, update_ts={self.update_ts}, bid_qty={self.bid_qty}, ask_qty={self.ask_qty})"


Quotations = dict[str, Quotation]


@dataclass
class Fundamental:
    exch_symbol: str
    price_change_24h: float = np.nan
    turnover_24h: float = np.nan
    open_interest: float = np.nan
    long_short_ratio: float = np.nan

    def to_dict(self) -> dict[str, Any]:
        return {
            "exch_symbol": self.exch_symbol,
            "price_change_24h": self.price_change_24h,
            "turnover_24h": self.turnover_24h,
            "open_interest": self.open_interest,
            "long_short_ratio": self.long_short_ratio,
        }

    def __str__(self):
        return (
            f"Fundamental(exch_symbol={self.exch_symbol}, "
            f"price_change_24h={self.price_change_24h}, "
            f"turnover_24h={self.turnover_24h}, "
            f"open_interest={self.open_interest}, "
            f"long_short_ratio={self.long_short_ratio})"
        )


Fundamentals = dict[str, Fundamental]


@dataclass
class Kline:
    exch_symbol: str
    interval: str
    start_ts: float = np.nan
    open: float = np.nan
    close: float = np.nan
    high: float = np.nan
    low: float = np.nan
    volume: float = np.nan
    turnover: float = np.nan
    taker_buy_base_asset_volume: float = np.nan
    taker_buy_quote_asset_volume: float = np.nan
    trade_num: float = np.nan
    ts: float = np.nan  # local recv ts
    confirm: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "exch_symbol": self.exch_symbol,
            "interval": self.interval,
            "start_ts": self.start_ts,
            "open": self.open,
            "close": self.close,
            "high": self.high,
            "low": self.low,
            "volume": self.volume,
            "turnover": self.turnover,
            "taker_buy_base_asset_volume": self.taker_buy_base_asset_volume,
            "taker_buy_quote_asset_volume": self.taker_buy_quote_asset_volume,
            "trade_num": self.trade_num,
            "ts": self.ts,
            "confirm": self.confirm,
        }

    def __str__(self):
        return (
            f"Kline(exch_symbol={self.exch_symbol}, interval={self.interval}, "
            f"start_ts={self.start_ts}, open={self.open}, close={self.close}, "
            f"high={self.high}, low={self.low}, volume={self.volume}, "
            f"turnover={self.turnover}, taker_buy_base_asset_volume={self.taker_buy_base_asset_volume}, "
            f"taker_buy_quote_asset_volume={self.taker_buy_quote_asset_volume}, trade_num={self.trade_num}, "
            f"ts={self.ts}, confirm={self.confirm})"
        )


@dataclass
class PremiumIndex:
    exch_symbol: str
    point_cnt: float = np.nan
    latest: float = np.nan
    weighted_sum: float = np.nan
    weighted_idx: float = np.nan
    ts: float = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "exch_symbol": self.exch_symbol,
            "point_cnt": self.point_cnt,
            "latest": self.latest,
            "weighted_sum": self.weighted_sum,
            "weighted_idx": self.weighted_idx,
            "ts": self.ts,
        }


@dataclass
class LiquidationMessage:
    exch_symbol: str
    qty: Decimal


# @dataclass
# class FundingFee:
#     exch_symbol: str
#     fee: float
#     ts: int


# FundingFees = list[FundingFee]
