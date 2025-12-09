from ..enum_type import OrderStatus, TimeInForce


STATUS_MAP = {
    "Created": OrderStatus.PENDING,
    "New": OrderStatus.LIVE,
    "Rejected": OrderStatus.REJECTED,
    "PartiallyFilled": OrderStatus.PARTIALLY_FILLED,
    "PartiallyFilledCanceled": OrderStatus.CANCELED,
    "Filled": OrderStatus.FILLED,
    "Cancelled": OrderStatus.CANCELED,
}

TIF_MAP = {
    "GTC": TimeInForce.GTC,
    "IOC": TimeInForce.IOC,
    "FOK": TimeInForce.FOK,
    "PostOnly": TimeInForce.GTX,
}

INTERVAL_MAP = {
    "1m": "1",
    "3m": "3",
    "5m": "5",
    "15m": "15",
    "30m": "30",
    "1h": "60",
    "2h": "120",
    "4h": "240",
    "6h": "360",
    "12h": "720",
    "1d": "D",
    "1w": "W",
    "1M": "M",
}

INVERT_INTERVAL_MAP = {v: k for k, v in INTERVAL_MAP.items()}
