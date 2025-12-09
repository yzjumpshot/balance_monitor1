from ..enum_type import OrderStatus, TimeInForce

STATUS_MAP = {
    "NEW": OrderStatus.LIVE,
    "PARTIALLY_FILLED": OrderStatus.PARTIALLY_FILLED,
    "FILLED": OrderStatus.FILLED,
    "CANCELED": OrderStatus.CANCELED,
    "EXPIRED": OrderStatus.CANCELED,
    "NEW_INSURANCE": OrderStatus.UNKNOWN,
    "NEW_ADL": OrderStatus.UNKNOWN,
}

TIF_MAP = {
    "FOK": TimeInForce.FOK,
    "GTC": TimeInForce.GTC,
    "IOC": TimeInForce.IOC,
    "GTX": TimeInForce.GTX,
}
