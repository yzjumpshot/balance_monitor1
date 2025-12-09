from ..enum_type import OrderStatus, TimeInForce

STATUS_MAP = {
    "open": OrderStatus.LIVE,
    "rejected": OrderStatus.REJECTED,
    "filled": OrderStatus.FILLED,
    "cancelled": OrderStatus.CANCELED,
}
TIF_MAP = {
    "good_til_cancelled": TimeInForce.GTC,
    "good_til_day": TimeInForce.GTD,
    "fill_or_kill": TimeInForce.FOK,
    "immediate_or_cancel": TimeInForce.IOC,
}
