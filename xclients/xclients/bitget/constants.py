from ..enum_type import OrderStatus, TimeInForce, OrderSide

STATUS_MAP = {
    "live": OrderStatus.LIVE,  # not sure `新建订单，orderbook中等待撮合`
    "partially_filled": OrderStatus.PARTIALLY_FILLED,
    "filled": OrderStatus.FILLED,
    "canceled": OrderStatus.CANCELED,  # sp
    "cancelled": OrderStatus.CANCELED,  # lps
}

TIF_MAP = {
    "gtc": TimeInForce.GTC,
    "ioc": TimeInForce.IOC,
    "fok": TimeInForce.FOK,
    "post_only": TimeInForce.GTX,
}

SIDE_MAP = {
    "buy": OrderSide.BUY,
    "sell": OrderSide.SELL,
}

POSMODE_MAP = {
    "hedge_mode": "BOTH",
    "one_way_mode": "ONE_WAY",
}
