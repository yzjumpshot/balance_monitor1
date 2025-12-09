from ..enum_type import TimeInForce

TIF_MAP = {
    "gtc": TimeInForce.GTC,
    "ioc": TimeInForce.IOC,
    "poc": TimeInForce.GTX,
    "fok": TimeInForce.FOK,
}
