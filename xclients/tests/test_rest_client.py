import asyncio
from xclients.get_client import get_rest_client
from xclients.enum_type import ExchangeName, MarketType, AccountType
import pytest
import time


@pytest.mark.asyncio
async def test_binance():
    r = get_rest_client(ExchangeName.BINANCE, MarketType.SPOT, AccountType.NORMAL, "4abntest1")
    assert await r.get_exchange_info()
    assert await r.get_price("ETHUSDT")
    assert await r.get_cross_margin_pair()
