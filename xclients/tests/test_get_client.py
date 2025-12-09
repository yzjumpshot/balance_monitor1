import asyncio
from xclients.get_client import get_rest_client, get_ws_client
from xclients.enum_type import ExchangeName, MarketType, AccountType
import pytest


@pytest.mark.asyncio
async def test_binance():
    cli = get_rest_client(ExchangeName.BINANCE, MarketType.UPERP, AccountType.NORMAL, "4abntest1")
    data = await cli.get_commission_rate("BTCUSDT")
    print(data)

    cli = get_rest_client(ExchangeName.BINANCE, MarketType.SPOT, AccountType.NORMAL)
    data = await cli.get_price("BTCUSDT")
    print(data)

    cli = get_ws_client(ExchangeName.BINANCE, MarketType.SPOT)


@pytest.mark.asyncio
async def test_okx():
    cli = get_rest_client(ExchangeName.OKX, MarketType.UPERP, AccountType.NORMAL)
    data = await cli.get_funding_rate("BTC-USD-SWAP")
    print(data)
    cli = get_rest_client(ExchangeName.OKX, MarketType.SPOT, AccountType.NORMAL, "4aokextest1")
    data = await cli.get_balance("USDT")
    print(data)

    cli = get_ws_client(ExchangeName.OKX, MarketType.SPOT)


@pytest.mark.asyncio
async def test_kucoin():
    cli = get_rest_client(ExchangeName.KUCOIN, MarketType.SPOT, AccountType.NORMAL, "4akcliq01")
    # data = asyncio.run(cli.get_spot_instrument_info())
    # print(data)
    import time

    data = await cli.inner_transfer(str(int(time.time())), "USDT", "1", "trade", "main")
    print(data)
    # cli = get_rest_client(ExchangeName.KUCOIN, MarketType.UPERP, AccountType.NORMAL, "4akucointest1")
    # data = asyncio.run(cli.transfer_in("USDT", "1000000000000", "MAIN"))
    # print(data)
    # data = asyncio.run(cli.transfer_out("USDT", "100000000000", "MAIN"))
    # print(data)
    # data = asyncio.run(cli.fetch_positions())
    # print(data)
    # data = asyncio.run(cli.get_swap_market("ETHUSDTM"))
    # print(data)

    cli = get_ws_client(ExchangeName.KUCOIN, MarketType.SPOT)
    cli = get_ws_client(ExchangeName.KUCOIN, MarketType.UPERP)


@pytest.mark.asyncio
async def test_bybit():
    cli = get_rest_client(ExchangeName.BYBIT, MarketType.SPOT, AccountType.NORMAL)
    data = await cli.get_cross_margin_borrow_info()
    print(data)
    cli = get_rest_client(ExchangeName.BYBIT, MarketType.SPOT, AccountType.NORMAL, "4abybittest1")
    data = await cli.get_balance("CONTRACT")
    print(data)

    cli = get_ws_client(ExchangeName.BYBIT, MarketType.SPOT)
    cli = get_ws_client(ExchangeName.BYBIT, MarketType.UPERP)


@pytest.mark.asyncio
async def test_gate():
    cli = get_rest_client(ExchangeName.GATE, MarketType.SPOT, AccountType.NORMAL)
    data = await cli.get_exchange_info()
    print(data)
    cli = get_rest_client(ExchangeName.GATE, MarketType.UPERP, AccountType.NORMAL, "4agatetest1")
    data = await cli.get_account_book()
    print(data)

    cli = get_ws_client(ExchangeName.GATE, MarketType.SPOT)
    cli = get_ws_client(ExchangeName.GATE, MarketType.UPERP)


def test_get_rest_clients():
    for e_str, e in ExchangeName.__members__.items():
        for i_str, i in MarketType.__members__.items():
            try:
                r = get_rest_client(e_str, i_str, account_type=AccountType.NORMAL)
            except Exception as err:
                print(e, i, err)
            else:
                print(e, i, r, type(r))


def test_get_ws_clients():
    for e_str, e in ExchangeName.__members__.items():
        for m_str, i in MarketType.__members__.items():
            try:
                r = get_ws_client(e_str, m_str)
            except Exception as err:
                print(e, i, err)
            else:
                print(e, i, r, type(r))
