import asyncio
from xclients.get_client import get_rest_client
from xclients.enum_type import ExchangeName, MarketType, AccountType


def test_binance():
    cli = get_rest_client(ExchangeName.BINANCE, MarketType.UPERP, AccountType.NORMAL, "4abntest1")
    order1 = asyncio.run(
        cli.raw_request(
            "POST",
            "/fapi/v1/order",
            {
                "symbol": "DOGEUSDT",
                "side": "BUY",
                "type": "LIMIT",
                "quantity": "10",
                "price": "0.0002",
                "timeInForce": "IOC",
            },
            auth=True,
        )
    )
    order2 = asyncio.run(cli.lps_order("DOGEUSDT", "BUY", "LIMIT", quantity=10, price=0.0002, timeInForce="IOC"))
    assert order1 == order2
    comm1 = asyncio.run(cli.get_commission_rate("BTCUSDT"))
    comm2 = asyncio.run(cli.raw_request("GET", "/fapi/v1/commissionRate", {"symbol": "BTCUSDT"}, auth=True))
    assert comm1["takerCommissionRate"] == comm2["takerCommissionRate"]


def test_okx():
    cli = get_rest_client(ExchangeName.OKX, MarketType.CPERP, AccountType.NORMAL, "4aokextest1")

    balance1 = asyncio.run(cli.raw_request("GET", "/api/v5/account/balance", {"ccy": "USDT"}, auth=True))
    balance2 = asyncio.run(cli.get_balance("USDT"))
    assert balance1["data"][0]["details"][0]["ccy"] == balance2["data"][0]["details"][0]["ccy"]

    funding1 = asyncio.run(cli.raw_request("GET", "/api/v5/public/funding-rate", {"instId": "BTC-USD-SWAP"}))
    funding2 = asyncio.run(cli.get_funding_rate("BTC-USD-SWAP"))
    assert funding1 == funding2

    post1 = asyncio.run(cli.raw_request("POST", "/api/v5/trade/cancel-order", {"instId": "BTC-USD-SWAP", "clOrdId": "233"}, auth=True))
    post2 = asyncio.run(cli.cancel_order("BTC-USD-SWAP", cl_ord_id="233"))
    assert post1 == post2


def test_bybit():
    cli = get_rest_client(ExchangeName.BYBIT, MarketType.CPERP, AccountType.UNIFIED, "4abybittest1")

    kline1 = asyncio.run(cli.get_market_kline("linear", "BTCPERP", "1", limit=1))
    kline2 = asyncio.run(cli.raw_request("GET", "/v5/market/kline", {"category": "linear", "symbol": "BTCPERP", "interval": "1", "limit": 1}))
    assert kline1["result"]["list"] == kline2["result"]["list"]

    balance1 = asyncio.run(cli.get_balance("UNIFIED", "USDT"))
    balance2 = asyncio.run(cli.raw_request("GET", "/v5/account/wallet-balance", {"accountType": "UNIFIED", "coin": "USDT"}, auth=True))
    assert balance1["result"]["list"] == balance2["result"]["list"]

    order1 = asyncio.run(
        cli.raw_request(
            "POST",
            "/v5/order/create",
            {"category": "linear", "symbol": "BTCPERP", "side": "Buy", "orderType": "Limit", "qty": "1", "price": "0"},
            auth=True,
        )
    )
    order2 = asyncio.run(cli.v5_order_create("linear", "BTCPERP", "Buy", "Limit", "1", "0"))
    assert order1["retMsg"] == order2["retMsg"]


def test_gate():
    cli = get_rest_client(ExchangeName.GATE, MarketType.SPOT, AccountType.NORMAL, "4agatetest1")

    market1 = asyncio.run(cli.raw_request("GET", "/spot/tickers", {"currency_pair": "BTC_USDT"}))
    market2 = asyncio.run(cli.get_market(currency_pair="BTC_USDT"))
    assert market1[0]["high_24h"] == market2[0]["high_24h"]

    open_order1 = asyncio.run(cli.raw_request("GET", "/spot/open_orders", {"limit": 1}, auth=True))
    open_order2 = asyncio.run(cli.get_open_orders(limit=1))
    assert open_order1 == open_order2

    cli = get_rest_client(ExchangeName.GATE, MarketType.UPERP, AccountType.NORMAL, "4agatetest1")
    order1 = asyncio.run(
        cli.raw_request(
            "POST",
            "/futures/usdt/orders",
            {"contract": "BTC_USDT", "size": 1, "price": "1", "text": "t-233", "tif": "gtc"},
            auth=True,
        )
    )
    order2 = asyncio.run(cli.place_order("t-233", "buy", "BTC_USDT", price="1", size=1, time_in_force="gtc"))
    assert order1 == order2


def test_kucoin():
    cli = get_rest_client(ExchangeName.KUCOIN, MarketType.SPOT, AccountType.NORMAL, "4akucoin")
    currency1 = asyncio.run(cli.get_spot_currency())
    currency2 = asyncio.run(cli.raw_request("GET", "/api/v1/currencies"))
    assert currency1["data"][0] == currency2["data"][0]

    account1 = asyncio.run(cli.get_account("USDT"))
    account2 = asyncio.run(cli.raw_request("GET", "/api/v1/accounts/", {"currency": "USDT"}, auth=True))
    print(account1, account2)
    if account1["data"]:
        assert account1["data"][0] == account2["data"][0]

    cli = get_rest_client(ExchangeName.KUCOIN, MarketType.UPERP, AccountType.NORMAL, "4akucointest1")
    cancel1 = asyncio.run(cli.cancel_order("233"))
    cancel2 = asyncio.run(cli.raw_request("POST", "/api/v1/orders/233", auth=True))
    assert cancel1 == cancel2
