from xclients.get_wrapper import get_rest_wrapper
from xclients.enum_type import (
    MarketType,
    ExchangeName,
    AccountType,
    Interval,
    OrderSide,
    TimeInForce,
    MarginMode,
    OrderType,
)
import pytest
from decimal import Decimal
import time
from tests.test_utils import exch_account


async def sp_basic_test(test_exch, test_account, acct_type, test_symbol):
    rest_wrapper = get_rest_wrapper(test_exch, MarketType.SPOT, acct_type, test_account)
    asset = await rest_wrapper.get_assets()
    print("\nspot direct", asset)
    assert asset["status"] == 0, asset
    # asset = await rest_wrapper.get_assets(from_redis=True)
    # print("\nspot redis", asset)
    # assert asset["status"] == 0, asset
    price = await rest_wrapper.get_price(test_symbol)
    print("\nprice", price)
    open_orders = await rest_wrapper.ccxt_sync_open_orders(test_symbol)
    print("\nopen orders", open_orders)
    end_time = int(time.time() * 1000)
    start_time = end_time - 24 * 60 * 60 * 1000
    trade_history = await rest_wrapper.get_trade_history(start_time, end_time, [test_symbol])
    print("trade history: ", trade_history)

    order_history = await rest_wrapper.get_order_history(start_time, end_time, [test_symbol])
    print("order history: ", order_history)


async def margin_basic_test(test_exch, test_account, acct_type, test_symbol):
    rest_wrapper = get_rest_wrapper(test_exch, MarketType.MARGIN, acct_type, test_account)
    asset = await rest_wrapper.get_assets()
    print("\nmargin direct", asset)
    assert asset["status"] == 0, asset
    # asset = await rest_wrapper.get_assets(from_redis=True)
    # print("\nmargin redis", asset)
    # assert asset["status"] == 0, asset
    repay = await rest_wrapper.repay("USDT", Decimal("1"))
    print("repay: ", repay)
    end_time = int(time.time() * 1000)
    start_time = end_time - 24 * 60 * 60 * 1000
    trade_history = await rest_wrapper.get_trade_history(start_time, end_time, [test_symbol])
    print("trade history: ", trade_history)


async def lps_basic_test(test_exch, test_account, acct_type, test_symbol):
    rest_wrapper = get_rest_wrapper(test_exch, MarketType.UPERP, acct_type, test_account)
    asset = await rest_wrapper.get_assets()
    print("\nlps direct asset: ", asset)
    assert asset["status"] == 0, asset
    positions = await rest_wrapper.get_positions()
    print("\nlps direct position: ", positions)
    assert positions["status"] == 0
    price = await rest_wrapper.get_price(test_symbol)
    print("\nprice", price)
    open_orders = await rest_wrapper.ccxt_sync_open_orders(test_symbol)
    print("\nopen orders", open_orders)
    assert price["status"] == 0

    # asset = await rest_wrapper.get_assets(from_redis=True)
    # print("\nlps redis asset", asset)
    # assert asset["status"] == 0, asset
    # positions = await rest_wrapper.get_positions(from_redis=True)
    # print("\nlps redis positon", positions)
    # assert positions["status"] == 0
    leverage = await rest_wrapper.set_symbol_leverage(test_symbol, 2)
    print(leverage)
    risk = await rest_wrapper.set_swap_risk_limit(test_symbol, 1)
    print(risk)

    end_time = int(time.time() * 1000)
    start_time = end_time - 24 * 60 * 60 * 1000
    trade_history = await rest_wrapper.get_trade_history(start_time, end_time, [test_symbol])
    print(trade_history)

    funding_fee = await rest_wrapper.get_funding_fee(look_back=5, symbol_list=["ETHUSDTM"])
    print("funding_fee", funding_fee)


async def ps_basic_test(test_exch, test_account, acct_type, test_symbol):
    rest_wrapper = get_rest_wrapper(test_exch, MarketType.CPERP, acct_type, test_account)
    # 获取assets、获取positions、获取价格、下单、撤单、查历史订单、查历史成交
    # 1.positions： 会同时返回PS,FU的position
    if test_exch in [ExchangeName.BYBIT, ExchangeName.GATE]:
        print(f"\n ps positions redis --> {test_exch} have no redis positions, pass it!")
    # else:
    #     positions_redis = await rest_wrapper.get_positions(from_redis=True)
    #     print("\n ps positions redis", positions_redis)
    positions_api = await rest_wrapper.get_positions(from_redis=False)
    print("\n ps positions api", positions_api)

    # 2.assets
    if test_exch in [ExchangeName.BYBIT, ExchangeName.GATE]:
        print(f"\n ps assets redis --> {test_exch} have no redis assets, pass it!")
    # else:
    #     assets_redis = await rest_wrapper.get_assets(from_redis=True)
    #     print("\n ps assets redis", assets_redis)
    assets_api = await rest_wrapper.get_assets(from_redis=False)
    print("\n ps assets api", assets_api)

    # 3.get_price： CPERP, CDELIVERY redis是不同的key: BTC_USD|CPERP|BINANCE, BTC_USD_NQ|CDELIVERY|BINANCE
    if test_exch in [ExchangeName.BYBIT, ExchangeName.GATE]:
        print(f"\n ps price redis --> {test_exch} have no redis price, pass it!")
    # else:
    #     price_redis = await rest_wrapper.get_price(symbol=test_symbol, from_redis=True)
    #     print("\n ps price redis", price_redis)
    price_api = await rest_wrapper.get_price(symbol=test_symbol)
    print("\n ps price api", price_api)

    # 4.place_order 下单  在async def test_order统一测试， 新增了FU的支持，它和PS用的一套 TODO 测试FU
    # 5.cancel_order 撤单 在async def test_order统一测试 只有margin, lps, ps(fu与ps是同一个api) 没有spot
    await test_order_ps_fu(
        {(test_exch, acct_type): test_account},
        [MarketType.CPERP],
        ccy=(test_symbol[: test_symbol.find("USD")]).replace("_", "").replace("-", ""),
    )
    # await test_real_trade_ps_fu({test_exch: test_account}, [MarketType.SPOT], ccy='ETH')

    # 6. 查询历史成交 get_trade_history
    end_time = int(time.time() * 1000)
    start_time = end_time - 24 * 60 * 60 * 1000
    trade_history = await rest_wrapper.get_trade_history(start_time, end_time, [test_symbol])
    print("\n ps trade_history api", trade_history)

    # 7.查历史订单
    order_history = await rest_wrapper.get_order_history(start_time, end_time, [test_symbol])
    print("\n ps order_history api", order_history)


async def fu_basic_test(test_exch, test_account, acct_type, test_symbol):
    rest_wrapper = get_rest_wrapper(test_exch, MarketType.CDELIVERY, acct_type, test_account)

    if test_exch in [ExchangeName.BYBIT]:
        print(f"\n fu positions redis --> {test_exch} have no redis positions, pass it!")
    # else:
    #     positions_redis = await rest_wrapper.get_positions(from_redis=True)
    #     print("\n fu positions redis", positions_redis)
    positions_api = await rest_wrapper.get_positions(from_redis=False)
    print("\n fu positions api", positions_api)

    # 2.assets
    if test_exch in [ExchangeName.BYBIT]:
        print(f"\n fu assets redis --> {test_exch} have no redis assets, pass it!")
    # else:
    #     assets_redis = await rest_wrapper.get_assets(from_redis=True)
    #     print("\n fu assets redis", assets_redis)
    assets_api = await rest_wrapper.get_assets(from_redis=False)
    print("\n fu assets api", assets_api)

    # 3.price
    if test_exch in [ExchangeName.BYBIT]:
        print(f"\n fu price redis --> {test_exch} have no redis price, pass it!")
    # else:
    #     price_redis = await rest_wrapper.get_price(symbol=test_symbol, from_redis=True)
    #     print("\n fu price redis", price_redis)
    price_api = await rest_wrapper.get_price(symbol=test_symbol)
    print("\n fu price api", price_api)

    end_time = int(time.time() * 1000)
    start_time = end_time - 24 * 60 * 60 * 1000
    trade_history = await rest_wrapper.get_trade_history(start_time, end_time, [test_symbol])
    print("\n fu trade_history api", trade_history)

    order_history = await rest_wrapper.get_order_history(start_time, end_time, [test_symbol])
    print("\n fu order_history api", order_history)

    # set_leveage = await rest_wrapper.set_ps_leverage(symbol="ETHUSDU24", leverage=10)
    # print("\n fu set_leveage api", set_leveage)

    await test_order_ps_fu(
        {(test_exch, acct_type): test_account}, [MarketType.CDELIVERY], ccy=test_symbol[: test_symbol.find("USD")]
    )


async def test_real_trade_ps_fu(
    exch_account: dict[tuple[ExchangeName, AccountType], str], market_types: list[MarketType], ccy: str = "BNB"
) -> None:
    for (exch, acct_type), account in exch_account.items():
        if exch not in [
            ExchangeName.BINANCE,
            ExchangeName.OKX,
            ExchangeName.BYBIT,
            ExchangeName.KUCOIN,
            ExchangeName.GATE,
        ]:
            continue
        for market_type in market_types:
            if market_type not in [
                MarketType.SPOT,
                MarketType.MARGIN,
                MarketType.UPERP,
                MarketType.CPERP,
                MarketType.CDELIVERY,
            ]:
                continue
            rest_wrapper = get_rest_wrapper(exch, market_type, acct_type, account)
            symbol = get_symbol(ccy, exch, market_type)
            print(f"ccy {ccy} get symbol: ", symbol)
            priceResp = await rest_wrapper.get_price(get_symbol(ccy, exch, market_type))
            print(f"start test {exch} {market_type}")
            size = Decimal("0.0025")
            if priceResp["status"] == 0:
                price = priceResp["data"]
                print(round(price * Decimal("1.01"), 2))
                extras = {}
                if exch in [ExchangeName.KUCOIN]:
                    extras = {"leverage": 10}
                order = await rest_wrapper.place_order(
                    symbol,
                    OrderSide.BUY,
                    qty=size,
                    price=round(price * Decimal("1.01"), 2),
                    order_time_in_force=TimeInForce.GTC,
                    params=extras,
                )
                print("order: ", order)
                time.sleep(1)
                if order["status"] == 0:
                    print(order)
                    order_id = order["data"].order_id
                    open_order = await rest_wrapper.ccxt_sync_open_orders(symbol)
                    print("open order: ", open_order)
                    time.sleep(1)
                    cancel = await rest_wrapper.cancel_order(
                        symbol, order_id
                    )  # 如果订单成交了，传入order_id会返回  {"code": -2011, "msg": "Unknown order sent."}
                    print("cancel: ", cancel)
                    time.sleep(1)
                    open_order = await rest_wrapper.ccxt_sync_open_orders(symbol)
                    print("open order: ", open_order)


async def test_order_ps_fu(
    exch_account: dict[tuple[ExchangeName, AccountType], str], market_types: list[MarketType], ccy: str = "BNB"
) -> None:
    for (exch, acct_type), account in exch_account.items():
        if exch not in [
            ExchangeName.BINANCE,
            ExchangeName.OKX,
            ExchangeName.BYBIT,
            ExchangeName.KUCOIN,
            ExchangeName.GATE,
        ]:
            continue
        for market_type in market_types:
            if market_type not in [
                MarketType.SPOT,
                MarketType.MARGIN,
                MarketType.UPERP,
                MarketType.CPERP,
                MarketType.CDELIVERY,
            ]:
                continue
            rest_wrapper = get_rest_wrapper(exch, market_type, acct_type, account)
            symbol = get_symbol(ccy, exch, market_type)
            print(f"ccy {ccy} get symbol: ", symbol)
            priceResp = await rest_wrapper.get_price(get_symbol(ccy, exch, market_type))
            print(f"start test {exch} {market_type}")
            # size = (
            #     1
            #     if market_type == MarketType.UPERP
            #     and exch in [ExchangeName.KUCOIN, ExchangeName.OKX, ExchangeName.GATE, ExchangeName.GATEUNIFIED]
            #     else 500
            # )
            size = Decimal("1")
            if priceResp["status"] == 0:
                price = priceResp["data"]
                if exch not in [ExchangeName.GATE]:
                    extras = {}
                    if exch in [ExchangeName.KUCOIN]:
                        extras = {"leverage": 10}
                    order = await rest_wrapper.place_order(
                        symbol,
                        OrderSide.BUY,
                        price=round(price * Decimal("0.97"), 2),
                        qty=size,
                        order_time_in_force=TimeInForce.GTC,
                        params=extras,
                    )
                    print("order: ", order)
                    time.sleep(1)
                    open_order = await rest_wrapper.ccxt_sync_open_orders(symbol)
                    print("open order: ", open_order)
                    cancel = await rest_wrapper.cancel_all(symbol)
                    print("cancel: ", cancel)
                    time.sleep(1)
                    open_order = await rest_wrapper.ccxt_sync_open_orders(symbol)
                    print("open order: ", open_order)
                # cancel single
                extras = {}
                if exch in [ExchangeName.KUCOIN]:
                    extras = {"leverage": 10}
                order = await rest_wrapper.place_order(
                    symbol,
                    OrderSide.BUY,
                    price=round(price * Decimal("0.97"), 2),
                    qty=size,
                    order_time_in_force=TimeInForce.GTC,
                    params=extras,
                )
                print("order: ", order)
                time.sleep(1)
                if order["status"] == 0:
                    print(order)
                    order_id = order["data"].order_id
                    open_order = await rest_wrapper.ccxt_sync_open_orders(symbol)
                    print("open order: ", open_order)
                    time.sleep(1)
                    cancel = await rest_wrapper.cancel_order(
                        symbol, order_id
                    )  # 如果订单成交了，传入order_id会返回  {"code": -2011, "msg": "Unknown order sent."}
                    print("cancel: ", cancel)
                    time.sleep(1)
                    open_order = await rest_wrapper.ccxt_sync_open_orders(symbol)
                    print("open order: ", open_order)


@pytest.mark.asyncio
async def test_binance():
    print("=======================test binance=========================")
    test_exch = ExchangeName.BINANCE
    test_account = "mpbntest01"
    acct_type = AccountType.NORMAL
    test_symbol = "LTCUSDT"
    await sp_basic_test(test_exch, test_account, acct_type, test_symbol)

    await margin_basic_test(test_exch, test_account, acct_type, test_symbol)
    await lps_basic_test(test_exch, test_account, acct_type, test_symbol)

    # ---------------CPERP, CDELIVERY test--------
    rest_wrapper = get_rest_wrapper(test_exch, MarketType.SPOT, acct_type, test_account)
    transfer_ps1 = await rest_wrapper.universal_transfer(Decimal("0.1"), "USDT", MarketType.SPOT, MarketType.UPERP)
    print(transfer_ps1)  # {'status': 0, 'data': TransferResponse(apply_id='160332323842')}
    transfer_ps2 = await rest_wrapper.universal_transfer(Decimal("0.1"), "BNB", MarketType.SPOT, MarketType.CPERP)
    print(transfer_ps2)  # {'status': 0, 'data': TransferResponse(apply_id='160336951331')}
    ps_symbol = "MATICUSD_PERP"
    await ps_basic_test(test_exch, test_account, acct_type, ps_symbol)
    fu_symbol = "BNBUSD_250926"
    await fu_basic_test(test_exch, test_account, acct_type, fu_symbol)
    # ---------------CPERP, CDELIVERY test--------

    rest_wrapper = get_rest_wrapper(test_exch, MarketType.SPOT, test_account)
    transfer1 = await rest_wrapper.universal_transfer(Decimal("1"), "USDT", MarketType.UPERP, MarketType.SPOT)
    transfer2 = await rest_wrapper.universal_transfer(Decimal("1"), "USDT", MarketType.SPOT, MarketType.UPERP)
    transfer3 = await rest_wrapper.universal_transfer(
        Decimal("1000000000000"), "USDT", MarketType.SPOT, MarketType.UPERP
    )
    print(transfer1, transfer2)
    assert transfer1["status"] == 0 and transfer2["status"] == 0
    print(transfer3)
    assert transfer3["status"] == -1

    rest_wrapper = get_rest_wrapper(test_exch, MarketType.MARGIN, test_account)
    repay = await rest_wrapper.repay("AAA", Decimal(1))
    print("repay: ", repay)


@pytest.mark.asyncio
async def test_okx():
    print("=======================test okx=========================")
    test_exch = ExchangeName.OKX
    test_account = "4aokextest1"
    acct_type = AccountType.NORMAL
    await sp_basic_test(test_exch, test_account, acct_type, "LTC-USDT")
    await margin_basic_test(test_exch, test_account, acct_type, "LTC-USDT")
    await lps_basic_test(test_exch, test_account, acct_type, "LTC-USDT-SWAP")

    rest_wrapper = get_rest_wrapper(test_exch, MarketType.SPOT, test_account)
    discount = await rest_wrapper.get_discount_rate("BTC")
    print("discount", discount)


@pytest.mark.asyncio
async def test_kucoin():
    print("=======================test kucoin=========================")
    test_exch = ExchangeName.KUCOIN
    test_account = "4akcliq01"
    # await sp_basic_test(test_exch, test_account, "LTC-USDT")
    await lps_basic_test(test_exch, test_account, AccountType.NORMAL, "LTCUSDTM")

    rest_wrapper = get_rest_wrapper(test_exch, MarketType.SPOT, AccountType.HFT, test_account)
    asset = await rest_wrapper.get_assets()
    print("\nhft direct", asset)
    assert asset["status"] == 0, asset
    # asset = await rest_wrapper.get_assets(from_redis=True)
    # print("\nhft redis", asset)
    # assert asset["status"] == 0, asset

    rest_wrapper = get_rest_wrapper(test_exch, MarketType.SPOT, AccountType.NORMAL, test_account)
    transfer1 = await rest_wrapper.universal_transfer(Decimal("1"), "USDT", MarketType.SPOT, MarketType.CDELIVERY)
    transfer2 = await rest_wrapper.universal_transfer(Decimal("1"), "USDT", MarketType.CDELIVERY, MarketType.SPOT)
    print(transfer1)
    print(transfer2)


@pytest.mark.asyncio
async def test_gate():
    print("=======================test gate=========================")
    test_exch = ExchangeName.GATE
    test_account = "4agatetest1"
    test_symbol = "LTC_USDT"
    acct_type = AccountType.NORMAL
    await sp_basic_test(test_exch, test_account, acct_type, test_symbol)
    await lps_basic_test(test_exch, test_account, acct_type, test_symbol)

    rest_wrapper = get_rest_wrapper(test_exch, MarketType.SPOT, test_account)
    # await test_real_trade_ps_fu({test_exch: test_account}, [MarketType.SPOT], ccy='BTC')

    transfer1 = await rest_wrapper.universal_transfer(Decimal("1"), "USDT", MarketType.UPERP, MarketType.SPOT)
    transfer2 = await rest_wrapper.universal_transfer(Decimal("1"), "USDT", MarketType.SPOT, MarketType.UPERP)
    print(transfer1)
    print(transfer2)
    # transfer3 = await rest_wrapper.universal_transfer(MarketType.UPERP, MarketType.SPOT, "USDT", Decimal("100"))
    # transfer4 = await rest_wrapper.universal_transfer(MarketType.SPOT, MarketType.CPERP, "BTC", Decimal("0.002")) # {'status': 0, 'data': TransferResponse(apply_id=1711089787336)}
    # print(transfer3)
    # print(transfer4)

    ps_symbol = "BTC_USD"
    await ps_basic_test(test_exch, test_account, acct_type, ps_symbol)


@pytest.mark.asyncio
async def test_gateunified():
    print("=======================test gate=========================")
    test_exch = ExchangeName.GATE
    test_account = "4agatetestpm01"
    test_symbol = "LTC_USDT"
    acct_type = AccountType.UNIFIED
    await sp_basic_test(test_exch, test_account, acct_type, test_symbol)
    # rest_wrapper = get_rest_wrapper(test_exch, MarketType.SPOT, test_account)
    # transfer = await rest_wrapper.universal_transfer(MarketType.SPOT, MarketType.CPERP, "BTC", Decimal("0.002")) # {'status': 0, 'data': TransferResponse(apply_id=1711334892074)}
    # print(transfer)

    await lps_basic_test(test_exch, test_account, acct_type, test_symbol)
    ps_symbol = "BTC_USD"
    await ps_basic_test(test_exch, test_account, acct_type, ps_symbol)


@pytest.mark.asyncio
async def test_bybit():
    print("=======================test bybit=========================")
    test_exch = ExchangeName.BYBIT
    test_account = "4abybittest4"
    acct_type = AccountType.UNIFIED
    await sp_basic_test(test_exch, test_account, acct_type, "LTCUSDT")
    await lps_basic_test(test_exch, test_account, acct_type, "LTCUSDT")

    rest_wrapper = get_rest_wrapper(test_exch, MarketType.SPOT, acct_type, test_account)
    transfer_ps1 = await rest_wrapper.universal_transfer(Decimal("0.01"), "ETH", MarketType.CPERP, MarketType.SPOT)
    transfer_ps2 = await rest_wrapper.universal_transfer(Decimal("0.01"), "ETH", MarketType.SPOT, MarketType.CDELIVERY)
    transfer_ps3 = await rest_wrapper.universal_transfer(
        Decimal("1000000"), "ETH", MarketType.SPOT, MarketType.CDELIVERY
    )
    print("transfer1", transfer_ps1)
    print("transfer2", transfer_ps2)
    print("transfer3", transfer_ps3)

    ps_symbol = "MANAUSD"
    await ps_basic_test(test_exch, test_account, acct_type, ps_symbol)
    fu_symbol = "ETHUSDU24"
    await fu_basic_test(test_exch, test_account, acct_type, fu_symbol)


@pytest.mark.asyncio
async def test_binanceunified():
    print("=======================test binance=========================")
    test_exch = ExchangeName.BINANCE
    acct_type = AccountType.UNIFIED
    test_account = "mpbnpmtest153"
    test_symbol = "BTCUSDT"
    await sp_basic_test(test_exch, test_account, acct_type, test_symbol)
    await margin_basic_test(test_exch, test_account, acct_type, test_symbol)
    await lps_basic_test(test_exch, test_account, acct_type, test_symbol)
    # ----- CPERP, CDELIVERY test---------
    #  get_positions, get_balance, get_prices, place_order, cancel_order, get_trade_history, get_order_history
    ps_symbol = "MATICUSD"
    await ps_basic_test(test_exch, test_account, acct_type, ps_symbol)

    fu_symbol = "BNBUSD_250926"
    await fu_basic_test(test_exch, test_account, acct_type, fu_symbol)
    # ----- CPERP, CDELIVERY test-----


@pytest.mark.asyncio
async def test_get_equity(exch_account):
    print("=======================test equity::get_equity()=========================")
    for (test_exch, acct_type), test_account in exch_account.items():
        rest_wrapper = get_rest_wrapper(test_exch, MarketType.SPOT, acct_type, test_account)
        equity = await rest_wrapper.get_equity()
        print(f"exch[{test_exch}] account[{test_account}] equity[{equity}]")


def get_symbol(ccy: str, exchange: ExchangeName, market_type: MarketType) -> str:
    ccy = ccy.upper()
    if ExchangeName.BINANCE == exchange:
        if market_type in [MarketType.UPERP, MarketType.SPOT, MarketType.MARGIN]:
            return ccy + "USDT"
        elif market_type == MarketType.CPERP:
            return ccy + "USD_PERP"
        elif market_type == MarketType.CDELIVERY:
            return ccy + "USD_250926"
    elif ExchangeName.OKX == exchange:
        if MarketType.SPOT == market_type:
            return ccy + "-USDT"
        elif MarketType.UPERP == market_type:
            return ccy + "-USDT-SWAP"
        elif MarketType.CPERP == market_type:
            return ccy + "-USD-SWAP"
        else:
            return ccy + "-USD"

    elif ExchangeName.BYBIT == exchange:
        if market_type in [MarketType.SPOT, MarketType.UPERP, MarketType.MARGIN]:
            return ccy + "USDT"
        elif market_type == MarketType.CPERP:
            return ccy + "USD"
        elif market_type == MarketType.CDELIVERY:
            return ccy + "USDU24"
    elif ExchangeName.KUCOIN == exchange:
        if MarketType.SPOT == market_type:
            return ccy + "-USDT"
        elif MarketType.UPERP == market_type:
            return ccy + "USDTM"
        elif MarketType.CPERP == market_type:
            return ccy + "USDM"
    elif ExchangeName.GATE == exchange:
        if market_type == MarketType.CPERP:
            return ccy + "_USD"
        else:
            return ccy + "_USDT"
    elif ExchangeName.BITGET == exchange:
        return ccy + "USDT"
    return ""


@pytest.mark.asyncio
async def test_funding_rate_history(exch_account):
    ccy = "ETH"

    for (exch, acct_type), account in exch_account.items():
        rest_wrapper = get_rest_wrapper(exch, MarketType.UPERP, acct_type, account)
        funding_rate = await rest_wrapper.get_historical_funding_rate(
            [get_symbol(ccy, exch, MarketType.UPERP)], days=3
        )
        print(f"{exch} funding_rate", funding_rate)


@pytest.mark.asyncio
async def test_kline():
    exch_list = [ExchangeName.BINANCE, ExchangeName.OKX, ExchangeName.GATE, ExchangeName.BYBIT, ExchangeName.KUCOIN]
    # exch_list = [ExchangeName.KUCOIN, ExchangeName.BYBIT]
    acct_type = AccountType.UNIFIED
    end_time = int(time.time() * 1000)
    num = 101
    start_time = end_time - num * 1 * 3600 * 1000
    ccy = "ETH"
    for exch in exch_list:
        for market_type in [MarketType.CPERP, MarketType.UPERP]:
            rest_wrapper = get_rest_wrapper(exch, market_type, acct_type)
            kline = await rest_wrapper.get_historical_kline(
                get_symbol(ccy, exch, market_type), Interval._1h, start_time, None, contract_type="CURRENT_QUARTER"
            )
            if kline["status"] != 0:
                print(f"{exch} {market_type} kline error: {kline}")
            elif num != len(kline["data"]):
                print(f"{exch} {market_type} kline data length mismatch: expected {num}, got {len(kline['data'])}")


@pytest.mark.asyncio
async def test_order(exch_account, trade_market_types=[MarketType.UPERP, MarketType.SPOT]):
    ccy = "SOL"
    for (exch, acct_type), account in exch_account.items():
        if exch not in [
            ExchangeName.BINANCE,
            ExchangeName.OKX,
            ExchangeName.BYBIT,
            ExchangeName.KUCOIN,
            ExchangeName.GATE,
            ExchangeName.BITGET,
        ]:
            continue
        for market_type in [MarketType.UPERP, MarketType.SPOT]:
            if market_type not in trade_market_types:
                continue
            rest_wrapper = get_rest_wrapper(exch, market_type, acct_type, account)
            symbol = get_symbol(ccy, exch, market_type)
            priceResp = await rest_wrapper.get_price(get_symbol(ccy, exch, market_type))
            print(f"start test {exch} {market_type}")
            size = (
                Decimal("1")
                if market_type == MarketType.UPERP
                and exch in [ExchangeName.KUCOIN, ExchangeName.OKX, ExchangeName.GATE]
                else Decimal("0.1")
            )
            if priceResp["status"] == 0:
                price = priceResp["data"]
                if exch not in [ExchangeName.GATE]:
                    params = {}
                    if exch in [ExchangeName.KUCOIN]:
                        params = {"leverage": 10}
                    order = await rest_wrapper.place_order(
                        symbol,
                        OrderSide.SELL,
                        price=Decimal(str(round(price * 1.02, 2))),
                        qty=size,
                        order_time_in_force=TimeInForce.GTC,
                        params=params,
                    )
                    print("order: ", order)
                    time.sleep(1)
                    open_order = await rest_wrapper.ccxt_sync_open_orders(symbol)
                    print("open orders: ", open_order)
                    if exch == ExchangeName.BITGET and open_order["status"] == 0:
                        for order in open_order["data"][symbol]:
                            cancel = await rest_wrapper.cancel_order(symbol=symbol, orderId=order.order_id)
                            print("cancel order: ", cancel)
                    else:
                        cancel = await rest_wrapper.cancel_all(symbol)
                        print("cancel all order: ", cancel)
                    time.sleep(1)
                    open_order = await rest_wrapper.ccxt_sync_open_orders(symbol)
                    print("open order: ", open_order)
                # cancel single
                params = {}
                if exch in [ExchangeName.KUCOIN]:
                    params = {"leverage": 10}
                order = await rest_wrapper.place_order(
                    symbol,
                    OrderSide.SELL,
                    price=Decimal(str(round(price * 1.02, 2))),
                    qty=size,
                    order_time_in_force=TimeInForce.GTC,
                    params=params,
                )
                print("open order: ", order)
                time.sleep(1)
                if order["status"] == 0:
                    print(order)
                    order_id = order["data"].order_id
                    open_order = await rest_wrapper.ccxt_sync_open_orders(symbol)
                    print("open orders: ", open_order)
                    time.sleep(1)
                    cancel = await rest_wrapper.cancel_order(symbol, order_id)
                    print("cancel order: ", cancel)
                    time.sleep(1)
                    open_order = await rest_wrapper.ccxt_sync_open_orders(symbol)
                    print("open orders: ", open_order)


@pytest.mark.asyncio
async def test_discount_rate(exch_account):
    ccy = "ETH"
    for exch in [ExchangeName.OKX, ExchangeName.BYBIT]:
        rest_wrapper = get_rest_wrapper(
            exch, MarketType.SPOT, AccountType.NORMAL, exch_account[(exch, AccountType.NORMAL)]
        )
        discount_rate = await rest_wrapper.get_discount_rate(ccy)
        print(discount_rate)


@pytest.mark.asyncio
async def test_get_loans():
    exch_account = {(ExchangeName.BINANCE, AccountType.NORMAL): "4abncroxn002"}
    for (exch, acct_type), acct in exch_account.items():
        rest_wrapper = get_rest_wrapper(exch, MarketType.MARGIN, acct_type, acct)
        loans = await rest_wrapper.get_loans()
        print(loans)


@pytest.mark.asyncio
async def test_leverage():
    exch_account = {(ExchangeName.BINANCE, AccountType.UNIFIED): "4abnarbxn005"}
    ccy = "ETH"
    market_type = MarketType.UPERP
    for (exch, acct_type), acct in exch_account.items():
        rest_wrapper = get_rest_wrapper(exch, market_type, acct_type, acct)
        leverage = await rest_wrapper.get_leverage(get_symbol(ccy, exch, market_type), MarginMode.ISOLATED)
        print(leverage)


@pytest.mark.asyncio
async def test_max_qty_notional(exch_account):
    ccy = "DOGE"
    exch_list = [ExchangeName.BINANCE]
    market_type_list = [MarketType.UPERP]
    acct_type = AccountType.UNIFIED
    ccy = "ETH"
    for exch in exch_list:
        for market_type in market_type_list:
            print(f"Checking {market_type}")
            rest_wrapper = get_rest_wrapper(exch, market_type, acct_type, exch_account[(exch, acct_type)])
            if exch == ExchangeName.OKX:
                max_qty = await rest_wrapper.get_max_open_quantity(
                    get_symbol(ccy, exch, market_type), MarginMode.CROSS
                )
                print(exch.name, max_qty)
            else:
                max_notional = await rest_wrapper.get_max_open_notional(get_symbol(ccy, exch, market_type))
                print(exch.name, max_notional)


@pytest.mark.asyncio
async def test_current_funding_rate():
    market_type = MarketType.UPERP
    ccy = "ETH"
    exch_list = [
        ExchangeName.BITGET,
        ExchangeName.OKX,
        ExchangeName.BINANCE,
        ExchangeName.GATE,
        ExchangeName.BYBIT,
        ExchangeName.KUCOIN,
    ]
    for exch in exch_list:
        print(f"Checking {exch}")
        rest_wrapper = get_rest_wrapper(exch, market_type)
        funding_rate = await rest_wrapper.get_current_funding_rate([get_symbol(ccy, exch, market_type)])
        print(funding_rate)


@pytest.mark.asyncio
async def test_historical_funding_rate():
    market_type = MarketType.UPERP
    ccy = "ETH"
    exch_list = [
        ExchangeName.BITGET,
        ExchangeName.OKX,
        ExchangeName.BINANCE,
        ExchangeName.GATE,
        ExchangeName.BYBIT,
        ExchangeName.KUCOIN,
    ]
    for exch in exch_list:
        print(f"Checking {exch}")
        rest_wrapper = get_rest_wrapper(exch, market_type)
        funding_rate = await rest_wrapper.get_historical_funding_rate(
            [get_symbol("ETH", exch, market_type), get_symbol("XRP", exch, market_type)], days=1
        )
        if funding_rate["status"] == 0:
            key = list(funding_rate["data"].keys())[0]
            print(len(funding_rate["data"][key]), funding_rate["data"][key])
        else:
            print(f"Error fetching funding rate for {exch}: {funding_rate['msg']}")


@pytest.mark.asyncio
async def test_commission_rate(exch_account):
    ccy = "ETH"
    account_type = AccountType.NORMAL
    market_type_list = [MarketType.SPOT, MarketType.UPERP]
    for exch in [
        ExchangeName.OKX,
        ExchangeName.BINANCE,
        ExchangeName.GATE,
        ExchangeName.BYBIT,
        ExchangeName.KUCOIN,
        ExchangeName.BITGET,
    ]:
        for market_type in market_type_list:
            rest_wrapper = get_rest_wrapper(exch, market_type, account_type, exch_account[(exch, account_type)])
            commision_rate_redis = await rest_wrapper.get_commission_rate(get_symbol(ccy, exch, market_type), True)
            commision_rate = await rest_wrapper.get_commission_rate(get_symbol(ccy, exch, market_type))
            print("------", exch, "-----")
            print(commision_rate_redis)
            print(commision_rate)
            print("-------------")


@pytest.mark.asyncio
async def test_get_interest_rates():
    print("=======================test binance ir=========================")
    test_exch = ExchangeName.BINANCE
    test_account = "4abntest1"
    rest_wrapper = get_rest_wrapper(test_exch, MarketType.SPOT, AccountType.NORMAL, test_account)
    interest_rates = await rest_wrapper.get_interest_rates(vip_level=4, vip_loan=True)
    print("\ninterest_rates", interest_rates)
    print()
    print("=======================test okx ir=========================")
    test_exch = ExchangeName.OKX
    test_account = "4aokextest1"
    rest_wrapper = get_rest_wrapper(test_exch, MarketType.SPOT, AccountType.NORMAL, test_account)
    interest_rates = await rest_wrapper.get_interest_rates(vip_level=5, vip_loan=True)
    print("\ninterest_rates", interest_rates)


@pytest.mark.asyncio
async def test_position_risk(exch_account):
    exch_list = [ExchangeName.BINANCE, ExchangeName.OKX, ExchangeName.GATE, ExchangeName.BYBIT]
    market_type_list = [MarketType.UPERP]
    acct_type = AccountType.UNIFIED
    ccy = "ETH"
    for exch in exch_list:
        print(exch)
        for market_type in market_type_list:
            rest_wrapper = get_rest_wrapper(exch, market_type, acct_type, exch_account[(exch, acct_type)])
            position_risk = await rest_wrapper.get_positions()
            print(position_risk)


@pytest.mark.asyncio
async def test_assets(exch_account):
    exch_list = [ExchangeName.BINANCE]
    market_type_list = [MarketType.MARGIN, MarketType.UPERP]
    acct_type = AccountType.UNIFIED
    ccy = "ETH"
    for exch in exch_list:
        print(exch)
        for market_type in market_type_list:
            print(market_type)
            rest_wrapper = get_rest_wrapper(exch, market_type, acct_type, exch_account[(exch, acct_type)])
            assets = await rest_wrapper.get_assets()
            print(assets)


@pytest.mark.asyncio
async def test_order_history(exch_account):
    end_time = int(time.time() * 1000)
    ccy = "DOGE"
    start_time = end_time - 36 * 60 * 1000
    acct_type = AccountType.NORMAL
    market_type_list = [MarketType.SPOT, MarketType.MARGIN, MarketType.UPERP]
    for exch in [ExchangeName.BINANCE, ExchangeName.OKX, ExchangeName.KUCOIN, ExchangeName.BYBIT]:
        for market_type in market_type_list:
            if exch not in [ExchangeName.BINANCE, ExchangeName.BYBIT] and market_type == MarketType.MARGIN:
                continue
            rest_wrapper = get_rest_wrapper(exch, market_type, acct_type, exch_account[(exch, acct_type)])
            symbol = get_symbol(ccy, exch, market_type)
            print(symbol, exch, market_type)
            order_history = await rest_wrapper.get_order_history(start_time, end_time, [symbol])
            print(order_history)


@pytest.mark.asyncio
async def test_long_short_ratio():
    market_type = MarketType.UPERP
    ccy = "DOGE"
    for exch in [ExchangeName.BINANCE, ExchangeName.OKX, ExchangeName.GATE, ExchangeName.BYBIT]:
        rest_wrapper = get_rest_wrapper(exch, market_type)
        symbol = get_symbol(ccy, exch, market_type)
        long_short = await rest_wrapper.get_long_short_ratio(symbol, 2, Interval._1h)
        print(long_short)


@pytest.mark.asyncio
async def test_prices():
    market_type = MarketType.SPOT
    for exch in [ExchangeName.BINANCE, ExchangeName.OKX, ExchangeName.GATE, ExchangeName.BYBIT]:
        rest_wrapper = get_rest_wrapper(exch, market_type)
        data = await rest_wrapper.get_prices()
        print(data)


@pytest.mark.asyncio
async def test_kucoin_order():
    account = "4akcliq01"
    market_type = MarketType.UPERP
    acct_type = AccountType.NORMAL
    test_symbol = "ETHUSDTM"
    rest_wrapper = get_rest_wrapper(ExchangeName.KUCOIN, market_type, acct_type, account)
    data = await rest_wrapper.ccxt_sync_open_orders(test_symbol)
    print(data)
    data = await rest_wrapper.cancel_order(test_symbol, clientOid="normal_place509165576")
    print(data)


@pytest.mark.asyncio
async def test_bitget():
    test_exch = ExchangeName.BITGET
    test_account = "bitgetcjtest01"
    test_symbol = "DOGEUSDT"
    acct_type = AccountType.NORMAL

    rest_wrapper = get_rest_wrapper(test_exch, MarketType.SPOT, acct_type, test_account)
    commission_rate = await rest_wrapper.get_commission_rate(test_symbol)
    print("\ncommission_rate: ", commission_rate)
    assert commission_rate["status"] == 0

    asset = await rest_wrapper.get_assets()
    print("\nspot direct", asset)
    assert asset["status"] == 0, asset

    price = await rest_wrapper.get_price(test_symbol)
    print("\nprice", price)
    open_orders = await rest_wrapper.ccxt_sync_open_orders(test_symbol)
    print("\nopen orders", open_orders)
    end_time = int(time.time() * 1000)
    start_time = end_time - 24 * 60 * 60 * 1000
    trade_history = await rest_wrapper.get_trade_history(start_time, end_time, [test_symbol])
    print("trade history: ", trade_history)

    order_history = await rest_wrapper.get_order_history(start_time, end_time, [test_symbol])
    print("order history: ", order_history)

    await test_order(exch_account={(test_exch, acct_type): test_account}, trade_market_types=[MarketType.SPOT])
    # trans = await rest_wrapper.inter_transfer(from_market_type=MarketType.SPOT, to_market_type=MarketType.UPERP, ccy="USDT", amount=Decimal(1))
    # print("transfer:", trans)
    asset = await rest_wrapper.get_assets()
    print("\nspot direct", asset)

    print("\n lps-----------------")
    commission_rate = await rest_wrapper.get_commission_rate(test_symbol)
    print("\ncommission_rate: ", commission_rate)
    assert commission_rate["status"] == 0

    rest_wrapper = get_rest_wrapper(test_exch, MarketType.UPERP, acct_type, test_account)
    asset = await rest_wrapper.get_assets()
    print("\nlps direct asset: ", asset)
    assert asset["status"] == 0, asset

    price = await rest_wrapper.get_price(test_symbol)
    print("\nprice: ", price)
    open_orders = await rest_wrapper.ccxt_sync_open_orders(test_symbol)
    print("\nopen orders: ", open_orders)
    assert price["status"] == 0

    leverage = await rest_wrapper.set_symbol_leverage(test_symbol, 3)
    print(f"\n{test_symbol} set leverage:", leverage)

    end_time = int(time.time() * 1000)
    start_time = end_time - 24 * 60 * 60 * 1000
    trade_history = await rest_wrapper.get_trade_history(start_time, end_time, [test_symbol])
    print("trade_history: ", trade_history)

    funding_fee = await rest_wrapper.get_funding_fee(look_back=5, symbol_list=["ETHUSDT".lower()])
    print("funding_fee: ", funding_fee)

    await test_order(exch_account={(test_exch, acct_type): test_account}, trade_market_types=[MarketType.UPERP])
    positions = await rest_wrapper.get_positions()
    print("\nlps direct position: ", positions)
    assert positions["status"] == 0

    asset = await rest_wrapper.get_assets()
    print("\nlps direct asset: ", asset)

    trade_history = await rest_wrapper.get_trade_history(start_time, end_time, [test_symbol])
    print("trade history: ", trade_history)

    order_history = await rest_wrapper.get_order_history(start_time, end_time, [test_symbol])
    print("order history: ", order_history)

    # TODO lps have DOGEUSDT position for test funding_fee data
