from xclients.get_client import get_ws_client, get_rest_client
from xclients.get_wrapper import get_account_ws_wrapper
from xclients.enum_type import ExchangeName, MarketType, AccountType, Event
import asyncio
import uuid
import pytest
from xclients.data_type import Balances, Balance, WssConfig, AccountMeta, MarketMeta

TO = 60


@pytest.mark.asyncio
async def test_binance():
    got_msg = False

    async def on_balance_message(account_meta: AccountMeta, data: Balances) -> None:
        print("on wrapper message", data)
        nonlocal got_msg
        got_msg = True
        asyncio.create_task(ws_wrapper.close())

    async def transfer(account: str):
        async with get_rest_client(ExchangeName.BINANCE, MarketType.SPOT, AccountType.NORMAL, account) as r:
            await asyncio.sleep(2)
            print(await r.sapi_asset_transfer("MAIN_UMFUTURE", "USDT", "0.01"))
            await asyncio.sleep(2)
            print(await r.sapi_asset_transfer("UMFUTURE_MAIN", "USDT", "0.01"))

    account = "4abntest1"
    ws_wrapper = get_account_ws_wrapper(ExchangeName.BINANCE, MarketType.SPOT, AccountType.NORMAL, account=account)
    ws_wrapper.subscribe_callback(Event.BALANCE, on_balance_message)
    await asyncio.wait([asyncio.create_task(ws_wrapper.run()), asyncio.create_task(transfer(account))], timeout=TO)

    assert got_msg, "no msg got from ws"


@pytest.mark.asyncio
async def test_binance_uni():
    got_msg = False

    async def on_balance_message(account_meta: AccountMeta, data: Balances) -> None:
        print("on wrapper message", data)
        nonlocal got_msg
        got_msg = True
        asyncio.create_task(ws_wrapper.close())

    async def transfer(account: str):
        async with get_rest_client(ExchangeName.BINANCE, MarketType.SPOT, AccountType.NORMAL, account) as r:
            await asyncio.sleep(5)
            print(await r.sapi_asset_transfer("MAIN_PORTFOLIO_MARGIN", "USDT", "0.09"))
            await asyncio.sleep(5)
            print(await r.sapi_asset_transfer("PORTFOLIO_MARGIN_MAIN", "USDT", "0.09"))

    account = "4abnpmtest01"
    ws_wrapper = get_account_ws_wrapper(ExchangeName.BINANCE, MarketType.SPOT, AccountType.NORMAL, account)
    ws_wrapper.subscribe_callback(Event.BALANCE, on_balance_message)
    await asyncio.wait([asyncio.create_task(ws_wrapper.run()), asyncio.create_task(transfer(account))], timeout=TO)

    assert got_msg, "no msg got from ws"


# @pytest.mark.asyncio
# async def test_kucoin():
#     got_msg = False

#     async def on_balance_message(account_meta: AccountMeta, data: Balances) -> None:
#         print("on wrapper message", data)
#         nonlocal got_msg
#         got_msg = True
#         asyncio.create_task(ws_wrapper.close())

#     async def transfer(account: str):
#         async with get_rest_client(ExchangeName.KUCOIN, MarketType.SPOT, AccountType.NORMAL, account) as r:
#             await asyncio.sleep(10)
#             print(await r.inner_transfer(str(uuid.uuid1()), "USDT", "1", "trade_hf", "main"))
#             await asyncio.sleep(10)
#             print(await r.inner_transfer(str(uuid.uuid1()), "USDT", "1", "main", "trade_hf"))

#     account = "4akucointest1"
#     ws_wrapper = get_account_ws_wrapper(ExchangeName.KUCOIN, MarketType.SPOT, account)
#     ws_client = ws_wrapper.get_private_ws(topic_cbs={Event.BALANCE: on_balance_message})
#     await asyncio.wait([asyncio.create_task(ws_wrapper.run()), asyncio.create_task(transfer(account))], timeout=TO)

#     assert got_msg, "no msg got from ws"


# @pytest.mark.asyncio
# async def test_bybit():
#     got_msg = False

#     async def on_balance_message(account_meta: AccountMeta, data: Balances) -> None:
#         print("on wrapper message", data)
#         nonlocal got_msg
#         got_msg = True
#         asyncio.create_task(ws_wrapper.close())

#     async def transfer(account: str):
#         async with get_rest_client(ExchangeName.BYBIT, MarketType.SPOT, AccountType.NORMAL, account) as r:
#             await asyncio.sleep(3)
#             print(await r.inter_transfer("USDT", "1", "CONTRACT", "SPOT"))
#             await asyncio.sleep(3)
#             print(await r.inter_transfer("USDT", "1", "SPOT", "CONTRACT"))

#     account = "4abybittest1"
#     ws_wrapper = get_account_ws_wrapper(ExchangeName.BYBIT, MarketType.SPOT, AccountType.UNIFIED, account)
#     ws_wrapper.subscribe_symbol("BTCUSDT")
#     ws_wrapper.subscribe_callback(Event.BALANCE, on_balance_message)
#     await asyncio.wait([asyncio.create_task(ws_wrapper.run()), asyncio.create_task(transfer(account))], timeout=TO)

#     assert got_msg, "no msg got from ws"


# @pytest.mark.asyncio
# async def test_gate():
#     got_msg = False

#     async def on_balance_message(account_meta: AccountMeta, data: Balances) -> None:
#         print("on wrapper message", data)
#         nonlocal got_msg
#         got_msg = True
#         asyncio.create_task(ws_wrapper.close())

#     async def transfer(account: str):
#         async with get_rest_client(ExchangeName.GATE, MarketType.SPOT, AccountType.NORMAL, account) as r:
#             await asyncio.sleep(10)
#             print(await r.transfer("USDT", "1", "spot", "futures", settle="USDT"))
#             await asyncio.sleep(10)
#             print(await r.transfer("USDT", "1", "futures", "spot", settle="USDT"))

#     account = "4agatetest1"
#     ws_wrapper = get_account_ws_wrapper(ExchangeName.BYBIT, MarketType.SPOT, AccountType.UNIFIED, account)
#     ws_wrapper.subscribe_symbol("BTCUSDT")
#     ws_wrapper.subscribe_callback(Event.BALANCE, on_balance_message)
#     await asyncio.wait([asyncio.create_task(ws_wrapper.run()), asyncio.create_task(transfer(account))], timeout=TO)

#     assert got_msg, "no msg got from ws"


# @pytest.mark.asyncio
# async def test_okx():
#     got_msg = False

#     async def on_balance_message(account_meta: AccountMeta, data: Balances) -> None:
#         print("on wrapper message", data)
#         nonlocal got_msg
#         got_msg = True
#         asyncio.create_task(ws_wrapper.close())

#     account = "4aokextest1"
#     ws_wrapper = get_account_ws_wrapper(ExchangeName.OKX, MarketType.SPOT, account)
#     ws_client = ws_wrapper.get_private_ws(topic_cbs={Event.BALANCE: on_balance_message})
#     await asyncio.wait([ws_client.run()], timeout=TO)

#     assert got_msg, "no msg got from ws"


# @pytest.mark.asyncio
# async def test_gate_uni():
#     got_msg = False

#     def on_balance_message(account_meta: AccountMeta, data: Balances) -> None:
#         print("on wrapper message", data)
#         nonlocal got_msg
#         got_msg = True
#         asyncio.create_task(ws_wrapper.close())

#     async def transfer(account: str):
#         async with get_rest_client(ExchangeName.GATE, MarketType.SPOT, AccountType.UNIFIED, account) as r:
#             await asyncio.sleep(10)
#             await r.transfer("USDT", "1", "spot", "futures")
#             await asyncio.sleep(10)
#             await r.transfer("USDT", "1", "futures", "spot")

#     account = "4agatetestpm01"
#     ws_wrapper = get_account_ws_wrapper(ExchangeName.GATE, MarketType.SPOT, AccountType.UNIFIED, account)
#     ws_wrapper.subscribe_symbol("BTCUSDT")
#     ws_wrapper.subscribe_callback(Event.BALANCE, on_balance_message)
#     await asyncio.wait([asyncio.create_task(ws_wrapper.run()), asyncio.create_task(transfer(account))], timeout=TO)

#     assert got_msg, "no msg got from ws"


# @pytest.mark.asyncio
# async def test_bitget():
#     got_msg = False

#     def on_balance_message(account_meta: AccountMeta, data: Balances) -> None:
#         print("on wrapper message", data)
#         nonlocal got_msg
#         got_msg = True
#         asyncio.create_task(ws_wrapper.close())

#     account = "4abitgettest01"
#     ws_wrapper = get_account_ws_wrapper(ExchangeName.BITGET, MarketType.SPOT, AccountType.NORMAL, account)
#     ws_wrapper.subscribe_symbol("BTCUSDT")
#     ws_wrapper.subscribe_callback(Event.BALANCE, on_balance_message)
#     await asyncio.wait([asyncio.create_task(ws_wrapper.run())], timeout=TO)
#     assert got_msg, "no msg got from ws"


# @pytest.mark.asyncio
# async def test_coinex():
#     got_msg = False

#     def on_balance_message(account_meta: AccountMeta, data: Balances) -> None:
#         print("on wrapper message", data)
#         nonlocal got_msg
#         got_msg = True
#         asyncio.create_task(ws_wrapper.close())

#     async def transfer(account: str):
#         async with get_rest_client(ExchangeName.COINEX, MarketType.SPOT, AccountType.NORMAL, account) as r:
#             await asyncio.sleep(10)
#             await r.transfer("SPOT", "FUTURES", "USDT", "1")
#             await asyncio.sleep(10)
#             await r.transfer("FUTURES", "SPOT", "USDT", "1")

#     account = "coinexcjtest01"
#     ws_wrapper = get_account_ws_wrapper(ExchangeName.COINEX, MarketType.SPOT, AccountType.NORMAL, account)
#     ws_wrapper.subscribe_symbol("BTCUSDT")
#     ws_wrapper.subscribe_callback(Event.BALANCE, on_balance_message)
#     await asyncio.wait([asyncio.create_task(ws_wrapper.run()), asyncio.create_task(transfer(account))], timeout=TO)

#     assert got_msg, "no msg got from ws"


# # binance()
# # kucoin()
# # bybit()
# # gate()
# # okx()
# # binance_uni()
# # gate_uni()
# # binance_raw()
