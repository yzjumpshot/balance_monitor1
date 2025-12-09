from xclients.get_wrapper import get_market_ws_wrapper
from xclients.enum_type import ExchangeName, MarketType, Event
import pytest
from typing import Any
from tests.test_utils import is_ms_ts
from xclients.data_type import MarketMeta, OrderBook, Kline, WssConfig, Tickers
import asyncio
import pytest
import time


TO = 60  # 超时时间（秒）
MSG_LIMIT = 10


async def generic_ws_test(
    market_meta: MarketMeta,
    wss_config: WssConfig,
    symbol_list: list[str],
    event_type: Event,
    on_msg_callback,
    test_name: str = "",
):
    """通用WebSocket测试函数"""
    msg_count = 0
    start = time.time()
    test_info = f"{market_meta.exch_name.name}-{market_meta.market_type.name}-{event_type.name}"
    if test_name:
        test_info = f"{test_name}-{test_info}"

    print(f"🔄 Starting WebSocket test: {test_info}")
    print(f"   📋 Symbols: {symbol_list}")

    # 包装回调函数以支持外部条件终止
    async def _on_msg_wrapper(*args, **kwargs):
        nonlocal msg_count
        msg_count += 1
        print(f"   📨 Message {msg_count}/{MSG_LIMIT} received for {test_info}")
        await on_msg_callback(*args, **kwargs)

        # 检查终止条件
        if msg_count >= MSG_LIMIT or (time.time() - start) >= TO:
            print(f"   ✅ Test completed for {test_info} (Messages: {msg_count}, Time: {time.time() - start:.1f}s)")
            await ws_wrapper.close()

    try:
        ws_wrapper = get_market_ws_wrapper(market_meta, wss_config)

        # 订阅符号
        for symbol in symbol_list:
            ws_wrapper.subscribe_symbol(symbol)
            print(f"   🔔 Subscribed to {symbol}")

        # 订阅事件回调
        ws_wrapper.subscribe_callback(event_type, _on_msg_wrapper)

        # 运行WebSocket
        await ws_wrapper.run()

        assert msg_count > 0, f"No message received from websocket for {test_info}"
        print(f"   ✅ WebSocket test passed: {test_info} (Total messages: {msg_count})")

    except Exception as e:
        print(f"   ❌ WebSocket test failed: {test_info} - {str(e)}")
        raise


async def on_book_message(market_meta: MarketMeta, data: OrderBook):
    """订单簿消息回调"""
    bids_count = len(data.bids) if data.bids else 0
    asks_count = len(data.asks) if data.asks else 0
    assert is_ms_ts(data.exch_ts)
    assert is_ms_ts(data.recv_ts)
    print(f"      📖 OrderBook - {market_meta.exch_name.name}: {bids_count} bids, {asks_count} asks")


async def on_kline_message(market_meta: MarketMeta, data: list[Kline]):
    """K线消息回调"""
    kline_count = len(data) if data else 0
    if data and len(data) > 0:
        sample_kline = data[0]
        assert is_ms_ts(sample_kline.start_ts)
        assert is_ms_ts(sample_kline.ts)
        print(f"      📊 Klines - {market_meta.exch_name.name}: {kline_count} klines, sample: {sample_kline}")
    else:
        print(f"      📊 Klines - {market_meta.exch_name.name}: {kline_count} klines")


async def on_premium_index_message(market_meta: MarketMeta, data: Any):
    """溢价指数消息回调"""
    print(f"      💰 Premium Index - {market_meta.exch_name.name}: {data}")


async def on_ticker_message(market_meta: MarketMeta, data: Tickers):
    """Ticker消息回调"""
    ticker_count = len(data) if data else 0
    if data and len(data) > 0:
        sample_ticker = next(iter(data.values()))
        assert is_ms_ts(sample_ticker.ts)
        assert is_ms_ts(sample_ticker.fr_ts) or sample_ticker.fr_ts == 0
        print(f"      🎯 Tickers - {market_meta.exch_name.name}: {ticker_count} tickers, sample: {sample_ticker}")
    else:
        print(f"      🎯 Tickers - {market_meta.exch_name.name}: {ticker_count} tickers")


# 测试 Bybit
@pytest.mark.asyncio
async def test_bybit():
    market_meta = MarketMeta(exch_name=ExchangeName.BYBIT, market_type=MarketType.SPOT)
    wss_config = WssConfig(extra_params={"kline_intervals": ["1m", "5m"]})
    await generic_ws_test(market_meta, wss_config, ["BTCUSDT"], Event.BOOK, on_book_message)
    await generic_ws_test(market_meta, wss_config, ["BTCUSDT"], Event.KLINE, on_kline_message)


# 测试 Binance
@pytest.mark.asyncio
async def test_binance_spot():
    market_meta = MarketMeta(exch_name=ExchangeName.BINANCE, market_type=MarketType.SPOT)
    wss_config = WssConfig(extra_params={"kline_intervals": ["1m", "5m"]})
    await generic_ws_test(market_meta, wss_config, ["BTCUSDT"], Event.BOOK, on_book_message)
    await generic_ws_test(market_meta, wss_config, ["BTCUSDT", "ETHUSDT"], Event.KLINE, on_kline_message)
    await generic_ws_test(market_meta, wss_config, ["BTCUSDT"], Event.PREMIUM_INDEX, on_book_message)
    await generic_ws_test(market_meta, wss_config, ["BTCUSDT", "ETHUSDT"], Event.TICKER, on_book_message)


@pytest.mark.asyncio
async def test_binance_uperp():
    market_meta = MarketMeta(exch_name=ExchangeName.BINANCE, market_type=MarketType.UPERP)
    wss_config = WssConfig(extra_params={"kline_intervals": ["1m", "5m"]})
    await generic_ws_test(market_meta, wss_config, ["BTCUSDT"], Event.BOOK, on_book_message)
    await generic_ws_test(market_meta, wss_config, ["BTCUSDT", "ETHUSDT"], Event.KLINE, on_kline_message)
    await generic_ws_test(market_meta, wss_config, ["BTCUSDT"], Event.PREMIUM_INDEX, on_book_message)
    await generic_ws_test(market_meta, wss_config, ["BTCUSDT", "ETHUSDT"], Event.TICKER, on_book_message)


@pytest.mark.asyncio
async def test_okx():
    market_meta = MarketMeta(exch_name=ExchangeName.OKX, market_type=MarketType.SPOT)
    wss_config = WssConfig(extra_params={})
    await generic_ws_test(market_meta, wss_config, ["BTC-USDT"], Event.BOOK, on_book_message)
    await generic_ws_test(market_meta, wss_config, ["BTC-USDT", "ETH-USDT"], Event.TICKER, on_ticker_message)

    market_meta = MarketMeta(exch_name=ExchangeName.OKX, market_type=MarketType.UPERP)
    wss_config = WssConfig(extra_params={})
    await generic_ws_test(market_meta, wss_config, ["BTC-USDT-SWAP"], Event.BOOK, on_book_message)
    await generic_ws_test(market_meta, wss_config, ["BTC-USDT-SWAP", "ETH-USDT-SWAP"], Event.TICKER, on_ticker_message)


@pytest.mark.asyncio
async def test_bitget():
    market_meta = MarketMeta(exch_name=ExchangeName.BITGET, market_type=MarketType.SPOT)
    wss_config = WssConfig(extra_params={})
    await generic_ws_test(market_meta, wss_config, ["BTCUSDT"], Event.BOOK, on_book_message)

    market_meta = MarketMeta(exch_name=ExchangeName.BITGET, market_type=MarketType.UPERP)
    wss_config = WssConfig(extra_params={})
    await generic_ws_test(market_meta, wss_config, ["BTCUSDT"], Event.BOOK, on_book_message)


@pytest.mark.asyncio
async def test_coinex():
    market_meta = MarketMeta(exch_name=ExchangeName.COINEX, market_type=MarketType.SPOT)
    wss_config = WssConfig(extra_params={})
    await generic_ws_test(market_meta, wss_config, ["BTCUSDT"], Event.BOOK, on_book_message)
    await generic_ws_test(market_meta, wss_config, ["BTCUSDT", "ETHUSDT"], Event.TICKER, on_ticker_message)

    market_meta = MarketMeta(exch_name=ExchangeName.COINEX, market_type=MarketType.UPERP)
    wss_config = WssConfig(extra_params={})
    await generic_ws_test(market_meta, wss_config, ["BTCUSDT"], Event.BOOK, on_book_message)
    await generic_ws_test(market_meta, wss_config, ["BTCUSDT", "ETHUSDT"], Event.TICKER, on_ticker_message)


@pytest.mark.asyncio
async def test_kucoin():
    market_meta = MarketMeta(exch_name=ExchangeName.KUCOIN, market_type=MarketType.SPOT)
    wss_config = WssConfig(extra_params={})
    await generic_ws_test(market_meta, wss_config, ["BTC-USDT"], Event.BOOK, on_book_message)

    market_meta = MarketMeta(exch_name=ExchangeName.KUCOIN, market_type=MarketType.UPERP)
    wss_config = WssConfig(extra_params={})
    await generic_ws_test(market_meta, wss_config, ["XBTUSDTM"], Event.BOOK, on_book_message)
