from xclients.get_wrapper import get_rest_wrapper
from xclients.enum_type import (
    MarketType,
    ExchangeName,
    AccountType,
    OrderSide,
    TimeInForce,
    OrderType,
    OrderStatus,
    PositionMode,
    Event,
)
from xclients.data_type import (
    PlaceOrderInstruction,
    CancelOrderInstruction,
    OrderSnapshot,
    SyncOrderInstruction,
    AccountMeta,
)
import pytest
from decimal import Decimal
import time
import asyncio

from xclients.base_wrapper import Resp
from tests.test_utils import (
    print_section_header,
    print_test_info,
    get_symbol,
    get_account,
    is_ms_ts,
)


def print_order_result(action: str, order_snapshot: "Resp", expected_status: list = []):
    """打印订单操作结果"""
    if order_snapshot["status"] == 0:
        data: OrderSnapshot = order_snapshot["data"]
        status_emoji = {
            OrderStatus.LIVE: "🟢",
            OrderStatus.FILLED: "✅",
            OrderStatus.CANCELED: "❌",
            OrderStatus.REJECTED: "⛔",
            OrderStatus.PARTIALLY_FILLED: "🟡",
        }.get(data.order_status, "❓")

        print(f"✅ {action} Success:")
        print(f"   Order ID: {data.order_id}")
        print(f"   Client Order ID: {data.client_order_id}")
        print(f"   Status: {status_emoji} {data.order_status.name}")
        print(f"   Side: {data.order_side.name}")
        print(f"   Type: {data.order_type.name}")
        print(f"   Price: ${data.price}")
        print(f"   Quantity: {data.qty}")
        print(f"   Time in Force: {data.order_time_in_force.name}")
        if data.order_status == OrderStatus.FILLED:
            print(f"   Filled Price: ${data.avg_price}")
            print(f"   Filled Quantity: {data.filled_qty}")
            print(f"   Fee: {data.fee}({data.fee_ccy})")
        if data.order_status == OrderStatus.REJECTED:
            print(f"   Reject Reason: {data.rejected_reason}")
            print(f"   Reject Message: {data.rejected_message}")
        if data.order_status.is_open() or data.order_status == OrderStatus.FILLED:
            assert is_ms_ts(float(data.place_ack_ts)), "place_ack_ts should be a valid millisecond timestamp"
            assert is_ms_ts(float(data.exch_update_ts)), "exch_update_ts should be a valid millisecond timestamp"
            assert is_ms_ts(float(data.local_update_ts)), "client_update_ts should be a valid millisecond timestamp"

    else:
        print(f"❌ {action} Failed:")
        print(f"   Error: {order_snapshot.get('msg', 'Unknown error')}")


@pytest.mark.asyncio
async def test_post_only_order(exch, acct_type, trade_market_types=[MarketType.UPERP, MarketType.SPOT]):
    print_section_header("POST ONLY ORDER TEST", 1)

    ccy = "SOL"
    account = get_account(exch, acct_type)

    for market_type in trade_market_types:
        print_section_header(f"Testing {exch.name} - {market_type.name}", 2)

        rest_wrapper = get_rest_wrapper(exch, market_type, acct_type, account)
        symbol = get_symbol(ccy, exch, market_type)

        cancel_result = await rest_wrapper.ccxt_cancel_all(symbol)
        if cancel_result:
            print("✅ All orders canceled successfully")
        else:
            print("⚠️ Cancel all orders completed (may have been no orders)")

        print("🔄 Fetching current price...")
        priceResp = await rest_wrapper.get_price(symbol)

        if priceResp["status"] != 0:
            print(f"❌ Failed to get price: {priceResp.get('msg', 'Unknown error')}")
            continue

        price: float = priceResp["data"]
        exec_price = Decimal(str(price * 1.004)).quantize(Decimal("0.00"))
        qty = (
            Decimal("1")
            if market_type == MarketType.UPERP and exch in [ExchangeName.KUCOIN, ExchangeName.OKX, ExchangeName.GATE]
            else Decimal("0.1")
        )

        print_test_info(exch, market_type, symbol, price)
        print(f"📦 Order Quantity: {qty}")
        print(f"💲 Order Price: ${exec_price} (99.6% of current price)")
        print(f"⏰ Time in Force: GTX (Post Only)")

        print("\n🔄 Placing POST ONLY order...")
        order_snapshot = await rest_wrapper.place_order(
            symbol,
            OrderSide.BUY,
            qty,
            exec_price,
            OrderType.LIMIT,
            TimeInForce.GTX,
            client_order_id="xclients" + str(int(time.time() * 1000)),
        )
        print_order_result("Place Order", order_snapshot)

        assert order_snapshot["status"] == 0, f"Order failed: {order_snapshot}"
        assert (
            order_snapshot["data"].order_id or order_snapshot["data"].order_status == OrderStatus.REJECTED
        ), "Order ID should not be None"
        if not order_snapshot["data"].order_status.is_completed():
            time.sleep(1)
            sync_order = await rest_wrapper.ccxt_sync_order(
                symbol,
                order_id=order_snapshot["data"].order_id,
                client_order_id=order_snapshot["data"].client_order_id,
            )

            print_order_result("Sync Order", sync_order)

            assert sync_order["status"] == 0 and sync_order["data"].order_status in (
                OrderStatus.CANCELED,
                OrderStatus.REJECTED,
            ), "Order should be canceled or rejected (Post Only behavior)"
            if exch not in (ExchangeName.BITGET, ExchangeName.COINEX):
                assert sync_order["data"].order_time_in_force == TimeInForce.GTX, "Order TIF should be GTX (Post Only)"
            assert sync_order["data"].order_side == OrderSide.BUY, "Order side should be BUY"


@pytest.mark.asyncio
async def test_ioc_order(exch, acct_type, trade_market_types=[MarketType.UPERP, MarketType.SPOT]):
    print_section_header("IOC ORDER TEST", 1)

    ccy = "SOL"
    account = get_account(exch, acct_type)

    for market_type in trade_market_types:
        print_section_header(f"Testing {exch.name} - {market_type.name}", 2)

        rest_wrapper = get_rest_wrapper(exch, market_type, acct_type, account)
        symbol = get_symbol(ccy, exch, market_type)

        print("🔄 Fetching current price...")
        priceResp = await rest_wrapper.get_price(symbol)

        if priceResp["status"] != 0:
            print(f"❌ Failed to get price: {priceResp['msg']}")
            continue

        price: float = priceResp["data"]
        exec_price = Decimal(str(price * 0.996)).quantize(Decimal("0.00"))
        qty = (
            Decimal("1")
            if market_type == MarketType.UPERP and exch in [ExchangeName.KUCOIN, ExchangeName.OKX, ExchangeName.GATE]
            else Decimal("0.1")
        )

        print_test_info(exch, market_type, symbol, price)
        print(f"📦 Order Quantity: {qty}")
        print(f"💲 Order Price: ${exec_price} (99.6% of current price)")
        print(f"⏰ Time in Force: IOC (Immediate or Cancel)")

        print("\n🔄 Placing IOC order...")
        order_snapshot = await rest_wrapper.place_order(
            symbol, OrderSide.BUY, qty, exec_price, OrderType.LIMIT, TimeInForce.IOC
        )

        print_order_result("Place Order", order_snapshot)

        assert order_snapshot["status"] == 0, f"Order failed: {order_snapshot}"
        assert (
            order_snapshot["data"].order_id or order_snapshot["data"].order_status == OrderStatus.REJECTED
        ), "Order ID should not be None"
        if not order_snapshot["data"].order_status.is_completed():
            time.sleep(1)
            sync_order = await rest_wrapper.ccxt_sync_order(
                symbol,
                order_id=order_snapshot["data"].order_id,
                client_order_id=order_snapshot["data"].client_order_id,
            )

            print_order_result("Sync Order", sync_order)

            assert sync_order["status"] == 0 and sync_order["data"].order_status in (
                OrderStatus.CANCELED,
                OrderStatus.REJECTED,
            ), "Order should be canceled or rejected (IOC behavior)"
            if exch not in (ExchangeName.BITGET, ExchangeName.COINEX):
                assert (
                    sync_order["data"].order_time_in_force == TimeInForce.IOC
                ), "Order TIF should be IOC (Immediate or Cancel)"
            assert sync_order["data"].order_side == OrderSide.BUY, "Order side should be BUY"


async def test_place_cancel_sync_order(exch, acct_type, trade_market_types=[MarketType.UPERP, MarketType.SPOT]):
    print_section_header("PLACE-CANCEL-SYNC ORDER TEST", 1)

    ccy = "SOL"
    account = get_account(exch, acct_type)

    for market_type in trade_market_types:
        print_section_header(f"Testing {exch.name} - {market_type.name}", 2)

        rest_wrapper = get_rest_wrapper(exch, market_type, acct_type, account)
        symbol = get_symbol(ccy, exch, market_type)

        print("🔄 Fetching current price...")
        priceResp = await rest_wrapper.get_price(symbol)

        if priceResp["status"] != 0:
            raise ValueError(f"Failed to get price for {symbol}: {priceResp['msg']}")

        price: float = priceResp["data"]
        exec_price = Decimal(str(price * 0.996)).quantize(Decimal("0.00"))
        qty = (
            Decimal("1")
            if market_type == MarketType.UPERP and exch in [ExchangeName.KUCOIN, ExchangeName.OKX, ExchangeName.GATE]
            else Decimal("0.1")
        )

        print_test_info(exch, market_type, symbol, price)
        print(f"📦 Order Quantity: {qty}")
        print(f"💲 Order Price: ${exec_price} (99.6% of current price)")
        print(f"⏰ Time in Force: GTC (Good Till Cancel)")
        time.sleep(1)
        print("\n🧹 Canceling all existing orders first...")
        cancel_result = await rest_wrapper.ccxt_cancel_all(symbol)
        if cancel_result:
            print("✅ All orders canceled successfully")
        else:
            print("⚠️ Cancel all orders completed (may have been no orders)")

        print("\n🔄 Placing limit order...")
        order_snapshot = await rest_wrapper.place_order(
            symbol, OrderSide.BUY, qty, exec_price, OrderType.LIMIT, TimeInForce.GTC
        )

        print_order_result("Place Order", order_snapshot)

        assert order_snapshot["status"] == 0, f"Order failed: {order_snapshot}"
        assert order_snapshot["data"].order_id is not None, "Order ID should not be None"
        if order_snapshot["data"].order_status != OrderStatus.UNKNOWN:
            assert order_snapshot["data"].order_status in (OrderStatus.LIVE,), "Order should be live"
        else:
            time.sleep(1)
            sync_order = await rest_wrapper.ccxt_sync_order(
                symbol,
                order_id=order_snapshot["data"].order_id,
                client_order_id=order_snapshot["data"].client_order_id,
            )

            print_order_result("Sync Order", sync_order)

            assert sync_order["status"] == 0 and sync_order["data"].order_status in (
                OrderStatus.LIVE,
            ), "Order should be live"

        print("\n🔍 Fetching open orders...")
        open_orders = await rest_wrapper.ccxt_sync_open_orders(symbol)
        if open_orders["status"] == 0:
            orders_count = len(open_orders["data"])
            print(f"✅ Found {orders_count} open order(s)")
            for i, order in enumerate(open_orders["data"], 1):
                print(f"   [{i}] {order.order_id} - {order.order_status.name}")
        else:
            print(f"❌ Failed to fetch open orders: {open_orders['msg'] if open_orders else 'Unknown error'}")

        time.sleep(1)

        print("\n❌ Canceling the order...")
        cancel_result = await rest_wrapper.ccxt_cancel_order(
            symbol,
            order_id=order_snapshot["data"].order_id,
            client_order_id=order_snapshot["data"].client_order_id,
        )

        if cancel_result["status"] == 0:
            if cancel_result["data"]:
                print_order_result("Cancel Order", cancel_result)
        else:
            print(f"❌ Cancel order failed: {cancel_result['msg'] if cancel_result else 'No response'}")

        time.sleep(1)

        print("\n🔄 Syncing order status...")
        sync_order = await rest_wrapper.ccxt_sync_order(
            symbol,
            order_id=order_snapshot["data"].order_id,
            client_order_id=order_snapshot["data"].client_order_id,
        )
        assert sync_order["status"] == 0, f"Sync order failed: {sync_order}"
        assert sync_order["data"].order_status == OrderStatus.CANCELED, "Order should be canceled"
        print_order_result("Sync Order", sync_order)


async def test_filled_order(exch, acct_type, trade_market_types=[MarketType.UPERP, MarketType.SPOT]):
    print_section_header("FILLED ORDER TEST", 1)

    ccy = "SOL"
    account = get_account(exch, acct_type)

    for market_type in trade_market_types:
        print_section_header(f"Testing {exch.name} - {market_type.name}", 2)

        rest_wrapper = get_rest_wrapper(exch, market_type, acct_type, account)
        symbol = get_symbol(ccy, exch, market_type)

        print("🔄 Fetching current price...")
        priceResp = await rest_wrapper.get_price(symbol)

        if priceResp["status"] != 0:
            raise ValueError(f"Failed to get price for {symbol}: {priceResp['msg']}")

        price: float = priceResp["data"]
        exec_price = Decimal(str(price * 1.004)).quantize(Decimal("0.00"))
        qty = (
            Decimal("1")
            if market_type == MarketType.UPERP and exch in [ExchangeName.KUCOIN, ExchangeName.OKX, ExchangeName.GATE]
            else Decimal("0.1")
        )

        print_test_info(exch, market_type, symbol, price)
        print(f"📦 Order Quantity: {qty}")

        print_section_header("BUY Order Test", 3)
        print(f"💲 Buy Price: ${exec_price} (100.4% of current price - should fill immediately)")

        print("\n🔄 Placing buy order...")
        order_snapshot = await rest_wrapper.place_order(
            symbol, OrderSide.BUY, qty, exec_price, OrderType.LIMIT, TimeInForce.GTC
        )

        print_order_result("Place Buy Order", order_snapshot)

        assert order_snapshot["status"] == 0, f"Order failed: {order_snapshot}"
        assert order_snapshot["data"].order_id is not None, "Order ID should not be None"
        time.sleep(2)
        print("\n🔄 Syncing buy order status...")
        sync_order_snapshot = await rest_wrapper.ccxt_sync_order(
            symbol,
            order_id=order_snapshot["data"].order_id,
            client_order_id=order_snapshot["data"].client_order_id,
        )

        if sync_order_snapshot["status"] == 0:
            print_order_result("Sync Buy Order", sync_order_snapshot)
            assert sync_order_snapshot["data"].order_status == OrderStatus.FILLED, "Buy order should be filled"
        else:
            print(
                f"❌ Sync buy order failed: {sync_order_snapshot['msg'] if sync_order_snapshot else 'Unknown error'}"
            )

        await rest_wrapper.ccxt_cancel_order(symbol, order_id=order_snapshot["data"].order_id)

        print_section_header("SELL Order Test", 3)

        print("\n🔄 Placing sell order...")
        exec_price = Decimal(str(price * 0.996)).quantize(Decimal("0.00"))
        print(f"💲 Sell Price: ${exec_price} (99.6% of current price - should fill immediately)")
        order_snapshot = await rest_wrapper.place_order(
            symbol,
            OrderSide.SELL,
            qty,
            exec_price,
            OrderType.LIMIT,
            TimeInForce.GTC,
            reduce_only=True,
        )

        print_order_result("Place Sell Order", order_snapshot)

        assert order_snapshot["status"] == 0, f"Order failed: {order_snapshot}"
        assert order_snapshot["data"].order_id is not None, "Order ID should not be None"
        time.sleep(2)
        print("\n🔄 Syncing sell order status...")
        sync_order_snapshot = await rest_wrapper.ccxt_sync_order(
            symbol,
            order_id=order_snapshot["data"].order_id,
            client_order_id=order_snapshot["data"].client_order_id,
        )

        if sync_order_snapshot["status"] == 0:
            print_order_result("Sync Sell Order", sync_order_snapshot)
            assert sync_order_snapshot["data"].order_status == OrderStatus.FILLED, "Sell order should be filled"
            assert sync_order_snapshot["data"].order_side == OrderSide.SELL, "Sell order side should be SELL"
        else:
            print(f"❌ Sync sell order failed: {sync_order_snapshot.get('msg', 'Unknown error')}")


@pytest.mark.asyncio
async def test_sync_order():
    exch = ExchangeName.DERIBIT
    acct_type = AccountType.UNIFIED
    rest_wrapper = get_rest_wrapper(exch, MarketType.SPOT, acct_type, get_account(exch, acct_type))
    order = await rest_wrapper.ccxt_sync_order(
        "SOL_USDC-PERPETUAL", order_id="USDC-57967437139", client_order_id="xclients"
    )
    # await rest_wrapper.ccxt_sync_open_orders("SOLUSDT")
    order_list = await rest_wrapper.ccxt_sync_open_orders("SOL_USDC-PERPETUAL")
    print(f"Open orders: {order_list}")


async def on_order_message(account_meta: AccountMeta, data: OrderSnapshot):
    """事件总线订单消息回调函数"""
    print(
        f"🔔 EventBus Received order message for account {account_meta.account_name} on exchange {account_meta.exch_name.name}"
    )
    print(f"   Order ID: {data.order_id}")
    print(f"   Client Order ID: {data.client_order_id}")
    print(f"   Status: {data.order_status.name}")
    print(f"   Side: {data.order_side.name}")
    print(f"   Type: {data.order_type.name}")
    print(f"   Price: ${data.price}")
    print(f"   Quantity: {data.qty}")
    print(f"   Time in Force: {data.order_time_in_force.name}")
    if data.order_status == OrderStatus.FILLED:
        print(f"   Filled Price: ${data.avg_price}")
        print(f"   Filled Quantity: {data.filled_qty}")
        print(f"   Fee: {data.fee}({data.fee_ccy})")
    if data.order_status == OrderStatus.REJECTED:
        print(f"   Reject Reason: {data.rejected_reason}")
        print(f"   Reject Message: {data.rejected_message}")
    assert is_ms_ts(data.place_ack_ts), "place_ack_ts should be a valid millisecond timestamp"
    assert is_ms_ts(data.exch_update_ts), "exch_update_ts should be a valid millisecond timestamp"
    assert is_ms_ts(data.local_update_ts), "client_update_ts should be a valid millisecond timestamp"


async def test_event_bus_for_exchange(
    exch: ExchangeName,
    acct_type: AccountType,
    market_type: MarketType = MarketType.SPOT,
):
    """为指定交易所测试事件总线功能"""
    print_section_header(f"EVENT BUS TEST - {exch.name}", 3)

    account = get_account(exch, acct_type)
    rest_wrapper = get_rest_wrapper(exch, market_type, acct_type, account)

    # 订阅事件回调
    rest_wrapper.subscribe_callback(Event.ORDER, on_order_message)

    symbol = get_symbol("SOL", exch, market_type)
    print(f"📈 Testing symbol: {symbol}")
    print("🔄 Fetching current price...")

    priceResp = await rest_wrapper.get_price(symbol)
    if priceResp["status"] != 0:
        print(f"❌ Failed to get price for {symbol}: {priceResp.get('msg', 'Unknown error')}")
        return

    price: float = priceResp["data"]
    exec_price = Decimal(str(price * 0.996)).quantize(Decimal("0.00"))
    qty = (
        Decimal("1")
        if market_type == MarketType.UPERP and exch in [ExchangeName.KUCOIN, ExchangeName.OKX, ExchangeName.GATE]
        else Decimal("0.1")
    )

    print(f"💰 Current Price: ${price}")
    print(f"📦 Order Quantity: {qty}")
    print(f"💲 Order Price: ${exec_price} (99.6% of current price)")

    # 使用事件总线下单
    place_instruction = PlaceOrderInstruction(
        symbol=symbol,
        order_side=OrderSide.BUY,
        qty=qty,
        price=exec_price,
        order_type=OrderType.LIMIT,
        order_time_in_force=TimeInForce.IOC,
        reduce_only=False,
        client_order_id=f"xclients{int(time.time() * 1000)}",
    )

    print("🔄 Submitting order via event bus...")
    await rest_wrapper.submit_place_order(place_instruction)

    # 等待事件处理
    await asyncio.sleep(2)
    print(f"✅ Event bus test completed for {exch.name}")


async def test_market_order_with_quote_qty(exch, acct_type, trade_market_types=[MarketType.UPERP, MarketType.SPOT]):
    """测试使用 quote_qty 的市价单 - 一买一卖自动平仓"""
    print_section_header("MARKET ORDER WITH QUOTE_QTY TEST", 1)

    ccy = "SOL"
    account = get_account(exch, acct_type)

    for market_type in trade_market_types:
        print_section_header(f"Testing {exch.name} - {market_type.name}", 2)

        rest_wrapper = get_rest_wrapper(exch, market_type, acct_type, account)
        symbol = get_symbol(ccy, exch, market_type)

        quote_qty = Decimal("10")  # 花 10 USDT 买入

        print(f"💵 Order Amount (quote): {quote_qty} USDT")

        # ========== 第一步：市价买单（使用 quote_qty） ==========
        print("\n🔄 Placing MARKET BUY order with quote_qty...")
        buy_order = await rest_wrapper.place_order(
            symbol,
            OrderSide.BUY,
            qty=None,
            price=None,
            order_type=OrderType.MARKET,
            extras={"quote_qty": quote_qty},
        )

        print(buy_order)

        assert buy_order["status"] == 0, f"Buy order failed: {buy_order}"
        assert buy_order["data"].order_id is not None, "Buy order ID should not be None"
        assert (
            buy_order["data"].order_type == OrderType.MARKET
        ), f"Order type should be MARKET, got {buy_order['data'].order_type}"

        if ExchangeName.GATE == exch:
            filled_qty = buy_order["data"].filled_qty
        else:
            # 市价单应该立即成交或被拒绝
            if buy_order["data"].order_status == OrderStatus.FILLED:
                print(f"✅ Market BUY order filled immediately")
                print(f"   Filled Qty: {buy_order['data'].filled_qty}")
                print(f"   Avg Price: ${buy_order['data'].avg_price}")
                filled_qty = buy_order["data"].filled_qty
            elif buy_order["data"].order_status == OrderStatus.REJECTED:
                print(f"⚠️ Market BUY order rejected: {buy_order['data'].rejected_message}")
                filled_qty = None
            else:
                # 如果不是立即成交，等待一下再同步
                time.sleep(1)
                sync_order = await rest_wrapper.ccxt_sync_order(
                    symbol,
                    order_id=buy_order["data"].order_id,
                    client_order_id=buy_order["data"].client_order_id,
                )
                print_order_result("Sync Market BUY Order", sync_order)
                assert sync_order["data"].order_status == OrderStatus.FILLED, "Market BUY order should be filled"
                filled_qty = sync_order["data"].filled_qty

        # 验证买单的 qty 是 base_qty（实际成交的币数量）
        assert filled_qty is not None and filled_qty > 0, "Filled qty should be greater than 0"
        print(f"✅ Buy order qty validation passed: {filled_qty} {ccy}")

        # ========== 第二步：市价卖单（使用 qty 平仓） ==========
        if filled_qty and filled_qty > 0:
            print(f"\n🔄 Placing MARKET SELL order with qty={filled_qty} to close position...")
            sell_order = await rest_wrapper.place_order(
                symbol,
                OrderSide.SELL,
                qty=filled_qty,  # 使用实际成交的数量（base_qty）
                price=None,
                order_type=OrderType.MARKET,
            )
            print_order_result("Place Market SELL Order (qty)", sell_order)

            assert sell_order["status"] == 0, f"Sell order failed: {sell_order}"
            assert sell_order["data"].order_id is not None, "Sell order ID should not be None"
            assert (
                sell_order["data"].order_type == OrderType.MARKET
            ), f"Order type should be MARKET, got {sell_order['data'].order_type}"

            # 验证卖单状态
            if sell_order["data"].order_status == OrderStatus.FILLED:
                print(f"✅ Market SELL order filled immediately")
                print(f"   Filled Qty: {sell_order['data'].filled_qty}")
                print(f"   Avg Price: ${sell_order['data'].avg_price}")
                sell_filled_qty = sell_order["data"].filled_qty
            elif sell_order["data"].order_status == OrderStatus.REJECTED:
                print(f"⚠️ Market SELL order rejected: {sell_order['data'].rejected_message}")
                sell_filled_qty = None
            else:
                time.sleep(1)
                sync_sell_order = await rest_wrapper.ccxt_sync_order(
                    symbol,
                    order_id=sell_order["data"].order_id,
                    client_order_id=sell_order["data"].client_order_id,
                )
                print_order_result("Sync Market SELL Order", sync_sell_order)
                assert sync_sell_order["data"].order_status == OrderStatus.FILLED, "Market SELL order should be filled"
                sell_filled_qty = sync_sell_order["data"].filled_qty

            # 验证卖单的 qty 是 base_qty
            assert sell_filled_qty is not None and sell_filled_qty > 0, "Sell filled qty should be greater than 0"
            print(f"✅ Sell order qty validation passed: {sell_filled_qty} {ccy}")
            print(f"✅ Position closed successfully!")
        else:
            print(f"⚠️ Skipping sell order as buy order was not filled")


async def use_quote_qty(exch, acct_type, trade_market_types):
    ccy = "SOL"
    account = get_account(exch, acct_type)

    for market_type in trade_market_types:
        rest_wrapper = get_rest_wrapper(exch, market_type, acct_type, account)
        symbol = get_symbol(ccy, exch, market_type)

        quote_qty = Decimal("10")  # 花 10 USDT 买入

        # ========== 第一步：市价买单（使用 quote_qty） ==========
        print("\n🔄 Placing MARKET BUY order with quote_qty...")
        buy_order = await rest_wrapper.place_order(
            symbol,
            OrderSide.BUY,
            qty=None,
            price=None,
            order_type=OrderType.MARKET,
            extras={"quote_qty": quote_qty},
        )


async def use_base_qty(exch, acct_type, trade_market_types):
    ccy = "SOL"
    account = get_account(exch, acct_type)

    for market_type in trade_market_types:
        rest_wrapper = get_rest_wrapper(exch, market_type, acct_type, account)
        symbol = get_symbol(ccy, exch, market_type)
        sell_order = await rest_wrapper.place_order(
            symbol,
            OrderSide.BUY,
            qty=0.1,  # 使用实际成交的数量（base_qty）
            price=None,
            order_type=OrderType.MARKET,
        )
        print_order_result("Place Market SELL Order (qty)", sell_order)


@pytest.mark.asyncio
async def test_binance():
    print_section_header("BINANCE EXCHANGE TESTING", 1)

    exch = ExchangeName.BINANCE
    acct_type = AccountType.NORMAL

    print(f"🏦 Exchange: {exch.name}")
    print(f"👤 Account Type: {acct_type.name}")
    print(f"📊 Markets: SPOT, UPERP")

    # await test_get_order_history(exch, acct_type, trade_market_types=[MarketType.SPOT])
    # await test_market_order_with_quote_qty(exch, acct_type, trade_market_types=[MarketType.SPOT])
    await test_post_only_order(exch, acct_type, trade_market_types=[MarketType.SPOT, MarketType.UPERP])
    await test_ioc_order(exch, acct_type, trade_market_types=[MarketType.SPOT, MarketType.UPERP])
    await test_place_cancel_sync_order(exch, acct_type, trade_market_types=[MarketType.SPOT, MarketType.UPERP])
    await test_filled_order(exch, acct_type, trade_market_types=[MarketType.SPOT, MarketType.UPERP])

    # 测试事件总线
    await test_event_bus_for_exchange(exch, acct_type, MarketType.SPOT)
    await test_event_bus_for_exchange(exch, acct_type, MarketType.UPERP)

    print_section_header("BINANCE TESTING COMPLETED", 1)
    print("🎉 All tests passed successfully!")


@pytest.mark.asyncio
async def test_binanceunified():
    print_section_header("BINANCE UNIFIED TESTING", 1)

    exch = ExchangeName.BINANCE
    acct_type = AccountType.UNIFIED

    print(f"🏦 Exchange: {exch.name}")
    print(f"👤 Account Type: {acct_type.name}")
    print(f"📊 Markets: SPOT, UPERP")

    await test_post_only_order(exch, acct_type, trade_market_types=[MarketType.SPOT, MarketType.UPERP])
    await test_ioc_order(exch, acct_type, trade_market_types=[MarketType.SPOT, MarketType.UPERP])
    await test_place_cancel_sync_order(exch, acct_type, trade_market_types=[MarketType.SPOT, MarketType.UPERP])
    await test_filled_order(exch, acct_type, trade_market_types=[MarketType.SPOT, MarketType.UPERP])

    # 测试事件总线
    await test_event_bus_for_exchange(exch, acct_type, MarketType.SPOT)
    await test_event_bus_for_exchange(exch, acct_type, MarketType.UPERP)

    print_section_header("BINANCE UNIFIED TESTING COMPLETED", 1)
    print("🎉 All tests passed successfully!")


@pytest.mark.asyncio
async def test_bybit():
    print_section_header("BYBIT EXCHANGE TESTING", 1)

    exch = ExchangeName.BYBIT
    acct_type = AccountType.UNIFIED

    print(f"🏦 Exchange: {exch.name}")
    print(f"👤 Account Type: {acct_type.name}")
    print(f"📊 Markets: SPOT, UPERP")

    # await test_get_order_history(exch, acct_type, trade_market_types=[MarketType.UPERP, MarketType.SPOT])
    # await test_market_order_with_quote_qty(exch, acct_type, trade_market_types=[MarketType.SPOT])
    await test_post_only_order(exch, acct_type, trade_market_types=[MarketType.SPOT, MarketType.UPERP])
    await test_ioc_order(exch, acct_type, trade_market_types=[MarketType.SPOT, MarketType.UPERP])
    await test_place_cancel_sync_order(exch, acct_type, trade_market_types=[MarketType.SPOT, MarketType.UPERP])
    await test_filled_order(exch, acct_type, trade_market_types=[MarketType.UPERP])

    # 测试事件总线
    await test_event_bus_for_exchange(exch, acct_type, MarketType.SPOT)
    await test_event_bus_for_exchange(exch, acct_type, MarketType.UPERP)

    print_section_header("BYBIT TESTING COMPLETED", 1)
    print("🎉 All tests passed successfully!")


@pytest.mark.asyncio
async def test_bitget():
    print_section_header("BITGET EXCHANGE TESTING", 1)

    exch = ExchangeName.BITGET
    acct_type = AccountType.NORMAL

    print(f"🏦 Exchange: {exch.name}")
    print(f"👤 Account Type: {acct_type.name}")
    print(f"📊 Markets: SPOT, UPERP")

    # await test_get_order_history(exch, acct_type, trade_market_types=[MarketType.SPOT])
    # await test_market_order_with_quote_qty(exch, acct_type, trade_market_types=[MarketType.SPOT])
    await test_ioc_order(exch, acct_type, trade_market_types=[MarketType.SPOT, MarketType.UPERP])
    await test_place_cancel_sync_order(exch, acct_type, trade_market_types=[MarketType.SPOT, MarketType.UPERP])
    await test_filled_order(exch, acct_type, trade_market_types=[MarketType.UPERP])

    # 测试事件总线
    await test_event_bus_for_exchange(exch, acct_type, MarketType.SPOT)
    await test_event_bus_for_exchange(exch, acct_type, MarketType.UPERP)

    print_section_header("BITGET TESTING COMPLETED", 1)
    print("🎉 All tests passed successfully!")


@pytest.mark.asyncio
async def test_okx():
    print_section_header("OKX EXCHANGE TESTING", 1)

    exch = ExchangeName.OKX
    acct_type = AccountType.NORMAL

    print(f"🏦 Exchange: {exch.name}")
    print(f"👤 Account Type: {acct_type.name}")
    print(f"📊 Markets: SPOT, UPERP")

    # await test_get_order_history(exch, acct_type, trade_market_types=[MarketType.SPOT])
    # await test_market_order_with_quote_qty(exch, acct_type, trade_market_types=[MarketType.SPOT])
    await test_post_only_order(exch, acct_type, trade_market_types=[MarketType.SPOT, MarketType.UPERP])
    await test_ioc_order(exch, acct_type, trade_market_types=[MarketType.SPOT, MarketType.UPERP])
    await test_place_cancel_sync_order(exch, acct_type, trade_market_types=[MarketType.SPOT, MarketType.UPERP])
    await test_filled_order(exch, acct_type, trade_market_types=[MarketType.UPERP])

    # 测试事件总线
    await test_event_bus_for_exchange(exch, acct_type, MarketType.SPOT)
    await test_event_bus_for_exchange(exch, acct_type, MarketType.UPERP)

    print_section_header("OKX TESTING COMPLETED", 1)
    print("🎉 All tests passed successfully!")


@pytest.mark.asyncio
async def test_coinex():
    print_section_header("COINEX EXCHANGE TESTING", 1)

    exch = ExchangeName.COINEX
    acct_type = AccountType.NORMAL

    print(f"🏦 Exchange: {exch.name}")
    print(f"👤 Account Type: {acct_type.name}")
    print(f"📊 Markets: SPOT, UPERP")

    # await test_get_order_history(exch, acct_type, trade_market_types=[MarketType.SPOT])
    # await test_market_order_with_quote_qty(exch, acct_type, trade_market_types=[MarketType.SPOT])
    await test_post_only_order(exch, acct_type, trade_market_types=[MarketType.UPERP])
    await test_ioc_order(exch, acct_type, trade_market_types=[MarketType.SPOT, MarketType.UPERP])
    await test_place_cancel_sync_order(exch, acct_type, trade_market_types=[MarketType.SPOT, MarketType.UPERP])
    await test_filled_order(exch, acct_type, trade_market_types=[MarketType.UPERP])

    # 测试事件总线
    await test_event_bus_for_exchange(exch, acct_type, MarketType.SPOT)
    await test_event_bus_for_exchange(exch, acct_type, MarketType.UPERP)

    print_section_header("COINEX TESTING COMPLETED", 1)
    print("🎉 All tests passed successfully!")


@pytest.mark.asyncio
async def test_deribit():
    print_section_header("DERIBIT EXCHANGE TESTING", 1)

    exch = ExchangeName.DERIBIT
    acct_type = AccountType.UNIFIED

    print(f"🏦 Exchange: {exch.name}")
    print(f"👤 Account Type: {acct_type.name}")
    print(f"📊 Markets: UPERP")

    # await test_market_order_with_quote_qty(exch, acct_type, trade_market_types=[MarketType.UPERP, MarketType.SPOT])
    await test_post_only_order(exch, acct_type, trade_market_types=[MarketType.UPERP])
    await test_ioc_order(exch, acct_type, trade_market_types=[MarketType.UPERP])
    await test_place_cancel_sync_order(exch, acct_type, trade_market_types=[MarketType.UPERP])
    await test_filled_order(exch, acct_type, trade_market_types=[MarketType.UPERP])

    # 测试事件总线
    await test_event_bus_for_exchange(exch, acct_type, MarketType.UPERP)

    print_section_header("DERIBIT TESTING COMPLETED", 1)
    print("🎉 All tests passed successfully!")


@pytest.mark.asyncio
async def test_gate():
    print_section_header("GATE EXCHANGE TESTING", 1)

    exch = ExchangeName.GATE
    acct_type = AccountType.UNIFIED

    print(f"🏦 Exchange: {exch.name}")
    print(f"👤 Account Type: {acct_type.name}")
    print(f"📊 Markets: SPOT, UPERP")

    # await test_get_order_history(exch, acct_type, trade_market_types=[MarketType.SPOT])
    # await test_market_order_with_quote_qty(exch, acct_type, trade_market_types=[MarketType.SPOT])
    await test_post_only_order(exch, acct_type, trade_market_types=[MarketType.SPOT, MarketType.UPERP])
    await test_ioc_order(exch, acct_type, trade_market_types=[MarketType.SPOT, MarketType.UPERP])
    await test_place_cancel_sync_order(exch, acct_type, trade_market_types=[MarketType.SPOT, MarketType.UPERP])
    await test_filled_order(exch, acct_type, trade_market_types=[MarketType.UPERP])

    # 测试事件总线
    await test_event_bus_for_exchange(exch, acct_type, MarketType.SPOT)
    await test_event_bus_for_exchange(exch, acct_type, MarketType.UPERP)

    print_section_header("GATE TESTING COMPLETED", 1)
    print("🎉 All tests passed successfully!")


@pytest.mark.asyncio
async def test_kucoin():
    print_section_header("KUCOIN EXCHANGE TESTING", 1)

    exch = ExchangeName.KUCOIN
    acct_type = AccountType.NORMAL

    print(f"🏦 Exchange: {exch.name}")
    print(f"👤 Account Type: {acct_type.name}")
    print(f"📊 Markets: SPOT, UPERP")

    await test_post_only_order(exch, acct_type, trade_market_types=[MarketType.SPOT, MarketType.UPERP])
    await test_ioc_order(exch, acct_type, trade_market_types=[MarketType.SPOT, MarketType.UPERP])
    await test_place_cancel_sync_order(exch, acct_type, trade_market_types=[MarketType.SPOT, MarketType.UPERP])
    await test_filled_order(exch, acct_type, trade_market_types=[MarketType.UPERP])

    # 测试事件总线
    await test_event_bus_for_exchange(exch, acct_type, MarketType.SPOT)
    await test_event_bus_for_exchange(exch, acct_type, MarketType.UPERP)

    print_section_header("KUCOIN TESTING COMPLETED", 1)
    print("🎉 All tests passed successfully!")


# 删除原来的单独的 test_using_event_bus 函数，因为现在每个交易所都会测试


# 可以添加一个综合的事件总线测试函数
@pytest.mark.asyncio
async def test_all_exchanges_event_bus():
    """测试所有交易所的事件总线功能"""
    print_section_header("ALL EXCHANGES EVENT BUS TESTING", 1)

    exchanges_config = [
        (ExchangeName.BINANCE, AccountType.NORMAL, [MarketType.SPOT, MarketType.UPERP]),
        (
            ExchangeName.BINANCE,
            AccountType.UNIFIED,
            [MarketType.SPOT, MarketType.UPERP],
        ),
        (ExchangeName.BYBIT, AccountType.UNIFIED, [MarketType.SPOT, MarketType.UPERP]),
        (ExchangeName.BITGET, AccountType.NORMAL, [MarketType.SPOT, MarketType.UPERP]),
        (ExchangeName.OKX, AccountType.NORMAL, [MarketType.SPOT, MarketType.UPERP]),
        (ExchangeName.COINEX, AccountType.NORMAL, [MarketType.SPOT, MarketType.UPERP]),
        (ExchangeName.DERIBIT, AccountType.UNIFIED, [MarketType.UPERP]),
        (ExchangeName.GATE, AccountType.UNIFIED, [MarketType.SPOT, MarketType.UPERP]),
        (ExchangeName.KUCOIN, AccountType.NORMAL, [MarketType.SPOT, MarketType.UPERP]),
    ]

    for exch, acct_type, market_types in exchanges_config:
        print_section_header(f"Testing {exch.name} - {acct_type.name}", 2)

        try:
            for market_type in market_types:
                await test_event_bus_for_exchange(exch, acct_type, market_type)
                await asyncio.sleep(1)  # 给事件处理一些时间
        except Exception as e:
            print(f"❌ Event bus test failed for {exch.name}: {str(e)}")
            continue

    print_section_header("ALL EXCHANGES EVENT BUS TESTING COMPLETED", 1)
    print("🎉 All event bus tests completed!")


async def test_get_order_history(exch, acct_type, trade_market_types=[MarketType.UPERP, MarketType.SPOT]):
    """测试获取历史订单"""
    print_section_header("GET ORDER HISTORY TEST", 1)

    ccy1 = "SOL"
    ccy2 = "BTC"
    account = get_account(exch, acct_type)

    for market_type in trade_market_types:
        print_section_header(f"Testing {exch.name} - {market_type.name}", 2)

        rest_wrapper = get_rest_wrapper(exch, market_type, acct_type, account)
        symbol1 = get_symbol(ccy1, exch, market_type)
        symbol2 = get_symbol(ccy2, exch, market_type)

        # 设置时间范围：最近1天
        end_time = int(time.time() * 1000)
        start_time = end_time - (1 * 24 * 60 * 60 * 1000)  # 1天前

        print(f"📅 Time Range:")
        print(f"   Start: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time / 1000))}")
        print(f"   End: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_time / 1000))}")

        # 获取历史订单
        print("\n🔄 Fetching order history...")
        order_history_resp = await rest_wrapper.get_order_history(
            start_time=start_time,
            end_time=end_time,
            symbol_list=[symbol1, symbol2],
        )

        # 验证响应
        assert order_history_resp[
                   "status"] == 0, f"Failed to get order history: {order_history_resp.get('msg', 'Unknown error')}"

        order_data = order_history_resp["data"]

        # 检查返回的数据结构
        assert isinstance(order_data, dict), "Order history data should be a dict"

