from xclients.data_type import (
    Tickers,
    OrderBook,
    Position,
    Balance,
    AccountInfo,
    LoanData,
    Fundamentals,
    FundingRate,
    FundingRateSimple,
    FundingRatesCur,
    FundingRatesHis,
    FundingRatesSimple,
    OrderSnapshotData,
    OrderSnapshot,
    Trade,
    InterestRates,
    InterestRate,
)
from xclients.get_wrapper import get_rest_wrapper
from xclients.enum_type import MarketType, ExchangeName, AccountType, Interval, MarginMode
import pytest
from tests.test_utils import print_section_header, get_symbol, get_supoorted_markets, is_ms_ts
from xclients.inst_mngr import InstrumentManager
from datetime import datetime, timedelta
from xclients.base_wrapper import BaseRestWrapper


async def test_get_assets(rest_wrapper):
    """测试获取资产"""
    print("💰 Testing get_assets...")
    try:
        # 测试直接API调用
        assets_api = await rest_wrapper.get_assets(from_redis=False)
        if assets_api["status"] == 0:
            asset_count = len(assets_api["data"])
            print(f"   ✅ API call success - Found {asset_count} assets")
            if asset_count > 0:
                sample_asset: Balance = next(iter(assets_api["data"].values()))
                assert is_ms_ts(sample_asset.ts)  # 确保时间戳是毫秒级别
                print(f"      Sample asset: {sample_asset}")
        else:
            print(f"   ⚠️ API call failed: {assets_api.get('msg', 'Unknown error')}")

    except Exception as e:
        print(f"   ❌ get_assets failed: {str(e)}")


async def test_get_positions(rest_wrapper):
    """测试获取持仓"""
    print("📈 Testing get_positions...")
    try:
        # 测试直接API调用
        positions_api = await rest_wrapper.get_positions(from_redis=False)
        if positions_api["status"] == 0:
            position_count = len(positions_api["data"])
            print(f"   ✅ API call success - Found {position_count} positions")
            if position_count > 0:
                sample_position: Position = next(iter(positions_api["data"].values()))
                assert is_ms_ts(sample_position.ts)
                print(f"      Sample position: {sample_position}")
        else:
            print(f"   ⚠️ API call failed: {positions_api.get('msg', 'Unknown error')}")

    except Exception as e:
        print(f"   ❌ get_positions failed: {str(e)}")


async def test_get_account_info(rest_wrapper):
    """测试获取账户信息"""
    print("🔍 Testing get_account_info...")
    try:
        account_info_resp = await rest_wrapper.get_account_info()
        if account_info_resp["status"] == 0:
            print(f"   ✅ Account info retrieved successfully")
            account_info: AccountInfo = account_info_resp["data"]
            print(f"      Account Type: {account_info}")
        else:
            print(f"   ⚠️ Account info failed: {account_info_resp.get('msg', 'Unknown error')}")
    except Exception as e:
        print(f"   ❌ get_account_info failed: {str(e)}")


async def test_get_tickers(rest_wrapper: BaseRestWrapper):
    """测试获取ticker数据"""
    print("📊 Testing get_tickers...")
    try:
        tickers = await rest_wrapper.get_tickers()
        if tickers["status"] == 0:
            ticker_data: Tickers = tickers["data"]
            ticker_count = len(ticker_data)
            sample_ticker = next(iter(ticker_data.values()))
            print(f"   ✅ Tickers retrieved successfully - Found {ticker_count} tickers")
            print(f"      Sample ticker: {sample_ticker}")
            assert is_ms_ts(sample_ticker.ts)  # 确保时间戳是毫秒级别
            assert is_ms_ts(sample_ticker.update_ts)  # 确保时间戳是毫秒级别
        else:
            print(f"   ⚠️ Tickers failed: {tickers.get('msg', 'Unknown error')}")
    except Exception as e:
        print(f"   ❌ get_tickers failed: {str(e)}")


async def test_get_orderbook_snapshot(rest_wrapper, exch: ExchangeName, market_type: MarketType, symbol: str):
    """测试获取订单簿快照"""
    print("📖 Testing get_orderbook_snapshot...")
    try:
        if hasattr(rest_wrapper, "get_orderbook_snapshot"):
            orderbook = await rest_wrapper.get_orderbook_snapshot(symbol)
            if orderbook["status"] == 0:
                data: OrderBook = orderbook["data"]
                bids_count = len(data.bids) if hasattr(data, "bids") else 0
                asks_count = len(data.asks) if hasattr(data, "asks") else 0
                assert is_ms_ts(data.exch_ts)  # 确保时间戳是毫秒级别
                assert is_ms_ts(data.recv_ts)  # 确保时间戳是毫秒级别
                print(f"   ✅ Orderbook retrieved - {bids_count} bids, {asks_count} asks")
            else:
                print(f"   ⚠️ Orderbook failed: {orderbook.get('msg', 'Unknown error')}")
        else:
            print(f"   ⚠️ get_orderbook_snapshot not implemented for {exch.name}")
    except Exception as e:
        print(f"   ❌ get_orderbook_snapshot failed: {str(e)}")


async def test_get_fundamentals(rest_wrapper):
    """测试获取基础数据"""
    print("📈 Testing get_fundamentals...")
    try:
        fundamentals_resp = await rest_wrapper.get_fundamentals()
        if fundamentals_resp["status"] == 0:
            fundamentals: Fundamentals = fundamentals_resp["data"]
            print(f"   ✅ Fundamentals retrieved successfully")
            sample_fundamentals = next(iter(fundamentals.values()))
            print(f"      Sample fundamentals: {sample_fundamentals}")
        else:
            print(f"   ⚠️ Fundamentals failed: {fundamentals_resp.get('msg', 'Unknown error')}")
    except Exception as e:
        print(f"   ❌ get_fundamentals failed: {str(e)}")


async def test_get_loans(rest_wrapper):
    """测试获取借贷信息"""
    print("💳 Testing get_loans...")
    try:
        loans_resp = await rest_wrapper.get_loans()
        if loans_resp["status"] == 0:
            data: LoanData = loans_resp["data"]
            loan_count = len(data) if data else 0
            print(f"   ✅ Loans retrieved successfully - Found {loan_count} loans")
            if loan_count > 0:
                sample_loan = next(iter(data.values()))
                print(f"      Sample loan: {sample_loan}")
        else:
            print(f"   ⚠️ Loans failed: {loans_resp.get('msg', 'Unknown error')}")
    except Exception as e:
        print(f"   ❌ get_loans failed: {str(e)}")


# 在现有代码基础上添加以下测试函数


async def test_get_collateral_ratio(rest_wrapper):
    """测试获取抵押率"""
    print("🔗 Testing get_collateral_ratio...")
    try:
        if hasattr(rest_wrapper, "get_collateral_ratio"):
            ratio_resp = await rest_wrapper.get_collateral_ratio()
            if ratio_resp["status"] == 0:
                ratio = ratio_resp["data"]
                print(f"   ✅ Collateral ratio retrieved successfully: {ratio}")
            else:
                print(f"   ⚠️ Collateral ratio failed: {ratio_resp.get('msg', 'Unknown error')}")
        else:
            print(f"   ⚠️ get_collateral_ratio not implemented")
    except Exception as e:
        print(f"   ❌ get_collateral_ratio failed: {str(e)}")


async def test_get_account_vip_level(rest_wrapper):
    """测试获取账户VIP等级"""
    print("👑 Testing get_account_vip_level...")
    try:
        if hasattr(rest_wrapper, "get_account_vip_level"):
            vip_resp = await rest_wrapper.get_account_vip_level()
            if vip_resp["status"] == 0:
                vip_level = vip_resp["data"]
                print(f"   ✅ VIP level retrieved successfully: {vip_level}")
            else:
                print(f"   ⚠️ VIP level failed: {vip_resp.get('msg', 'Unknown error')}")
        else:
            print(f"   ⚠️ get_account_vip_level not implemented")
    except Exception as e:
        print(f"   ❌ get_account_vip_level failed: {str(e)}")


async def test_get_symbol_leverage_and_margin_mode(rest_wrapper, symbol: str):
    """测试获取交易对杠杆和保证金模式"""
    print("⚖️ Testing get_symbol_leverage_and_margin_mode...")
    try:
        if hasattr(rest_wrapper, "get_symbol_leverage_and_margin_mode"):
            leverage_resp = await rest_wrapper.get_symbol_leverage_and_margin_mode(symbol)
            if leverage_resp["status"] == 0:
                leverage_info = leverage_resp["data"]
                print(f"   ✅ Leverage and margin mode retrieved successfully")
                print(f"      Symbol: {symbol}")
                print(f"      Info: {leverage_info}")
            else:
                print(f"   ⚠️ Leverage and margin mode failed: {leverage_resp.get('msg', 'Unknown error')}")
        else:
            print(f"   ⚠️ get_symbol_leverage_and_margin_mode not implemented")
    except Exception as e:
        print(f"   ❌ get_symbol_leverage_and_margin_mode failed: {str(e)}")


async def test_get_prices(rest_wrapper):
    """测试获取价格"""
    print("💲 Testing get_prices...")
    try:
        prices_resp = await rest_wrapper.get_prices()
        if prices_resp["status"] == 0:
            prices = prices_resp["data"]
            price_count = len(prices) if prices else 0
            print(f"   ✅ Prices retrieved successfully - Found {price_count} prices")
            if price_count > 0:
                sample_price = next(iter(prices.values()))
                print(f"      Sample price: {sample_price}")
        else:
            print(f"   ⚠️ get_prices not implemented")
    except Exception as e:
        print(f"   ❌ get_prices failed: {str(e)}")


async def test_get_trade_history(rest_wrapper, symbol: str):
    """测试获取交易历史"""
    print("📈 Testing get_trade_history...")
    try:

        end_dt = datetime.now()
        start_dt = end_dt - timedelta(days=1)
        start_time = int(start_dt.timestamp() * 1000)
        end_time = int(end_dt.timestamp() * 1000)

        trades_resp = await rest_wrapper.get_trade_history(start_time, end_time, [symbol])
        if trades_resp["status"] == 0:
            trades = trades_resp["data"]
            trade_count = len(trades) if trades else 0
            print(f"   ✅ Trade history retrieved successfully - Found {trade_count} trades")
            if trade_count > 0:
                sample_trade: Trade = next(iter(trades.values()))[0]
                print(f"      Sample trade: {sample_trade}")
                assert is_ms_ts(sample_trade.create_ts)
                assert is_ms_ts(sample_trade.fill_ts)
        else:
            print(f"   ⚠️ Trade history failed: {trades_resp.get('msg', 'Unknown error')}")
    except Exception as e:
        print(f"   ❌ get_trade_history failed: {str(e)}")


async def test_get_order_history(rest_wrapper, symbol: str):
    """测试获取订单历史"""
    print("📜 Testing get_order_history...")
    try:

        end_dt = datetime.now()
        start_dt = end_dt - timedelta(days=1)
        start_time = int(start_dt.timestamp() * 1000)
        end_time = int(end_dt.timestamp() * 1000)

        orders_resp = await rest_wrapper.get_order_history(start_time, end_time, [symbol])
        if orders_resp["status"] == 0:
            orders: OrderSnapshotData = orders_resp["data"]
            order_count = len(orders) if orders else 0
            print(f"   ✅ Order history retrieved successfully - Found {order_count} orders")
            if order_count > 0:
                sample_order: OrderSnapshot = next(iter(orders.values()))[0]
                print(f"      Sample order: {sample_order}")
                assert is_ms_ts(sample_order.local_update_ts)
                assert is_ms_ts(sample_order.exch_update_ts)
                assert is_ms_ts(sample_order.place_ack_ts)
        else:
            print(f"   ⚠️ Order history failed: {orders_resp.get('msg', 'Unknown error')}")
    except Exception as e:
        print(f"   ❌ get_order_history failed: {str(e)}")


async def test_get_funding_fee(rest_wrapper, symbol: str):
    """测试获取资金费用"""
    print("💰 Testing get_funding_fee...")
    try:
        if hasattr(rest_wrapper, "get_funding_fee"):
            fee_resp = await rest_wrapper.get_funding_fee(look_back=5, symbol_list=[symbol])
            if fee_resp["status"] == 0:
                fees = fee_resp["data"]
                fee_count = len(fees) if fees else 0
                print(f"   ✅ Funding fees retrieved successfully - Found {fee_count} fees")
                if fee_count > 0:
                    sample_fee = next(iter(fees.values()))
                    print(f"      Sample fee: {sample_fee}")
            else:
                print(f"   ⚠️ Funding fees failed: {fee_resp.get('msg', 'Unknown error')}")
        else:
            print(f"   ⚠️ get_funding_fee not implemented")
    except Exception as e:
        print(f"   ❌ get_funding_fee failed: {str(e)}")


async def test_get_historical_funding_rate(rest_wrapper, symbol: str):
    """测试获取历史资金费率"""
    print("📊 Testing get_historical_funding_rate...")
    try:
        if hasattr(rest_wrapper, "get_historical_funding_rate"):

            end_dt = datetime.now()
            start_dt = end_dt - timedelta(days=7)
            start_time = int(start_dt.timestamp() * 1000)

            rates_resp = await rest_wrapper.get_historical_funding_rate([symbol], start_time, 1)
            if rates_resp["status"] == 0:
                rates: FundingRatesHis = rates_resp["data"]
                rate_count = len(rates[symbol]) if symbol in rates else 0
                print(f"   ✅ Historical funding rates retrieved successfully - Found {rate_count} rates")
                if rate_count > 0:
                    sample_rate: FundingRate = rates[symbol][0]
                    print(f"      Sample rate: {sample_rate}")
                    assert is_ms_ts(sample_rate.funding_ts)
            else:
                print(f"   ⚠️ Historical funding rates failed: {rates_resp.get('msg', 'Unknown error')}")
        else:
            print(f"   ⚠️ get_historical_funding_rate not implemented")
    except Exception as e:
        print(f"   ❌ get_historical_funding_rate failed: {str(e)}")


async def test_get_current_funding_rate(rest_wrapper, symbol: str):
    """测试获取当前资金费率"""
    print("🔄 Testing get_current_funding_rate...")
    try:
        if hasattr(rest_wrapper, "get_current_funding_rate"):
            rates_resp = await rest_wrapper.get_current_funding_rate([symbol])
            if rates_resp["status"] == 0:
                rates: FundingRatesCur = rates_resp["data"]
                # rate_count = len(rates[symbol]) if symbol in rates else 0
                # print(f"   ✅ Current funding rates retrieved successfully - Found {rate_count} rates")
                # if rate_count > 0:
                #     sample_rate: FundingRate = rates[symbol][0]
                sample_rate: FundingRate = rates[symbol]
                print(f"      Sample rate: {sample_rate}")
                assert is_ms_ts(sample_rate.funding_ts)
            else:
                print(f"   ⚠️ Current funding rates failed: {rates_resp.get('msg', 'Unknown error')}")
        else:
            print(f"   ⚠️ get_current_funding_rate not implemented")
    except Exception as e:
        print(f"   ❌ get_current_funding_rate failed: {str(e)}")

async def test_get_current_simple_funding_rate(rest_wrapper, symbol: str):
    """测试获取当前简易版资金费率"""
    print("🔄 Testing get_current_simple_funding_rate...")
    try:
        if hasattr(rest_wrapper, "get_current_simple_funding_rate"):
            rates_resp = await rest_wrapper.get_current_simple_funding_rate([symbol])
            if rates_resp["status"] == 0:
                rates: FundingRatesSimple = rates_resp["data"]
                sample_rate: FundingRateSimple = rates[symbol]
                print(f"      Sample rate: {sample_rate}")
                assert is_ms_ts(sample_rate.funding_ts)
            else:
                print(f"   ⚠️ Current funding rates failed: {rates_resp.get('msg', 'Unknown error')}")
        else:
            print(f"   ⚠️ get_current_simple_funding_rate not implemented")
    except Exception as e:
        print(f"   ❌ get_current_simple_funding_rate failed: {str(e)}")


async def test_get_historical_kline(rest_wrapper, symbol: str):
    """测试获取历史K线"""
    print("📈 Testing get_historical_kline...")
    try:
        if hasattr(rest_wrapper, "get_historical_kline"):

            end_dt = datetime.now()
            start_dt = end_dt - timedelta(hours=24)
            start_time = int(start_dt.timestamp() * 1000)
            end_time = int(end_dt.timestamp() * 1000)

            kline_resp = await rest_wrapper.get_historical_kline(symbol, Interval._1h, start_time, end_time)
            if kline_resp["status"] == 0:
                klines = kline_resp["data"]
                kline_count = len(klines) if klines else 0
                print(f"   ✅ Historical klines retrieved successfully - Found {kline_count} klines")
                if kline_count > 0:
                    sample_kline = klines[0]
                    print(f"      Sample kline: {sample_kline}")
                    assert is_ms_ts(sample_kline.start_ts)
            else:
                print(f"   ⚠️ Historical klines failed: {kline_resp.get('msg', 'Unknown error')}")
        else:
            print(f"   ⚠️ get_historical_kline not implemented")
    except Exception as e:
        print(f"   ❌ get_historical_kline failed: {str(e)}")


async def test_get_leverage(rest_wrapper, symbol: str):
    """测试获取杠杆"""
    print("⚖️ Testing get_leverage...")
    try:
        if hasattr(rest_wrapper, "get_leverage"):
            leverage_resp = await rest_wrapper.get_leverage(symbol, MarginMode.CROSS)
            if leverage_resp["status"] == 0:
                leverage = leverage_resp["data"]
                print(f"   ✅ Leverage retrieved successfully: {leverage}")
            else:
                print(f"   ⚠️ Leverage failed: {leverage_resp.get('msg', 'Unknown error')}")
        else:
            print(f"   ⚠️ get_leverage not implemented")
    except Exception as e:
        print(f"   ❌ get_leverage failed: {str(e)}")


async def test_get_max_open_quantity(rest_wrapper, symbol: str):
    """测试获取最大开仓数量"""
    print("📊 Testing get_max_open_quantity...")
    try:
        if hasattr(rest_wrapper, "get_max_open_quantity"):
            from xclients.enum_type import OrderSide

            max_qty_resp = await rest_wrapper.get_max_open_quantity(symbol, OrderSide.BUY)
            if max_qty_resp["status"] == 0:
                max_qty = max_qty_resp["data"]
                print(f"   ✅ Max open quantity retrieved successfully: {max_qty}")
            else:
                print(f"   ⚠️ Max open quantity failed: {max_qty_resp.get('msg', 'Unknown error')}")
        else:
            print(f"   ⚠️ get_max_open_quantity not implemented")
    except Exception as e:
        print(f"   ❌ get_max_open_quantity failed: {str(e)}")


async def test_get_max_open_notional(rest_wrapper, symbol: str):
    """测试获取最大开仓名义价值"""
    print("💵 Testing get_max_open_notional...")
    try:
        if hasattr(rest_wrapper, "get_max_open_notional"):
            from xclients.enum_type import OrderSide

            max_notional_resp = await rest_wrapper.get_max_open_notional(symbol, MarginMode.CROSS)
            if max_notional_resp["status"] == 0:
                max_notional = max_notional_resp["data"]
                print(f"   ✅ Max open notional retrieved successfully: {max_notional}")
            else:
                print(f"   ⚠️ Max open notional failed: {max_notional_resp.get('msg', 'Unknown error')}")
        else:
            print(f"   ⚠️ get_max_open_notional not implemented")
    except Exception as e:
        print(f"   ❌ get_max_open_notional failed: {str(e)}")


async def test_get_commission_rate(rest_wrapper, symbol: str):
    """测试获取手续费率"""
    print("💳 Testing get_commission_rate...")
    try:
        if hasattr(rest_wrapper, "get_commission_rate"):
            commission_resp = await rest_wrapper.get_commission_rate(symbol)
            if commission_resp["status"] == 0:
                commission = commission_resp["data"]
                print(f"   ✅ Commission rate retrieved successfully: {commission}")
            else:
                print(f"   ⚠️ Commission rate failed: {commission_resp.get('msg', 'Unknown error')}")
        else:
            print(f"   ⚠️ get_commission_rate not implemented")
    except Exception as e:
        print(f"   ❌ get_commission_rate failed: {str(e)}")


async def test_get_interest_rates_cur(rest_wrapper):
    """测试获取利率"""
    print("📈 Testing get_interest_rates_cur...")
    try:
        if hasattr(rest_wrapper, "get_interest_rates_cur"):
            rates_resp = await rest_wrapper.get_interest_rates_cur(asset="USDT")
            if rates_resp["status"] == 0:
                rates: InterestRates = rates_resp["data"]
                rate_count = len(rates) if rates else 0
                print(f"   ✅ Interest rates retrieved successfully - Found {rate_count} rates")
                if rate_count > 0:
                    sample_rate: InterestRate = rates[0]
                    print(f"      Sample rate: {sample_rate}")
            else:
                print(f"   ⚠️ Interest rates failed: {rates_resp.get('msg', 'Unknown error')}")
        else:
            print(f"   ⚠️ get_interest_rates_cur not implemented")
    except Exception as e:
        print(f"   ❌ get_interest_rates_cur failed: {str(e)}")


async def test_get_interest_rates_his(rest_wrapper):
    """测试获取利率"""
    print("📈 Testing get_interest_rates_his...")
    try:
        if hasattr(rest_wrapper, "get_interest_rates_his"):
            end_dt = datetime.now()
            start_dt = end_dt - timedelta(days=7)
            start_time = int(start_dt.timestamp() * 1000)
            end_time = int(end_dt.timestamp() * 1000)
            rates_resp = await rest_wrapper.get_interest_rates_his(
                asset="USDT", start_time=start_time, end_time=end_time
            )
            if rates_resp["status"] == 0:
                rates: InterestRates = rates_resp["data"]
                rate_count = len(rates) if rates else 0
                print(f"   ✅ Interest rates retrieved successfully - Found {rate_count} rates")
                if rate_count > 0:
                    sample_rate: InterestRate = rates[0]
                    print(f"      Sample rate: {sample_rate}")
            else:
                print(f"   ⚠️ Interest rates failed: {rates_resp.get('msg', 'Unknown error')}")
        else:
            print(f"   ⚠️ get_interest_rates_his not implemented")
    except Exception as e:
        print(f"   ❌ get_interest_rates_his failed: {str(e)}")


async def test_get_long_short_ratio(rest_wrapper, symbol: str):
    """测试获取多空比"""
    print("⚖️ Testing get_long_short_ratio...")
    try:
        if hasattr(rest_wrapper, "get_long_short_ratio"):
            ratio_resp = await rest_wrapper.get_long_short_ratio(symbol, 10, Interval._1h)
            if ratio_resp["status"] == 0:
                ratio = ratio_resp["data"]
                print(f"   ✅ Long short ratio retrieved successfully: {ratio}")
            else:
                print(f"   ⚠️ Long short ratio failed: {ratio_resp.get('msg', 'Unknown error')}")
        else:
            print(f"   ⚠️ get_long_short_ratio not implemented")
    except Exception as e:
        print(f"   ❌ get_long_short_ratio failed: {str(e)}")


async def test_get_equity(rest_wrapper):
    """测试获取权益"""
    print("💎 Testing get_equity...")
    try:
        if hasattr(rest_wrapper, "get_equity"):
            equity_resp = await rest_wrapper.get_equity()
            if equity_resp["status"] == 0:
                equity = equity_resp["data"]
                print(f"   ✅ Equity retrieved successfully: {equity}")
            else:
                print(f"   ⚠️ Equity failed: {equity_resp.get('msg', 'Unknown error')}")
        else:
            print(f"   ⚠️ get_equity not implemented")
    except Exception as e:
        print(f"   ❌ get_equity failed: {str(e)}")


# 修改主测试函数，添加新的测试
@pytest.mark.asyncio
async def test_all_exchanges_fetch_functions(exch_account):
    """测试所有交易所的各种函数"""

    print_section_header("COMPREHENSIVE API TESTING FOR ALL EXCHANGES", 1)

    for (exch, acct_type), account in exch_account.items():
        print_section_header(f"Testing {exch.name} - {acct_type.name}", 2)
        print(f"🏦 Exchange: {exch.name}")
        print(f"👤 Account Type: {acct_type.name}")
        print(f"📝 Account: {account}")
        inst_mngr = InstrumentManager()
        # 获取该交易所支持的市场类型
        markets = get_supoorted_markets(exch)

        for market_type in markets:
            print_section_header(f"Market Type: {market_type.name}", 3)

            try:
                rest_wrapper = get_rest_wrapper(exch, market_type, acct_type, account)
                await inst_mngr.init_instruments_from_wrapper(rest_wrapper)
                symbol = get_symbol("SOL", exch, market_type)

                # 基础数据测试
                await test_get_assets(rest_wrapper)
                await test_get_account_info(rest_wrapper)
                await test_get_tickers(rest_wrapper)
                await test_get_orderbook_snapshot(rest_wrapper, exch, market_type, symbol)
                await test_get_fundamentals(rest_wrapper)
                await test_get_prices(rest_wrapper)

                # 期货市场特有测试
                if market_type in [MarketType.UPERP, MarketType.CPERP, MarketType.UDELIVERY, MarketType.CDELIVERY]:
                    await test_get_positions(rest_wrapper)
                    await test_get_collateral_ratio(rest_wrapper)
                    await test_get_leverage(rest_wrapper, symbol)
                    await test_get_max_open_quantity(rest_wrapper, symbol)
                    await test_get_max_open_notional(rest_wrapper, symbol)
                    await test_get_symbol_leverage_and_margin_mode(rest_wrapper, symbol)

                if market_type in [MarketType.UPERP, MarketType.CPERP]:
                    await test_get_funding_fee(rest_wrapper, symbol)
                    await test_get_historical_funding_rate(rest_wrapper, symbol)
                    if market_type in [MarketType.UPERP]:
                        await test_get_current_funding_rate(rest_wrapper, symbol)
                        await test_get_long_short_ratio(rest_wrapper, symbol)

                # 保证金市场特有测试
                if market_type == MarketType.SPOT:
                    await test_get_loans(rest_wrapper)
                    await test_get_interest_rates_cur(rest_wrapper)
                    await test_get_interest_rates_his(rest_wrapper)

                if market_type in [MarketType.MARGIN, MarketType.SPOT]:
                    await test_get_equity(rest_wrapper)
                    await test_get_account_vip_level(rest_wrapper)

                # 通用测试
                await test_get_trade_history(rest_wrapper, symbol)
                await test_get_order_history(rest_wrapper, symbol)
                await test_get_historical_kline(rest_wrapper, symbol)
                await test_get_commission_rate(rest_wrapper, symbol)

                print("✅ All tests passed for this configuration\n")

            except Exception as e:
                import traceback

                traceback.print_exc()
                print(f"❌ Error testing {exch.name}-{market_type.name}: {str(e)}\n")
                continue

    print_section_header("COMPREHENSIVE TESTING COMPLETED", 1)
    print("🎉 All exchange testing completed!")


# 现有的单个交易所测试函数保持不变...


# 专门测试单个交易所的函数
@pytest.mark.asyncio
async def test_binance_fetch_functions():
    """专门测试Binance的所有功能"""
    exch_account = {
        (ExchangeName.BINANCE, AccountType.NORMAL): "mpbntest01",
        (ExchangeName.BINANCE, AccountType.UNIFIED): "mpbnpmtest153",
    }
    await test_all_exchanges_fetch_functions(exch_account)


@pytest.mark.asyncio
async def test_bybit_fetch_functions():
    """专门测试Bybit的所有功能"""
    exch_account = {
        (ExchangeName.BYBIT, AccountType.UNIFIED): "mpbybittest01",
    }
    await test_all_exchanges_fetch_functions(exch_account)


@pytest.mark.asyncio
async def test_okx_fetch_functions():
    """专门测试OKX的所有功能"""
    exch_account = {
        (ExchangeName.OKX, AccountType.NORMAL): "mpokextest01",
    }
    await test_all_exchanges_fetch_functions(exch_account)


@pytest.mark.asyncio
async def test_gate_fetch_functions():
    """专门测试Gate的所有功能"""
    exch_account = {
        (ExchangeName.GATE, AccountType.UNIFIED): "gatecjtest01",
    }
    await test_all_exchanges_fetch_functions(exch_account)


@pytest.mark.asyncio
async def test_kucoin_fetch_functions():
    """专门测试Kucoin的所有功能"""
    exch_account = {
        (ExchangeName.KUCOIN, AccountType.NORMAL): "mpkcliq01",
    }
    await test_all_exchanges_fetch_functions(exch_account)


@pytest.mark.asyncio
async def test_bitget_fetch_functions():
    """专门测试Bitget的所有功能"""
    exch_account = {
        (ExchangeName.BITGET, AccountType.NORMAL): "bitgetcjtest01",
    }
    await test_all_exchanges_fetch_functions(exch_account)


@pytest.mark.asyncio
async def test_deribit_fetch_functions():
    """专门测试Deribit的所有功能"""
    exch_account = {
        (ExchangeName.DERIBIT, AccountType.UNIFIED): "mpderibittest01",
    }
    await test_all_exchanges_fetch_functions(exch_account)


@pytest.mark.asyncio
async def test_coinex_fetch_functions():
    """专门测试Coinex的所有功能"""
    exch_account = {
        (ExchangeName.COINEX, AccountType.NORMAL): "coinexcjtest01",
    }
    await test_all_exchanges_fetch_functions(exch_account)


# 快速测试所有交易所的基础功能
@pytest.mark.asyncio
async def test_all_exchanges_quick(exch_account):
    """快速测试所有交易所的基础功能"""
    print_section_header("QUICK TEST FOR ALL EXCHANGES", 1)

    # 只测试主要功能
    for (exch, acct_type), account in exch_account.items():
        print(f"\n🔄 Quick test for {exch.name}-{acct_type.name}")

        try:
            # 测试现货
            rest_wrapper = get_rest_wrapper(exch, MarketType.SPOT, acct_type, account)
            assets = await rest_wrapper.get_assets()
            status = "✅" if assets["status"] == 0 else "❌"
            print(f"   SPOT Assets: {status}")

            # 如果支持期货，测试期货
            if exch in [
                ExchangeName.BINANCE,
                ExchangeName.BYBIT,
                ExchangeName.OKX,
                ExchangeName.GATE,
                ExchangeName.KUCOIN,
                ExchangeName.BITGET,
            ]:
                rest_wrapper = get_rest_wrapper(exch, MarketType.UPERP, acct_type, account)
                positions = await rest_wrapper.get_positions()
                status = "✅" if positions["status"] == 0 else "❌"
                print(f"   UPERP Positions: {status}")

        except Exception as e:
            print(f"   ❌ Error: {str(e)}")

    print_section_header("QUICK TEST COMPLETED", 1)


@pytest.mark.asyncio
async def test_all_get_funding_rates():
    exch_account = {
        (ExchangeName.BINANCE, AccountType.NORMAL): "mpbntest01",
        (ExchangeName.BYBIT, AccountType.UNIFIED): "mpbybittest01",
        (ExchangeName.OKX, AccountType.NORMAL): "mpokextest01",
        (ExchangeName.GATE, AccountType.UNIFIED): "gatecjtest01",
        (ExchangeName.BITGET, AccountType.NORMAL): "bitgetcjtest01",
        (ExchangeName.DERIBIT, AccountType.UNIFIED): "mpderibittest01",
        (ExchangeName.COINEX, AccountType.NORMAL): "coinexcjtest01",
    }
    for exch_info, account in exch_account.items():
        exch, acct_type = exch_info
        inst_mngr = InstrumentManager()
        for market_type in [MarketType.UPERP, MarketType.CPERP]:
            rest_wrapper = get_rest_wrapper(exch, market_type, acct_type, account)
            await inst_mngr.init_instruments_from_wrapper(rest_wrapper)
            await test_get_historical_funding_rate(rest_wrapper, symbol=get_symbol("BTC", exch, market_type))
            if market_type == MarketType.UPERP:
                await test_get_current_funding_rate(rest_wrapper, symbol=get_symbol("BTC", exch, market_type))
                await test_get_current_simple_funding_rate(rest_wrapper, symbol=get_symbol("BTC", exch, market_type))
            print(exch_info, market_type, '\n')
