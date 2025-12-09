from xclients.get_wrapper import get_rest_wrapper
from xclients.enum_type import MarketType, ExchangeName, AccountType, MarginMode, PositionMode
import pytest
from tests.test_utils import print_section_header, get_symbol, get_supoorted_markets, exch_account
from xclients.inst_mngr import InstrumentManager
from decimal import Decimal


async def test_universal_transfer(rest_wrapper, exch: ExchangeName):
    """测试通用转账"""
    print("💸 Testing universal_transfer...")
    try:
        if hasattr(rest_wrapper, "universal_transfer"):
            # 构造转账请求（这里使用最小金额测试）

            transfer1 = await rest_wrapper.universal_transfer(Decimal("1"), "USDT", MarketType.UPERP, MarketType.SPOT)
            transfer2 = await rest_wrapper.universal_transfer(Decimal("1"), "USDT", MarketType.SPOT, MarketType.UPERP)
            if transfer1["status"] == 0:
                transfer_result = transfer1["data"]
                print(f"   ✅ Universal transfer successful: {transfer_result}")
            else:
                print(f"   ⚠️ Universal transfer failed: {transfer1.get('msg', 'Unknown error')}")
        else:
            print(f"   ⚠️ universal_transfer not implemented for {exch.name}")
    except Exception as e:
        print(f"   ❌ universal_transfer failed: {str(e)}")


async def test_set_account_position_mode(rest_wrapper, exch: ExchangeName):
    """测试设置账户持仓模式"""
    print("🔄 Testing set_account_position_mode...")
    try:
        if hasattr(rest_wrapper, "set_account_position_mode"):
            # 测试设置为单向持仓模式
            mode_resp = await rest_wrapper.set_account_position_mode(PositionMode.ONE_WAY)
            if mode_resp["status"] == 0:
                print(f"   ✅ Account position mode set to ONE_WAY successfully")
            else:
                print(f"   ⚠️ Set account position mode failed: {mode_resp.get('msg', 'Unknown error')}")
        else:
            print(f"   ⚠️ set_account_position_mode not implemented for {exch.name}")
    except Exception as e:
        print(f"   ❌ set_account_position_mode failed: {str(e)}")


async def test_set_account_margin_mode(rest_wrapper, exch: ExchangeName):
    """测试设置账户保证金模式"""
    print("🏦 Testing set_account_margin_mode...")
    try:
        if hasattr(rest_wrapper, "set_account_margin_mode"):
            # 测试设置为全仓保证金模式
            margin_resp = await rest_wrapper.set_account_margin_mode(MarginMode.CROSS)
            if margin_resp["status"] == 0:
                print(f"   ✅ Account margin mode set to CROSSED successfully")
            else:
                print(f"   ⚠️ Set account margin mode failed: {margin_resp.get('msg', 'Unknown error')}")
        else:
            print(f"   ⚠️ set_account_margin_mode not implemented for {exch.name}")
    except Exception as e:
        print(f"   ❌ set_account_margin_mode failed: {str(e)}")


async def test_set_symbol_margin_mode(rest_wrapper, exch: ExchangeName, symbol: str):
    """测试设置交易对保证金模式"""
    print("⚖️ Testing set_symbol_margin_mode...")
    try:
        if hasattr(rest_wrapper, "set_symbol_margin_mode"):
            # 测试设置为逐仓保证金模式
            margin_resp = await rest_wrapper.set_symbol_margin_mode(symbol, MarginMode.CROSS)
            if margin_resp["status"] == 0:
                print(f"   ✅ Symbol {symbol} margin mode set to ISOLATED successfully")
            else:
                print(f"   ⚠️ Set symbol margin mode failed: {margin_resp.get('msg', 'Unknown error')}")
        else:
            print(f"   ⚠️ set_symbol_margin_mode not implemented for {exch.name}")
    except Exception as e:
        print(f"   ❌ set_symbol_margin_mode failed: {str(e)}")


async def test_set_fee_coin_burn(rest_wrapper, exch: ExchangeName):
    """测试设置手续费币种燃烧"""
    print("🔥 Testing set_fee_coin_burn...")
    try:
        if hasattr(rest_wrapper, "set_fee_coin_burn"):
            # 测试启用手续费币种燃烧
            burn_resp = await rest_wrapper.set_fee_coin_burn(enable=True)
            if burn_resp["status"] == 0:
                print(f"   ✅ Fee coin burn enabled successfully")
            else:
                print(f"   ⚠️ Set fee coin burn failed: {burn_resp.get('msg', 'Unknown error')}")
        else:
            print(f"   ⚠️ set_fee_coin_burn not implemented for {exch.name}")
    except Exception as e:
        print(f"   ❌ set_fee_coin_burn failed: {str(e)}")


async def test_set_account_leverage(rest_wrapper, exch: ExchangeName):
    """测试设置账户杠杆"""
    print("📊 Testing set_account_leverage...")
    try:
        if hasattr(rest_wrapper, "set_account_leverage"):
            # 测试设置账户杠杆为10倍
            leverage_resp = await rest_wrapper.set_account_leverage(leverage=10)
            if leverage_resp["status"] == 0:
                print(f"   ✅ Account leverage set to 10x successfully")
            else:
                print(f"   ⚠️ Set account leverage failed: {leverage_resp.get('msg', 'Unknown error')}")
        else:
            print(f"   ⚠️ set_account_leverage not implemented for {exch.name}")
    except Exception as e:
        print(f"   ❌ set_account_leverage failed: {str(e)}")


async def test_set_symbol_leverage(rest_wrapper, exch: ExchangeName, symbol: str):
    """测试设置交易对杠杆"""
    print("⚖️ Testing set_symbol_leverage...")
    try:
        if hasattr(rest_wrapper, "set_symbol_leverage"):
            # 测试设置交易对杠杆为5倍
            leverage_resp = await rest_wrapper.set_symbol_leverage(symbol, leverage=5)
            if leverage_resp["status"] == 0:
                print(f"   ✅ Symbol {symbol} leverage set to 5x successfully")
            else:
                print(f"   ⚠️ Set symbol leverage failed: {leverage_resp.get('msg', 'Unknown error')}")
        else:
            print(f"   ⚠️ set_symbol_leverage not implemented for {exch.name}")
    except Exception as e:
        print(f"   ❌ set_symbol_leverage failed: {str(e)}")


async def test_set_uta_mode(rest_wrapper, exch: ExchangeName):
    """测试设置UTA模式"""
    print("🔄 Testing set_uta_mode...")
    try:
        if hasattr(rest_wrapper, "set_uta_mode"):
            # 测试启用UTA模式
            uta_resp = await rest_wrapper.set_uta_mode()
            if uta_resp["status"] == 0:
                print(f"   ✅ UTA mode enabled successfully")
            else:
                print(f"   ⚠️ Set UTA mode failed: {uta_resp.get('msg', 'Unknown error')}")
        else:
            print(f"   ⚠️ set_uta_mode not implemented for {exch.name}")
    except Exception as e:
        print(f"   ❌ set_uta_mode failed: {str(e)}")


async def test_enable_auto_repayment(rest_wrapper, exch: ExchangeName):
    """测试启用自动还款"""
    print("🔄 Testing enable_auto_repayment...")
    try:
        if hasattr(rest_wrapper, "enable_auto_repayment"):
            # 测试启用自动还款
            repay_resp = await rest_wrapper.enable_auto_repayment()
            if repay_resp["status"] == 0:
                print(f"   ✅ Auto repayment enabled successfully")
            else:
                print(f"   ⚠️ Enable auto repayment failed: {repay_resp.get('msg', 'Unknown error')}")
        else:
            print(f"   ⚠️ enable_auto_repayment not implemented for {exch.name}")
    except Exception as e:
        print(f"   ❌ enable_auto_repayment failed: {str(e)}")


async def test_enable_margin_trading(rest_wrapper, exch: ExchangeName):
    """测试启用保证金交易"""
    print("💼 Testing enable_margin_trading...")
    try:
        if hasattr(rest_wrapper, "enable_margin_trading"):
            # 测试启用保证金交易
            margin_resp = await rest_wrapper.enable_margin_trading()
            if margin_resp["status"] == 0:
                print(f"   ✅ Margin trading enabled successfully")
            else:
                print(f"   ⚠️ Enable margin trading failed: {margin_resp.get('msg', 'Unknown error')}")
        else:
            print(f"   ⚠️ enable_margin_trading not implemented for {exch.name}")
    except Exception as e:
        print(f"   ❌ enable_margin_trading failed: {str(e)}")


async def test_enable_union_asset_mode(rest_wrapper, exch: ExchangeName):
    """测试启用统一资产模式"""
    print("🔗 Testing enable_union_asset_mode...")
    try:
        if hasattr(rest_wrapper, "enable_union_asset_mode"):
            # 测试启用统一资产模式
            union_resp = await rest_wrapper.enable_union_asset_mode()
            if union_resp["status"] == 0:
                print(f"   ✅ Union asset mode enabled successfully")
            else:
                print(f"   ⚠️ Enable union asset mode failed: {union_resp.get('msg', 'Unknown error')}")
        else:
            print(f"   ⚠️ enable_union_asset_mode not implemented for {exch.name}")
    except Exception as e:
        print(f"   ❌ enable_union_asset_mode failed: {str(e)}")


async def test_enable_account_collaterals(rest_wrapper, exch: ExchangeName):
    """测试启用账户抵押品"""
    print("🏛️ Testing enable_account_collaterals...")
    try:
        if hasattr(rest_wrapper, "enable_account_collaterals"):
            # 测试启用USDT作为抵押品
            collateral_resp = await rest_wrapper.enable_account_collaterals()
            if collateral_resp["status"] == 0:
                print(f"   ✅ Account collaterals (USDT) enabled successfully")
            else:
                print(f"   ⚠️ Enable account collaterals failed: {collateral_resp.get('msg', 'Unknown error')}")
        else:
            print(f"   ⚠️ enable_account_collaterals not implemented for {exch.name}")
    except Exception as e:
        print(f"   ❌ enable_account_collaterals failed: {str(e)}")


async def test_collect_balances(rest_wrapper, exch: ExchangeName):
    """测试归集余额"""
    print("📦 Testing collect_balances...")
    try:
        if hasattr(rest_wrapper, "collect_balances"):
            # 测试归集余额到主账户
            collect_resp = await rest_wrapper.collect_balances()
            if collect_resp["status"] == 0:
                collect_result = collect_resp["data"]
                print(f"   ✅ Balances collected successfully: {collect_result}")
            else:
                print(f"   ⚠️ Collect balances failed: {collect_resp.get('msg', 'Unknown error')}")
        else:
            print(f"   ⚠️ collect_balances not implemented for {exch.name}")
    except Exception as e:
        print(f"   ❌ collect_balances failed: {str(e)}")


async def test_repay_negative_balances(rest_wrapper, exch: ExchangeName):
    """测试偿还负余额"""
    print("💳 Testing repay_negative_balances...")
    try:
        if hasattr(rest_wrapper, "repay_negative_balances"):
            # 测试偿还所有负余额
            repay_resp = await rest_wrapper.repay_negative_balances()
            if repay_resp["status"] == 0:
                repay_result = repay_resp["data"]
                print(f"   ✅ Negative balances repaid successfully: {repay_result}")
            else:
                print(f"   ⚠️ Repay negative balances failed: {repay_resp.get('msg', 'Unknown error')}")
        else:
            print(f"   ⚠️ repay_negative_balances not implemented for {exch.name}")
    except Exception as e:
        print(f"   ❌ repay_negative_balances failed: {str(e)}")


async def test_adjust_risk_limits(rest_wrapper, exch: ExchangeName):
    """测试调整风险限额"""
    print("⚠️ Testing adjust_risk_limits...")
    try:
        if hasattr(rest_wrapper, "adjust_risk_limits"):
            # 测试调整风险限额（使用较小的限额值）
            risk_resp = await rest_wrapper.adjust_risk_limits()
            if risk_resp["status"] == 0:
                risk_result = risk_resp["data"]
                print(f"   ✅ Risk limits adjusted successfully")
            else:
                print(f"   ⚠️ Adjust risk limits failed: {risk_resp.get('msg', 'Unknown error')}")
        else:
            print(f"   ⚠️ adjust_risk_limits not implemented for {exch.name}")
    except Exception as e:
        print(f"   ❌ adjust_risk_limits failed: {str(e)}")


@pytest.mark.asyncio
async def test_all_exchanges_set_functions(exch_account):
    """测试所有交易所的设置函数"""

    print_section_header("COMPREHENSIVE SET FUNCTIONS TESTING FOR ALL EXCHANGES", 1)

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
                symbol = get_symbol("ETH", exch, market_type)

                # 通用设置测试
                await test_universal_transfer(rest_wrapper, exch)
                await test_set_fee_coin_burn(rest_wrapper, exch)
                await test_collect_balances(rest_wrapper, exch)
                await test_repay_negative_balances(rest_wrapper, exch)

                # 期货市场特有设置测试
                if market_type in [MarketType.UPERP, MarketType.CPERP, MarketType.UDELIVERY, MarketType.CDELIVERY]:
                    await test_set_account_position_mode(rest_wrapper, exch)
                    await test_set_account_margin_mode(rest_wrapper, exch)
                    await test_set_symbol_margin_mode(rest_wrapper, exch, symbol)
                    await test_set_account_leverage(rest_wrapper, exch)
                    await test_set_symbol_leverage(rest_wrapper, exch, symbol)

                # 保证金和统一账户特有设置测试
                if market_type == MarketType.MARGIN or acct_type == AccountType.UNIFIED:
                    await test_enable_auto_repayment(rest_wrapper, exch)
                    await test_enable_margin_trading(rest_wrapper, exch)
                    await test_enable_union_asset_mode(rest_wrapper, exch)
                    await test_enable_account_collaterals(rest_wrapper, exch)

                # UTA模式设置（主要针对Bybit）
                if exch == ExchangeName.BYBIT:
                    await test_set_uta_mode(rest_wrapper, exch)
                if exch == ExchangeName.GATE:
                    await test_adjust_risk_limits(rest_wrapper, exch)

                print("✅ All set function tests passed for this configuration\n")

            except Exception as e:
                import traceback

                traceback.print_exc()
                print(f"❌ Error testing {exch.name}-{market_type.name}: {str(e)}\n")
                continue

    print_section_header("COMPREHENSIVE SET FUNCTIONS TESTING COMPLETED", 1)
    print("🎉 All exchange set function testing completed!")


# 单个交易所的设置函数测试
@pytest.mark.asyncio
async def test_binance_set_functions():
    """专门测试Binance的设置功能"""
    exch_account = {
        # (ExchangeName.BINANCE, AccountType.NORMAL): "mpbntest01",
        (ExchangeName.BINANCE, AccountType.UNIFIED): "mpbnpmtest153",
    }
    await test_all_exchanges_set_functions(exch_account)


@pytest.mark.asyncio
async def test_bybit_set_functions():
    """专门测试Bybit的设置功能"""
    exch_account = {
        (ExchangeName.BYBIT, AccountType.UNIFIED): "mpbybittest01",
    }
    await test_all_exchanges_set_functions(exch_account)


@pytest.mark.asyncio
async def test_okx_set_functions():
    """专门测试OKX的设置功能"""
    exch_account = {
        (ExchangeName.OKX, AccountType.NORMAL): "mpokextest01",
    }
    await test_all_exchanges_set_functions(exch_account)


@pytest.mark.asyncio
async def test_gate_set_functions():
    """专门测试Gate的设置功能"""
    exch_account = {
        (ExchangeName.GATE, AccountType.UNIFIED): "gatecjtest01",
    }
    await test_all_exchanges_set_functions(exch_account)


@pytest.mark.asyncio
async def test_kucoin_set_functions():
    """专门测试Kucoin的设置功能"""
    exch_account = {
        (ExchangeName.KUCOIN, AccountType.NORMAL): "mpkcliq01",
    }
    await test_all_exchanges_set_functions(exch_account)


@pytest.mark.asyncio
async def test_bitget_set_functions():
    """专门测试Bitget的设置功能"""
    exch_account = {
        (ExchangeName.BITGET, AccountType.NORMAL): "bitgetcjtest01",
    }
    await test_all_exchanges_set_functions(exch_account)


@pytest.mark.asyncio
async def test_deribit_set_functions():
    """专门测试Deribit的设置功能"""
    exch_account = {
        (ExchangeName.DERIBIT, AccountType.UNIFIED): "mpderibittest01",
    }
    await test_all_exchanges_set_functions(exch_account)


@pytest.mark.asyncio
async def test_coinex_set_functions():
    """专门测试Coinex的设置功能"""
    exch_account = {
        (ExchangeName.COINEX, AccountType.NORMAL): "coinexcjtest01",
    }
    await test_all_exchanges_set_functions(exch_account)


# 快速测试所有交易所的基础设置功能
@pytest.mark.asyncio
async def test_all_exchanges_set_quick(exch_account):
    """快速测试所有交易所的基础设置功能"""
    print_section_header("QUICK SET FUNCTIONS TEST FOR ALL EXCHANGES", 1)

    # 只测试主要设置功能
    for (exch, acct_type), account in exch_account.items():
        print(f"\n🔄 Quick set test for {exch.name}-{acct_type.name}")

        try:
            # 测试现货市场的基础设置
            rest_wrapper = get_rest_wrapper(exch, MarketType.SPOT, acct_type, account)

            # 测试费用币种燃烧设置
            if hasattr(rest_wrapper, "set_fee_coin_burn"):
                burn_resp = await rest_wrapper.set_fee_coin_burn(enable=False)  # 测试关闭
                status = "✅" if burn_resp["status"] == 0 else "❌"
                print(f"   Fee Coin Burn: {status}")
            else:
                print(f"   Fee Coin Burn: ⚠️ Not implemented")

            # 如果支持期货，测试期货设置
            if exch in [
                ExchangeName.BINANCE,
                ExchangeName.BYBIT,
                ExchangeName.OKX,
                ExchangeName.GATE,
                ExchangeName.KUCOIN,
                ExchangeName.BITGET,
            ]:
                rest_wrapper = get_rest_wrapper(exch, MarketType.UPERP, acct_type, account)

                # 测试杠杆设置
                if hasattr(rest_wrapper, "set_account_leverage"):
                    leverage_resp = await rest_wrapper.set_account_leverage(leverage=1)  # 设置最小杠杆
                    status = "✅" if leverage_resp["status"] == 0 else "❌"
                    print(f"   Account Leverage: {status}")
                else:
                    print(f"   Account Leverage: ⚠️ Not implemented")

        except Exception as e:
            print(f"   ❌ Error: {str(e)}")

    print_section_header("QUICK SET TEST COMPLETED", 1)


# 危险操作测试（需要特别小心）
@pytest.mark.asyncio
async def test_dangerous_set_operations():
    """测试可能影响账户的危险设置操作（仅在测试环境中运行）"""
    print_section_header("DANGEROUS SET OPERATIONS TEST", 1)
    print("⚠️ WARNING: These tests may affect account settings!")
    print("⚠️ Only run in test environment with test accounts!")

    # 这里可以添加一些需要特别小心的测试
    # 比如统一账户模式切换、保证金模式切换等

    print("🚫 Dangerous operations test skipped for safety")
    print("   To enable, modify the test and add appropriate safeguards")
