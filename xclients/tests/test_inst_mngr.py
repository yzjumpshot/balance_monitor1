from xclients.inst_mngr import Instrument, InstrumentManager
from xclients.enum_type import ExchangeName, MarketType, InstStatus
import asyncio


def test_all():
    inst_mngr = InstrumentManager()
    for market_type in ["SPOT", "UPERP"]:
        for exchange in ["BINANCE", "KUCOIN", "GATE", "BYBIT", "OKX"]:
            asyncio.run(inst_mngr.init_instruments(exchange, market_type))
            for inst in inst_mngr.get_insts_by_exchange(exchange=exchange, market_type=market_type).values():
                assert (
                    inst_mngr.get_exchange_symbol_by_unified_symbol(exchange, market_type, inst.unified_symbol)
                    == inst.exchange_symbol
                )
                assert (
                    inst_mngr.get_unified_symbol_by_exchange_symbol(exchange, market_type, inst.exchange_symbol)
                    == inst.unified_symbol
                )
                # add test for unified_symbol initialization
                assert isinstance(inst.unified_symbol, str), f"Invalid unified_symbol type for {inst.unified_symbol}"
                assert len(inst.unified_symbol) > 0, "unified_symbol is not initialized"
                # add test for price_multiplier initialization
                assert isinstance(
                    inst.price_multiplier, (int, float)
                ), f"Invalid price_multiplier type for {inst.price_multiplier}"
                assert (
                    inst.price_multiplier > 0
                ), "price_multiplier is not initialized or initialized to non-positive value"
                assert not inst.unified_symbol.startswith("1000"), str(inst)
    # assert inst.price_multiplier == 1000, f"price_multiplier[{inst.price_multiplier}]"
    # assert inst.unified_symbol == "SHIB_USDT", f"unified_symbol[{inst.unified_symbol}]"


def test_save_instruments_to_redis():
    inst_mngr = InstrumentManager()
    for market_type in ["SPOT", "UPERP"]:
        for exchange in ["BINANCE", "KUCOIN", "GATE", "BYBIT", "OKX"]:
            asyncio.run(inst_mngr.init_instruments(exchange, market_type))

    asyncio.run(inst_mngr.save_instruments_to_redis())


def test_init_instruments_from_redis():
    inst_mngr = InstrumentManager()
    for market_type in ["SPOT", "UPERP"]:
        for exchange in ["BINANCE", "KUCOIN", "GATE", "BYBIT", "OKX"]:
            asyncio.run(inst_mngr.init_instruments(exchange, market_type, from_redis=True))
            for inst in inst_mngr.get_insts_by_exchange(exchange=exchange, market_type=market_type).values():
                assert (
                    inst_mngr.get_exchange_symbol_by_unified_symbol(exchange, market_type, inst.unified_symbol)
                    == inst.exchange_symbol
                )
                assert (
                    inst_mngr.get_unified_symbol_by_exchange_symbol(exchange, market_type, inst.exchange_symbol)
                    == inst.unified_symbol
                )
                # add test for unified_symbol initialization
                assert isinstance(inst.unified_symbol, str), f"Invalid unified_symbol type for {inst.unified_symbol}"
                assert len(inst.unified_symbol) > 0, "unified_symbol is not initialized"
                # add test for price_multiplier initialization
                assert isinstance(
                    inst.price_multiplier, (int, float)
                ), f"Invalid price_multiplier type for {inst.price_multiplier}"
                assert (
                    inst.price_multiplier > 0
                ), "price_multiplier is not initialized or initialized to non-positive value"


def test_status():
    inst_mngr = InstrumentManager()
    for market_type in ["SPOT", "UPERP"]:
        for exchange in ["BINANCE", "KUCOIN", "GATE", "BYBIT", "OKX"]:
            asyncio.run(inst_mngr.init_instruments(exchange, market_type))
            for inst in inst_mngr.get_insts_by_exchange(exchange=exchange, market_type=market_type).values():
                if inst.status != InstStatus.TRADING:
                    assert inst.is_untradable


def test_all_ps_fu():
    print("\n")
    inst_mngr = InstrumentManager()
    # for market_type in ["SPOT", "UPERP"]:
    for market_type in ["CPERP", "CDELIVERY", "SPOT", "UPERP"]:
        for exchange in ["BINANCE", "KUCOIN", "GATE", "BYBIT", "OKX"]:
            if market_type == "CDELIVERY" and exchange in ["GATE"]:
                continue
            asyncio.run(inst_mngr.init_instruments(exchange, market_type))
            for inst in inst_mngr.get_insts_by_exchange(exchange=exchange, market_type=market_type).values():
                # print(exchange, market_type, inst.unified_symbol)
                assert (
                    inst_mngr.get_exchange_symbol_by_unified_symbol(exchange, market_type, inst.unified_symbol)
                    == inst.exchange_symbol
                )
                assert (
                    inst_mngr.get_unified_symbol_by_exchange_symbol(exchange, market_type, inst.exchange_symbol)
                    == inst.unified_symbol
                )
                if market_type not in ["UDELIVERY", "CDELIVERY"]:
                    assert inst.fu_contract_types == []
                # add test for unified_symbol initialization
                # print('exchange_symbol:',inst.exchange_symbol, '  symbol:' , inst.symbol, '  unified_symbol:',inst.unified_symbol)
                assert isinstance(inst.unified_symbol, str), f"Invalid unified_symbol type for {inst.unified_symbol}"
                assert len(inst.unified_symbol) > 0, "unified_symbol is not initialized"
                # add test for price_multiplier initialization
                assert isinstance(
                    inst.price_multiplier, (int, float)
                ), f"Invalid price_multiplier type for {inst.price_multiplier}"
                assert (
                    inst.price_multiplier > 0
                ), "price_multiplier is not initialized or initialized to non-positive value"
            if exchange == "KUCOIN" and market_type != "SPOT":
                base_coin = "XBT"
            else:
                base_coin = "BTC"
            if market_type == "CPERP":
                inst = inst_mngr.get_inst_by_unified_symbol(exchange, market_type, f"{base_coin}_USD")
            elif market_type == "CDELIVERY":
                inst = inst_mngr.get_inst_by_unified_symbol(exchange, market_type, f"{base_coin}_USD_250926")
            else:
                inst = inst_mngr.get_inst_by_unified_symbol(exchange, market_type, f"{base_coin}_USDT")
            print(f"{market_type}-{exchange}>>>>: {inst}")


def test_all_lfu():
    print("\n")
    inst_mngr = InstrumentManager()
    for market_type in [MarketType.CDELIVERY, MarketType.UDELIVERY]:
        for exchange in ["BINANCE", "KUCOIN", "GATE", "BYBIT", "OKX"]:
            # for exchange in ["BYBIT", "KUCOIN"]:
            print("-------------->", exchange, market_type)
            if exchange == "GATE":
                continue
            asyncio.run(inst_mngr.init_instruments(exchange, market_type))
            for inst in inst_mngr.get_insts_by_exchange(exchange=exchange, market_type=market_type).values():
                print(
                    exchange,
                    market_type,
                    inst.unified_symbol,
                    inst.symbol,
                    inst.exchange_symbol,
                    inst.fu_contract_types,
                )
                assert (
                    inst_mngr.get_exchange_symbol_by_unified_symbol(exchange, market_type, inst.unified_symbol)
                    == inst.exchange_symbol
                )
                assert (
                    inst_mngr.get_unified_symbol_by_exchange_symbol(exchange, market_type, inst.exchange_symbol)
                    == inst.unified_symbol
                )
                # add test for unified_symbol initialization
                # print('exchange_symbol:',inst.exchange_symbol, '  symbol:' , inst.symbol, '  unified_symbol:',inst.unified_symbol)
                assert isinstance(inst.unified_symbol, str), f"Invalid unified_symbol type for {inst.unified_symbol}"
                assert len(inst.unified_symbol) > 0, "unified_symbol is not initialized"
                # add test for price_multiplier initialization
                assert isinstance(
                    inst.price_multiplier, (int, float)
                ), f"Invalid price_multiplier type for {inst.price_multiplier}"
                assert (
                    inst.price_multiplier > 0
                ), "price_multiplier is not initialized or initialized to non-positive value"
            if exchange == "KUCOIN" and market_type != "SPOT":
                base_coin = "XBT"
            else:
                base_coin = "BTC"
            if market_type == "UDELIVERY":
                if exchange == "BYBIT":
                    quote = "USDC"
                else:
                    quote = "USDT"
                inst = inst_mngr.get_inst_by_unified_symbol(exchange, market_type, f"{base_coin}_{quote}_250926")
            else:
                inst = inst_mngr.get_inst_by_unified_symbol(exchange, market_type, f"{base_coin}_USDT")
            # print(f"{market_type}-{exchange}>>>>: {inst}")


def test_offline_ps_fu():
    inst_mngr = InstrumentManager()
    for market_type in [MarketType.CPERP, MarketType.CDELIVERY]:
        for exchange in [
            ExchangeName.BINANCE,
            ExchangeName.KUCOIN,
            ExchangeName.GATE,
            ExchangeName.BYBIT,
            ExchangeName.OKX,
        ]:
            if market_type == MarketType.CDELIVERY and exchange in [ExchangeName.GATE]:
                continue
            asyncio.run(inst_mngr.init_instruments(exchange, market_type))
            inst = Instrument(
                unified_symbol="TEST_USD",
                exchange_symbol="TEST_USD",
                exchange=exchange,
                market_type=market_type,
                base_asset="TEST",
                quote_asset="USD",
            )
            inst_mngr.add_inst(exchange, market_type, [inst])
            asyncio.run(inst_mngr.init_instruments(exchange, market_type))


def test_offline():
    inst_mngr = InstrumentManager()
    # for market_type in [MarketType.SPOT, MarketType.UPERP]:
    for market_type in [MarketType.SPOT]:
        # for exchange in [ExchangeName.BINANCE, ExchangeName.KUCOIN, ExchangeName.GATE, ExchangeName.BYBIT, ExchangeName.OKX]:
        for exchange in [ExchangeName.KUCOIN]:
            asyncio.run(inst_mngr.init_instruments(exchange, market_type))
            inst = Instrument(
                unified_symbol="TEST_USDT",
                exchange_symbol="TEST_USDT",
                exchange=exchange,
                market_type=market_type,
                base_asset="TEST",
                quote_asset="USD",
            )
            inst_mngr.add_inst(exchange, market_type, [inst])
            asyncio.run(inst_mngr.init_instruments(exchange, market_type))
            inst = inst_mngr.get_inst_by_unified_symbol(exchange, market_type, "TEST_USDT")
            assert inst and inst.is_offline


# def test_redis():
#     inst_mngr = InstrumentManager()
#     for market_type in ["SPOT", "UPERP"]:
#         for exchange in ["BINANCE", "KUCOIN", "GATE", "BYBIT", "OKX", "DERIBIT"]:
#             asyncio.run(inst_mngr.init_instruments(exchange, market_type))

#     inst_mngr.save_to_redis()
#     for market_type in ["SPOT", "UPERP"]:
#         for exchange in ["BINANCE", "KUCOIN", "GATE", "BYBIT", "OKX", "DERIBIT"]:
#             asyncio.run(inst_mngr.init_instruments(exchange, market_type, from_redis=True))


def test_ps_fu_multiplier():
    print("\n")
    inst_mngr = InstrumentManager()
    for market_type in ["CPERP", "CDELIVERY"]:
        # BINANCE CPERP, FU正常取 info["contractSize"]
        # KUCOIN CPERP,CDELIVERY 从info["multiplier"]为-1，网页下单BTC_USD，ETH_USD都是 1张=1USD;  inst_mngr.multiplier设置为1
        # GATE CPERP BTC_USD 的quanto_multiplier为0，网页下单BTC_USD 1张=1USD; inst_mngr.multiplier设置为1
        # BYBIT 全为1，下单网页也是直接显示的USD，没有张；inst_mngr.multiplier设置为1
        # OKX CPERP, CDELIVERY 正常取 Decimal(info["ctVal"]) * Decimal(info["ctMult"])
        for exchange in ["BINANCE", "OKX", "BYBIT", "GATE", "KUCOIN"]:
            if exchange == "GATE" and market_type == "CDELIVERY":
                continue
            asyncio.run(inst_mngr.init_instruments(exchange, market_type))
            for inst in inst_mngr.get_insts_by_exchange(exchange=exchange, market_type=market_type).values():
                # print(exchange, market_type, inst.unified_symbol)
                print(exchange, inst.exchange_symbol, market_type, inst.unified_symbol, inst.quantity_multiplier)
                # assert inst.multiplier == 1


def test_futures():
    inst_mngr = InstrumentManager()

    for market_type in ["CDELIVERY", "UDELIVERY"]:
        for exchange in ["DERIBIT", "BYBIT", "BINANCE"]:
            asyncio.run(inst_mngr.init_instruments(exchange, market_type))
            for inst in inst_mngr.get_insts_by_exchange(exchange=exchange, market_type=market_type).values():
                print(inst.exchange_symbol, inst.unified_symbol, inst.symbol, inst.fu_contract_types)


def test_bitget():
    inst_mngr = InstrumentManager()

    for market_type in ["UPERP", "SPOT", "CPERP", "CDELIVERY", "UDELIVERY"]:
        for exchange in ["BITGET"]:
            asyncio.run(inst_mngr.init_instruments(exchange, market_type))
            for inst in inst_mngr.get_insts_by_exchange(exchange=exchange, market_type=market_type).values():
                if market_type in ["UPERP", "SPOT", "CPERP"]:
                    assert (
                        inst_mngr.get_exchange_symbol_by_unified_symbol(exchange, market_type, inst.unified_symbol)
                        == inst.exchange_symbol
                    )
                    if inst.exchange_symbol in ["BTCUSDT", "BTCUSD"]:
                        print(inst)
                elif market_type in ["CDELIVERY", "UDELIVERY"]:
                    assert (
                        inst_mngr.get_exchange_symbol_by_unified_symbol(exchange, market_type, inst.unified_symbol)
                        == inst.exchange_symbol
                    )
                    if "BTC" in inst.exchange_symbol:
                        print(inst)
                assert (
                    inst_mngr.get_unified_symbol_by_exchange_symbol(exchange, market_type, inst.exchange_symbol)
                    == inst.unified_symbol
                )
                assert isinstance(inst.unified_symbol, str), f"Invalid unified_symbol type for {inst.unified_symbol}"
                assert len(inst.unified_symbol) > 0, "unified_symbol is not initialized"
                # add test for price_multiplier initialization
                assert isinstance(
                    inst.price_multiplier, (int, float)
                ), f"Invalid price_multiplier type for {inst.price_multiplier}"
                assert (
                    inst.price_multiplier > 0
                ), "price_multiplier is not initialized or initialized to non-positive value"
