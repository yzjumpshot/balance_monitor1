# xclients -- python api wrapper

## 使用指南

xclients可以直接使用的模块有
* REST Wrapper  -- 封装了各个交易所 API 请求和响应的对象，主要用来执行账户、市场数据等 RESTful 操作
* REST Client -- 用于执行具体的 REST 操作，是直接发起请求的客户端
* Market WebSocket Wrapper -- 通过 WebSocket 连接获取市场数据，以统一格式返回
* Account WebSocket Wrapper -- 通过 WebSocket 连接获取账户相关数据，以统一格式返回
* WebSocket Client -- 直接通过 WebSocket 协议与交易所连接获取实时数据

都建议用get_client的方式初始化


### 初始化方式

有以下四种初始化方式

#### 使用MarketMeta初始化
```python
market_meta = MarketMeta(...)  # 已经定义好的 MarketMeta
rest_config = RestConfig(...)  # 可选，若不提供，则使用默认配置
wss_config = WssConfig(...)  # 可选，若不提供，则使用默认配置

rest_wrapper = get_rest_wrapper(market_meta, rest_config)
rest_client = get_rest_client(market_meta, rest_config)
market_ws_wrapper = get_market_ws_wrapper(market_meta, wss_config)
ws_client = get_ws_client(market_meta, wss_config)
# account_ws_wrapper 不可使用marketMeta初始化

```

#### 使用AccountMeta初始化

```python
account_meta = AccountMeta(...)  # 已经定义好的 AccountMeta
account_config = AccountConfig(...)  # 已经定义好的 AccountConfig
rest_config = RestConfig(...)  # 可选，若不提供，则使用默认配置
wss_config = WssConfig(...)  # 可选

rest_wrapper = get_rest_wrapper(account_meta, account_config, rest_config)
rest_client = get_rest_client(account_meta, account_config, rest_config)
market_ws_wrapper = get_market_ws_wrapper(account_meta, wss_config)
account_ws_wrapper = get_account_ws_wrapper(account_meta, account_config, wss_config)
ws_client = get_ws_client(account_meta, account_config, wss_config)

```

#### 使用基本参数初始化
```python
rest_wrapper = get_rest_wrapper(
    exch_name="BINANCE",  # 交易所名称，如 "BINANCE" 或 ExchangeName.BINANCE
    market_type="SPOT",    # 市场类型，如 "SPOT" 或 MarketType.SPOT
    account_type="NORMAL", # 账户类型，如 "NORMAL" 或 AccountType.NORMAL
    account_name="my_account",  # 账户名称，传入此参数会自动从redis获取对应的 api_key, secret_key 等信息
    api_key="your_api_key",  # 可选，可以直接传递 api_key、secret_key 等信息
    secret_key="your_secret_key",  # 可选，直接传递 secret_key
    passphrase="your_passphrase",  # 可选，直接传递 passphrase
    uid="your_uid",  # 可选，直接传递 uid
    rest_config=RestConfig()  # 可选，若不提供，则使用默认配置
)

rest_client = get_rest_client(
    exch_name="BINANCE",  # 交易所名称，如 "BINANCE" 或 ExchangeName.BINANCE
    market_type="SPOT",    # 市场类型，如 "SPOT" 或 MarketType.SPOT
    account_type="NORMAL", # 账户类型，如 "NORMAL" 或 AccountType.NORMAL
    account_name="my_account",  # 账户名称，传入此参数会自动获取对应的 api_key, secret_key 等信息
    api_key="your_api_key",  # 可选，可以直接传递 api_key、secret_key 等信息
    secret_key="your_secret_key",  # 可选，直接传递 secret_key
    passphrase="your_passphrase",  # 可选，直接传递 passphrase
    uid="your_uid",  # 可选，直接传递 uid
    rest_config=RestConfig()  # 可选，若不提供，则使用默认配置
)

market_ws_wrapper = get_market_ws_wrapper(
    exch_name="BINANCE",  # 交易所名称，如 "BINANCE" 或 ExchangeName.BINANCE
    market_type="SPOT",    # 市场类型，如 "SPOT" 或 MarketType.SPOT
    account_type="NORMAL", # 账户类型，如 "NORMAL" 或 AccountType.NORMAL
    account_name="my_account",  # 账户名称，传入此参数会自动获取对应的 api_key, secret_key 等信息
    api_key="your_api_key",  # 可选，可以直接传递 api_key、secret_key 等信息
    secret_key="your_secret_key",  # 可选，直接传递 secret_key
    passphrase="your_passphrase",  # 可选，直接传递 passphrase
    uid="your_uid",  # 可选，直接传递 uid
    wss_config=WssConfig()  # 可选，若不提供，则使用默认配置
)

account_ws_wrapper = get_account_ws_wrapper(
    exch_name="BINANCE",  # 交易所名称，如 "BINANCE" 或 ExchangeName.BINANCE
    market_type="SPOT",    # 市场类型，如 "SPOT" 或 MarketType.SPOT
    account_type="NORMAL", # 账户类型，如 "NORMAL" 或 AccountType.NORMAL
    account_name="my_account",  # 账户名称，传入此参数会自动获取对应的 api_key, secret_key 等信息
    api_key="your_api_key",  # 可选，可以直接传递 api_key、secret_key 等信息
    secret_key="your_secret_key",  # 可选，直接传递 secret_key
    passphrase="your_passphrase",  # 可选，直接传递 passphrase
    uid="your_uid",  # 可选，直接传递 uid
    wss_config=WssConfig()  # 可选，若不提供，则使用默认配置
)

ws_client = get_ws_client(
    exch_name="BINANCE",  # 交易所名称，如 "BINANCE" 或 ExchangeName.BINANCE
    market_type="SPOT",    # 市场类型，如 "SPOT" 或 MarketType.SPOT
    account_type="NORMAL", # 账户类型，如 "NORMAL" 或 AccountType.NORMAL
    account_name="my_account",  # 账户名称，传入此参数会自动获取对应的 api_key, secret_key 等信息
    api_key="your_api_key",  # 可选，可以直接传递 api_key、secret_key 等信息
    secret_key="your_secret_key",  # 可选，直接传递 secret_key
    passphrase="your_passphrase",  # 可选，直接传递 passphrase
    uid="your_uid",  # 可选，直接传递 uid
    wss_config=WssConfig()  # 可选，若不提供，则使用默认配置
)

```

## wrapper使用

### REST Wrapper

市场数据
```python
from xclients.get_wrapper import get_rest_wrapper
from xclients.enum_type import MarketType, ExchangeName, AccountType

exch = ExchangeName.BINANCE
market_type = MarketType.UPERP
rest_wrapper = get_rest_wrapper(exch, market_type)
funding_rate = await rest_wrapper.get_current_funding_rate(["BTCUSDT"])
```
需要鉴权的数据
```python
exch = ExchangeName.BINANCE
market_type = MarketType.SPOT
acct_type = AccountType.NORMAL
test_account = "4abntest1"
rest_wrapper = get_rest_wrapper(exch, market_type, acct_type, test_account)
asset = await rest_wrapper.get_assets()
```

更复杂的例子

```python
# 创建 AccountMeta 实例
account_meta = AccountMeta(
    exch_name=ExchangeName.BYBIT,         # 交易所名称
    account_type=AccountType.UNIFIED,        # 账户类型
    market_type=MarketType.SPOT,            # 市场类型
)

# 创建 AccountConfig 实例
account_config = AccountConfig(
    api_key="your_api_key",                 # API 密钥
    secret_key="your_secret_key",           # Secret 密钥
    passphrase="your_passphrase",           # 可选的 passphrase
    uid="your_uid"                          # 可选的 uid
)

# 创建 RestConfig 实例
rest_config = RestConfig(
    timeout=10,                             # 请求超时时间（秒）
    tracing=False,                          # 是否开启 tracing
    proxy=None,                             # 可选代理
    bind_ips=["127.0.0.1"],                 # 可选绑定 IP
)

# 使用 account_meta, account_config 和 rest_config 初始化 RestWrapper
rest_wrapper = get_rest_wrapper(account_meta, account_config, rest_config)

# 使用返回的 RestWrapper 执行 API 请求（例如获取币种信息）
response = await rest_wrapper.get_assets()
```

### Market WS Wrapper

市场 WebSocket 示例（具体可见tests/test_market_ws.py）：

```python
from xclients.get_wrapper import get_market_ws_wrapper
from xclients.enum_type import MarketType, ExchangeName, Event
from xclients.data_type import MarketMeta, OrderBook, WssConfig

# 初始化市场 WebSocket 客户端
exch = ExchangeName.BINANCE
market_type = MarketType.SPOT
symbol_list = ["BTC_USDT", "ETH_USDT"]
event_type = Event.BOOK

# 回调函数第一个参数均为MarketMeta，第二个参数为实际返回结构体，且必须是异步函数
async def on_book_message(market_meta: MarketMeta, data: OrderBook):
    print("on_book:", market_meta, data)

# 获取市场 WebSocket 客户端
market_ws_wrapper = get_market_ws_wrapper(exch, market_type, wss_config=wss_config)

# 订阅symbol
for symbol in symbol_list:
    ws_wrapper.subscribe_symbol(symbol)

# 注册回调函数
ws_wrapper.subscribe_callback(event_type, on_book_message)

# 运行
await ws_wrapper.run()
```

更复杂的例子

```python
from xclients.get_wrapper import get_market_ws_wrapper
from xclients.enum_type import MarketType, ExchangeName, Event
from xclients.data_type import AccountMeta, Kline, WssConfig

symbol_list = ["BTC_USDT", "ETH_USDT"]
event_type = Event.KLINE
account_meta = AccountMeta(
    exch_name=ExchangeName.BINANCE,     # 交易所名称
    account_type=AccountType.NORMAL,    # 账户类型
    market_type=MarketType.SPOT,        # 市场类型
    account_name="my_account"           # 账户名称(market ws wrapper也可以鉴权，有些交易所也有需要鉴权的行情)
)

# 创建 WssConfig 实例（配置 WebSocket 相关的参数）
wss_config = WssConfig(
    heartbeat_interval=15,                  # 心跳间隔（秒）
    reconnect_interval=5,                   # 重连间隔（秒）
    bind_ip="127.0.0.1",
    extra_params={"kline_intervals": ["1m", "5m"]}
)


# 回调函数第一个参数均为MarketMeta，第二个参数为实际返回结构体，且必须是异步函数
async def on_kline_message(market_meta: MarketMeta, data: list[Kline]):
    print("on_kline:", market_meta, data)

# 使用 account_meta 和 wss_config 初始化 Market WS Wrapper
market_ws_wrapper = get_market_ws_wrapper(account_meta, wss_config)

# 订阅symbol
for symbol in symbol_list:
    ws_wrapper.subscribe_symbol(symbol)

# 注册回调函数
ws_wrapper.subscribe_callback(event_type, on_kline_message)

# 运行
await ws_wrapper.run()
```

目前支持的Market Event有BOOK, FUNDING_RATE, OPEN_INTEREST, TICKER, PREMIUM_INDEX, KLINE
以及两个基础Event：CONNECTED, DISCONNECTED


### Account WS Wrapper


市场 WebSocket 示例（具体可见tests/test_account_ws.py）：

```python
from xclients.get_wrapper import get_account_ws_wrapper
from xclients.enum_type import MarketType, ExchangeName, AccountType, Event
from xclients.data_type import AccountMeta, WssConfig, Balances

# 初始化账户 WebSocket 客户端
exch = ExchangeName.BINANCE
market_type = MarketType.SPOT
acct_type = AccountType.NORMAL
account_name = "4abntest1"
event_type = Event.BALANCE

# 回调函数第一个参数均为AccountMeta，第二个参数为实际返回结构体，且必须是异步函数
async def on_balance_message(account_meta: AccountMeta, data: Balances):
    print("on_balance:", account_meta, data)

# 创建 WssConfig 实例（配置 WebSocket 相关的参数）
wss_config = WssConfig(
    heartbeat_interval=15,                  # 心跳间隔（秒）
    reconnect_interval=5,                   # 重连间隔（秒）
    bind_ip="127.0.0.1",                    # 可选的绑定 IP
)

# 获取账户 WebSocket 客户端
account_ws_wrapper = get_account_ws_wrapper(exch, market_type, acct_type, account_name, wss_config=wss_config)

# 注册回调函数
account_ws_wrapper.subscribe_callback(event_type, on_balance_message)

# 运行
await account_ws_wrapper.run()
```


目前支持的Account Event有ORDER, USER_TRADE, BALANCE, POSITION
以及两个基础Event：CONNECTED, DISCONNECTED


## inst_manager

xclients的所有wrapper、client的传参和返回值均是交易所的symbol，inst管理放在inst_mngr.py

#### Instrument 类

Instrument 类表示一个交易品种（例如 BTC/USDT 交易对），包含了定义交易品种属性的多个字段。

属性:
* exchange_symbol (str): 交易所中的交易品种符号（例如 "BTC/USDT"）。

* exchange (ExchangeName): 交易品种所在的交易所（例如 ExchangeName.BINANCE）。

* market_type (MarketType): 市场类型（例如 MarketType.SPOT MarketType.UPERP MarketType.UDELIVERY）。

* base_ccy (str): 交易品种的基础货币（例如 "BTC"）。(TODO, base_ccy or real_base_ccy?)

* quote_ccy (str): 交易品种的报价货币（例如 "USDT"）。

* price_tick (Decimal): 价格最小波动（最小价格变动单位）。

* lot_size (Decimal): 交易数量的最小单位（最小交易量）。

* price_precision (int): 价格精度，基于 price_tick 计算。（TODO: 计算方式需要与go一致）

* quantity_precision (int): 数量精度，基于 lot_size 计算。（TODO: 计算方式需要与go一致）

* min_order_size (Decimal): 最小订单数量。

* min_order_notional (Decimal): 最小订单名义价值。

* max_market_order_size (Decimal): 最大市场订单数量。

* max_market_order_notional (Decimal): 最大市场订单名义价值。

* max_position_size (Decimal): 最大仓位数量。

* max_position_notional (Decimal): 最大仓位名义价值。

* max_slippage (Decimal): 最大滑点。

* multiplier (Decimal): 合约倍数。

* unified_symbol (str): 统一符号（不同交易所可能有不同符号，这里是为了统一）。

* price_multiplier (int): 价格乘数。

* trade_in_notional (bool): 是否使用名义价值进行交易。

* status (InstStatus): 当前交易品种的状态（例如 InstStatus.TRADING 表示正在交易）。

属性说明:
is_tradable: 判断该品种是否可以交易，取决于当前状态是否是 TRADING 或 DELISTING。

is_untradable: 判断该品种是否不可交易，取决于当前状态是否是 UNTRADABLE。

is_offline: 判断该品种是否离线，取决于当前状态是否是 OFFLINE。

symbol: 返回统一符号、市场类型和交易所名称的拼接字符串（例如 "BTC_USDT|SPOT|BINANCE"


#### InstrumentManager 类

InstrumentManager 是一个单例类，用于管理多个交易所和市场类型的交易品种。它负责处理符号的映射、从 Redis 加载数据、以及初始化交易品种。

对外提供的函数
1. add_inst
用于向 InstrumentManager 中添加新的交易品种。

2. clear
清除所有已加载的交易品种数据。

3. get_inst_by_unified_symbol
通过统一符号获取交易品种对象。

4. get_inst_by_exchange_symbol
通过交易所符号获取交易品种对象。

5. get_insts_by_exchange
获取指定交易所和市场类型下的所有交易品种。

6. init_instruments
初始化指定交易所和市场类型的交易品种。

7. check_is_inited
检查指定交易所和市场类型的交易品种是否已初始化。

8. save_to_redis
将当前交易品种数据保存到 Redis 中。

9. init_instruments_from_redis
从 Redis 中加载交易品种数据。

10. init_instruments_from_rest_wrapper
通过该rest_wrapper来初始化instruments（可以初始化需要鉴权的字段margin_trading）

## 改动

* [x] ExchangeName -- 把BINANCEUNIFIED和GATEUNIFIED改成Binance和Gate

* [x] InstType+AccountType TO MarketType+AccountType
    ```
    InstType: SP, LPS, PS, FU
    改为
    MarketType: SPOT, MARGIN, UFUTURES, CFUTURES, CDELIVERY

    AccountType: FUND, SP, LPS, PS, FU
    改为
    AccountType: NORMAL, UNIFIED, CLASSIC_UNIFIED, HFT
    ```

* [x] Balance、Position等枚举字段向pyexcalibur靠齐

* [x] inst 命名改动（去掉std_symbol, meta_symbol改成unified_symbol，交割合约unified_symbol加上delivery_time）

* [x] get_client传参改动，去掉之前的各种**kwargs传参，改用RestConfig和WssConfig，引入pyexcalibur的Account、Market类并改名
AccountMeta，MarketMeta，引入pyexcalibur的AccountConfig，兼容cryptoex_clients原来的传参

* [x] 新增MarketWssWrapper和AccountWssWrapper，代码基本可以直接从pyexcalibur复制过来，加上了Event.CONNECTED和Event.DISCONNECTED

* [x] BaseRestClient 绑定单个ip改成绑定多个ip，修改了BaseRestClient和BaseWsClient的初始化方式

* [x] 给其他交易所加上AccountWssWrapper并测试

* [] 检查所有的from_redis=True的，如果拿不到了就去掉

* [] gate的统一账户get_assets未实现

### 20250908

* [x] 给其他交易所加上MarketWssWrapper并测试

* [x] FundingRate 结构体改成pyexcalibur的，ts单位从datetime改成了毫秒

* [x] InterestRate 结构体改成pyexcalibur的，ts单位从datetime改成了毫秒

* [x] 常量单独放在每个交易所的constants.py中，然后其他文件引用

* [x] 完善并测试各个交易所的发单，撤单查单改成调用ccxt接口

* [x] 添加了`submit_place_order`, `submit_cancel_order`, `submit_sync_order`函数，以及`PlaceOrderInstruction`, `CancelOrderInstruction`, `SyncOrderInstruction` 三个指令，`submit_xxx`函数的返回的结果从`event_bus`返回（兼容pyexcalibur目前的下单逻辑）
* [x] 检查并补上pyexcalibur交易需要用到的函数并测试
    ```python
    set_account_position_mode
    set_account_margin_mode
    set_fee_coin_burn
    set_account_leverage

    # bn统一、经典统一
    enable_auto_repayment
    set_uta_mode
    enable_margin_trading
    set_hft_mode
    enable_union_asset_mode
    collect_balances
    repay_negative_balances

    # bn u本位 / bybit u本位
    set_account_leverage

    # bybit
    enable_account_collaterals

    # kucoin
    get_symbol_leverage
    set_symbol_leverage
    get_symbol_margin_mode
    set_symbol_margin_mode

    # bitget u本位
    get_symbol_leverage_and_margin_mode
    set_symbol_margin_mode

    # gate u本位
    adjust_risk_limits
    ```

* [x] **所有ts的单位统一为ms**

* [x] inst_mngr添加了init_instruments_from_wrapper函数，该函数在rest_wrapper的market_type是margin的时候会调用_patch_{exch}_margin_trading函数， 并调用rest_wrapper的set_instruments函数（目前只有get_fundamentals和gate的adjust_risk_limits必须要用到insts）

* 添加了`test_wrapper_fetch`, `test_wrapper_set`， `test_order` 三个测试文件，分别用于测试get相关函数，set相关函数，以及发撤查单逻辑

## breaking change

* `Trade`结构体的create_time --> create_ts, fill_time --> fill_ts
* `OrderBook`结构体的ts改成了ms
* 去掉了`OrderDetail`结构体，改成了OrderSnapshot（同步excalibur）
* 原先pyexcalibur的Fundamental，Kline, OrderBook, Ticker里面的inst都改成了exch_symbol
* 原先pyexcalibur的Position，PlaceOrderInstruction， CancelOrderInstruction， SyncOrderInstruction， OrderSnapshot里面的inst都改成了exch_symbol