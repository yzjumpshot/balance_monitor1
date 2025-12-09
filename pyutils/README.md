# PyUtils

一个实用的 Python 工具库，提供了配置加载、通知管理、账户凭证管理和任务管理等功能。

## 功能特性

- **ConfigLoader**: 灵活的配置加载器，支持从多个来源加载和合并配置
- **Notifier**: 通知管理模块，支持 Lark 和 Slack 通知
- **AccountCredentialManager**: 账户凭证管理器，从 Redis 中安全获取交易所凭证
- **TaskManager**: 异步任务管理器，基于 asyncio.TaskGroup 实现
- **GlobalAsyncLimiter**: 基于 Redis 的全局异步限流器，支持分布式环境下的精确速率控制
- **Querier**: 通用的查询器,用于执行ClickHouse数据库的查询和数据操作
- **BaseStorage**: 抽象基类,用于定义ClickHouse存储的基本操作

## 安装

```bash
pip install git+https://github.com/4alphaCapital/pyutils.git
```

## 使用指南

### ConfigLoader

ConfigLoader 用于从多个来源加载和合并配置文件。

#### 假如在用户根目录`~/.myproject/config.toml`配置了
```toml
[clickhouse]
user = "demo_user"
password = "123456789"
```

#### 假如在当前目录目录`./.myproject/config.toml`配置了
```toml
[clickhouse]
password = "abcdefg"
host = "xxx.xxx.xxx.xxx"
```

#### 假如环境变量中配置了`MYPROJECT`开头的变量
```bash
export MYPROJECT__CLICKHOUSE_DATABASE="market_data"
```

#### 配置加载
```python
from pyutils import ConfigLoader

# 初始化配置加载器
cl = ConfigLoader(
    config_dir=".myproject",      # 配置目录
    config_name="config.toml",    # 配置文件名
    env_prefix="MYPROJECT"        # 环境变量前缀
)

# 加载配置
config = cl.load_config()
# {'clickhouse': {'user': 'demo_user', 'password': 'abcdefg', 'host': 'xxx.xxx.xxx.xxx', 'database': 'market_data'}}

# 也可以传入额外的配置参数
extra_config = {"clickhouse": {"port": 8443}}
config = cl.load_config(extra_config)
# {'clickhouse': {'user': 'demo_user', 'password': 'abcdefg', 'host': 'xxx.xxx.xxx.xxx', 'port': 8443, 'database': 'market_data'}}
```

#### 配置加载顺序

配置按以下顺序加载，后面的会覆盖前面的：

1. 用户家目录：`~/.myproject/config.toml`
2. 当前工作目录：`./.myproject/config.toml`
3. 环境变量：以 `MYPROJECT__` 开头的环境变量
4. 传入的 config 参数

### Notifier

Notifier 提供统一的通知接口，支持 Lark 和 Slack。

```python
from pyutils import Notifier

# 方式1: 直接初始化
notifier = Notifier(
    url="https://your-notify-service.com",
    user_id="your_user_id"
)

# 方式2: 从环境变量初始化`NOTIFIER_URL`, `NOTIFIER_USER_ID`
notifier = Notifier.from_env()


# 方式3: 从配置文件初始化
notifier = Notifier.from_config(
    config_dir=".myproject",
    config_name="config.toml"
)
```

#### 管理通知频道

```python
# 添加通知频道
await notifier.add_channel(
    channel="alerts",
    bot_type="lark",  # 或 "slack"
    bot_url="https://open.larksuite.com/open-apis/bot/v2/hook/xxx",
    bot_note="项目告警通知"
)

# 获取频道信息
channel_info = await notifier.get_channel("alerts")

# 删除频道
await notifier.del_channel("alerts")
```

#### 发送通知

```python
# 发送文本消息
await notifier.notify(
    channel="alerts",
    msg="系统运行正常",
    at=["user1", "user2"],  # @指定用户
    timeout=5
)

# 发送结构化消息（Lark）
lark_msg = {
    "msg_type": "interactive",
    "card": {
        "elements": [
            {
                "tag": "div",
                "text": {
                    "content": "**告警信息**",
                    "tag": "lark_md"
                }
            }
        ]
    }
}

await notifier.notify(
    channel="alerts",
    msg=lark_msg
)
```

### AccountCredentialManager

安全地从 Redis 中获取交易所账户凭证。

```python
from pyutils import AccountCredentialManager

# 初始化
acm = AccountCredentialManager(redis_acc_url="redis://localhost:6379/0")

# 或者使用环境变量 REDIS_ACC_URL
acm = AccountCredentialManager()

# 获取凭证
access_key, secret_key, passphrase, uid = acm.get_credential(
    account="my_account",
    exchange="binance",
    market_type="spot",
    account_type=None  # 统一账户设置为 "unified"
)
```

### TaskManager

基于 asyncio.TaskGroup 的任务管理器，支持动态添加和删除任务。
- TaskManager用于管理一组 asyncio 任务.
- 与py3.11新增的TaskGroup类似，多用来管理常驻任务
- 由于目前最低版本是 3.12, 所以直接优化逻辑使用TaskGroup来实现TaskManager
- 不同于TaskGroup的是, TaskManager可以动态(在async with代码块之外)添加和删除任务, 这使得它在某些场景下更加灵活.
- 另外会加入一些日志, 记录任务添加, 取消或报错的相关信息

```python
import asyncio
from pyutils import TaskManager

async def worker_task(name, duration):
    print(f"任务 {name} 开始")
    await asyncio.sleep(duration)
    print(f"任务 {name} 完成")

async def main():
    task_manager = TaskManager()
    
    # 添加任务
    task_manager.add_task(
        worker_task("task1", 2),
        name="worker-1"
    )
    
    task_manager.add_task(
        worker_task("task2", 3),
        name="worker-2"
    )
    
    # 运行任务管理器
    await task_manager.run()

# 运行
asyncio.run(main())
```

#### 动态管理任务

```python
# 假设一种场景，ws 连接需要动态地增减
async def dynamic_ws():
    task_manager = TaskManager()

    # 模拟常驻ws任务
    async def simulate_ws(delay: int, msg: str = ""):
        for i in range(100):
            print(msg, i)
            await asyncio.sleep(delay)
        
        print(msg, "done")

    # 加入需要执行的 task
    task_manager.add_task(simulate_ws(2, "ws1 running..."), name="ws1")
    task_manager.add_task(simulate_ws(3, "ws2 running..."), name="ws2")

    # 模拟任务调度
    async def schedule():

        # 动态新增ws任务
        await asyncio.sleep(10)
        task_manager.add_task(simulate_ws(1, "ws3 running..."), name="ws3")

        # 动态删除ws任务
        await asyncio.sleep(10)
        ws1_task = task_manager.tasks["ws1"]
        task_manager.del_task(ws1_task)

    await asyncio.gather(task_manager.run(), schedule())
```

### GlobalAsyncLimiter

`GlobalAsyncLimiter` 是一个基于 Redis 的全局异步限流器，支持分布式环境下的速率限制。使用滑动窗口算法，在指定的时间周期内控制请求的最大数量。

#### 基本使用

```python
from pyutils import GlobalAsyncLimiter

# 初始化限流器
# 同一个redis的{prefix}:{key}|{max_rate}|{time_period}组成唯一的限流标识
limiter = GlobalAsyncLimiter(
    redis_url="redis://localhost:6379/0",
    key="api_calls",          # 限流器唯一标识
    max_rate=100,             # 最大速率：100次请求
    time_period=60,           # 时间周期：60秒
    prefix="MyApp"            # 前缀，默认为 "GlobalLimiter"
)

# 方式1: 使用上下文管理器（推荐）
async with limiter:  # 这里默认行为是增加一次调用计数
    # 在这里执行需要限流的操作
    await make_api_call()

# 方式2: 手动获取许可
await limiter.acquire(amount=3)  # 获取3个请求许可
await make_api_call()
```

#### 分布式场景示例

```python
import asyncio
from pyutils import GlobalAsyncLimiter

# 多个实例共享同一个限流器
async def worker(worker_id: int):
    limiter = GlobalAsyncLimiter(
        redis_url="redis://localhost:6379/0",
        key="shared_api",
        max_rate=50,      # 全局限制：50次/分钟
        time_period=60
    )
    
    for i in range(10):
        async with limiter:
            print(f"Worker {worker_id} - 请求 {i}")
            await asyncio.sleep(0.1)

# 启动多个 worker，它们会共享限流配额
async def main():
    await asyncio.gather(
        worker(1),
        worker(2),
        worker(3)
    )

asyncio.run(main())
```

#### 配置参数说明

- `redis_url`: Redis 连接URL
- `key`: 限流器的标识
- `max_rate`: 在 `time_period` 时间内允许的最大请求数
- `time_period`: 时间窗口大小（秒），默认60秒
- `prefix`: Redis key 的前缀，默认 "GlobalLimiter"
- {prefix}:{key}|{max_rate}|{time_period}标注唯一限流器，多实例共用请求次数

#### 工作原理

`GlobalAsyncLimiter` 使用 Redis 的有序集合（Sorted Set）实现滑动窗口限流：

1. 每个请求都会在 Redis 中记录一个带时间戳的条目
2. 使用 Lua 脚本保证操作的原子性
3. 自动清理过期的请求记录
4. 支持多实例间的协调限流

### BaseStorage, Querier

`BaseStorage` 和 `Querier` 是基于 ClickHouse 的数据存储和查询工具，提供了异步的数据库操作接口。

#### Querier

`Querier` 是一个基础的 ClickHouse 查询器，提供异步查询功能。

```python
from pyutils import Querier, ConfigLoader

# 配置文件 .myproject/config.toml
"""
[clickhouse]
host = "localhost"
port = 8123
username = "default"
password = ""
database = "analytics"
"""

cl = ConfigLoader(config_dir=".myproject", config_name="config.toml")
CH_INFO = cl.load_config()["clickhouse"]

# 初始化 Querier
querier = Querier(**CH_INFO)

# 执行查询并返回 QueryResult
result = await querier.query("SELECT * FROM users LIMIT 10")
print(result.result_rows)  # 获取查询结果

# 执行查询并返回 DataFrame
df = await querier.query_df("SELECT * FROM users LIMIT 10")
print(df.head())
```

#### BaseStorage

`BaseStorage` 是一个抽象基类，继承自 `Querier`，提供了完整的数据存储和管理功能。您需要继承此类并实现必要的抽象方法。

##### 实现自定义存储类

```python
from pyutils import BaseStorage
from clickhouse_connect.driver.summary import QuerySummary

cl = ConfigLoader(config_dir=".myproject", config_name="config.toml")
CH_INFO = cl.load_config()["clickhouse"]

class UserStorage(BaseStorage):
    @property
    def db(self) -> str:
        return "user_data"
    
    @property
    def table(self) -> str:
        return "users"
    
    async def create_table(self) -> QuerySummary:
        """创建用户表"""
        sql = f"""
        CREATE TABLE IF NOT EXISTS {self.full_table_name} (
            id UInt64,
            name String,
            email String,
            created_at DateTime
        ) ENGINE = MergeTree()
        ORDER BY id
        """
        return await self.query(sql)

# 使用示例
user_storage = UserStorage(**CH_INFO)

# 创建表
await user_storage.create_table()
```

##### 数据操作

```python
# 插入数据
data = [
    (1, "Alice", "alice@example.com", "2024-01-01 10:00:00"),
    (2, "Bob", "bob@example.com", "2024-01-02 11:00:00")
]
await user_storage.insert(
    data=data,
    column_names=["id", "name", "email", "created_at"]
)

# 使用 DataFrame 插入数据
import pandas as pd
df = pd.DataFrame({
    "id": [3, 4],
    "name": ["Charlie", "David"],
    "email": ["charlie@example.com", "david@example.com"],
    "created_at": ["2024-01-03 12:00:00", "2024-01-04 13:00:00"]
})
await user_storage.insert_df(df)

# 查询数据
users_df = await user_storage.query_df("SELECT * FROM users ORDER BY id")
print(users_df)
```

##### 表管理和监控

```python
# 获取建表语句
ddl = await user_storage.get_ddl_sql()
print(ddl)

# 获取表的基本信息
table_info = await user_storage.get_table_info()
print(table_info)

# 获取表的分区信息
part_info = await user_storage.get_part_info()
print(part_info)

# 获取列的存储信息
column_info = await user_storage.get_column_info()
print(column_info)
```

#### 连接池管理

`BaseClient` 使用共享的连接池管理器，提高并发性能：

```python
# 多个存储实例会共享同一个连接池
storage1 = UserStorage(host="localhost", port=8123)
storage2 = UserStorage(host="localhost", port=8123)
# 两个实例共享连接池，提高资源利用率
```

#### 更多用法

```python
# 批量插入大量数据
async def batch_insert_users(storage: UserStorage, users_data):
    batch_size = 1000
    for i in range(0, len(users_data), batch_size):
        batch = users_data[i:i + batch_size]
        await storage.insert(batch, column_names=["id", "name", "email", "created_at"])
        print(f"已插入 {min(i + batch_size, len(users_data))} 条记录")

# 复杂查询示例
async def get_user_stats(storage: UserStorage):
    sql = """
    SELECT 
        toDate(created_at) as date,
        count() as user_count
    FROM users 
    WHERE created_at >= today() - 30
    GROUP BY date
    ORDER BY date
    """
    return await storage.query_df(sql)
```

## 配置示例

### 环境变量示例

```bash
# Notifier 配置
export NOTIFIER_URL="https://your-notify-service.com"
export NOTIFIER_USER_ID="your_user_id"

# Redis 配置
export REDIS_ACC_URL="redis://localhost:6379/0"

# 自定义项目配置（使用 MYPROJECT 前缀）
export MYPROJECT__DATABASE_HOST="localhost"
```

## 依赖

- Python >= 3.12
- redis >= 5.4, < 7.0.0
- aiohttp >= 3.8.0, < 4.0.0
- loguru >= 0.6.0, < 0.8.0
