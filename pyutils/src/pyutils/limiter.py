import asyncio
from contextlib import AbstractAsyncContextManager
from types import TracebackType
from typing import Dict, Optional, Type, Tuple
from redis.asyncio import Redis
from redis.asyncio.connection import ConnectionPool
import uuid
import time


LUA_PUT = """
local bucket = KEYS[1]
local now = tonumber(ARGV[1])
local space_required = tonumber(ARGV[2])
local item_name = ARGV[3]
local interval = tonumber(ARGV[4])
local limit = tonumber(ARGV[5])

local count = redis.call('ZCOUNT', bucket, now - interval, now)
local space_available = limit - tonumber(count)

if space_available >= space_required then
    for i=1,space_required do
        redis.call('ZADD', bucket, now, item_name..i)
    end
end

return space_available - space_required
"""


class GlobalAsyncLimiter(AbstractAsyncContextManager):
    """GlobalAsyncLimiter是一个全局异步限流器,使用Redis作为存储."""

    _redis_pools = {}

    def __init__(
        self, redis_url: str, key: str, max_rate: float, time_period: float = 60, prefix: str = "GlobalLimiter"
    ) -> None:
        """初始化全局异步限流器.同一个redis的{prefix}:{key}|{max_rate}|{time_period}组成唯一的限流标识
        在time_period滚动时间内,最多允许max_rate次请求.

        Args:
            redis_url (str): Redis连接URL
            key (str): 限流器的唯一标识
            max_rate (float): 最大限流速率
            time_period (float, optional): _description_. Defaults to 60.
            prefix (str, optional): _description_. Defaults to "GlobalLimiter".
        """
        self._id = str(uuid.uuid4())
        self._req_id = 0
        if redis_url in self._redis_pools:
            self._redis_client = Redis.from_pool(self._redis_pools[redis_url])
        else:
            self._redis_pools[redis_url] = ConnectionPool.from_url(redis_url)
            self._redis_client = Redis.from_pool(self._redis_pools[redis_url])

        self.max_rate = max_rate
        self.time_period = time_period
        self.bucket = f"{prefix}:{key}|{self.max_rate}|{self.time_period}"
        self._rate_per_sec = time_period / max_rate
        self._script_hash = None
        self._inited = asyncio.Event()

    async def init(self):
        self._script_hash = await self._redis_client.script_load(LUA_PUT)
        self._inited.set()

        return self

    async def put(self, amount: int = 1, now: Optional[float] = None) -> bool:
        now = now or time.time()
        next_req_id = self._req_id + 1

        args = [
            now,  # now
            amount,  # space_required
            f"{self._id}|{next_req_id}|",  # item_name
            self.time_period,  # interval
            self.max_rate,  # limit
        ]

        space_count: int = await self._redis_client.evalsha(self._script_hash, 1, self.bucket, *args)  # type: ignore

        is_ok = space_count >= 0

        if is_ok:
            self._req_id = next_req_id

        return is_ok

    async def leak(self, now: Optional[float] = None) -> int:
        now = now or time.time()
        leak_count = await self._redis_client.zremrangebyscore(self.bucket, 0, now - self.time_period)

        return leak_count

    async def count(self):
        count = await self._redis_client.zcard(self.bucket)

        return count

    async def peek(self, index: int) -> Optional[Tuple[str, float]]:
        items = await self._redis_client.zrange(
            self.bucket, -1 - index, -1 - index, withscores=True, score_cast_func=float
        )

        if not items:
            return

        return items[0]

    async def acquire(self, amount: int = 1) -> None:
        if amount > self.max_rate:
            raise ValueError("Can't acquire more than the maximum capacity")

        while True:
            now = time.time()
            is_ok = await self.put(amount=amount, now=now)
            if is_ok:
                break

            await asyncio.sleep(self._rate_per_sec)

        await self.leak(now=now)

    async def __aenter__(self) -> None:
        if not self._inited.is_set():
            await self.init()

        await self.acquire()

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> None:
        pass

    def __await__(self):
        if not self._inited.is_set():
            asyncio.create_task(self.init())

        while not self._inited.is_set():
            yield

        return self
