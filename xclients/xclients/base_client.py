import asyncio
import time
from functools import wraps, cached_property
import uuid
from contextlib import suppress, asynccontextmanager
from loguru import logger
from types import TracebackType
from typing import (
    Any,
    Literal,
    Optional,
    Tuple,
    Type,
    Callable,
    Set,
    Coroutine,
    Hashable,
    Awaitable,
    Iterable,
    TypeVar,
    Union,
    ParamSpec,
)

import orjson
import websockets
from websockets import ClientConnection, ConnectionClosed, State
from aiolimiter import AsyncLimiter

from pyutils import TaskManager
from .utils import HttpSession
from .setting import IS_DEBUG
from .data_type import WssConfig, AccountConfig, RestConfig

P = ParamSpec("P")
T = TypeVar("T")


def catch_it(func: Callable[P, Coroutine[Any, Any, T]]) -> Callable[P, Coroutine[Any, Any, T | None]]:
    @wraps(func)
    async def wrapper_func(*args: P.args, **kwargs: P.kwargs):
        try:
            return await func(*args, **kwargs)
        except Exception as ex:
            if IS_DEBUG:
                logger.exception(f"client exception in {func.__name__} with args={args} kwargs={kwargs}: {ex}")
            else:
                logger.error(f"client exception in {func.__name__} with args={args} kwargs={kwargs}: {ex}")
        return None

    return wrapper_func


class BaseRestClient:
    def __init__(
        self,
        account_config: AccountConfig,
        rest_config: RestConfig,
    ):
        self.api_key = account_config.api_key
        self.secret_key = account_config.secret_key
        self.passphrase = account_config.passphrase
        self.base_url = rest_config.url
        local_addrs = rest_config.bind_ips or [None]
        timeout = rest_config.timeout
        tracing = rest_config.tracing
        proxy = rest_config.proxy
        self._http_sessions: list[HttpSession] = []
        self._session_index: int = 0
        self._http_sessions: list[HttpSession] = [
            HttpSession(bind_ip=ip, timeout=timeout, tracing=tracing, proxy=proxy) for ip in local_addrs
        ]

    @property
    def http_sess(self):
        return self._http_sessions[self._session_index]

    def is_auth(self):
        return bool(self.api_key and self.secret_key)

    async def close(self):
        for sess in self._http_sessions:
            await sess.close()

    async def __aenter__(self):
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        await self.close()

    def gen_request(
        self,
        method: Literal["GET", "POST", "PUT", "DELETE"],
        path: str = "",
        params: Optional[dict[str, Any]] = None,
        auth: bool = False,
        payload: Optional[Any] = None,
    ) -> Tuple[str, Optional[dict[str, Any]], Optional[dict[str, Any]], Optional[str]]:
        raise NotImplementedError

    @catch_it
    async def raw_request(
        self,
        method: Literal["GET", "POST", "PUT", "DELETE"],
        path: str = "",
        params: Optional[dict[str, Any]] = None,
        auth: bool = False,
        payload: Optional[Any] = None,
    ):
        url, headers, params, payload_string = self.gen_request(
            method, path, params=params, auth=auth, payload=payload
        )

        session = self._http_sessions[self._session_index]
        self._session_index = (self._session_index + 1) % len(self._http_sessions)
        raw_data = await session.request(method, url, headers=headers, params=params, data=payload_string)

        return raw_data


class BaseWsClient:
    _conn_limiter: AsyncLimiter | None = None

    def __init__(
        self,
        wss_config: WssConfig,
    ):
        self._url = wss_config.url
        self._topics = set(wss_config.topics or [])
        self._timeout = wss_config.timeout
        self._client_id = uuid.uuid4()
        self._name = wss_config.name
        self._reconn_interval = wss_config.reconnect_interval
        self._heartbeat_interval = wss_config.heartbeat_interval
        self._heartbeat_timeout = wss_config.heartbeat_timeout
        self._req_timeout = wss_config.request_timeout

        self._ws: Optional[ClientConnection] = None
        self._msg_callbacks: Set[Callable[[Any], Awaitable[None]]] = set()
        self._connected_callback: Callable[[str], Awaitable[None]] | None = None
        self._disconnected_callback: Callable[[str], Awaitable[None]] | None = None
        self._closed = asyncio.Event()
        self._closed.set()
        self._ws_ready = asyncio.Event()
        self._heartbeat_tasks: Set[asyncio.Task] = set()
        self._heartbeat_failed_count = 0
        self._conn_count = 0
        self._data_recv_count = 0
        self._req_futures: dict[Hashable, asyncio.Future] = {}
        self._req_id = int(time.time() * 1000000)
        self._logger = logger.bind(name=self._name, client_id=self._client_id)
        self._task_mngr = TaskManager()
        self._loop = None

        if wss_config.bind_ip:
            self._local_addr = (wss_config.bind_ip, 0)
        else:
            self._local_addr = None

    @classmethod
    def set_conn_limiter(cls, max_rate: float, time_period: float):
        cls._conn_limiter = AsyncLimiter(max_rate, time_period)

    @property
    def client_id(self):
        return self._client_id

    @property
    def url(self):
        return self._url

    @property
    def topics(self):
        return self._topics

    @property
    def closed(self):
        return self._closed.is_set()

    @property
    def logger(self):
        return self._logger

    @property
    def tasks(self):
        return self._task_mngr.tasks

    @property
    def req_id_key(self):
        raise NotImplementedError

    @property
    def curr_req_id(self) -> int:
        return self._req_id

    @property
    def next_req_id(self) -> int:
        self._req_id += 1
        return self._req_id

    @cached_property
    def name(self) -> str:
        return f"<{self._name}-wscli|{self._client_id}>"

    def __str__(self) -> str:
        return self.name

    def get_topics(self) -> list[str]:
        return list(self._topics)

    def add_task(self, coro: Coroutine[Any, None, None], *, name=None):
        self._task_mngr.add_task(coro, name=name)

    def del_task(self, task: asyncio.Task):
        self._task_mngr.del_task(task)

    def register_msg_callback(self, msg_callback: Callable[[Any], Awaitable[None]]):
        self._msg_callbacks.add(msg_callback)

    def unregister_msg_callback(self, msg_callback: Callable[[Any], Awaitable[None]]):
        self._msg_callbacks.discard(msg_callback)

    def register_connected_callback(self, connected_callback: Callable[[str], Awaitable[None]]):
        self._connected_callback = connected_callback
        self._logger.debug(
            "{uname} register_connected_callback - {callback}",
            uname=self,
            callback=connected_callback,
        )

    def register_disconnected_callback(self, disconnected_callback: Callable[[str], Awaitable[None]]):
        self._disconnected_callback = disconnected_callback

    def skip_msg(self, msg: dict[str, Any]) -> bool:
        return False

    async def heartbeat(self, timeout: float):
        if not self._ws:
            return
        try:
            latency = await self.request_heartbeat(timeout)
            self._logger.debug("{uname} heartbeat - latency={latency}", uname=self, latency=latency)
        except asyncio.TimeoutError:
            self._heartbeat_failed_count += 1
            self._logger.warning(
                "{uname} heartbeat timeout - timeout={timeout} failed_count={failed_count}",
                uname=self,
                timeout=timeout,
                failed_count=self._heartbeat_failed_count,
            )
        except Exception as e:
            self._heartbeat_failed_count += 1
            self._logger.error(
                "{uname} heartbeat failed - e={e} failed_count={failed_count}",
                uname=self,
                e=e,
                failed_count=self._heartbeat_failed_count,
            )
        else:
            self._heartbeat_failed_count = 0
            if latency >= timeout / 2:
                self._logger.warning(
                    "{uname} heartbeat request elapsed time might be too long - latency={latency} failed_count={failed_count}",
                    uname=self,
                    latency=latency,
                    failed_count=self._heartbeat_failed_count,
                )

    async def request_heartbeat(self, timeout: float) -> float:
        return 0

    async def before_connect(self):
        pass

    async def on_connected(self):
        await self.subscribe(self.topics)
        if self._connected_callback:
            await self._connected_callback(self._name)

    async def on_disconnected(self):
        if self._disconnected_callback:
            await self._disconnected_callback(self._name)

    async def on_raw_msg(self, msg: Union[list[Any], dict[str, Any]]):
        pass

    async def handle_raw_msg(self, raw_msg: str) -> Optional[dict[str, Any]]:
        msg = orjson.loads(raw_msg)
        await self.on_raw_msg(msg)

        # set resp
        if (
            isinstance(msg, dict)
            and (_id := msg.get(self.req_id_key))
            and (fut := self._req_futures.pop(str(_id), None))
            and (not fut.done())
        ):
            fut.set_result(msg)

        if self.skip_msg(msg):
            return

        return msg

    @asynccontextmanager
    async def wrap_conn(self, *coros):
        try:
            tasks = [asyncio.create_task(coro) for coro in coros]
            self._logger.debug("{uname} on_connected", uname=self)
            await self.on_connected()
            yield asyncio.gather(*tasks)
        finally:
            self._logger.debug("{uname} on_disconnected", uname=self)
            await self.on_disconnected()

    async def subscribe(self, topics: Optional[Iterable[str]]):
        topics = list(topics or [])
        self._logger.info("{uname} try subscribe - topic_count={topic_count}", uname=self, topic_count=len(topics))

        try:
            if topics:
                await self.request_subscribe(topics)
        except Exception as e:
            self._logger.exception("{uname} subscribe failed - {topics}", uname=self, topics=topics)
            raise e
        else:
            self._topics.update(topics)
            self._logger.success("{uname} subscribe done - {topics}", uname=self, topics=topics)

    async def unsubscribe(self, topics: Optional[Iterable[str]]):
        topics = list(topics or [])
        self._logger.info("{uname} try unsubscribe - topic_count={topic_count}", uname=self, topic_count=len(topics))

        try:
            if topics:
                await self.request_unsubscribe(topics)
        except Exception as e:
            self._logger.exception("{uname} unsubscribe failed - topics={topics}", uname=self, topics=topics)
            raise e
        else:
            self._topics.difference_update(topics)
            self._logger.success("{uname} unsubscribe done - topics={topics}", uname=self, topics=topics)

    async def request_subscribe(self, topics: Iterable[str]):
        pass

    async def request_unsubscribe(self, topics: Iterable[str]):
        pass

    async def _msg_loop(self):
        while not self.closed:
            raw_msg = await self.recv()
            msg = await self.handle_raw_msg(raw_msg=raw_msg)

            if not msg:
                continue

            self._data_recv_count += 1
            if not self._msg_callbacks:
                continue

            await asyncio.gather(*(cb(msg) for cb in self._msg_callbacks))

    async def _run_ws(self):
        while not self.closed:
            try:
                if self._conn_limiter:
                    await self._conn_limiter.acquire()  # add limiter to resolve api limit

                self._conn_count += 1
                self._heartbeat_failed_count = 0
                self._logger.debug("{uname} before_connect", uname=self)
                await self.before_connect()
                self._logger.info("{uname} ws connecting - {url}", uname=self, url=self.url)

                conn_params = {"ping_timeout": None, "max_queue": 2**8, "local_addr": self._local_addr}
                async with websockets.connect(self.url, **conn_params) as self._ws:
                    self._reconn_interval = max(1, self._reconn_interval / 2)
                    self._logger.success(
                        "{uname} ws connected - conn_count={conn_count} url={url}",
                        uname=self,
                        conn_count=self._conn_count,
                        url=self.url,
                    )
                    self._ws_ready.set()
                    async with self.wrap_conn(self._msg_loop()) as task:
                        await task

            except asyncio.TimeoutError:
                self._logger.warning(
                    "{uname} ws recv timeout - timeout={timeout} conn_count={conn_count}",
                    uname=self,
                    timeout=self._timeout,
                    conn_count=self._conn_count,
                )
            except ConnectionClosed as e:
                self._logger.warning(
                    "{uname} ws closed - {e} - conn_count={conn_count}",
                    uname=self,
                    e=e,
                    conn_count=self._conn_count,
                )
            except Exception as e:
                self._logger.exception(
                    "{uname} ws failed! - conn_count={conn_count}", uname=self, conn_count=self._conn_count
                )
                self._reconn_interval = min(30, self._reconn_interval * 5)
            finally:
                # set ws not ready
                self._ws_ready.clear()

                # clear heartbeat tasks
                for t in self._heartbeat_tasks:
                    t.cancel()
                self._heartbeat_tasks.clear()

                # clear task manager tasks

                # clear req_futures
                for req_id, fut in self._req_futures.items():
                    if not fut.done():
                        fut.set_exception(Exception(f"ws#{self._client_id} was disconnected"))
                    else:
                        self._logger.warning(
                            "{uname} request: {req_id} was done but still in cache.", uname=self, req_id=req_id
                        )
                self._req_futures.clear()

            await self.sleep_or_closed(self._reconn_interval)
            if not self.closed:
                self._logger.info(
                    "{uname} ws reconnect - conn_count={conn_count} reconn_interval={reconn_interval}",
                    uname=self,
                    conn_count=self._conn_count,
                    reconn_interval=self._reconn_interval,
                )
            # else:
            #     for t in self.tasks:
            #         t.cancel()

    async def _run_heartbeat(self):
        while not self.closed:
            await self.sleep_or_closed(1)

            # wait until connected
            try:
                await asyncio.wait_for(self._ws_ready.wait(), timeout=5)
            except asyncio.TimeoutError:
                continue

            await self.time_for_heartbeat()
            heartbeat_task = asyncio.create_task(self.heartbeat(self._heartbeat_timeout))
            self._heartbeat_tasks.add(heartbeat_task)
            heartbeat_task.add_done_callback(lambda t: self._heartbeat_tasks.discard(t))

            if self._heartbeat_failed_count >= 10:
                self._logger.warning(
                    "{uname} close ws because of heartbeat failed - failed_count={failed_count}",
                    uname=self,
                    failed_count=self._heartbeat_failed_count,
                )
                if self._ws is not None:
                    await self._ws.close()

    async def _check_data_timeout(self):
        while not self.closed:
            await self.sleep_or_closed(10)
            loop = asyncio.get_event_loop()
            latest_ts = loop.time()
            count = self._data_recv_count
            while self._ws is not None and self._ws.state == State.OPEN:
                await self.sleep_or_closed(30)
                if not self.topics:
                    self._logger.debug(
                        "{uname} check data timeout - no topic - cur_count={cur_count} last_count={last_count}",
                        uname=self,
                        cur_count=self._data_recv_count,
                        last_count=count,
                    )
                    continue

                if self._data_recv_count > count:
                    self._logger.debug(
                        "{uname} check data timeout - count - cur_count={cur_count} last_count={last_count}",
                        uname=self,
                        cur_count=self._data_recv_count,
                        last_count=count,
                    )
                    count = self._data_recv_count
                    latest_ts = loop.time()
                    continue

                cur_ts = loop.time()
                if cur_ts - latest_ts > self._timeout and self._ws is not None:
                    self._logger.error(
                        "{uname} check data timeout - reach timeout - timeout={timeout} data_recv_count={data_recv_count}",
                        uname=self,
                        timeout=self._timeout,
                        data_recv_count=self._data_recv_count,
                    )
                    await self._ws.close()
                    break

    def init_tasks(self):
        self.add_task(self._run_ws(), name=f"ws#{self._client_id}")
        self.add_task(self._run_heartbeat(), name=f"heartbeat#{self._client_id}")
        self.add_task(self._check_data_timeout(), name=f"check_data_timeout#{self._client_id}")

    async def run(self):
        if not self.closed:
            raise Exception(
                f"ws client[{self._name}|{self._client_id}] is running. please close it before rerun again"
            )

        self._loop = asyncio.get_event_loop()

        self._closed.clear()
        self.init_tasks()

        await self._task_mngr.run()

    async def close(self):
        if self._ws:
            self._logger.info("{uname} closing ws", uname=self)
            self._closed.set()
            await self._ws.close()
            self._ws = None

    async def sleep_or_closed(self, delay: float):
        with suppress(asyncio.TimeoutError):
            await asyncio.wait_for(self._closed.wait(), timeout=delay)

    async def time_for_heartbeat(self):
        await self.sleep_or_closed(self._heartbeat_interval)

    async def _send(self, msg: str):
        if not self._ws:
            raise ConnectionError(f"WebSocket client {self._client_id} is not connected.")
        else:
            self._logger.debug("{uname} send - msg={msg}", uname=self, msg=msg)
            await self._ws.send(msg)

    async def _recv(self) -> str:
        if self._ws:
            msg = await self._ws.recv()
            self._logger.debug("{uname} recv - msg={msg}", uname=self, msg=msg)
            if isinstance(msg, bytes):
                msg = msg.decode()
            elif not isinstance(msg, str):
                raise TypeError(f"Received message is not a string or bytes: {type(msg)}")
            return msg
        else:
            raise ConnectionError(f"WebSocket client {self._client_id} is not connected.")

    async def send(self, payload: dict[str, Any]) -> None:
        await asyncio.wait_for(self._ws_ready.wait(), timeout=60)
        if self.req_id_key not in payload:
            payload[self.req_id_key] = self.next_req_id

        raw_msg = orjson.dumps(payload).decode()
        await self._send(raw_msg)

    async def recv(self) -> str:
        raw_msg = await asyncio.wait_for(self._recv(), timeout=self._timeout)
        return raw_msg

    async def request(self, payload: dict[str, Any], timeout: Optional[float] = None) -> dict[str, Any]:
        req_id = self.next_req_id
        self._logger.debug("{uname} request - start - req_id={req_id}", uname=self, req_id=req_id)
        payload[self.req_id_key] = req_id
        loop = self._loop or asyncio.get_running_loop()
        fut = loop.create_future()
        self._req_futures[str(req_id)] = fut

        await self.send(payload)
        timeout = timeout or self._req_timeout
        result = await asyncio.wait_for(fut, timeout=timeout)
        self._logger.debug("{uname} request - end - req_id={req_id}", uname=self, req_id=req_id)
        return result
