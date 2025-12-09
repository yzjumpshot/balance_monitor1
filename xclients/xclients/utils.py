import asyncio
import inspect
import os
import time
from decimal import Decimal
from functools import wraps
from types import TracebackType
from typing import Any, Coroutine, Literal, Optional, Type, Union, Callable, Awaitable, Tuple
from urllib.parse import urlencode
from .enum_type import Event
import aiohttp
import orjson
from loguru import logger

from pyutils import AccountCredentialManager
from .setting import XCLIENTS_CONFIG_LOADER
from .enum_type import ExchangeName, MarketType, AccountType
from .data_type import AccountMeta, AccountConfig, WssConfig


class HttpSession:
    def __init__(
        self,
        bind_ip: Optional[str] = None,
        timeout: Optional[int] = None,
        tracing: Optional[bool] = None,
        proxy: Optional[str] = None,
    ):
        """A HTTP session, which maintains a connection pool and provides a unified interface for requests.

        Args:
            bind_ip (Optional[str], optional): Source IP to bind to. Defaults to None.
            timeout (int, optional): Timeout in seconds. Defaults to 300.
            tracing (bool, optional): Whether to enable tracing. Defaults to False.
            proxy (Optional[str], optional): Proxy URL to use for requests. Defaults to None.
        """
        self.hs_config = self._gen_hs_config(bind_ip, timeout, tracing, proxy)
        self._conn_config = self._gen_conn_config()
        self._sess_config = self._gen_sess_config()
        self._multiplex = bool(self.hs_config.get("multiplex", False))
        self._sess: aiohttp.ClientSession | None = None

    @property
    def closed(self):
        return self._sess is None or self._sess.closed

    def _gen_hs_config(
        self,
        bind_ip: Optional[str] = None,
        timeout: Optional[int] = None,
        tracing: Optional[bool] = None,
        proxy: Optional[str] = None,
    ) -> dict:
        user_config = {
            "http_session": {
                k: v
                for k, v in {"bind_ip": bind_ip, "timeout": timeout, "tracing": tracing, "proxy": proxy}.items()
                if v is not None
            }
        }
        try:
            config = XCLIENTS_CONFIG_LOADER.load_config(user_config)["http_session"]
        except Exception:
            logger.exception("[HttpSession] load config using ConfigLoader failed, treat as no config at all")
            config = {}

        return config

    def _gen_conn_config(self):
        """Generates the connection configuration.

        Args:
            bind_ip (Optional[str]): Source IP to bind to.

        Returns:
            dict: Connection configuration.
        """
        conn_kwargs = {}
        bind_ip = self.hs_config.get("bind_ip")
        if bind_ip:
            conn_kwargs["local_addr"] = (bind_ip, 0)
        return conn_kwargs

    def _gen_sess_config(self):
        """Generates the session configuration.

        Args:
            timeout (int): Timeout for requests in seconds.
            tracing (bool): Whether to enable tracing.

        Returns:
            dict: Session configuration.
        """
        sess_kwargs = {}
        timeout = self.hs_config.get("timeout", 300)  # default is 300 s
        tracing = self.hs_config.get("tracing", False)
        proxy = self.hs_config.get("proxy")

        sess_kwargs["timeout"] = aiohttp.ClientTimeout(total=timeout)

        if tracing:
            trace_config = aiohttp.TraceConfig()
            trace_config.on_request_start.append(self._on_request_start)
            trace_config.on_request_exception.append(self._on_request_exception)
            trace_config.on_request_end.append(self._on_request_end)
            sess_kwargs["trace_configs"] = [trace_config]

        if proxy:
            sess_kwargs["proxy"] = proxy
        else:
            for env_key in ["HTTPS_PROXY", "https_proxy", "HTTP_PROXY", "http_proxy"]:
                if proxy := os.getenv(env_key):
                    sess_kwargs["proxy"] = proxy
                    break
        return sess_kwargs

    @staticmethod
    async def _on_request_start(session, context, params):
        context.start_ts = time.time()
        logger.info(f"[on_request_start]{params.method} {params.url} start - start_ts={context.start_ts}")

    @staticmethod
    async def _on_request_exception(session, context, params):
        logger.info(f"[on_request_exception]{params.method} {params.url} failed - exception={params.exception}")

    @staticmethod
    async def _on_request_end(session, context, params):
        context.end_ts = time.time()
        elapsed = context.end_ts - context.start_ts
        logger.info(f"[on_request_end]{params.method} {params.url} end - end_ts={context.end_ts} elapsed={elapsed}")

    @staticmethod
    def _json_serialize(obj: Any) -> str:
        return orjson.dumps(obj).decode()

    @staticmethod
    def _json_deserialize(s: str) -> Any:
        return orjson.loads(s)

    async def request(
        self,
        method: Literal["GET", "POST", "PUT", "DELETE"],
        url: str = "",
        *,
        headers: Optional[dict[str, Any]] = None,
        params: Optional[dict[str, Any]] = {},
        data: Any = None,
        raise_if_err: bool = False,
    ):
        if self._multiplex:
            if self.closed:
                connector = aiohttp.TCPConnector(**self._conn_config)
                self._sess = aiohttp.ClientSession(
                    connector=connector, json_serialize=self._json_serialize, **self._sess_config
                )
            assert self._sess is not None, "Session is not initialized, please check the connection settings."
            async with self._sess.request(method, url, headers=headers, params=params, data=data) as resp:
                if raise_if_err and not resp.ok:
                    raise aiohttp.ClientResponseError(
                        resp.request_info,
                        resp.history,
                        status=resp.status,
                        message=await resp.text(),
                        headers=resp.headers,
                    )
                data = await resp.json(loads=self._json_deserialize, content_type=None)
        else:
            connector = aiohttp.TCPConnector(**self._conn_config)
            async with aiohttp.ClientSession(
                connector=connector, json_serialize=self._json_serialize, **self._sess_config
            ) as sess:
                async with sess.request(method, url, headers=headers, params=params, data=data) as resp:
                    if raise_if_err and not resp.ok:
                        raise aiohttp.ClientResponseError(
                            resp.request_info,
                            resp.history,
                            status=resp.status,
                            message=await resp.text(),
                            headers=resp.headers,
                        )
                    data = await resp.json(loads=self._json_deserialize, content_type=None)

        return data

    async def close(self):
        if not self.closed:
            if self._sess is not None:
                await self._sess.close()

            self._sess = None

    async def __aenter__(self):
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        await self.close()


def clean_none_value(d: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for k in d.keys():
        if d[k] is not None:
            out[k] = d[k]
        if isinstance(d[k], bool):
            out[k] = "true" if d[k] else "false"
    return out


def get_current_sec():
    return int(time.time())


def get_current_ms():
    return int(time.time() * 1000)


def get_current_us():
    return int(time.time() * 1000000)


def encoded_string(query: dict[str, Any]):
    return urlencode(query, True).replace("%40", "@")


def to_decimal(v: Union[str, float, int]):
    return Decimal(str(v))


def decimal_to_string(obj):
    if isinstance(obj, Decimal):
        return str(obj)
    raise TypeError(f"Object of type '{type(obj).__name__}' is not JSON serializable")


class EventBus:
    def __init__(self):
        self._subscribers: dict[Event, list[Callable[..., Awaitable[None]]]] = {}

    def subscribe(self, event: Event, callback: Callable[..., Awaitable[None]]):
        if not inspect.iscoroutinefunction(callback):
            raise ValueError("Callback must be a coroutine function")
        if event not in self._subscribers:
            self._subscribers[event] = []
        self._subscribers[event].append(callback)

    def unsubscribe(self, event: Event, callback: Callable[..., Awaitable[None]]):
        if event in self._subscribers:
            self._subscribers[event].remove(callback)
            if not self._subscribers[event]:
                del self._subscribers[event]

    async def publish(self, event: Event, *args: Any, **kwargs: Any):
        if event in self._subscribers:
            for callback in self._subscribers[event]:
                try:
                    await callback(*args, **kwargs)
                except Exception as e:
                    logger.exception(f"Error in event callback for {event}: {e}")

    def get_registered_events(self) -> list[Event]:
        return list(self._subscribers.keys())


# 提取公共的参数处理逻辑
def gen_account_meta_and_config(*args: Any, **kwargs: Any) -> Tuple[AccountMeta, AccountConfig]:
    # 处理参数并返回 account_meta 和 account_config
    exch_name = kwargs.get("exch_name") or args[0]
    market_type = kwargs.get("market_type") or (args[1] if len(args) > 1 else MarketType.SPOT)
    account_type = kwargs.get("account_type") or (args[2] if len(args) > 2 else AccountType.NORMAL)
    account_name = kwargs.get("account_name") or (args[3] if len(args) > 3 else "")
    api_key = kwargs.get("api_key") or (args[4] if len(args) > 4 else "")
    secret_key = kwargs.get("secret_key") or (args[5] if len(args) > 5 else "")
    passphrase = kwargs.get("passphrase") or (args[6] if len(args) > 6 else "")
    uid = kwargs.get("uid") or (args[7] if len(args) > 7 else "")

    account_meta = AccountMeta(
        exch_name=ExchangeName[exch_name] if isinstance(exch_name, str) else exch_name,
        market_type=MarketType[market_type] if isinstance(market_type, str) else market_type,
        account_type=AccountType[account_type] if isinstance(account_type, str) else account_type,
        account_name=account_name,
    )
    if account_name and not (api_key and secret_key):
        acm = AccountCredentialManager()
        api_key, secret_key, passphrase, uid = acm.get_credential(
            account_name, account_meta.exch_name.value, account_meta.market_type.value, account_meta.account_type.value
        )
        account_config = AccountConfig(api_key=api_key, secret_key=secret_key, passphrase=passphrase, uid=uid)
    else:
        account_config = AccountConfig(api_key=api_key, secret_key=secret_key, passphrase=passphrase, uid=uid)

    return account_meta, account_config
