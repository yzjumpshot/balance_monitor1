import asyncio

from typing import Iterable
from .rest import BinanceUnifiedRestClient, BinanceSpotRestClient, BinanceLinearRestClient, BinanceInverseRestClient
from ..base_client import BaseWsClient
from ..data_type import WssConfig
from ..get_client import get_rest_client
from ..enum_type import MarketType
from ..data_type import MarketMeta, AccountMeta, AccountConfig


class BinanceWsClient(BaseWsClient):
    def __init__(self, market_meta: MarketMeta, wss_config: WssConfig):
        if not wss_config.name:
            wss_config.name = str(market_meta)
        super().__init__(wss_config)

    @property
    def req_id_key(self):
        return "id"

    async def on_connected(self):
        await super().on_connected()

    async def request_heartbeat(self, timeout: float) -> float:
        if self._ws:
            pong_waiter = await self._ws.ping()
            latency = await asyncio.wait_for(pong_waiter, timeout=timeout)
            return latency
        return 0.0

    async def request_subscribe(self, topics: Iterable[str]):
        topics_list = list(topics)
        for i in range(0, len(topics_list), 100):
            payload = {"method": "SUBSCRIBE", "params": topics_list[i : i + 100]}
            await self.request(payload)

    async def request_unsubscribe(self, topics: Iterable[str]):
        topics_list = list(topics)
        for i in range(0, len(topics_list), 100):
            payload = {"method": "UNSUBSCRIBE", "params": topics_list[i : i + 100]}
            await self.request(payload)


class BinancePrivateWsClient(BinanceWsClient):
    def __init__(
        self,
        account_meta: AccountMeta,
        account_config: AccountConfig,
        wss_config: WssConfig,
    ):
        if not wss_config.name:
            wss_config.name = str(account_meta)

        super().__init__(account_meta.market, wss_config)
        self._rest_client = get_rest_client(account_meta, account_config)
        self._listen_key = ""
        self._market_type = account_meta.market_type

    @property
    def url(self):
        return f"{self._url}/{self._listen_key}"

    async def before_connect(self):
        self._listen_key = await self.get_listen_key()

    async def delay_listen_key(self) -> None:
        cli = self._rest_client
        match self._market_type:
            case MarketType.SPOT if isinstance(cli, BinanceSpotRestClient):
                await cli.api_v3_delay_listen_key(self._listen_key)
            case MarketType.MARGIN if isinstance(cli, BinanceSpotRestClient):
                await cli.sapi_v1_delay_listen_key(self._listen_key)
            case MarketType.UPERP | MarketType.UDELIVERY if isinstance(cli, BinanceLinearRestClient):
                await cli.fapi_v1_delay_listen_key(self._listen_key)
            case MarketType.CPERP | MarketType.CDELIVERY if isinstance(cli, BinanceInverseRestClient):
                await cli.dapi_v1_delay_listen_key(self._listen_key)
            case _:
                raise ValueError(f"Unsupported market type: {self._market_type}")

    async def get_listen_key(self) -> str:
        cli = self._rest_client
        match self._market_type:
            case MarketType.SPOT if isinstance(cli, BinanceSpotRestClient):
                res = await cli.api_v3_listen_key()
            case MarketType.MARGIN if isinstance(cli, BinanceSpotRestClient):
                res = await cli.sapi_v1_listen_key()
            case MarketType.UPERP | MarketType.UDELIVERY if isinstance(cli, BinanceLinearRestClient):
                res = await cli.fapi_v1_listen_key()
            case MarketType.CPERP | MarketType.CDELIVERY if isinstance(cli, BinanceInverseRestClient):
                res = await cli.dapi_v1_listen_key()
            case _:
                raise ValueError(f"Unsupported market type: {self._market_type}")

        if not res:
            return ""

        listen_key = res["listenKey"]
        return listen_key

    async def _run_delay_key(self):
        while not self.closed:
            await asyncio.sleep(60 * 30)
            if self._listen_key:
                await self.delay_listen_key()

    def init_tasks(self):
        super().init_tasks()
        self.add_task(self._run_delay_key(), name=f"delay_key#{self._client_id}")


class BinanceUnifiedPrivateWsClient(BinanceWsClient):
    def __init__(
        self,
        account_meta: AccountMeta,
        account_config: AccountConfig,
        wss_config: WssConfig,
    ):
        if not wss_config.name:
            wss_config.name = str(account_meta)

        super().__init__(account_meta.market, wss_config)
        self._rest_client: BinanceUnifiedRestClient = get_rest_client(account_meta, account_config)
        self._listen_key = ""

    @property
    def url(self):
        return f"{self._url}/ws/{self._listen_key}"

    async def before_connect(self):
        self._listen_key = await self.get_listen_key()

    async def get_listen_key(self) -> str:
        res = await self._rest_client.papi_v1_listen_key()
        if not res:
            return ""
        listen_key = res["listenKey"]
        return listen_key

    async def delay_listen_key(self) -> None:
        await self._rest_client.papi_v1_delay_listen_key()
