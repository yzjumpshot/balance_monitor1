import time
from typing import Any
from ..base_wrapper import BaseMarketWssWrapper
from loguru import logger
from ..enum_type import Event
from ..data_type import *


class DeribitMarketWssWrapper(BaseMarketWssWrapper):
    def __init__(self, account_meta: AccountMeta, account_config: AccountConfig, wss_config: WssConfig):
        super().__init__(account_meta, account_config, wss_config)

    async def subscribe_stream(self, symbols: list[str] | None = None) -> None:
        if not symbols:
            symbols = list(self._subscribed_symbols)
        assert self._ws_client, "WebSocket client is not initialized"

        topic_list = []
        for event in self.registered_events:
            for s in symbols:
                match event:
                    case Event.BOOK:
                        topic_list.append(f"book.{s}.none.20.100ms")

        await self._ws_client.subscribe(topic_list)
