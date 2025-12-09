import asyncio
from xclients.get_client import get_ws_client
from xclients.get_wrapper import get_account_ws_wrapper
from xclients.enum_type import ExchangeName, MarketType, AccountType, Event
from xclients.data_type import AccountMeta, AccountConfig, WssConfig, MarketMeta
import typer
from typing import Literal, Any, Optional

from loguru import logger


def run_ws(
    exchange: str,
    market_type: str,
    account_type: str,
    accounts: Optional[list[str]] = typer.Option(None),
    raw: bool = False,
    topics: Optional[list[str]] = typer.Option(None),
):
    loop = asyncio.get_event_loop()
    if raw:
        if accounts:
            task = asyncio.gather(
                *[_run_raw_ws(exchange, market_type, account_type, acc, topics=topics) for acc in accounts]
            )
        else:
            task = asyncio.gather(_run_raw_ws(exchange, market_type, account_type, None, topics=topics))
    else:
        if accounts:
            task = asyncio.gather(*[_run_ws_wrapper(exchange, market_type, account_type, acc) for acc in accounts])
        else:
            task = asyncio.gather(_run_ws_wrapper(exchange, market_type, account_type, None))

    loop.run_until_complete(task)


async def _run_raw_ws(
    exchange: str, market_type: str, account_type: str, account: Optional[str], topics: Optional[list] = None
):
    topics = topics or []

    async def on_message(data: Any):
        logger.info(f"{account}@{exchange}|{market_type} on message: {data}")

    ws_client = get_ws_client(exch_name=ExchangeName[exchange], market_type=MarketType[market_type], account_type=AccountType[account_type], account=account, wss_config=WssConfig(topics=topics))  # type: ignore
    ws_client.register_msg_callback(on_message)
    await ws_client.run()


async def _run_ws_wrapper(exchange: str, market_type: str, account_type: str, account: Optional[str]):
    async def print_msg(event: Event, data: Any):
        logger.info(f"{account}@{exchange}|{market_type} on {event}: {data}")

    ws_wrapper = get_account_ws_wrapper(
        ExchangeName[exchange], MarketType[market_type], AccountType[account_type], account_name=account
    )
    ws_wrapper.subscribe_callback(Event.BALANCE, print_msg)
    ws_wrapper.subscribe_callback(Event.ORDER, print_msg)
    ws_wrapper.subscribe_callback(Event.USER_TRADE, print_msg)
    ws_wrapper.subscribe_callback(Event.POSITION, print_msg)

    await ws_wrapper.run()


if __name__ == "__main__":
    typer.run(run_ws)
