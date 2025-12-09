import asyncio
from xclients.get_client import get_rest_client
from xclients.enum_type import ExchangeName, MarketType, AccountType
import pytest
import time


@pytest.mark.asyncio
async def test_sapi_loan_vip():
    cli = get_rest_client(ExchangeName.BINANCE, MarketType.SPOT, AccountType.NORMAL, "4abntest1")

    request = await cli.sapi_loan_vip_request_data()
    print(request)

    collateral_data = await cli.sapi_loan_vip_collateral_data()
    print(collateral_data)

    loanable_data = await cli.sapi_loan_vip_loanable_data()
    print(loanable_data)
