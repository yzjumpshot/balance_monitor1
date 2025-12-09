# !/usr/bin/env python
# -*-coding:utf-8 -*-

"""
Author     : Kevin Leung
File       : kucoin.py
time       : 2023/3/7 13:54
Description: None
IDE        : PyCharm
"""

import base64
import hashlib
import hmac
import json
import time
from decimal import Decimal
from typing import Any, Literal, Optional, Union
from urllib.parse import urlencode
from uuid import uuid4

from ..data_type import AccountConfig, RestConfig
from ..base_client import BaseRestClient, catch_it
from ..utils import clean_none_value


class KucoinRestClient(BaseRestClient):
    def __init__(
        self,
        account_config: AccountConfig,
        rest_config: RestConfig,
    ):
        super().__init__(account_config, rest_config)
        if self.passphrase and self.secret_key:
            self.crypto_passphrase = base64.b64encode(
                hmac.new(self.secret_key.encode("utf-8"), self.passphrase.encode("utf-8"), hashlib.sha256).digest()
            )

    def _sign_request(self, http_method, endpoint, params=None, payload_string=None):
        if not self.secret_key:
            raise ValueError("secret_key is required")

        ts = int(time.time() * 1000)
        query_string = urlencode(params or {})
        payload_string = payload_string or ""
        if http_method in ("GET", "DELETE") and query_string:
            str_to_sign = f"{ts}{http_method}{endpoint}?{query_string}{payload_string}"
        else:
            str_to_sign = f"{ts}{http_method}{endpoint}{payload_string}"

        sign = base64.b64encode(
            hmac.new(self.secret_key.encode("utf-8"), str_to_sign.encode("utf-8"), hashlib.sha256).digest()
        )

        headers = {
            "KC-API-SIGN": sign.decode("utf-8"),
            "KC-API-TIMESTAMP": str(ts),
            "KC-API-KEY": self.api_key,
            "KC-API-PASSPHRASE": self.crypto_passphrase.decode("utf-8"),
            "KC-API-KEY-VERSION": "2",
            "Content-Type": "application/json",
        }
        return headers

    def gen_request(
        self,
        method: Literal["GET", "POST", "PUT", "DELETE"],
        path: str = "",
        params: Optional[dict[str, Any]] = None,
        auth: bool = False,
        payload: Optional[Any] = None,
    ):
        url = f"{self.base_url}{path}"
        headers = None
        params = clean_none_value(params or {})
        payload = clean_none_value(payload or {})
        payload_string = json.dumps(payload) if payload else ""
        if auth == True:
            headers = self._sign_request(method, path, params, payload_string)

        return url, headers, params, payload_string

    @catch_it
    async def fetch_fills(
        self,
        symbol: Optional[str] = None,
        side: Optional[str] = None,
        start_at: Optional[int] = None,
        end_at: Optional[int] = None,
        trade_type: Optional[str] = None,
        current_page: Optional[int] = None,
        page_size: Optional[int] = None,
        last_id: Optional[int] = None,
        isHf: bool = False,
    ):
        """
        获取成交记录(last_id仅限于isHf为True的时候使用)
        """
        path = "/api/v1/fills" if not isHf else "/api/v1/hf/fills"
        if not isHf:
            params = {
                "symbol": symbol,
                "side": side,
                "startAt": start_at,
                "endAt": end_at,
                "tradeType": trade_type,
                "currentPage": current_page,
                "pageSize": page_size,
            }
        else:
            params = {
                "symbol": symbol,
                "side": side,
                "startAt": start_at,
                "endAt": end_at,
                "lastId": last_id,
                "limit": page_size,
            }

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def get_order_history(
        self,
        status: Optional[str] = None,
        symbol: Optional[str] = None,
        side: Optional[str] = None,
        type: Optional[str] = None,
        start_at: Optional[int] = None,
        end_at: Optional[int] = None,
        current_page: Optional[int] = None,
        page_size: Optional[int] = None,
        last_id: Optional[int] = None,
        isHf: bool = False,
    ):
        """
        获取完成的订单列表(last_id仅限于isHf为True的时候使用)
        """
        if isHf:
            path = f"/api/v1/hf/orders/{status}"
            params = {
                "symbol": symbol,
                "side": side,
                "type": type,
                "startAt": start_at,
                "endAt": end_at,
                "lastId": last_id,
                "limit": page_size,
            }
        else:
            path = "/api/v1/orders"
            params = {
                "status": status,
                "symbol": symbol,
                "side": side,
                "type": type,
                "startAt": start_at,
                "endAt": end_at,
                "currentPage": current_page,
                "pageSize": page_size,
            }

        return await self.raw_request("GET", path, params=params, auth=True)


class KucoinSpotRestClient(KucoinRestClient):
    def __init__(
        self,
        account_config: AccountConfig,
        rest_config: RestConfig,
    ):
        rest_config.url = "https://api.kucoin.com"
        super().__init__(account_config, rest_config)

    @catch_it
    async def get_spot_instrument_info(self):
        path = "/api/v2/symbols"

        return await self.raw_request("GET", path)

    @catch_it
    async def get_spot_level1(self, symbol: str):
        path = "/api/v1/market/orderbook/level1"
        params = {"symbol": symbol}

        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def get_spot_currency(self):
        path = "/api/v1/currencies"

        return await self.raw_request("GET", path)

    @catch_it
    async def get_spot_kline(
        self,
        symbol: str,
        interval: Literal[
            "1min",
            "3min",
            "5min",
            "15min",
            "30min",
            "1hour",
            "2hour",
            "4hour",
            "6hour",
            "8hour",
            "12hour",
            "1day",
            "1week",
            "1month",
        ],
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
    ):
        path = "/api/v1/market/candles"
        params = {"symbol": symbol, "type": interval, "startAt": start_time, "endAt": end_time}

        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def get_user_info(self):
        path = "/api/v2/user-info"
        return await self.raw_request("GET", path, auth=True)

    @catch_it
    async def get_hf_account_opened(self):
        path = "/api/v1/hf/accounts/opened"
        return await self.raw_request("GET", path, auth=True)

    @catch_it
    async def get_account(self, currency: Optional[str] = None, type: Optional[str] = None):
        """获取账户信息
        type: main, trade, margin, trade_hf
        """
        path = "/api/v1/accounts"
        params = {
            "currency": currency,
            "type": type,
        }

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def get_account_ledgers(
        self,
        currency: Optional[str] = None,
        direction: Optional[str] = None,
        bizType: Optional[str] = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
    ):
        """获取账户流水信息"""
        path = "/api/v1/accounts/ledgers"
        params = {
            "currency": currency,
            "direction": direction,
            "bizType": bizType,
            "startAt": start_time,
            "endAt": end_time,
        }

        # TODO: hf account ledgeer has different params

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def get_public_bullet(self):
        path = "/api/v1/bullet-public"

        return await self.raw_request("POST", path)

    @catch_it
    async def get_private_bullet(self):
        path = "/api/v1/bullet-private"

        return await self.raw_request("POST", path, auth=True)

    @catch_it
    async def get_spot_market(self, symbol: str):
        path = "/api/v1/market/orderbook/level1"
        params = {"symbol": symbol}

        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def inner_transfer(
        self,
        clientOid: str,
        currency: str,
        amount: str,
        from_account_type: Literal[
            "main",
            "trade",
            "trade_hf",
            "margin",
            "margin_v2",
            "isolated",
            "isolated_v2",
        ],
        to_account_type: Literal[
            "main",
            "trade",
            "trade_hf",
            "margin",
            "margin_v2",
            "isolated",
            "isolated_v2",
            "contract",
        ],
        from_tag: Optional[str] = None,
        to_tag: Optional[str] = None,
    ):
        path = "/api/v2/accounts/inner-transfer"
        params = {
            "clientOid": clientOid,
            "currency": currency,
            "amount": amount,
            "from": from_account_type,
            "to": to_account_type,
            "from_tag": from_tag,
            "to_tag": to_tag,
        }

        return await self.raw_request("POST", path, auth=True, payload=params)

    @catch_it
    async def universal_transfer(
        self,
        clientOid: str,
        currency: str,
        amount: str,
        type: Literal["INTERNAL", "PARENT_TO_SUB", "SUB_TO_PARENT"],
        fromAccountType: Literal["MAIN", "TRADE", "CONTRACT", "MARGIN", "TRADE_HF", "MARGIN_V2", "ISOLATED_V2"],
        toAccountType: Literal["MAIN", "TRADE", "CONTRACT", "MARGIN", "TRADE_HF", "MARGIN_V2", "ISOLATED_V2"],
        fromUserId: Optional[str] = None,
        toUserId: Optional[str] = None,
        fromAccountTag: Optional[str] = None,
        toAccountTag: Optional[str] = None,
    ):
        """(Requires higher privilege)

        Args:
            clientOid (str): clientOid (str): should use str(uuid()) as `clientOid`
            currency (str): _description_
            amount (str): _description_
            type (Literal[&quot;INTERNAL&quot;, &quot;PARENT_TO_SUB&quot;, &quot;SUB_TO_PARENT&quot;]): _description_
            fromAccountType (Literal[&quot;MAIN&quot;, &quot;TRADE&quot;, &quot;CONTRACT&quot;, &quot;MARGIN&quot;, &quot;TRADE_HF&quot;, &quot;MARGIN_V2&quot;, &quot;ISOLATED_V2&quot;]): _description_
            toAccountType (Literal[&quot;MAIN&quot;, &quot;TRADE&quot;, &quot;CONTRACT&quot;, &quot;MARGIN&quot;, &quot;TRADE_HF&quot;, &quot;MARGIN_V2&quot;, &quot;ISOLATED_V2&quot;]): _description_
            fromUserId (Optional[str], optional): _description_. Defaults to None.
            toUserId (Optional[str], optional): _description_. Defaults to None.
            fromAccountTag (Optional[str], optional): _description_. Defaults to None.
            toAccountTag (Optional[str], optional): _description_. Defaults to None.
        """
        path = "/api/v3/accounts/universal-transfer"
        params = {
            "clientOid": clientOid,
            "currency": currency,
            "amount": amount,
            "fromUserId": fromUserId,
            "fromAccountType": fromAccountType,
            "fromAccountTag": fromAccountTag,
            "type": type,
            "toUserId": toUserId,
            "toAccountType": toAccountType,
            "toAccountTag": toAccountTag,
        }

        return await self.raw_request("POST", path, auth=True, payload=params)

    @catch_it
    async def accounts_transferable(
        self,
        currency: str,
        type: Literal["MAIN", "TRADE", "TRADE_HF", "MARGIN", "ISOLATED"],
        tag: Optional[str] = None,
    ):
        path = "/api/v1/accounts/transferable"
        params = {
            "currency": currency,
            "type": type,
            "tag": tag,
        }

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def place_order(
        self,
        clientOid: str,
        side: Literal["buy", "sell"],
        symbol: str,
        type: Optional[Literal["limit", "market"]] = None,
        price: Optional[str] = None,
        size: Optional[str] = None,
        timeInForce: Optional[Literal["GTC", "GTT", "IOC", "FOK"]] = None,
        cancelAfter: Optional[str] = None,
        postOnly: Optional[str] = None,
        isHf: bool = False,
        **kwargs,
    ):
        """下单"""
        path = "/api/v1/orders" if not isHf else "/api/v1/hf/orders"
        params = {
            "clientOid": clientOid,
            "side": side,
            "symbol": symbol,
            "type": type,
            "price": price,
            "size": size,
            "timeInForce": timeInForce,
            "cancelAfter": cancelAfter,
            "postOnly": postOnly,
            **kwargs,
        }

        return await self.raw_request("POST", path, auth=True, payload=params)

    @catch_it
    async def cancel_order(
        self,
        symbol: Optional[str] = None,
        order_id: Optional[str] = None,
        clientOid: Optional[str] = None,
        isHf: bool = False,
    ):
        assert order_id or clientOid
        prefix = "/api/v1/hf" if isHf else "/api/v1"
        if order_id:
            path = f"{prefix}/orders/{order_id}"
        elif clientOid:
            path = f"{prefix}/orders/client-order/{clientOid}"
        else:
            raise ValueError("Either order_id or clientOid is required.")

        params = {"symbol": symbol}

        return await self.raw_request("DELETE", path, params=params, auth=True)

    @catch_it
    async def cancel_all_orders(
        self,
        symbol: Optional[str] = None,
        trade_type: Optional[Literal["TRADE", "MARGIN_TRADE", "MARGIN_ISOLATED_TRADE"]] = None,
        isHf: bool = False,
    ):
        prefix = "/api/v1/hf" if isHf else "/api/v1"
        path = f"{prefix}/orders"
        params = {"symbol": symbol, "tradeType": trade_type}

        return await self.raw_request("DELETE", path, params=params, auth=True)

    @catch_it
    async def get_commission_rate(self, symbol: str):
        path = "/api/v1/trade-fees"
        params = {"symbols": symbol}

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def get_all_tickers(self):
        path = "/api/v1/market/allTickers"

        return await self.raw_request("GET", path)

    @catch_it
    async def get_orderbook(self, symbol: str):
        path = "/api/v3/market/orderbook/level2"
        params = {"symbol": symbol}

        return await self.raw_request("GET", path, params=params)


class KucoinFutureRestClient(KucoinRestClient):
    def __init__(
        self,
        account_config: AccountConfig,
        rest_config: RestConfig,
    ):
        rest_config.url = "https://api-futures.kucoin.com"
        super().__init__(account_config, rest_config)

    @catch_it
    async def get_commission_rate(self, symbol: str):
        path = "/api/v1/trade-fees"
        params = {"symbol": symbol}
        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def get_history_funding_rate(
        self,
        symbol: str,
        start_time: int,
        end_time: int,
    ):
        path = "/api/v1/contract/funding-rates"
        params = {"symbol": symbol, "from": start_time, "to": end_time}

        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def get_swap_instrument_info(self):
        path = "/api/v1/contracts/active"

        return await self.raw_request("GET", path)

    @catch_it
    async def get_swap_kline(
        self,
        symbol: str,
        interval: Literal[1, 5, 15, 30, 60, 120, 240, 480, 720, 1440, 10080],
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
    ):
        path = "/api/v1/kline/query"
        params = {"symbol": symbol, "granularity": interval, "from": start_time, "to": end_time}

        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def change_auto_deposit_status(
        self,
        symbol: str,
        status: bool,
    ):
        path = "/api/v1/position/margin/auto-deposit-status"
        params = {"symbol": symbol, "status": status}

        return await self.raw_request("POST", path, auth=True, payload=params)

    @catch_it
    async def deposit_margin(
        self,
        symbol: str,
        margin: Union[str, Decimal],
    ):
        path = "/api/v1/position/margin/deposit-margin"
        params = {"symbol": symbol, "margin": str(margin), "bizNo": str(uuid4())}

        return await self.raw_request("POST", path, auth=True, payload=params)

    @catch_it
    async def change_risk_limit(
        self,
        symbol: str,
        level: int,
    ):
        path = "/api/v1/position/risk-limit-level/change"
        params = {"symbol": symbol, "level": level}

        return await self.raw_request("POST", path, auth=True, payload=params)

    @catch_it
    async def fetch_funding_history(
        self, symbol: str, start_at: Optional[int] = None, end_at: Optional[int] = None, reverse: Optional[bool] = None
    ):
        path = "/api/v1/funding-history"
        params = {"symbol": symbol, "startAt": start_at, "endAt": end_at, "reverse": reverse}

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def fetch_future_balance(self):
        path = "/api/v1/account-overview"
        params = {"currency": "USDT"}

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def fetch_positions(self):
        path = "/api/v1/positions"

        return await self.raw_request("GET", path, auth=True)

    @catch_it
    async def fetch_position(self, symbol: str):
        path = "/api/v1/position"
        params = {"symbol": symbol}

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def fetch_risk_limit(
        self,
        symbol: str,
    ):
        path = f"/api/v1/contracts/risk-limit/{symbol}"

        return await self.raw_request("GET", path, auth=True)

    @catch_it
    async def get_margin_mode(self, symbol: str):
        path = "/api/v2/position/getMarginMode"
        params = {"symbol": symbol}
        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def set_margin_mode(self, symbol: str, marginMode: Literal["ISOLATED", "CROSS"] = "CROSS"):
        path = "/api/v2/position/changeMarginMode"
        payload = {"symbol": symbol, "marginMode": marginMode}
        return await self.raw_request("POST", path, payload=payload, auth=True)

    @catch_it
    async def get_cross_margin_leverage(self, symbol: str):
        path = "/api/v2/getCrossUserLeverage"
        params = {"symbol": symbol}
        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def set_cross_margin_leverage(self, symbol: str, leverage: Union[str, int]):
        path = "/api/v2/changeCrossUserLeverage"
        payload = {"symbol": symbol, "leverage": str(leverage)}
        return await self.raw_request("POST", path, payload=payload, auth=True)

    @catch_it
    async def get_public_bullet(self):
        path = "/api/v1/bullet-public"

        return await self.raw_request("POST", path)

    @catch_it
    async def get_private_bullet(self):
        path = "/api/v1/bullet-private"

        return await self.raw_request("POST", path, auth=True)

    @catch_it
    async def get_future_market(self, symbol: str):
        path = "/api/v1/ticker"
        params = {"symbol": symbol}

        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def transfer_out(
        self, currency: Literal["XBT", "USDT"], amount: str, account_type: Literal["MAIN", "TRADE"]
    ):
        path = "/api/v3/transfer-out"
        params = {"amount": amount, "recAccountType": account_type, "currency": currency}

        return await self.raw_request("POST", path, auth=True, payload=params)

    @catch_it
    async def transfer_in(self, currency: Literal["XBT", "USDT"], amount: str, account_type: Literal["MAIN", "TRADE"]):
        path = "/api/v1/transfer-in"
        params = {"amount": amount, "payAccountType": account_type, "currency": currency}

        return await self.raw_request("POST", path, auth=True, payload=params)

    @catch_it
    async def get_premium_index(
        self,
        symbol: str,
        start_at: Optional[int] = None,
        end_at: Optional[int] = None,
        reverse: Optional[bool] = None,
        offset: Optional[int] = None,
        forward: Optional[bool] = None,
        max_count: Optional[int] = None,
    ):
        path = "/api/v1/premium/query"
        params = {
            "symbol": symbol,
            "startAt": start_at,
            "endAt": end_at,
            "reverse": reverse,
            "offset": offset,
            "forward": forward,
            "maxCount": max_count,
        }

        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def place_order(
        self,
        clientOid: str,
        side: Literal["buy", "sell"],
        symbol: str,
        leverage: str,
        marginMode: Literal["CROSS", "ISOLATED"] = "CROSS",
        type: Optional[Literal["limit", "market"]] = None,
        price: Optional[str] = None,
        size: Optional[str] = None,
        qty: Optional[str] = None,
        timeInForce: Optional[Literal["GTC", "IOC"]] = None,
        cancelAfter: Optional[str] = None,
        postOnly: Optional[str] = None,
        reduceOnly: Optional[bool] = None,
        **kwargs,
    ):
        """下单"""
        path = "/api/v1/orders"
        params = {
            "clientOid": clientOid,
            "side": side,
            "symbol": symbol,
            "type": type,
            "leverage": leverage,
            "marginMode": marginMode,
            "price": price,
            "size": size,
            "qty": qty,
            "timeInForce": timeInForce,
            "cancelAfter": cancelAfter,
            "postOnly": postOnly,
            "reduceOnly": reduceOnly,
            **kwargs,
        }

        return await self.raw_request("POST", path, auth=True, payload=params)

    @catch_it
    async def cancel_order(
        self, symbol: Optional[str], orderId: Optional[str] = None, clientOid: Optional[str] = None
    ):
        assert orderId or clientOid
        if orderId:
            path = f"/api/v1/orders/{orderId}"
        elif clientOid:
            path = f"/api/v1/orders/client-order/{clientOid}"
        else:
            raise ValueError("Please provide either clientOid or orderId")

        params = {"symbol": symbol}

        return await self.raw_request("DELETE", path, params=params, auth=True)

    @catch_it
    async def cancel_all_orders(
        self, symbol: str, trade_type: Optional[Literal["TRADE", "MARGIN_TRADE", "MARGIN_ISOLATED_TRADE"]] = None
    ):
        path = "/api/v1/orders"
        params = {"symbol": symbol, "tradeType": trade_type}

        return await self.raw_request("DELETE", path, params=params, auth=True)

    @catch_it
    async def get_current_funding_rate(self, symbol: str):
        path = f"/api/v1/funding-rate/{symbol}/current"

        return await self.raw_request("GET", path)

    @catch_it
    async def get_contract_detail(self, symbol: str):
        path = f"/api/v1/contracts/{symbol}"

        return await self.raw_request("GET", path)

    @catch_it
    async def get_all_tickers(self):
        path = "/api/v1/allTickers"

        return await self.raw_request("GET", path)

    @catch_it
    async def get_orderbook(self, symbol: str):
        path = "/api/v1/level2/snapshot"
        params = {"symbol": symbol}

        return await self.raw_request("GET", path, params=params)
