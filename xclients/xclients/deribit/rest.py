import hashlib
import string
import random
import hmac
import json
import time
from decimal import Decimal
from typing import Any, Literal, Optional, Union
from urllib.parse import urlencode

from ..data_type import AccountConfig, RestConfig
from ..base_client import BaseRestClient, catch_it
from ..utils import clean_none_value

K = Literal["future", "option", "spot", "future_combo", "option_combo"]
OT = Literal[
    "all",
    "limit",
    "trigger_all",
    "stop_all",
    "stop_limit",
    "stop_market",
    "take_all",
    "take_limit",
    "take_market",
    "trailing_all",
    "trailing_stop",
]


class DeribitRestClient(BaseRestClient):
    def __init__(
        self,
        account_config: AccountConfig,
        rest_config: RestConfig,
    ):
        rest_config.url = "https://www.deribit.com"
        super().__init__(account_config, rest_config)
        self.prefix = "/api/v2"

    def _gen_header(self, http_method, url_path, params=None, payload_string=None):
        if not self.secret_key:
            raise ValueError("secret_key is required")

        timestamp = str(int(time.time() * 1000))
        nonce = str(int(time.time() * 1000))
        requestBody = ""
        request = url_path
        if params:
            request += "?" + urlencode(params)
        requestData = http_method + "\n" + request + "\n" + requestBody + "\n"  # eslint-disable-line quotes
        auth = timestamp + "\n" + nonce + "\n" + requestData  # eslint-disable-line quotes
        hash_value = hmac.new(self.secret_key.encode("utf-8"), auth.encode("utf-8"), hashlib.sha256)
        signature = hash_value.hexdigest()
        headers = {
            "Authorization": f"deri-hmac-sha256 id={self.api_key},ts={timestamp},sig={signature},nonce={nonce}",
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
        endpoint = f"{self.prefix}{path}"
        url = f"{self.base_url}{endpoint}"
        headers = {"Content-Type": "application/json"}
        params = clean_none_value(params or {})
        payload = clean_none_value(payload or {})
        payload_string = json.dumps(payload) if payload else ""
        if auth:
            headers = self._gen_header(method, endpoint, params, payload_string)
        return url, headers, params, payload_string

    """
    ###############
    # market data #
    ###############
    """

    @catch_it
    async def get_book_summary_by_instrument(self, instrument_name: str):
        path = "/public/get_book_summary_by_instrument"
        params = {"instrument_name": instrument_name}
        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def get_book_summary_by_currency(self, currency: str, kind: Optional[K] = None):
        path = "/public/get_book_summary_by_currency"
        params = {"currency": currency, "kind": kind}
        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def get_instrument_info(self, currency: str = "any", kind: Optional[K] = None):
        path = "/public/get_instruments"
        params = {"currency": currency, "kind": kind}
        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def get_funding_rate_history(self, instrument_name: str, start_timestamp: int, end_timestamp: int):
        path = "/public/get_funding_rate_history"
        params = {
            "instrument_name": instrument_name,
            "start_timestamp": start_timestamp,
            "end_timestamp": end_timestamp,
        }
        return await self.raw_request("GET", path, params=params)

    """
    ################
    # account data #  TODO
    ################
    """

    @catch_it
    async def auth(
        self,
        grant_type: Literal["client_credentials", "client_signature", "refresh_token"],
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        refresh_token: Optional[str] = None,
        timestamp: Optional[int] = None,
        signature: Optional[str] = None,
        nonce: Optional[str] = None,
        data: Optional[str] = None,
        state: Optional[str] = None,
        scope: Optional[str] = None,
    ):
        path = "/public/auth"
        params = {
            "grant_type": grant_type,
            "client_id": client_id,
            "client_secret": client_secret,
            "refresh_token": refresh_token,
            "timestamp": timestamp,
            "signature": signature,
            "nonce": nonce,
            "data": data,
            "state": state,
            "scope": scope,
        }

        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def get_account_summaries(
        self,
        subaccount_id: Optional[int] = None,
        extended: Optional[bool] = None,
    ):
        path = "/private/get_account_summaries"
        params = {"subaccount_id": subaccount_id, "extended": extended}

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def get_account_summary(
        self,
        currency: Literal["BTC", "ETH", "USDC", "USDT", "EURR"],
        subaccount_id: Optional[int] = None,
        extended: Optional[bool] = None,
    ):
        path = "/private/get_account_summary"
        params = {"currency": currency, "subaccount_id": subaccount_id, "extended": extended}

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def get_position(
        self,
        instrument_name: str,
    ):
        path = "/private/get_position"
        params = {"instrument_name": instrument_name}

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def get_positions(
        self,
        currency: Optional[Literal["BTC", "ETH", "USDC", "USDT", "EURR"]] = None,
        kind: Optional[K] = None,
        subaccount_id: Optional[int] = None,
    ):
        path = "/private/get_positions"
        params = {"currency": currency, "kind": kind, "subaccount_id": subaccount_id}

        return await self.raw_request("GET", path, params=params, auth=True)

    """
    ################
    # trading      #  
    ################
    """

    @catch_it
    async def buy(
        self,
        instrument_name: str,
        amount: Optional[Decimal] = None,
        contracts: Optional[Decimal] = None,
        type: Optional[
            Literal[
                "limit",
                "stop_limit",
                "take_limit",
                "market",
                "stop_market",
                "take_market",
                "market_limit",
                "trailing_stop",
            ]
        ] = None,
        label: Optional[str] = None,
        price: Optional[Union[str, Decimal]] = None,
        time_in_force: Optional[
            Literal["good_til_cancelled", "good_til_day", "fill_or_kill", "immediate_or_cancel"]
        ] = None,
        post_only: Optional[bool] = None,
        reduce_only: Optional[bool] = None,
        **kwargs,
    ):
        path = "/private/buy"
        params = {
            "instrument_name": instrument_name,
            "amount": str(amount) if amount else None,
            "contracts": str(contracts) if contracts else None,
            "type": type,
            "label": label,
            "price": str(price),
            "time_in_force": time_in_force,
            "post_only": post_only,
            "reduce_only": reduce_only,
            **kwargs,
        }
        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def sell(
        self,
        instrument_name: str,
        amount: Optional[Union[str, Decimal]] = None,
        contracts: Optional[Union[str, Decimal]] = None,
        type: Optional[
            Literal[
                "limit",
                "stop_limit",
                "take_limit",
                "market",
                "stop_market",
                "take_market",
                "market_limit",
                "trailing_stop",
            ]
        ] = None,
        label: Optional[str] = None,
        price: Optional[Union[str, Decimal]] = None,
        time_in_force: Optional[
            Literal["good_til_cancelled", "good_til_day", "fill_or_kill", "immediate_or_cancel"]
        ] = None,
        post_only: Optional[bool] = None,
        reduce_only: Optional[bool] = None,
        **kwargs,
    ):
        path = "/private/sell"
        params = {
            "instrument_name": instrument_name,
            "amount": str(amount) if amount else None,
            "contracts": str(contracts) if contracts else None,
            "type": type,
            "label": label,
            "price": str(price),
            "time_in_force": time_in_force,
            "post_only": post_only,
            "reduce_only": reduce_only,
            **kwargs,
        }

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def cancel(self, order_id: str):
        path = "/private/cancel"
        params = {"order_id": order_id}

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def cancel_by_label(
        self, label: str, currency: Optional[Literal["BTC", "ETH", "USDC", "USDT", "EURR"]] = None
    ):
        path = "/private/cancel"
        params = {"label": label, "currency": currency}

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def cancel_all(self, detailed: Optional[bool] = None, freeze_quotes: Optional[bool] = None):
        path = "/private/cancel"
        params = {"detailed": detailed, "freeze_quotes": freeze_quotes}

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def get_open_orders(self, kind: Optional[K] = None, order_type: Optional[OT] = None):
        path = "/private/get_open_orders"
        params = {"kind": kind, "type": order_type}

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def get_open_orders_by_instrument(self, instrument_name: str, order_type: Optional[OT] = None):
        path = "/private/get_open_orders_by_instrument"
        params = {"instrument_name": instrument_name, "type": order_type}

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def get_order_history_by_currency(
        self,
        currency: Literal["BTC", "ETH", "USDC", "USDT", "EURR"],
        kind: Optional[K] = None,
        count: Optional[int] = None,
        offset: Optional[int] = None,
        include_old: Optional[bool] = None,  # include older than 2 days
        include_unfilled: Optional[bool] = None,
    ):
        path = "/private/get_order_history_by_currency"
        params = {
            "currency": currency,
            "kind": kind,
            "count": count,
            "offset": offset,
            "include_old": include_old,
            "include_unfilled": include_unfilled,
        }

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def get_order_history_by_instrument(
        self,
        instrument_name: str,
        count: Optional[int] = None,
        offset: Optional[int] = None,
        include_old: Optional[bool] = None,  # include older than 2 days
        include_unfilled: Optional[bool] = None,
    ):
        path = "/private/get_order_history_by_instrument"
        params = {
            "instrument_name": instrument_name,
            "count": count,
            "offset": offset,
            "include_old": include_old,
            "include_unfilled": include_unfilled,
        }

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def get_user_trades_by_currency(
        self,
        currency: Literal["BTC", "ETH", "USDC", "USDT", "EURR"],
        kind: Optional[K] = None,
        start_id: Optional[str] = None,
        end_id: Optional[str] = None,
        count: Optional[int] = None,
        start_timestamp: Optional[int] = None,
        end_timestamp: Optional[int] = None,
        sorting: Optional[Literal["asc", "desc", "default"]] = None,
        subaccount_id: Optional[int] = None,
    ):
        path = "/private/get_user_trades_by_currency"
        params = {
            "currency": currency,
            "kind": kind,
            "start_id": start_id,
            "end_id": end_id,
            "count": count,
            "start_timestamp": start_timestamp,
            "end_timestamp": end_timestamp,
            "sorting": sorting,
            "subaccount_id": subaccount_id,
        }

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def get_user_trades_by_currency_and_time(
        self,
        currency: Literal["BTC", "ETH", "USDC", "USDT", "EURR"],
        start_timestamp: int,
        end_timestamp: int,
        kind: Optional[K] = None,
        count: Optional[int] = None,
        sorting: Optional[Literal["asc", "desc", "default"]] = None,
    ):
        path = "/private/get_user_trades_by_currency_and_time"
        params = {
            "currency": currency,
            "kind": kind,
            "start_timestamp": start_timestamp,
            "end_timestamp": end_timestamp,
            "count": count,
            "sorting": sorting,
        }

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def get_user_trades_by_instrument(
        self,
        instrument_name: str,
        start_seq: Optional[str] = None,
        end_seq: Optional[str] = None,
        count: Optional[int] = None,
        start_timestamp: Optional[int] = None,
        end_timestamp: Optional[int] = None,
        sorting: Optional[Literal["asc", "desc", "default"]] = None,
    ):
        path = "/private/get_user_trades_by_instrument"
        params = {
            "instrument_name": instrument_name,
            "start_seq": start_seq,
            "end_seq": end_seq,
            "count": count,
            "start_timestamp": start_timestamp,
            "end_timestamp": end_timestamp,
            "sorting": sorting,
        }

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def get_user_trades_by_instrument_and_time(
        self,
        instrument_name: str,
        start_timestamp: int,
        end_timestamp: int,
        count: Optional[int] = None,
        sorting: Optional[Literal["asc", "desc", "default"]] = None,
    ):
        path = "/private/get_user_trades_by_instrument_and_time"
        params = {
            "instrument_name": instrument_name,
            "start_timestamp": start_timestamp,
            "end_timestamp": end_timestamp,
            "count": count,
            "sorting": sorting,
        }

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def get_user_trades_by_order(
        self, order_id: str, sorting: Optional[Literal["asc", "desc", "default"]] = None
    ):
        path = "/private/get_user_trades_by_order"
        params = {
            "order_id": order_id,
            "sorting": sorting,
        }

        return await self.raw_request("GET", path, params=params, auth=True)
