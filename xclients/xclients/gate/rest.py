import json
import time
from decimal import Decimal
import hmac
import hashlib
from urllib.parse import urlencode
from typing import Optional, Any, Literal

from ..data_type import AccountConfig, RestConfig
from ..base_client import BaseRestClient, catch_it
from ..utils import clean_none_value


class GateRestClient(BaseRestClient):
    def __init__(
        self,
        account_config: AccountConfig,
        rest_config: RestConfig,
    ):
        rest_config.url = "https://api.gateio.ws"
        super().__init__(account_config, rest_config)
        self.headers = {"X-MBX-APIKEY": self.api_key}
        self.prefix = "/api/v4"

    def _sign_request(self, http_method, endpoint, params=None, payload_string=None):
        if not self.secret_key:
            raise ValueError("secret_key is required")

        common_headers = {"Accept": "application/json", "Content-Type": "application/json"}
        query_string = urlencode(params or {})
        payload_string = payload_string or ""
        t = time.time()
        m = hashlib.sha512()
        m.update((payload_string).encode("utf-8"))
        hashed_payload = m.hexdigest()
        s = "%s\n%s\n%s\n%s\n%s" % (http_method, endpoint, query_string, hashed_payload, t)
        sign = hmac.new(self.secret_key.encode("utf-8"), s.encode("utf-8"), hashlib.sha512).hexdigest()
        sign_header = {"KEY": self.api_key, "Timestamp": str(t), "SIGN": sign}
        sign_header.update(common_headers)
        return sign_header

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
        headers = headers = {"Accept": "application/json", "Content-Type": "application/json"}
        params = clean_none_value(params or {})
        payload = clean_none_value(payload or {})
        payload_string = json.dumps(payload) if payload else ""
        if auth:
            headers = self._sign_request(method, endpoint, params, payload_string)

        return url, headers, params, payload_string


class GateSpotRestClient(GateRestClient):
    def __init__(
        self,
        account_config: AccountConfig,
        rest_config: RestConfig,
    ):
        super().__init__(account_config, rest_config)
        self.prefix = "/api/v4"

    @catch_it
    async def get_exchange_info(self, trade_type: Literal["spot", "margin"] = "spot"):
        """
        trade_type: spot/margin
        """
        path = f"/{trade_type}/currency_pairs"

        return await self.raw_request("GET", path)

    @catch_it
    async def get_market(self, currency_pair: Optional[str] = None):
        path = "/spot/tickers"
        params = {"currency_pair": currency_pair}

        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def get_orderbook(
        self, currency_pair: str, limit: int = 100, interval: Optional[str] = None, with_id: bool = True
    ):
        path = "/spot/order_book"
        params = {
            "currency_pair": currency_pair,
            "limit": limit,
            "interval": interval,
            "with_id": with_id,
        }

        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def get_cross_margin_currency(self):
        path = "/margin/cross/currencies"

        return await self.raw_request("GET", path)

    @catch_it
    async def get_total_balance(self):
        path = "/wallet/total_balance"

        return await self.raw_request("GET", path, auth=True)

    @catch_it
    async def get_history_kline(
        self,
        currency_pair: str,
        interval: Literal["10s", "1m", "5m", "15m", "30m", "1h", "4h", "8h", "1d", "7d", "30d"],
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        limit: Optional[int] = None,
    ):
        path = "/spot/candlesticks"
        params = {
            "currency_pair": currency_pair,
            "interval": interval,
            "from": start_time,
            "to": end_time,
            "limit": limit,
        }

        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def transfer(
        self,
        currency: str,
        amount: str,
        from_market_type: Literal["spot", "margin", "futures", "delivery", "cross_margin", "options"],
        to_market_type: Literal["spot", "margin", "futures", "delivery", "cross_margin", "options"],
        currency_pair: Optional[str] = None,
        settle: Optional[str] = None,
    ):
        path = f"/wallet/transfers"
        payload = {
            "currency": currency,
            "from": from_market_type,
            "to": to_market_type,
            "amount": amount,
            "currency_pair": currency_pair,
            "settle": settle,
        }

        return await self.raw_request("POST", path, auth=True, payload=payload)

    @catch_it
    async def place_order(
        self,
        side: Literal["buy", "sell"],
        symbol: str,
        type: Optional[Literal["limit", "market"]] = None,
        price: Optional[str] = None,
        size: Optional[str] = None,
        time_in_force: Optional[Literal["gtc", "ioc", "poc", "fok"]] = None,
        account: Optional[Literal["spot", "margin", "cross_margin", "unified"]] = None,
        cid: Optional[str] = None,
        **kwargs,
    ):
        """side: buy/sell, type: limit/market"""
        path = f"/spot/orders"
        payload = {
            "side": side,
            "currency_pair": symbol,
            "type": type,
            "price": price,
            "amount": size,
            "time_in_force": time_in_force,
            "account": account,
            "text": cid,
            **kwargs,
        }

        return await self.raw_request("POST", path, auth=True, payload=payload)

    @catch_it
    async def get_open_orders(
        self, page: Optional[str] = None, limit: Optional[str] = None, account: Optional[str] = None
    ):
        path = "/spot/open_orders"
        params = {"page": page, "limit": limit, "account": account}

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def get_orders(
        self,
        currency_pair: str,
        status: Literal["open", "finished"],
        page: Optional[int] = None,
        limit: Optional[int] = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        account: Optional[str] = None,
        side: Optional[Literal["buy", "sell"]] = None,
    ):
        """status: finished/open"""
        path = "/spot/orders"
        params = {
            "currency_pair": currency_pair,
            "status": status,
            "page": page,
            "from": start_time,
            "to": end_time,
            "limit": limit,
            "account": account,
            "side": side,
        }

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def get_account(self, currency: Optional[str] = None, trade_type: Literal["spot", "margin"] = "spot"):
        assert trade_type in ("spot", "margin"), f"Unknown trade_type - {trade_type}"
        path = f"/{trade_type}/accounts"
        params = {"currency": currency}

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def get_trades(
        self,
        symbol: Optional[str] = None,
        order_id: Optional[str] = None,
        limit: Optional[int] = None,
        page: Optional[int] = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        account: Optional[str] = None,
    ):
        path = "/spot/my_trades"
        params = {
            "currency_pair": symbol,
            "order_id": order_id,
            "page": page,
            "limit": limit,
            "from": start_time,
            "to": end_time,
            "account": account,
        }

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def cancel_order(
        self,
        symbol: str,
        order_id: Optional[str] = None,
        custom_id: Optional[str] = None,
        account: Optional[str] = None,
    ):
        assert order_id or custom_id, "Either order_id or custom_id is required."
        id = order_id if order_id else custom_id
        path = f"/spot/orders/{id}"
        params = {"currency_pair": symbol, "account": account}

        return await self.raw_request("DELETE", path, params=params, auth=True)

    @catch_it
    async def get_commission_rate(self, symbol: str, settle: Optional[str] = "usdt"):
        path = "/wallet/fee"
        params = {"currency_pair": symbol, "settle": settle}

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def get_account_book(
        self,
        currency: Optional[str] = None,
        limit: Optional[int] = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        type: Optional[str] = None,
        page: Optional[int] = None,
    ):
        path = f"/spot/account_book"
        params = {"limit": limit, "from": start_time, "to": end_time, "type": type, "page": page, "currency": currency}

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def get_wallet_small_balance(self):
        path = f"/wallet/small_balance"

        return await self.raw_request("GET", path, auth=True)

    @catch_it
    async def post_wallet_small_balance(self, currency: Optional[list[str]] = None, is_all: Optional[bool] = None):
        path = f"/wallet/small_balance"
        payload = {"currency": currency, "is_all": is_all}

        return await self.raw_request("POST", path, auth=True, payload=payload)

    @catch_it
    async def create_virtual_subaccount(
        self,
        login_name: str,
        remark: Optional[str] = None,
        password: Optional[str] = None,
        email: Optional[str] = None,
    ):
        """
        subAccountList: 需要创建的虚拟子账户列表(虚拟昵称 长度必须为8位的纯英文字母组合 全局唯一)
        """
        path = "/sub_accounts"
        params = {
            "login_name": login_name,
            "remark": remark,
            "password": password,
            "email": email,
        }
        return await self.raw_request("POST", path, payload=params, auth=True)

    @catch_it
    async def create_virtual_subaccount_api_key(
        self,
        user_id: str,
        name: str,
        perms_name: list[str],
        read_only: bool = False,
        ip_whitelist: Optional[list[str]] = None,
        mode: int = 1,
    ):
        """
        perms_name: 权限列表, 包括(wallet: 钱包 spot: 现货/杠杆 futures: 永续合约, delivery: 交割合约, earn: 理财, custody: 托管
        options: 期权, account: 账户信息, loan: 借贷, margin: 杠杆, unified: 统一账户, copy: 跟单)
        mode: 1 - 经典帐户 2 - 统一账户
        name: API Key名称
        """
        path = f"/sub_accounts/{user_id}/keys"
        params = {"mode": mode, "name": name, "ip_whitelist": ip_whitelist, "perms": []}
        for perm in perms_name:
            params["perms"].append(
                {
                    "name": perm,
                    "read_only": read_only,
                }
            )
        return await self.raw_request("POST", path, payload=params, auth=True)

    @catch_it
    async def remove_virtual_subaccount_api_key(self, user_id: str, key: str):
        """
        user_id: 虚拟子账户ID
        key: 虚拟子账户API Key ID
        """
        path = f"/sub_accounts/{user_id}/keys/{key}"
        return await self.raw_request("DELETE", path, auth=True)


class GateFutureRestClient(GateRestClient):
    def __init__(
        self,
        account_config: AccountConfig,
        rest_config: RestConfig,
    ):
        super().__init__(account_config, rest_config)
        self.prefix = "/api/v4"

    @catch_it
    async def get_exchange_info(self, settle: Literal["usdt", "btc", "usd"] = "usdt"):
        assert settle in ("usdt", "btc", "usd")
        path = f"/futures/{settle}/contracts"

        return await self.raw_request("GET", path)

    @catch_it
    async def get_trade_info(self, settle: Literal["usdt", "btc"] = "usdt"):
        assert settle in ("usdt", "btc")
        path = f"/futures/{settle}/tickers"

        return await self.raw_request("GET", path)

    @catch_it
    async def get_tickers(self, settle: Literal["usdt", "btc"] = "usdt", contract: Optional[str] = None):
        assert settle in ("usdt", "btc")
        path = f"/futures/{settle}/tickers"
        params = {"contract": contract}

        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def get_orderbook(
        self, contract: str, settle: Literal["usdt", "btc"] = "usdt", limit: int = 100, with_id: bool = True
    ):
        assert settle in ("usdt", "btc")
        path = f"/futures/{settle}/order_book"
        params = {"contract": contract, "limit": limit, "with_id": with_id}

        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def get_funding_rate(
        self,
        contract: str,
        settle: Literal["usdt", "btc", "usd"] = "usdt",
        limit: int = 1000,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
    ):
        assert settle in ("usdt", "btc", "usd")
        path = f"/futures/{settle}/funding_rate"
        params = {
            "contract": contract,
            "limit": limit,
            "from": start_time,
            "to": end_time,
        }

        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def get_future_orders(
        self,
        settle: Literal["usdt", "btc", "usd"] = "usdt",
        contract: Optional[str] = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        status: Optional[Literal["open", "finished"]] = None,
        last_id: Optional[str] = None,
    ):
        assert settle in ("usdt", "btc", "usd")
        path = f"/futures/{settle}/orders_timerange"
        params = {
            "contract": contract,
            "from": start_time,
            "to": end_time,
            "limit": limit,
            "offset": offset,
            "status": status,
            "last_id": last_id,
        }

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def get_history_kline(
        self,
        contract: str,
        interval: str,
        settle: Literal["usdt", "btc", "usd"] = "usdt",
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        limit: Optional[int] = None,
    ):
        assert settle in ("usdt", "btc", "usd")
        path = f"/futures/{settle}/candlesticks"
        params = {
            "contract": contract,
            "interval": interval,
            "settle": settle,
            "from": start_time,
            "to": end_time,
            "limit": limit,
        }

        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def get_account_book(
        self,
        settle: Literal["usdt", "btc", "usd"] = "usdt",
        limit: Optional[int] = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        type: Optional[str] = None,
        offset: Optional[int] = None,
    ):
        assert settle in ("usdt", "btc", "usd")
        path = f"/futures/{settle}/account_book"
        params = {"limit": limit, "from": start_time, "to": end_time, "type": type, "offset": offset}

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def get_account(self, settle: Literal["usdt", "btc", "usd"] = "usdt"):
        assert settle in ("usdt", "btc", "usd")
        path = f"/futures/{settle}/accounts"

        return await self.raw_request("GET", path, auth=True)

    @catch_it
    async def get_positions(self, settle: Literal["usdt", "btc", "usd"] = "usdt"):
        assert settle in ("usdt", "btc", "usd")
        path = f"/futures/{settle}/positions"

        return await self.raw_request("GET", path, auth=True)

    @catch_it
    async def get_position(self, symbol: str, settle: Literal["usdt", "btc", "usd"] = "usdt"):
        assert settle in ("usdt", "btc", "usd")
        path = f"/futures/{settle}/positions/{symbol}"

        return await self.raw_request("GET", path, auth=True)

    @catch_it
    async def get_order(
        self,
        order_id: Optional[str] = None,
        custom_id: Optional[str] = None,
        settle: Literal["usdt", "btc", "usd"] = "usdt",
    ):
        assert settle in ("usdt", "btc", "usd")
        assert order_id or custom_id
        id = order_id if order_id else custom_id
        path = f"/futures/{settle}/orders/{id}"

        return await self.raw_request("GET", path, auth=True)

    @catch_it
    async def get_orders(
        self,
        contract: str,
        status: Literal["open", "finished"],
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        last_id: Optional[int] = None,
        settle: Literal["usdt", "btc", "usd"] = "usdt",
    ):
        """status: finished or open"""
        assert settle in ("usdt", "btc", "usd")
        path = f"/futures/{settle}/orders"
        params = {
            "contract": contract,
            "status": status,
            "offset": offset,
            "last_id": last_id,
            "limit": limit,
        }

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def get_trades(
        self,
        symbol: str,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        settle: Literal["usdt", "btc", "usd"] = "usdt",
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        role: Optional[Literal["taker", "maker"]] = None,
    ):
        """status: finished or open"""
        assert settle in ("usdt", "btc", "usd")
        path = f"/futures/{settle}/my_trades_timerange"
        params = {
            "contract": symbol,
            "offset": offset,
            "limit": limit,
            "from": start_time,
            "to": end_time,
            "role": role,
        }

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def place_order(
        self,
        side: Literal["buy", "sell"],
        symbol: str,
        type: Optional[str] = None,
        price: Optional[str] = None,
        size: Optional[str] = None,
        time_in_force: Optional[Literal["gtc", "ioc", "poc", "fok"]] = None,
        close: Optional[bool] = None,
        reduce_only: Optional[bool] = None,
        settle: Literal["usdt", "btc", "usd"] = "usdt",
        cid: Optional[str] = None,
        **kwargs,
    ):
        assert settle in ("usdt", "btc", "usd")
        path = f"/futures/{settle}/orders"
        if size is not None:
            decimal_size = Decimal(size)
            if side.upper() == "SELL" and decimal_size > 0:
                size = "-" + size
        if type and type.lower() == "market":
            price = "0"
            time_in_force = "ioc"

        if close:
            size = "0"

        params = {
            "contract": symbol,
            "tif": time_in_force,
            "price": price,
            "size": size,
            "close": close,
            "reduce_only": reduce_only,
            "text": cid,
            **kwargs,
        }

        return await self.raw_request("POST", path, auth=True, payload=params)

    @catch_it
    async def cancel_order(
        self,
        order_id: Optional[str] = None,
        custom_id: Optional[str] = None,
        settle: Literal["usdt", "btc", "usd"] = "usdt",
    ):
        assert settle in ("usdt", "btc", "usd")
        assert order_id or custom_id
        id = order_id if order_id else custom_id
        path = f"/futures/{settle}/orders/{id}"

        return await self.raw_request("DELETE", path, auth=True)

    @catch_it
    async def get_premium_index(
        self,
        symbol: str,
        interval: Literal["1m", "5m", "15m", "30m", "1h", "4h", "6h", "8h", "1d", "7d", "30d"],
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        limit: Optional[int] = None,
        settle: Literal["usdt", "btc", "usd"] = "usdt",
    ):
        """max limit: 1000, limit cannot used with start_time and end_time"""
        assert settle in ("usdt", "btc", "usd")
        path = f"/futures/{settle}/premium_index"
        params = {"contract": symbol, "from": start_time, "to": end_time, "interval": interval, "limit": limit}

        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def set_leverage(
        self,
        contract: str,
        leverage: str,
        cross_leverage_limit: Optional[str] = None,
        settle: Literal["usdt", "btc", "usd"] = "usdt",
    ):
        assert settle in ("usdt", "btc", "usd")
        path = f"/futures/{settle}/positions/{contract}/leverage"
        params = {"leverage": leverage, "cross_leverage_limit": cross_leverage_limit}

        return await self.raw_request("POST", path, params=params, auth=True)

    @catch_it
    async def get_commission_rate(self, symbol: str, settle: Literal["usdt", "btc", "usd"] = "usdt"):
        assert settle in ("usdt", "btc", "usd")
        path = f"/futures/{settle}/fee"
        params = {"contract": symbol}

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def get_long_short_ratio(
        self,
        symbol: str,
        period: Literal["5m", "15m", "30m", "1h", "4h", "1d"],
        start_time: Optional[int] = None,
        limit: Optional[int] = None,
        settle: Literal["usdt", "btc", "usd"] = "usdt",
    ):
        assert settle in ("usdt", "btc", "usd")
        path = f"/futures/{settle}/contract_stats"
        params = {"contract": symbol, "interval": period, "from": start_time, "limit": limit}

        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def get_risk_limit_tiers(self, contract: str | None = None, settle: Literal["usdt", "btc"] = "usdt"):
        assert settle in ("usdt", "btc")
        path = f"/futures/{settle}/risk_limit_tiers"
        params = {"contract": contract}

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def update_risk_limit(self, contract: str, risk_limit: str, settle: Literal["usdt", "btc"] = "usdt"):
        assert settle in ("usdt", "btc")
        path = f"/futures/{settle}/positions/{contract}/risk_limit"
        params = {"risk_limit": risk_limit}

        return await self.raw_request("POST", path, params=params, auth=True)


class GateDeliveryRestClient(GateRestClient):
    def __init__(
        self,
        account_config: AccountConfig,
        rest_config: RestConfig,
    ):
        super().__init__(account_config, rest_config)
        self.prefix = "/api/v4"

    @catch_it
    async def get_exchange_info(self, settle: Literal["usdt", "btc"] = "usdt"):
        assert settle in ("usdt", "btc")
        path = f"/delivery/{settle}/contracts"

        return await self.raw_request("GET", path)

    @catch_it
    async def get_trade_info(self, settle: Literal["usdt", "btc"] = "usdt"):
        assert settle in ("usdt", "btc")
        path = f"/delivery/{settle}/tickers"

        return await self.raw_request("GET", path)

    @catch_it
    async def get_tickers(self, settle: Literal["usdt", "btc"] = "usdt", contract: Optional[str] = None):
        assert settle in ("usdt", "btc")
        path = f"/delivery/{settle}/tickers"
        params = {"contract": contract}

        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def get_delivery_orders(
        self,
        settle: Literal["usdt", "btc"] = "usdt",
        contract: Optional[str] = None,
        status: Optional[Literal["open", "finished"]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        last_id: Optional[str] = None,
    ):
        assert settle in ("usdt", "btc")
        path = f"/delivery/{settle}/orders"
        params = {
            "contract": contract,
            "limit": limit,
            "offset": offset,
            "status": status,
            "last_id": last_id,
        }

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def get_history_kline(
        self,
        contract: str,
        interval: str,
        settle: Literal["usdt", "btc"] = "usdt",
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        limit: Optional[int] = None,
    ):
        assert settle in ("usdt", "btc")
        path = f"/delivery/{settle}/candlesticks"
        params = {
            "contract": contract,
            "interval": interval,
            "settle": settle,
            "from": start_time,
            "to": end_time,
            "limit": limit,
        }

        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def get_account_book(
        self,
        settle: Literal["usdt", "btc"] = "usdt",
        limit: Optional[int] = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        type: Optional[str] = None,
        offset: Optional[int] = None,
    ):
        assert settle in ("usdt", "btc")
        path = f"/delivery/{settle}/account_book"
        params = {"limit": limit, "from": start_time, "to": end_time, "type": type}

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def get_account(self, settle: Literal["usdt", "btc"] = "usdt"):
        assert settle in ("usdt", "btc")
        path = f"/delivery/{settle}/accounts"

        return await self.raw_request("GET", path, auth=True)

    @catch_it
    async def get_positions(self, settle: Literal["usdt", "btc"] = "usdt"):
        assert settle in ("usdt", "btc")
        path = f"/delivery/{settle}/positions"

        return await self.raw_request("GET", path, auth=True)

    @catch_it
    async def get_position(self, symbol: str, settle: Literal["usdt", "btc"] = "usdt"):
        assert settle in ("usdt", "btc")
        path = f"/delivery/{settle}/positions/{symbol}"

        return await self.raw_request("GET", path, auth=True)

    @catch_it
    async def get_order(
        self,
        order_id: Optional[str] = None,
        custom_id: Optional[str] = None,
        settle: Literal["usdt", "btc"] = "usdt",
    ):
        assert settle in ("usdt", "btc")
        assert order_id or custom_id
        id = order_id if order_id else custom_id
        path = f"/delivery/{settle}/orders/{id}"

        return await self.raw_request("GET", path, auth=True)

    @catch_it
    async def get_orders(
        self,
        contract: str,
        status: Literal["open", "finished"],
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        last_id: Optional[int] = None,
        settle: Literal["usdt", "btc"] = "usdt",
    ):
        """status: finished or open"""
        assert settle in ("usdt", "btc")
        path = f"/delivery/{settle}/orders"
        params = {
            "contract": contract,
            "status": status,
            "offset": offset,
            "last_id": last_id,
            "limit": limit,
        }

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def get_trades(
        self,
        symbol: str,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        last_id: Optional[str] = None,
        order_id: Optional[str] = None,
        settle: Literal["usdt", "btc"] = "usdt",
    ):
        """status: finished or open"""
        assert settle in ("usdt", "btc")
        path = f"/delivery/{settle}/my_trades"
        params = {
            "contract": symbol,
            "offset": offset,
            "limit": limit,
            "last_id": last_id,
            "order_id": order_id,
        }

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def place_order(
        self,
        side: Literal["buy", "sell"],
        symbol: str,
        type: Optional[str] = None,
        price: Optional[str] = None,
        size: Optional[str] = None,
        time_in_force: Optional[Literal["gtc", "ioc", "poc", "fok"]] = None,
        close: Optional[bool] = None,
        reduce_only: Optional[bool] = None,
        settle: Literal["usdt", "btc"] = "usdt",
        cid: Optional[str] = None,
        **kwargs,
    ):
        assert settle in ("usdt", "btc")
        path = f"/delivery/{settle}/orders"
        if size is not None:
            decimal_size = Decimal(size)
            if side.upper() == "SELL" and decimal_size > 0:
                size = "-" + size
        if type and type.lower() == "market":
            price = "0"
            time_in_force = "ioc"

        if close:
            size = "0"

        params = {
            "contract": symbol,
            "tif": time_in_force,
            "price": price,
            "size": size,
            "close": close,
            "reduce_only": reduce_only,
            "text": cid,
            **kwargs,
        }

        return await self.raw_request("POST", path, auth=True, payload=params)

    @catch_it
    async def cancel_order(
        self,
        order_id: Optional[str] = None,
        custom_id: Optional[str] = None,
        settle: Literal["usdt", "btc"] = "usdt",
    ):
        assert settle in ("usdt", "btc")
        assert order_id or custom_id
        id = order_id if order_id else custom_id
        path = f"/delivery/{settle}/orders/{id}"

        return await self.raw_request("DELETE", path, auth=True)

    @catch_it
    async def set_leverage(
        self,
        contract: str,
        leverage: str,
        settle: Literal["usdt", "btc"] = "usdt",
    ):
        assert settle in ("usdt", "btc")
        path = f"/delivery/{settle}/positions/{contract}/leverage"
        params = {"leverage": leverage}

        return await self.raw_request("POST", path, params=params, auth=True)


class GateUnifiedSpotRestClient(GateSpotRestClient):
    def __init__(
        self,
        account_config: AccountConfig,
        rest_config: RestConfig,
    ):
        super().__init__(account_config, rest_config)
        self.prefix = "/api/v4"

    @catch_it
    async def get_account(self, currency: Optional[str] = None):  # type: ignore[override]
        path = "/unified/accounts"
        params = {"currency": currency}

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def get_history_loan_rate(self, currency: str, tier: str | None, page: int | None, limit: int | None):
        path = "/unified/history_loan_rate"
        params = {"currency": currency, "tier": tier, "page": page, "limit": limit}
        return await self.raw_request("GET", path, params=params)


class GateUnifiedFutureRestClient(GateFutureRestClient):
    def __init__(
        self,
        account_config: AccountConfig,
        rest_config: RestConfig,
    ):
        super().__init__(account_config, rest_config)
        self.prefix = "/api/v4"

    @catch_it
    async def get_account(self, currency: Optional[str] = None):  # type: ignore[override]
        path = "/unified/accounts"
        params = {"currency": currency}

        return await self.raw_request("GET", path, params=params, auth=True)


class GateUnifiedDeliveryRestClient(GateDeliveryRestClient):
    def __init__(
        self,
        account_config: AccountConfig,
        rest_config: RestConfig,
    ):
        super().__init__(account_config, rest_config)
        self.prefix = "/api/v4"

    @catch_it
    async def get_account(self, currency: Optional[str] = None):  # type: ignore[override]
        path = "/unified/accounts"
        params = {"currency": currency}

        return await self.raw_request("GET", path, params=params, auth=True)
