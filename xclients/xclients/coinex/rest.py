import json
import time
import hmac
import hashlib
from urllib.parse import urlencode
from typing import Optional, Any, Literal, Union

from ..data_type import AccountConfig, RestConfig
from ..base_client import BaseRestClient, catch_it
from ..utils import clean_none_value

COINEX_BASE_URL = "https://api.coinex.com"

MARKET_TYPE = Literal["SPOT", "MARGIN", "FUTURES"]
SIDE = Literal["buy", "sell"]
ORDER_TYPE = Literal["limit", "market", "maker_only", "ioc", "fok"]


class CoinexRestClient(BaseRestClient):
    def __init__(
        self,
        account_config: AccountConfig,
        rest_config: RestConfig,
    ):
        rest_config.url = COINEX_BASE_URL
        super().__init__(account_config, rest_config)
        self.prefix = "/v2"

    def _gen_headers(self, http_method, endpoint, params=None, payload_string=None):
        if not self.secret_key:
            raise ValueError("secret_key is required")

        query_string = f"?{urlencode(params or {})}" if params else ""
        payload_string = payload_string or ""
        ts = str(int(time.time() * 1000))
        s = f"{http_method}{endpoint}{query_string}{payload_string}{ts}"
        sign = hmac.new(self.secret_key.encode("utf-8"), s.encode("utf-8"), hashlib.sha256).hexdigest().lower()
        headers = {
            "X-COINEX-KEY": self.api_key,
            "X-COINEX-SIGN": sign,
            "X-COINEX-TIMESTAMP": ts,
            "Content-Type": "application/json; charset=utf-8",
            #    "X-COINEX-WINDOWTIME": 5000,  # default: 5000
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
        params = clean_none_value(params or {})
        payload = clean_none_value(payload or {})
        payload_string = json.dumps(payload) if payload else ""
        if auth:
            headers = self._gen_headers(method, endpoint, params, payload_string)
        else:
            headers = {
                "X-COINEX-TIMESTAMP": str(int(time.time() * 1000)),
                "Content-Type": "application/json; charset=utf-8",
            }

        return url, headers, params, payload_string

    """
    ###############
    # market data #
    ###############
    """

    @catch_it
    async def get_spot_market(self, market: Optional[Union[str, list[str]]] = None):
        if isinstance(market, list):
            market = ",".join(market)

        path = "/spot/market"
        params = {"market": market}

        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def get_spot_ticker(self, market: Optional[Union[str, list[str]]] = None):
        if isinstance(market, list):
            market = ",".join(market)

        path = "/spot/ticker"
        params = {"market": market}

        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def get_spot_depth(
        self,
        market: str,
        limit: Literal[5, 10, 20, 50],
        interval: str = "0",
    ):
        path = "/spot/depth"
        params = {
            "market": market,
            "limit": limit,
            "interval": interval,
        }

        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def get_spot_deals(
        self,
        market: str,
        limit: Optional[int] = None,  # default: 100   max: 1000
        last_id: Optional[int] = None,
    ):
        path = "/spot/deals"
        params = {
            "market": market,
            "limit": limit,
            "last_id": last_id,
        }

        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def get_spot_kline(
        self,
        market: str,
        period: Literal[
            "1min",
            "3min",
            "5min",
            "15min",
            "30min",
            "1hour",
            "2hour",
            "4hour",
            "6hour",
            "12hour",
            "1day",
            "3day",
            "1week",
        ],
        price_type: Optional[Literal["latest_price", "mark_price", "index_price"]] = None,  # default: latest_price
        limit: Optional[int] = None,  # default: 100
    ):
        path = "/spot/kline"
        params = {
            "market": market,
            "period": period,
            "price_type": price_type,
            "limit": limit,
        }

        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def get_spot_index(self, market: Optional[Union[str, list[str]]] = None):
        if isinstance(market, list):
            market = ",".join(market)

        path = "/spot/index"
        params = {
            "market": market,
        }

        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def get_swap_market(self, market: Optional[Union[str, list[str]]] = None):
        if isinstance(market, list):
            market = ",".join(market)

        path = "/futures/market"
        params = {"market": market}

        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def get_future_ticker(self, market: Optional[Union[str, list[str]]] = None):
        if isinstance(market, list):
            market = ",".join(market)

        path = "/futures/ticker"
        params = {"market": market}

        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def get_future_depth(
        self,
        market: str,
        limit: Literal[5, 10, 20, 50],
        interval: str = "0",
    ):
        path = "/futures/depth"
        params = {
            "market": market,
            "limit": limit,
            "interval": interval,
        }

        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def get_future_deals(
        self,
        market: str,
        limit: Optional[int] = None,  # default: 100   max: 1000
        last_id: Optional[int] = None,
    ):
        path = "/futures/deals"
        params = {
            "market": market,
            "limit": limit,
            "last_id": last_id,
        }

        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def get_future_kline(
        self,
        market: str,
        period: Literal[
            "1min",
            "3min",
            "5min",
            "15min",
            "30min",
            "1hour",
            "2hour",
            "4hour",
            "6hour",
            "12hour",
            "1day",
            "3day",
            "1week",
        ],
        price_type: Optional[Literal["latest_price", "mark_price", "index_price"]] = None,  # default: latest_price
        limit: Optional[int] = None,  # default: 100 max: 1000
    ):
        path = "/futures/kline"
        params = {
            "market": market,
            "period": period,
            "price_type": price_type,
            "limit": limit,
        }

        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def get_future_index(self, market: Optional[Union[str, list[str]]] = None):
        if isinstance(market, list):
            market = ",".join(market)

        path = "/futures/index"
        params = {
            "market": market,
        }

        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def get_future_position_level(self, market: Optional[Union[str, list[str]]] = None):
        if isinstance(market, list):
            market = ",".join(market)

        path = "/futures/position-level"
        params = {
            "market": market,
        }

        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def get_funding_rate_current(self, market: str | None = None):
        path = "/futures/funding-rate"
        params = {"market": market}
        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def get_funding_rate_history(
        self,
        market: Union[str, list[str]],
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        page: Optional[int] = None,  # default: 1
        limit: Optional[int] = None,  # default: 10
    ):
        if isinstance(market, list):
            market = ",".join(market)

        path = "/futures/funding-rate-history"
        params = {
            "market": market,
            "start_time": start_time,
            "end_time": end_time,
            "page": page,
            "limit": limit,
        }

        return await self.raw_request("GET", path, params=params)

    """
    ################
    # account data #
    ################
    """

    @catch_it
    async def get_subaccount_info(self):
        path = "/account/subs"
        return await self.raw_request("GET", path, auth=True)

    @catch_it
    async def get_spot_balance(self):
        path = "/assets/spot/balance"

        return await self.raw_request("GET", path, auth=True)

    @catch_it
    async def get_spot_subaccount_balance(self, sub_user_name: str, ccy: Optional[str] = None):
        path = "/account/subs/spot-balance"
        params = {"sub_user_name": sub_user_name, "ccy": ccy}
        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def get_future_balance(self):
        path = "/assets/futures/balance"

        return await self.raw_request("GET", path, auth=True)

    @catch_it
    async def get_margin_balance(self):
        path = "/assets/margin/balance"

        return await self.raw_request("GET", path, auth=True)

    @catch_it
    async def get_financial_balance(self):
        path = "/assets/financial/balance"

        return await self.raw_request("GET", path, auth=True)

    @catch_it
    async def get_amm_liquidity(self):
        path = "/assets/amm/liquidity"

        return await self.raw_request("GET", path, auth=True)

    @catch_it
    async def get_credit_info(self):
        path = "/assets/credit/info"

        return await self.raw_request("GET", path, auth=True)

    @catch_it
    async def get_credit_balance(self):
        path = "/assets/credit/balance"

        return await self.raw_request("GET", path, auth=True)

    @catch_it
    async def get_spot_transcation_history(
        self,
        type: Literal["deposit", "withdraw", "trade", "maker_cash_back"],
        ccy: Optional[str] = None,
        start_time: Optional[int] = None,
        end_time: Optional[str] = None,
        page: Optional[int] = None,  # default: 1
        limit: Optional[int] = None,  # default: 10
    ):
        path = "/assets/spot/transcation-history"
        params = {
            "type": type,
            "ccy": ccy,
            "start_time": start_time,
            "end_time": end_time,
            "page": page,
            "limit": limit,
        }

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def margin_borrow(
        self,
        market: str,
        ccy: str,
        borrow_amount: str,
        is_auto_renew: bool,
    ):
        path = "/assets/margin/borrow"
        params = {
            "market": market,
            "ccy": ccy,
            "borrow_amount": borrow_amount,  # FIXME: borrow_amount or loan_amount
            "is_auto_renew": is_auto_renew,
        }

        return await self.raw_request("POST", path, payload=params, auth=True)

    @catch_it
    async def margin_repay(
        self,
        market: str,
        ccy: str,
        amount: str,
        borrow_id: Optional[int] = None,
    ):
        path = "/assets/margin/repay"
        params = {
            "market": market,
            "ccy": ccy,
            "amount": amount,
            "borrow_id": borrow_id,
        }

        return await self.raw_request("POST", path, payload=params, auth=True)

    @catch_it
    async def get_margin_borrow_history(
        self,
        market: Optional[str] = None,
        status: Optional[Literal["loan", "debt", "liquidated", "finish"]] = None,
        page: Optional[int] = None,  # default: 1
        limit: Optional[int] = None,  # default: 10
    ):
        path = "/assets/margin/borrow-history"
        params = {
            "market": market,
            "status": status,
            "page": page,
            "limit": limit,
        }

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def get_margin_interest_limit(
        self,
        market: str,
        ccy: str,
    ):
        path = "/assets/margin/interest-limit"
        params = {
            "market": market,
            "ccy": ccy,
        }

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def transfer(
        self,
        from_account_type: Literal["SPOT", "MARGIN", "FUTURES"],
        to_account_type: Literal["SPOT", "MARGIN", "FUTURES"],
        ccy: str,
        amount: str,
        market: Optional[str] = None,
    ):
        path = "/assets/transfer"
        params = {
            "from_account_type": from_account_type,
            "to_account_type": to_account_type,
            "ccy": ccy,
            "amount": amount,
            "market": market,
        }

        return await self.raw_request("POST", path, payload=params, auth=True)

    @catch_it
    async def get_transfer_history(
        self,
        ccy: str,
        transfer_type: Literal["MARGIN", "FUTURES"],
        market: Optional[str] = None,
        start_time: Optional[int] = None,
        end_time: Optional[str] = None,
        page: Optional[int] = None,  # default: 1
        limit: Optional[int] = None,  # default: 10
    ):
        path = "/assets/transfer-history"
        params = {
            "ccy": ccy,
            "transfer_type": transfer_type,
            "market": market,
            "start_time": start_time,
            "end_time": end_time,
            "page": page,
            "limit": limit,
        }

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def subaccount_transfer(
        self,
        from_account_type: Literal["SPOT", "FUTURES"],
        to_account_type: Literal["SPOT", "FUTURES"],
        ccy: str,
        amount: str,
        from_user_name: Optional[str] = None,
        to_user_name: Optional[str] = None,
    ):
        """
        from_user_name 和 to_user_name 至少提供一个
        如果未提供 from_user_name, 默认是从主账号转出
        如果未提供 to_user_name, 默认是转入主账号
        如果两个子账户之间互转，则只允许从 现货账户 转入到 现货账户
        """
        if (not to_user_name) and (not from_user_name):
            raise ValueError("Either Parameters `to_user_name` or `from_user_name` is needed")

        path = "/account/subs/transfer"
        params = {
            "from_account_type": from_account_type,
            "to_account_type": to_account_type,
            "from_user_name": from_user_name,
            "to_user_name": to_user_name,
            "ccy": ccy,
            "amount": amount,
        }
        return await self.raw_request("POST", path, payload=params, auth=True)

    @catch_it
    async def withdraw(
        self,
        ccy: str,
        amount: str,
        to_address: str,
        chain: Optional[str] = None,
        withdraw_method: Literal["on_chain", "inter_user"] = "on_chain",
    ):
        path = "/assets/withdraw"
        if withdraw_method == "on_chain" and not chain:
            raise ValueError("on_chain withdraw `chain` param is needed")

        params = {
            "ccy": ccy,
            "chain": chain,
            "to_address": to_address,
            "withdraw_method": withdraw_method,
            "amount": amount,
        }
        return await self.raw_request("POST", path, payload=params, auth=True)

    @catch_it
    async def withdraw_records(
        self,
        ccy: Optional[str] = None,
        withdraw_id: Optional[int] = None,
        status: Optional[str] = None,
        page: Optional[int] = None,
        limit: Optional[int] = 100,
    ):
        path = "/assets/withdraw"
        params = {
            "ccy": ccy,
            "withdraw_id": withdraw_id,
            "status": status,
            "page": page,
            "limit": limit,
        }
        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def add_amm_liquidity(
        self,
        market: str,
        base_ccy_amount: str,
        quote_ccy_amount: str,
    ):
        path = "/assets/amm/add-liquidity"
        params = {
            "market": market,
            "base_ccy_amount": base_ccy_amount,
            "quote_ccy_amount": quote_ccy_amount,
        }

        return await self.raw_request("POST", path, payload=params, auth=True)

    @catch_it
    async def remove_amm_liquidity(
        self,
        market: str,
    ):
        path = "/assets/amm/remove-liquidity"
        params = {"market": market}

        return await self.raw_request("POST", path, payload=params, auth=True)

    @catch_it
    async def get_commission_rate(self, symbol: str, market_type: Literal["SPOT", "FUTURES"] = "SPOT"):
        path = "/account/trade-fee-rate"
        params = {"market": symbol, "market_type": market_type}
        return await self.raw_request("GET", path, params=params, auth=True)

    """
    ################
    # trade data #
    ################
    """

    @catch_it
    async def place_spot_order(
        self,
        market: str,
        market_type: Literal["SPOT", "MARGIN"],
        side: SIDE,
        type: ORDER_TYPE,
        amount: str,
        price: Optional[str] = None,
        client_id: Optional[str] = None,
        ccy: Optional[str] = None,
        is_hide: Optional[bool] = None,
        stp_mode: Optional[Literal["ct", "cm", "both"]] = None,
    ):
        path = "/spot/order"
        params = {
            "market": market,
            "market_type": market_type,
            "side": side,
            "type": type,
            "amount": amount,
            "price": price,
            "client_id": client_id,
            "ccy": ccy,
            "is_hide": is_hide,
            "stp_mode": stp_mode,
        }

        return await self.raw_request("POST", path, payload=params, auth=True)

    @catch_it
    async def cancel_spot_order(
        self,
        market: str,
        market_type: Literal["SPOT", "MARGIN"],
        order_id: str,
    ):
        path = "/spot/cancel-order"
        params = {
            "market": market,
            "market_type": market_type,
            "order_id": order_id,
        }

        return await self.raw_request("POST", path, payload=params, auth=True)

    @catch_it
    async def cancel_all_spot_orders(
        self,
        market: str,
        market_type: Literal["SPOT", "MARGIN"],
        side: Optional[SIDE] = None,
    ):
        path = "/spot/cancel-all-order"
        params = {
            "market": market,
            "market_type": market_type,
            "side": side,
        }

        return await self.raw_request("POST", path, payload=params, auth=True)

    @catch_it
    async def get_spot_order(
        self,
        market: str,
        order_id: str,
    ):
        path = "/spot/order-status"
        params = {
            "market_type": market,
            "order_id": order_id,
        }

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def get_spot_pending_orders(
        self,
        market_type: Literal["SPOT", "MARGIN"],
        market: Optional[str] = None,
        side: Optional[SIDE] = None,
        client_id: Optional[str] = None,
        page: Optional[int] = None,  # default: 1
        limit: Optional[int] = None,  # default: 10
    ):
        path = "/spot/pending-order"
        params = {
            "market_type": market_type,
            "market": market,
            "side": side,
            "client_id": client_id,
            "page": page,
            "limit": limit,
        }

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def get_spot_finished_orders(
        self,
        market_type: Literal["SPOT", "MARGIN"],
        market: Optional[str] = None,
        side: Optional[SIDE] = None,
        page: Optional[int] = None,  # default: 1
        limit: Optional[int] = None,  # default: 10
    ):
        path = "/spot/finished-order"
        params = {
            "market_type": market_type,
            "market": market,
            "side": side,
            "page": page,
            "limit": limit,
        }

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def get_spot_trade(
        self,
        market: str,
        market_type: Literal["SPOT", "MARGIN"],
        side: Optional[SIDE] = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        page: Optional[int] = None,  # default: 1
        limit: Optional[int] = None,  # default: 10
    ):
        path = "/spot/user-deals"
        params = {
            "market": market,
            "market_type": market_type,
            "side": side,
            "start_time": start_time,
            "end_time": end_time,
            "page": page,
            "limit": limit,
        }

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def place_future_order(
        self,
        market: str,
        market_type: Literal["FUTURES"],
        side: SIDE,
        type: ORDER_TYPE,
        amount: str,
        price: Optional[str] = None,
        client_id: Optional[str] = None,
        is_hide: Optional[bool] = None,
        stp_mode: Optional[Literal["ct", "cm", "both"]] = None,
    ):
        path = "/futures/order"
        params = {
            "market": market,
            "market_type": market_type,
            "side": side,
            "type": type,
            "amount": amount,
            "price": price,
            "client_id": client_id,
            "is_hide": is_hide,
            "stp_mode": stp_mode,
        }

        return await self.raw_request("POST", path, payload=params, auth=True)

    @catch_it
    async def cancel_future_order(
        self,
        market: str,
        market_type: Literal["FUTURES"],
        order_id: str,
    ):
        path = "/futures/cancel-order"
        params = {
            "market": market,
            "market_type": market_type,
            "order_id": order_id,
        }

        return await self.raw_request("POST", path, payload=params, auth=True)

    @catch_it
    async def cancel_all_future_orders(
        self,
        market: str,
        market_type: Literal["FUTURES"],
        side: Optional[SIDE] = None,
    ):
        path = "/futures/cancel-all-order"
        params = {
            "market": market,
            "market_type": market_type,
            "side": side,
        }

        return await self.raw_request("POST", path, payload=params, auth=True)

    @catch_it
    async def get_future_order(
        self,
        market: str,
        order_id: str,
    ):
        path = "/futures/order-status"
        params = {
            "market_type": market,
            "order_id": order_id,
        }

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def get_future_pending_orders(
        self,
        market_type: Literal["FUTURES"],
        market: Optional[str] = None,
        side: Optional[SIDE] = None,
        client_id: Optional[str] = None,
        page: Optional[int] = None,  # default: 1
        limit: Optional[int] = None,  # default: 10
    ):
        path = "/futures/pending-order"
        params = {
            "market_type": market_type,
            "market": market,
            "side": side,
            "client_id": client_id,
            "page": page,
            "limit": limit,
        }

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def get_future_finished_orders(
        self,
        market_type: Literal["FUTURES"],
        market: Optional[str] = None,
        side: Optional[SIDE] = None,
        page: Optional[int] = None,  # default: 1
        limit: Optional[int] = None,  # default: 10
    ):
        path = "/futures/finished-order"
        params = {
            "market_type": market_type,
            "market": market,
            "side": side,
            "page": page,
            "limit": limit,
        }

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def get_future_trade(
        self,
        market: str,
        market_type: Literal["FUTURES"],
        side: Optional[SIDE] = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        page: Optional[int] = None,  # default: 1
        limit: Optional[int] = None,  # default: 10
    ):
        path = "/futures/user-deals"
        params = {
            "market": market,
            "market_type": market_type,
            "side": side,
            "start_time": start_time,
            "end_time": end_time,
            "page": page,
            "limit": limit,
        }

        return await self.raw_request("GET", path, params=params, auth=True)

    """
    ################
    # position data #
    ################
    """

    @catch_it
    async def close_position(
        self,
        market: str,
        market_type: Literal["FUTURES"],
        type: Literal[ORDER_TYPE],
        price: Optional[str] = None,
        amount: Optional[str] = None,
        client_id: Optional[str] = None,
        is_hide: Optional[bool] = None,
        stp_mode: Optional[str] = None,
    ):
        path = "/futures/close-position"
        params = {
            "market": market,
            "market_type": market_type,
            "type": type,
            "price": price,
            "amount": amount,
            "client_id": client_id,
            "is_hide": is_hide,
            "stp_mode": stp_mode,
        }

        return await self.raw_request("POST", path, params=params, auth=True)

    @catch_it
    async def adjust_position_margin(
        self,
        market: str,
        market_type: Literal["FUTURES"],
        amount: str,
    ):
        path = "/futures/adjust-position-margin"
        params = {
            "market": market,
            "market_type": market_type,
            "amount": amount,
        }

        return await self.raw_request("POST", path, params=params, auth=True)

    @catch_it
    async def adjust_position_leverage(
        self,
        market: str,
        market_type: Literal["FUTURES"],
        margin_mode: Literal["isolated", "cross"],
        leverage: int,
    ):
        path = "/futures/adjust-position-leverage"
        params = {
            "market": market,
            "market_type": market_type,
            "margin_mode": margin_mode,
            "leverage": leverage,
        }

        return await self.raw_request("POST", path, params=params, auth=True)

    @catch_it
    async def get_current_position(
        self,
        market_type: Literal["FUTURES"],
        market: Optional[str] = None,
        page: Optional[int] = None,  # default: 1
        limit: Optional[int] = None,  # default: 10
    ):
        path = "/futures/pending-position"
        params = {
            "market_type": market_type,
            "market": market,
            "page": page,
            "limit": limit,
        }

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def get_finished_position(
        self,
        market_type: Literal["FUTURES"],
        market: Optional[str] = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        page: Optional[int] = None,  # default: 1
        limit: Optional[int] = None,  # default: 10
    ):
        path = "/futures/finished-position"
        params = {
            "market_type": market_type,
            "market": market,
            "start_time": start_time,
            "end_time": end_time,
            "page": page,
            "limit": limit,
        }

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def get_funding_history(
        self,
        market_type: Literal["FUTURES"],
        market: Optional[str] = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        page: Optional[int] = None,  # default: 1
        limit: Optional[int] = None,  # default: 10
    ):
        path = "/futures/position-funding-history"
        params = {
            "market_type": market_type,
            "market": market,
            "start_time": start_time,
            "end_time": end_time,
            "page": page,
            "limit": limit,
        }

        return await self.raw_request("GET", path, params=params, auth=True)

    async def enable_cet_discount(self, enable: bool = True):
        """
        Enable CET discount for trading fees.
        """
        path = "/account/settings"
        return await self.raw_request("POST", path, params={"cet_discount_enabled": enable}, auth=True)
