import hashlib
import hmac
import json
from urllib.parse import urlencode
from decimal import Decimal
from typing import Any, Literal, Optional, Union


from ..base_client import BaseRestClient, catch_it
from ..utils import clean_none_value, encoded_string, get_current_ms
from ..data_type import AccountConfig, RestConfig


class BinanceRestClient(BaseRestClient):
    def __init__(
        self,
        account_config: AccountConfig,
        rest_config: RestConfig,
    ):
        super().__init__(account_config, rest_config)
        self.headers: dict[str, Any] = {"X-MBX-APIKEY": self.api_key} if self.api_key else {}

    def _get_sign(self, data: str) -> str:
        assert self.secret_key is not None, "secret_key is required for signing requests"
        m = hmac.new(self.secret_key.encode("utf-8"), data.encode("utf-8"), hashlib.sha256)
        return m.hexdigest()

    def _get_request_timestamp(self):
        return get_current_ms()

    def _prepare_params(self, params):
        return encoded_string(clean_none_value(params))

    def _sign_request(
        self, http_method: str, url_path: str, params: dict[str, Any] = {}, payload_string: str | None = None
    ) -> dict[str, Any]:
        if not self.secret_key:
            raise ValueError("secret_key is required")

        params["timestamp"] = get_current_ms()
        query_string = urlencode(params, True, safe=",@")
        payload_string = payload_string or ""
        params["signature"] = self._get_sign(f"{query_string}{payload_string}")
        return params

    def gen_request(
        self,
        method: Literal["GET", "POST", "PUT", "DELETE"],
        path: str = "",
        params: Optional[dict[str, Any]] = None,
        auth: bool = False,
        payload: Optional[Any] = None,
    ):
        url = f"{self.base_url}{path}"
        params = clean_none_value(params or {})
        payload = clean_none_value(payload or {})
        payload_string = urlencode(payload)
        headers = self.headers
        if auth == True:
            params = self._sign_request(method, path, params, payload_string)

        return url, headers, params, payload_string


class BinanceSpotRestClient(BinanceRestClient):
    def __init__(
        self,
        account_config: AccountConfig,
        rest_config: RestConfig,
    ):
        rest_config.url = "https://api.binance.com"
        super().__init__(account_config, rest_config)

    @catch_it
    async def get_exchange_info(self, symbol: Optional[str] = None):
        path = "/api/v3/exchangeInfo"
        params = {"symbol": symbol}

        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def get_margin_delist_schedule(self):
        path = "/sapi/v1/margin/delist-schedule"

        return await self.raw_request("GET", path)

    @catch_it
    async def get_spot_delist_schedule(self):
        path = "/sapi/v1/spot/delist-schedule"

        return await self.raw_request("GET", path)

    @catch_it
    async def get_price(self, symbol: Optional[str] = None):
        path = "/api/v3/ticker/price"
        params = {"symbol": symbol}

        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def get_ticker(self, symbol: Optional[str] = None):
        path = "/api/v3/ticker/bookTicker"
        params = {"symbol": symbol}

        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def get_depth(self, symbol: str, limit: Optional[int] = 100):
        path = "/api/v3/depth"
        params = {"symbol": symbol, "limit": limit}

        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def get_cross_margin_pair(self):
        path = "/sapi/v1/margin/allPairs"

        return await self.raw_request("GET", path, auth=True)

    @catch_it
    async def get_cross_margin_asset(self):
        path = "/sapi/v1/margin/allAssets"

        return await self.raw_request("GET", path, auth=True)

    @catch_it
    async def get_asset_dividend(
        self,
        asset: Optional[str] = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        limit: Optional[int] = None,
    ):
        path = "/sapi/v1/asset/assetDividend"
        params = {"asset": asset, "startTime": start_time, "endTime": end_time, "limit": limit}

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def get_trade_history(
        self,
        symbol: str,
        startTime: Optional[int] = None,
        endTime: Optional[int] = None,
        from_id: Optional[str] = None,
        limit: Optional[int] = None,
        order_id: Optional[str] = None,
    ):
        path = "/api/v3/myTrades"
        params = {
            "symbol": symbol,
            "startTime": startTime,
            "endTime": endTime,
            "limit": limit,
            "fromId": from_id,
            "orderId": order_id,
        }

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def sapi_get_trade_history(
        self,
        symbol: str,
        startTime: Optional[int] = None,
        endTime: Optional[int] = None,
        from_id: Optional[str] = None,
        limit: Optional[int] = None,
        order_id: Optional[str] = None,
    ):
        path = "/sapi/v1/margin/myTrades"
        params = {
            "symbol": symbol,
            "startTime": startTime,
            "endTime": endTime,
            "limit": limit,
            "fromId": from_id,
            "orderId": order_id,
        }

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def get_interest_history(
        self,
        asset: Optional[str] = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        current: Optional[int] = 1,
        size: Optional[int] = 100,
    ):
        path = "/sapi/v1/margin/interestHistory"
        params = {"asset": asset, "startTime": start_time, "endTime": end_time, "current": current, "size": size}

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def query_spot_order(
        self, symbol: str, orderId: Optional[int] = None, origClientOrderId: Optional[str] = None
    ):
        path = "/api/v3/order"
        params = {
            "symbol": symbol,
            "orderId": orderId,
            "origClientOrderId": origClientOrderId,
        }

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def api_v3_allOrders(
        self,
        symbol: str,
        orderId: Optional[str] = None,
        startTime: Optional[int] = None,
        endTime: Optional[int] = None,
        limit: Optional[int] = 1000,
    ):
        path = "/api/v3/allOrders"
        params = {"symbol": symbol, "orderId": orderId, "startTime": startTime, "endTime": endTime, "limit": limit}

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def sapi_v1_allOrders(
        self,
        symbol: str,
        orderId: Optional[str] = None,
        startTime: Optional[int] = None,
        endTime: Optional[int] = None,
        limit: Optional[int] = 1000,
    ):
        path = "/sapi/v1/margin/allOrders"
        params = {"symbol": symbol, "orderId": orderId, "startTime": startTime, "endTime": endTime, "limit": limit}

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def query_margin_order(
        self,
        symbol: str,
        orderId: Optional[int] = None,
        origClientOrderId: Optional[str] = None,
        isIsolated: Optional[str] = None,
    ):
        path = "/sapi/v1/margin/order"
        params = {
            "symbol": symbol,
            "orderId": orderId,
            "origClientOrderId": origClientOrderId,
            "isIsolated": isIsolated,
        }

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def sapi_cross_margin_collateral_ratio(self):
        path = "/sapi/v1/margin/crossMarginCollateralRatio"

        return await self.raw_request("GET", path)

    @catch_it
    async def sapi_qry_asset_transfer(
        self,
        type: str,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        current: Optional[int] = 1,
        size: Optional[int] = 100,
        from_symbol: Optional[str] = None,
        to_symbol: Optional[str] = None,
    ):
        path = "/sapi/v1/asset/transfer"
        params = {
            "type": type,
            "startTime": start_time,
            "endTime": end_time,
            "current": current,
            "size": size,
            "fromSymbol": from_symbol,
            "toSymbol": to_symbol,
        }

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def sapi_asset_dustable(self):
        path = "/sapi/v1/asset/dust-btc"

        return await self.raw_request("POST", path, auth=True)

    @catch_it
    async def sapi_asset_dust(self, asset: Union[str, list[str]]):
        path = "/sapi/v1/asset/dust"
        params = {"asset": asset}

        return await self.raw_request("POST", path, params=params, auth=True)

    @catch_it
    async def sapi_asset_dribblet(self):
        path = "/sapi/v1/asset/dribblet"

        return await self.raw_request("GET", path, auth=True)

    @catch_it
    async def sapi_margin_dustable(self):
        path = "/sapi/v1/asset/dust-btc"
        params = {"accountType": "MARGIN"}

        return await self.raw_request("POST", path, params=params, auth=True)

    @catch_it
    async def sapi_margin_dust(self, asset: Union[str, list[str]]):
        """
        @param asset: str array e.g. ETH,BTC
        """
        path = "/sapi/v1/asset/dust"
        params = {"accountType": "MARGIN", "asset": asset}

        return await self.raw_request("POST", path, params=params, auth=True)

    @catch_it
    async def sapi_margin_dribblet(self):
        path = "/sapi/v1/asset/dribblet"
        params = {"accountType": "MARGIN"}

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def sapi_margin_account(self):
        path = "/sapi/v1/margin/account"

        return await self.raw_request("GET", path, auth=True)

    # Query Cross Margin Fee Data
    @catch_it
    async def sapi_margin_cross_margin_data(self, coin: Optional[str] = None):
        path = "/sapi/v1/margin/crossMarginData"
        params = {"coin": coin}

        return await self.raw_request("GET", path, params=params, auth=True)

    # Margin Account Borrow
    @catch_it
    async def sapi_margin_loan(
        self, asset: str, amount: str, isIsolated: Optional[bool] = False, symbol: Optional[str] = ""
    ):
        path = "/sapi/v1/margin/borrow-repay"
        isolated = "TRUE" if isIsolated else "FALSE"
        params = {"asset": asset, "amount": amount, "type": "BORROW", "isIsolated": isolated, "symbol": symbol}
        return await self.raw_request("POST", path, params=params, auth=True)

    # Margin Account Repay
    @catch_it
    async def sapi_margin_repay(
        self, asset: str, amount: str, isIsolated: Optional[bool] = False, symbol: Optional[str] = ""
    ):
        path = "/sapi/v1/margin/borrow-repay"
        isolated = "TRUE" if isIsolated else "FALSE"
        params = {"asset": asset, "amount": amount, "type": "REPAY", "isIsolated": isolated, "symbol": symbol}
        return await self.raw_request("POST", path, params=params, auth=True)

    # User Universal Transfer
    @catch_it
    async def sapi_asset_transfer(self, type: str, asset: str, amount: str, **kwargs):
        path = "/sapi/v1/asset/transfer"
        params = {"type": type, "asset": asset, "amount": str(amount), **kwargs}

        return await self.raw_request("POST", path, params=params, auth=True)

    # Transfer, Subaccount to Subaccount
    @catch_it
    async def sapi_sub_account_transfer_sub2sub(self, toEmail: str, asset: str, amount: Union[str, Decimal]):
        path = "/sapi/v1/sub-account/transfer/subToSub"
        params = {"toEmail": toEmail, "asset": asset, "amount": str(amount)}
        return await self.raw_request("POST", path, params=params, auth=True)

    @catch_it
    async def spot_order(self, symbol: str, side: Literal["BUY", "SELL"], type: Literal["LIMIT", "MARKET"], **kwargs):
        """https://binance-docs.github.io/apidocs/spot/cn/#trade-3

        Args:
            symbol (str): _description_
            side (str): _description_
            type (str): _description_

        Returns:
            _type_: _description_
        """
        path = "/api/v3/order"
        params = {"symbol": symbol, "side": side, "type": type, **kwargs}
        return await self.raw_request("POST", path, params=params, auth=True)

    @catch_it
    async def cancel_order(self, symbol: str, orderId: Optional[str] = None, clientOid: Optional[str] = None):
        path = "/api/v3/order"
        params = {"symbol": symbol, "orderId": orderId, "origClientOrderId": clientOid}

        return await self.raw_request("DELETE", path, params=params, auth=True)

    @catch_it
    async def sapi_margin_order(self, symbol: str, side: str, type: str, **kwargs):
        path = "/sapi/v1/margin/order"
        params = {"symbol": symbol, "side": side, "type": type, **kwargs}

        return await self.raw_request("POST", path, params=params, auth=True)

    @catch_it
    async def sapi_cancel_order(self, symbol: str, orderId: Optional[str] = None, clientOid: Optional[str] = None):
        path = "/sapi/v1/margin/order"
        params = {"symbol": symbol, "orderId": orderId, "origClientOrderId": clientOid}

        return await self.raw_request("DELETE", path, params=params, auth=True)

    @catch_it
    async def sapi_bnb_burn_post(self, spot_bnb_burn: Optional[str] = None, interest_bnb_burn: Optional[str] = None):
        path = "/sapi/v1/bnbBurn"
        params = {"spotBNBBurn": spot_bnb_burn, "interestBNBBurn": interest_bnb_burn}

        return await self.raw_request("POST", path, params=params, auth=True)

    @catch_it
    async def sapi_bnb_burn_get(self):
        path = "/sapi/v1/bnbBurn"

        return await self.raw_request("GET", path, auth=True)

    @catch_it
    async def get_spot_account(self):
        path = "/api/v3/account"

        return await self.raw_request("GET", path, auth=True)

    @catch_it
    async def get_history_kline(
        self,
        symbol: str,
        interval: str,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        limit: Optional[int] = 1000,
    ):
        path = "/api/v3/klines"
        params = {"symbol": symbol, "interval": interval, "startTime": start_time, "endTime": end_time, "limit": limit}

        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def cancel_all_orders(self, symbol: str):
        path = "/api/v3/openOrders"
        params = {"symbol": symbol}

        return await self.raw_request("DELETE", path, params=params, auth=True)

    @catch_it
    async def get_open_orders(self, symbol: Optional[str] = None):
        path = "/api/v3/openOrders"
        params = {"symbol": symbol}

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def sapi_cancel_all_orders(self, symbol: str):
        path = "/sapi/v1/margin/openOrders"
        params = {"symbol": symbol}

        return await self.raw_request("DELETE", path, params=params, auth=True)

    @catch_it
    async def sapi_get_commission_rate(self, symbol: Optional[str] = None):
        path = "/sapi/v1/asset/tradeFee"
        params = {"symbol": symbol}

        return await self.raw_request("GET", path, params=params, auth=True)

    # listenKey
    @catch_it
    async def api_v3_listen_key(self):
        path = "/api/v3/userDataStream"

        return await self.raw_request("POST", path)

    @catch_it
    async def api_v3_delay_listen_key(self, listenKey: str):
        path = "/api/v3/userDataStream"
        params = {"listenKey": listenKey}

        return await self.raw_request("PUT", path, params=params)

    @catch_it
    async def delete_listen_key(self, listenKey: str):
        path = "/api/v3/userDataStream"
        params = {"listenKey": listenKey}

        return await self.raw_request("DELETE", path, params=params)

    @catch_it
    async def sapi_v1_listen_key(self):
        path = "/sapi/v1/userDataStream"

        return await self.raw_request("POST", path)

    @catch_it
    async def sapi_v1_delay_listen_key(self, listenKey: str):
        path = "/sapi/v1/userDataStream"
        params = {"listenKey": listenKey}

        return await self.raw_request("PUT", path, params=params)

    @catch_it
    async def sapi_delete_listen_key(self, listenKey: str):
        path = "/sapi/v1/userDataStream"
        params = {"listenKey": listenKey}

        return await self.raw_request("DELETE", path, params=params)

    @catch_it
    async def get_24h_info(
        self,
        symbol: Optional[str] = None,
        symbols: Optional[list] = None,
        type: Optional[Literal["FULL", "MINI"]] = None,
    ):
        path = "/api/v3/ticker/24hr"
        assert symbol is None or symbols is None, "symbol and symbols cannot used together"
        params = {"symbol": symbol, "symbols": json.dumps(symbols).replace(" ", "") if symbols else None, "type": type}

        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def sapi_loan_vip_ongoing_order(
        self,
        orderId: Optional[int] = None,
        collateralAccountId: Optional[int] = None,
        loanCoin: Optional[str] = None,
        collateralCoin: Optional[str] = None,
        current: Optional[int] = None,
        limit: Optional[int] = None,
    ):
        path = "/sapi/v1/loan/vip/ongoing/orders"
        params = {
            "orderId": orderId,
            "collateralAccountId": collateralAccountId,
            "loanCoin": loanCoin,
            "collateralCoin": collateralCoin,
            "current": current,
            "limit": limit,
        }
        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def sapi_loan_vip_repay(self, orderId: int, amount: Decimal):
        path = "/sapi/v1/loan/vip/repay"
        params = {
            "amount": str(amount),
            "orderId": orderId,
        }
        return await self.raw_request("POST", path, params=params, auth=True)

    @catch_it
    async def sapi_loan_vip_repay_history(
        self,
        orderId: Optional[int] = None,
        loanCoin: Optional[str] = None,
        startTime: Optional[int] = None,
        endTime: Optional[int] = None,
        current: Optional[int] = None,
        limit: Optional[int] = None,
    ):
        path = "/sapi/v1/loan/vip/repay/history"
        params = {
            "orderId": orderId,
            "loanCoin": loanCoin,
            "current": current,
            "limit": limit,
            "startTime": startTime,
            "endTime": endTime,
        }
        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def sapi_loan_vip_renew(self, orderId: int, loanTerm: Literal[30, 60]):
        path = "/sapi/v1/loan/vip/renew"
        params = {"orderId": orderId, "loanTerm": loanTerm}
        return await self.raw_request("POST", path, params=params, auth=True)

    @catch_it
    async def sapi_asset_wallet_balance(self):
        path = "/sapi/v1/asset/wallet/balance"
        return await self.raw_request("GET", path, auth=True)

    @catch_it
    async def sapi_loan_vip_borrow(
        self,
        loanAccountId: int,
        loanCoin: str,
        loanAmount: Decimal,
        collateralAccountId: Union[str, list],
        collateralCoin: Union[str, list],
        isFlexibleRate: bool = True,
        loanTerm: Optional[Literal[30, 60]] = None,
    ):
        path = "/sapi/v1/loan/vip/borrow"
        params = {
            "loanAccountId": loanAccountId,
            "loanCoin": loanCoin,
            "loanAmount": str(loanAmount),
            "collateralAccountId": collateralAccountId,
            "collateralCoin": collateralCoin,
            "isFlexibleRate": str(isFlexibleRate).upper(),
            "loanTerm": loanTerm,
        }
        return await self.raw_request("POST", path, params=params, auth=True)

    @catch_it
    async def bapi_vip_loanable_asset(self, vipLevel: int):
        """Fetch viploan data via Binance website rest api
        API source: https://www.binance.com/en/vip-loan

        Args:
            vipLevel (int): vip level

        Returns:
            dict: API response
        """
        url = "https://www.binance.com/bapi/margin/v2/friendly/collateral/loans/loan-data/vip/loanable-asset"
        params = {"vipLevel": vipLevel}

        return await self.http_sess.request("GET", url, params=params)

    @catch_it
    async def sapi_loan_vip_loanable_data(
        self,
        loanCoin: Optional[str] = None,
        vipLevel: Optional[int] = None,
    ):
        path = "/sapi/v1/loan/vip/loanable/data"
        params = {
            "loanCoin": loanCoin,
            "vipLevel": vipLevel,
        }
        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def sapi_loan_vip_collateral_data(self, collateralCoin: Optional[str] = None):
        path = "/sapi/v1/loan/vip/collateral/data"
        params = {"collateralCoin": collateralCoin}
        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def sapi_loan_vip_request_data(
        self,
        requestId: Optional[str] = None,
        current: Optional[int] = None,
        limit: Optional[int] = None,
    ):
        # requestId参数在文档上没有，币安开发说有这个参数可以用
        path = "/sapi/v1/loan/vip/request/data"
        params = {
            "requestId": requestId,
            "current": current,
            "limit": limit,
        }
        return await self.raw_request("GET", path, params, auth=True)

    @catch_it
    async def sapi_loan_vip_request_interest_rate(
        self,
        loanCoin: str,
    ):
        path = "/sapi/v1/loan/vip/request/interestRate"
        params = {"loanCoin": loanCoin}
        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def sapi_loan_p2p_market(self, coin: str, current: int | None = None, size: int | None = None):
        # NOTE 此接口bn官网文档未公布，霁哥找bn要的预发布的pdf
        params = {"borrowCoin": coin, "current": current, "limit": 100}
        path = "/sapi/v1/loan/fixed/data/borrow"
        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def sapi_loan_flexible_loanable(self, loanCoin: str | None = None):
        path = "/sapi/v2/loan/flexible/loanable/data"
        return await self.raw_request("GET", path, params={"loanCoin": loanCoin if loanCoin else None}, auth=True)

    @catch_it
    async def sapi_loan_interest_history(
        self,
        coin: str,
        startTime: int | None = None,
        endTime: int | None = None,
        current: int | None = None,
        limit: int = 100,
    ):
        assert coin, "param `coin` is empty"
        if startTime and endTime:
            assert endTime - startTime <= 90 * 24 * 60 * 60 * 1000, "startTime 和 endTime 之间的最大间隔为90天"
        path = "/sapi/v2/loan/interestRateHistory"

        params = {
            "coin": coin,
            "startTime": startTime,
            "endTime": endTime,
            "current": current,
            "limit": limit,
        }
        res = await self.raw_request("GET", path, params=params, auth=True)
        return res

    @catch_it
    async def sapi_loan_vip_request_interest_history(
        self,
        coin: str,
        startTime: Optional[int] = None,
        endTime: Optional[int] = None,
        current: Optional[int] = None,
        limit: Optional[int] = 100,
    ):
        path = "/sapi/v1/loan/vip/interestRateHistory"
        params = {
            "coin": coin,
            "startTime": startTime,
            "endTime": endTime,
            "current": current,
            "limit": limit,
        }
        res = await self.raw_request("GET", path, params=params, auth=True)
        return res

    @catch_it
    async def sapi_margin_interest_rate_cur(
        self,
        asset: str,
        isIsolated: bool = False,
    ):
        path = "/sapi/v1/margin/next-hourly-interest-rate"
        params = {"assets": asset, "isIsolated": isIsolated}
        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def sapi_margin_interest_rate_his(
        self,
        asset: str,
        vipLevel: int | None = None,
        startTime: Optional[int] = None,
        endTime: Optional[int] = None,
    ):
        path = "/sapi/v1/margin/interestRateHistory"
        params = {"asset": asset, "vipLevel": vipLevel, "startTime": startTime, "endTime": endTime}
        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def sapi_staking_sol_rate_history(
        self,
        startTime: Optional[int] = None,
        endTime: Optional[int] = None,
        current: Optional[int] = 1,
        size: Optional[int] = 100,
    ):
        path = "/sapi/v1/sol-staking/sol/history/rateHistory"
        params = {"startTime": startTime, "endTime": endTime, "current": current, "size": size}
        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def sapi_staking_eth_rate_history(
        self,
        startTime: Optional[int] = None,
        endTime: Optional[int] = None,
        current: Optional[int] = 1,
        size: Optional[int] = 100,
    ):
        path = "/sapi/v1/eth-staking/eth/history/rateHistory"
        params = {"startTime": startTime, "endTime": endTime, "current": current, "size": size}
        return await self.raw_request("GET", path, params=params, auth=True)

    # pm pro
    @catch_it
    async def change_repay_type(self, auto_repay: bool):
        path = "/sapi/v1/portfolio/repay-futures-switch"
        params = {"autoRepay": auto_repay}
        return await self.raw_request("POST", path, params=params, auth=True)

    @catch_it
    async def query_repay_type(self):
        path = "/sapi/v1/portfolio/repay-futures-switch"
        return await self.raw_request("GET", path, auth=True)

    @catch_it
    async def sapi_get_asset_index_price(self, asset: Optional[str] = None):
        path = "/sapi/v1/portfolio/asset-index-price"
        params = {"asset": asset}
        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def sapi_get_margin_asset_leverage(self):
        path = "/sapi/v1/portfolio/margin-asset-leverage"
        return await self.raw_request("GET", path, auth=True)

    @catch_it
    async def sapi_v2_get_collateral_rate(self):
        path = "/sapi/v2/portfolio/collateralRate"
        return await self.raw_request("GET", path, auth=True)

    @catch_it
    async def get_collateral_rate(self):
        url = f"https://www.binance.com/bapi/margin/v2/public/margin/collateral-rate"

        return await self.http_sess.request("GET", url)

    @catch_it
    async def sapi_get_account(self):
        path = "/sapi/v1/portfolio/account"
        return await self.raw_request("GET", path, auth=True)

    @catch_it
    async def sapi_bnb_transfer(self, amount: Union[str, Decimal], transfer_side: Literal["TO_UM", "FROM_UM"]):
        path = "/sapi/v1/portfolio/bnb-transfer"
        params = {"amount": str(amount), "transferSide": transfer_side}

        return await self.raw_request("POST", path, params=params, auth=True)

    @catch_it
    async def sapi_auto_collection(self):
        path = "/sapi/v1/portfolio/auto-collection"

        return await self.raw_request("POST", path, auth=True)

    @catch_it
    async def sapi_asset_collection(self, asset: str):
        path = "/sapi/v1/portfolio/asset-collection"
        params = {"asset": asset}

        return await self.raw_request("POST", path, params=params, auth=True)

    @catch_it
    async def sapi_repay(self, from_type: Optional[Literal["SPOT", "MARGIN"]] = None):
        path = "/sapi/v1/portfolio/repay"
        params = {"from": from_type}

        return await self.raw_request("POST", path, params=params, auth=True)

    @catch_it
    async def sapi_get_pm_loan(self):
        path = "/sapi/v1/portfolio/pmLoan"

        return await self.raw_request("GET", path, auth=True)

    @catch_it
    async def sapi_repay_futures_negative_balance(self, from_type: Optional[Literal["SPOT", "MARGIN"]] = None):
        path = "/sapi/v1/portfolio/repay-futures-negative-balance"
        params = {"from": from_type}

        return await self.raw_request("POST", path, params=params, auth=True)

    @catch_it
    async def sapi_repay_futures_switch(self, auto_repay: bool):
        path = "/sapi/v1/portfolio/repay-futures-switch"
        params = {"autoRepay": str(auto_repay).lower()}  # true, false

        return await self.raw_request("POST", path, params=params, auth=True)

    @catch_it
    async def sapi_get_repay_futures_switch(self):
        path = "/sapi/v1/portfolio/repay-futures-switch"

        return await self.raw_request("GET", path, auth=True)

    @catch_it
    async def sapi_get_interest_history(
        self,
        asset: Optional[str] = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        size: Optional[int] = None,  # default: 10  max: 100
    ):
        path = "/sapi/v1/portfolio/interest-history"
        params = {
            "asset": asset,
            "startTime": start_time,
            "endTime": end_time,
            "size": size,
        }

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def sapi_v2_get_account(self):
        path = "/sapi/v2/portfolio/account"

        return await self.raw_request("GET", path, auth=True)

    @catch_it
    async def sapi_get_balance(self, asset: Optional[str] = None):
        path = "/sapi/v1/portfolio/balance"
        params = {"asset": asset}

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def sapi_v1_account_info(self):
        path = "/sapi/v1/account/info"

        return await self.raw_request("GET", path, auth=True)


class BinanceLinearRestClient(BinanceRestClient):
    def __init__(
        self,
        account_config: AccountConfig,
        rest_config: RestConfig,
    ):
        rest_config.url = "https://fapi.binance.com"
        super().__init__(account_config, rest_config)

    @catch_it
    async def get_exchange_info(self):
        path = "/fapi/v1/exchangeInfo"

        return await self.raw_request("GET", path)

    @catch_it
    async def get_funding_rate(
        self,
        symbol: Optional[str] = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        limit: Optional[int] = 1000,
    ):
        path = "/fapi/v1/fundingRate"
        params = {"symbol": symbol, "limit": limit, "startTime": start_time, "endTime": end_time}

        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def get_leverage_bracket(self, symbol: Optional[str] = None):
        path = "/fapi/v1/leverageBracket"
        params = {"symbol": symbol}

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def get_linear_swap_position(self, symbol: Optional[str] = None):
        path = "/fapi/v2/positionRisk"
        params = {"symbol": symbol}

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def get_price(self, symbol: Optional[str] = None):
        path = "/fapi/v1/ticker/price"
        params = {"symbol": symbol}

        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def get_ticker(self, symbol: Optional[str] = None):
        path = "/fapi/v1/ticker/bookTicker"
        params = {"symbol": symbol}

        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def get_depth(self, symbol: str, limit: Optional[int] = 100):
        path = "/fapi/v1/depth"
        params = {"symbol": symbol, "limit": limit}

        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def get_linear_swap_premium_index(self, symbol: Optional[str] = None):
        path = "/fapi/v1/premiumIndex"
        params = {"symbol": symbol}

        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def get_funding_info(self):
        path = "/fapi/v1/fundingInfo"

        return await self.raw_request("GET", path)

    @catch_it
    async def fapi_v1_listen_key(self):
        path = "/fapi/v1/listenKey"

        return await self.raw_request("POST", path)

    @catch_it
    async def fapi_v1_delay_listen_key(self, listen_key: str):
        path = "/fapi/v1/listenKey"
        params = {"listenKey": listen_key}

        return await self.raw_request("PUT", path, params=params)

    @catch_it
    async def delete_listen_key(self):
        path = "/fapi/v1/listenKey"
        params = {}

        return await self.raw_request("DELETE", path, params=params)

    @catch_it
    async def get_position_side_dual(self):
        path = "/fapi/v1/positionSide/dual"

        return await self.raw_request("GET", path, auth=True)

    @catch_it
    async def fapi_position_side_dual(self, position_side_dual: bool = False):
        path = "/fapi/v1/positionSide/dual"
        params = {"dualSidePosition": position_side_dual}

        return await self.raw_request("POST", path, params=params, auth=True)

    @catch_it
    async def get_fapi_multi_assets_margin(self):
        path = "/fapi/v1/multiAssetsMargin"

        return await self.raw_request("GET", path, auth=True)

    @catch_it
    async def fapi_v1_multi_assets_margin(self, multi_assets_margin: bool = True):
        path = "/fapi/v1/multiAssetsMargin"
        params = {"multiAssetsMargin": multi_assets_margin}

        return await self.raw_request("POST", path, params=params, auth=True)

    @catch_it
    async def set_leverage(self, symbol: str, leverage: int):
        path = "/fapi/v1/leverage"
        params = {"symbol": symbol, "leverage": leverage}

        return await self.raw_request("POST", path, params=params, auth=True)

    @catch_it
    async def fapi_v2_account(self):
        path = "/fapi/v2/account"

        return await self.raw_request("GET", path, auth=True)

    @catch_it
    async def fapi_v3_account(self):
        path = "/fapi/v3/account"

        return await self.raw_request("GET", path, auth=True)

    async def fapi_v2_balance(self):
        path = "/fapi/v2/balance"

        return await self.raw_request("GET", path, auth=True)

    @catch_it
    async def fapi_v1_income(
        self,
        symbol: Optional[str] = None,
        incomeType: Optional[str] = None,
        startTime: Optional[int] = None,
        endTime: Optional[int] = None,
        limit: Optional[int] = 1000,
    ):
        path = "/fapi/v1/income"
        params = {
            "symbol": symbol,
            "incomeType": incomeType,
            "startTime": startTime,
            "endTime": endTime,
            "limit": limit,
        }

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def fapi_v1_allOrders(
        self,
        symbol: Optional[str] = None,
        orderId: Optional[str] = None,
        startTime: Optional[int] = None,
        endTime: Optional[int] = None,
        limit: Optional[int] = 1000,
    ):
        path = "/fapi/v1/allOrders"
        params = {"symbol": symbol, "orderId": orderId, "startTime": startTime, "endTime": endTime, "limit": limit}

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def fapi_v1_userTrades(
        self,
        symbol: Optional[str] = None,
        startTime: Optional[int] = None,
        endTime: Optional[int] = None,
        orderId: Optional[str] = None,
        limit: Optional[int] = 1000,
    ):
        path = "/fapi/v1/userTrades"
        params = {"symbol": symbol, "orderId": orderId, "startTime": startTime, "endTime": endTime, "limit": limit}

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def get_open_orders(self, symbol: Optional[str] = None):
        path = "/fapi/v1/openOrders"
        params = {"symbol": symbol}

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def lps_order(self, symbol: str, side: str, type: str, **kwargs):
        """https://binance-docs.github.io/apidocs/futures/cn/#trade-3

        Args:
            symbol (str): _description_
            side (str): _description_
            type (str): _description_

        Returns:
            _type_: _description_
        """
        path = "/fapi/v1/order"
        params = {"symbol": symbol, "side": side, "type": type, **kwargs}

        return await self.raw_request("POST", path, params=params, auth=True)

    @catch_it
    async def query_order(self, symbol: str, orderId: Optional[int] = None, origClientOrderId: Optional[str] = None):
        path = "/fapi/v1/order"
        params = {
            "symbol": symbol,
            "orderId": orderId,
            "origClientOrderId": origClientOrderId,
        }

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def cancel_order(self, symbol: str, orderId: Optional[str] = None, clientOid: Optional[str] = None):
        path = "/fapi/v1/order"
        params = {"symbol": symbol, "orderId": orderId, "origClientOrderId": clientOid}

        return await self.raw_request("DELETE", path, params=params, auth=True)

    @catch_it
    async def cancel_all_orders(self, symbol: str):
        path = "/fapi/v1/allOpenOrders"
        params = {"symbol": symbol}

        return await self.raw_request("DELETE", path, params=params, auth=True)

    @catch_it
    async def get_history_kline(
        self,
        symbol: str,
        interval: str,
        contract_type: Literal["PERPETUAL", "CURRENT_QUARTER", "NEXT_QUARTER"] = "PERPETUAL",
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        limit: Optional[int] = 1500,
    ):
        path = "/fapi/v1/continuousKlines"
        params = {
            "pair": symbol,
            "contractType": contract_type,
            "interval": interval,
            "startTime": start_time,
            "endTime": end_time,
            "limit": limit,
        }

        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def get_commission_rate(self, symbol: str):
        path = "/fapi/v1/commissionRate"
        params = {"symbol": symbol}

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def get_open_interest(self, symbol: str):
        path = "/fapi/v1/openInterest"
        params = {"symbol": symbol}

        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def get_long_short_ratio(
        self,
        symbol: str,
        period: Literal["5m", "15m", "30m", "1h", "2h", "4h", "6h", "12h", "1d"],
        limit: Optional[int] = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
    ):
        path = "/futures/data/globalLongShortAccountRatio"
        params = {"symbol": symbol, "period": period, "limit": limit, "start_time": start_time, "end_time": end_time}

        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def get_24h_info(self, symbol: Optional[str] = None):
        path = "/fapi/v1/ticker/24hr"
        params = {"symbol": symbol}

        return await self.raw_request("GET", path, params=params)


class BinanceInverseRestClient(BinanceRestClient):
    def __init__(
        self,
        account_config: AccountConfig,
        rest_config: RestConfig,
    ):
        rest_config.url = "https://dapi.binance.com"
        super().__init__(account_config, rest_config)

    @catch_it
    async def get_exchange_info(self):
        path = "/dapi/v1/exchangeInfo"

        return await self.raw_request("GET", path)

    @catch_it
    async def get_history_kline(
        self,
        symbol: str,
        interval: str,
        contract_type: Literal["PERPETUAL", "CURRENT_QUARTER", "NEXT_QUARTER"] = "PERPETUAL",
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        limit: Optional[int] = 1500,
    ):
        path = "/dapi/v1/continuousKlines"
        params = {
            "pair": symbol,
            "contractType": contract_type,
            "interval": interval,
            "startTime": start_time,
            "endTime": end_time,
            "limit": limit,
        }

        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def get_funding_rate(
        self,
        symbol: str,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        limit: Optional[int] = 1000,
    ):
        path = "/dapi/v1/fundingRate"
        params = {"symbol": symbol, "limit": limit, "startTime": start_time, "endTime": end_time}

        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def cancel_all_orders(self, symbol: str):
        path = "/dapi/v1/allOpenOrders"
        params = {"symbol": symbol}

        return await self.raw_request("DELETE", path, params=params, auth=True)

    @catch_it
    async def cancel_order(self, symbol: str, orderId: Optional[str] = None, clientOid: Optional[str] = None):
        path = "/dapi/v1/order"
        params = {"symbol": symbol, "orderId": orderId, "origClientOrderId": clientOid}

        return await self.raw_request("DELETE", path, params=params, auth=True)

    @catch_it
    async def get_commission_rate(self, symbol: str):
        path = "/dapi/v1/commissionRate"
        params = {"symbol": symbol}

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def get_open_interest(self, symbol: str):
        path = "/dapi/v1/openInterest"
        params = {"symbol": symbol}
        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def get_long_short_ratio(
        self,
        pair: str,
        period: Literal["5m", "15m", "30m", "1h", "2h", "4h", "6h", "12h", "1d"],
        limit: Optional[int] = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
    ):
        path = "/futures/data/globalLongShortAccountRatio"
        params = {"pair": pair, "period": period, "limit": limit, "start_time": start_time, "end_time": end_time}
        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def ps_order(self, symbol: str, side: str, type: str, **kwargs):
        """https://binance-docs.github.io/apidocs/futures/cn/#trade-3

        Args:
            symbol (str): _description_
            side (str): _description_
            type (str): _description_

        Returns:
            _type_: _description_
        """
        path = "/dapi/v1/order"
        params = {"symbol": symbol, "side": side, "type": type, **kwargs}

        return await self.raw_request("POST", path, params=params, auth=True)

    @catch_it
    async def dapi_v1_account(self):
        path = "/dapi/v1/account"

        return await self.raw_request("GET", path, auth=True)

    @catch_it
    async def get_price(self, symbol: Optional[str] = None, pair: Optional[str] = None):
        path = "/dapi/v1/ticker/price"
        params = {"symbol": symbol, "pair": pair}

        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def get_ticker(self, symbol: Optional[str] = None, pair: Optional[str] = None):
        path = "/dapi/v1/ticker/bookTicker"
        params = {"symbol": symbol, "pair": pair}

        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def get_depth(self, symbol: str, limit: Optional[int] = 100):
        path = "/dapi/v1/depth"
        params = {"symbol": symbol, "limit": limit}

        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def get_inverse_swap_position(
        self, margin_asset: Optional[str] = None, pair: Optional[str] = None
    ) -> Union[list, dict] | None:
        path = "/dapi/v1/positionRisk"
        params = {"marginAsset": margin_asset, "pair": pair}

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def dapi_v2_balance(self):
        path = "/dapi/v1/balance"

        return await self.raw_request("GET", path, auth=True)

    @catch_it
    async def dapi_v1_userTrades(
        self,
        symbol: Optional[str] = None,
        pair: Optional[str] = None,
        startTime: Optional[int] = None,
        endTime: Optional[int] = None,
        orderId: Optional[str] = None,
        limit: Optional[int] = 100,
    ):
        assert (symbol or pair) and not (symbol and pair), "Either Parameters `symbol` and `pair` is required."
        path = "/dapi/v1/userTrades"
        params = {
            "symbol": symbol,
            "pair": pair,
            "orderId": orderId,
            "startTime": startTime,
            "endTime": endTime,
            "limit": limit,
        }

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def dapi_v1_income(
        self,
        symbol: Optional[str] = None,
        incomeType: Optional[str] = None,
        startTime: Optional[int] = None,
        endTime: Optional[int] = None,
        limit: Optional[int] = 1000,
    ):
        path = "/dapi/v1/income"
        params = {
            "symbol": symbol,
            "incomeType": incomeType,
            "startTime": startTime,
            "endTime": endTime,
            "limit": limit,
        }

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def get_open_orders(self, symbol: Optional[str] = None, pair: Optional[str] = None):
        path = "/dapi/v1/openOrders"
        params = {"symbol": symbol, "pair": pair}

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def dapi_v1_allOrders(
        self,
        symbol: Optional[str] = None,
        pair: Optional[str] = None,
        orderId: Optional[str] = None,
        startTime: Optional[int] = None,
        endTime: Optional[int] = None,
        limit: Optional[int] = 100,
    ):
        assert (symbol or pair) and not (symbol and pair), "Either Parameters `symbol` and `pair` is required."
        path = "/dapi/v1/allOrders"
        params = {
            "symbol": symbol,
            "pair": pair,
            "orderId": orderId,
            "startTime": startTime,
            "endTime": endTime,
            "limit": limit,
        }

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def get_24h_info(self, symbol: Optional[str] = None, pair: Optional[str] = None):
        path = "/dapi/v1/ticker/24hr"
        params = {"symbol": symbol, "pair": pair}

        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def dapi_v1_listen_key(self):
        path = "/dapi/v1/listenKey"

        return await self.raw_request("POST", path)

    @catch_it
    async def dapi_v1_delay_listen_key(self, listen_key: str):
        path = "/dapi/v1/listenKey"
        params = {"listenKey": listen_key}

        return await self.raw_request("PUT", path, params=params)


class BinanceUnifiedRestClient(BinanceRestClient):
    def __init__(
        self,
        account_config: AccountConfig,
        rest_config: RestConfig,
    ):
        rest_config.url = "https://papi.binance.com"
        super().__init__(account_config, rest_config)

    @catch_it
    async def get_account(self, symbol: Optional[str] = None):  # TODO: symbol not needed here
        path = "/papi/v1/account"
        params = {"symbol": symbol}

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def get_position_risk(self, category: Literal["linear", "inverse"], symbol: Optional[str] = None):
        assert category in ("linear", "inverse")
        path = {"linear": "/papi/v1/um/positionRisk", "inverse": "/papi/v1/cm/positionRisk"}[category]
        params = {"symbol": symbol}

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def get_um_position_risk(self, symbol: Optional[str] = None):
        path = "/papi/v1/um/positionRisk"
        params = {"symbol": symbol}

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def get_cm_position_risk(self, margin_asset: Optional[str] = None, pair: Optional[str] = None):
        assert not (margin_asset and pair), "Parameters `marginAsset` and `pair` cannot be provided at the same time"
        path = "/papi/v1/cm/positionRisk"
        params = {"marginAsset": margin_asset, "pair": pair}

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def get_balance(self, asset: Optional[str] = None):
        path = "/papi/v1/balance"
        params = {"asset": asset}

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def get_contract_account(self, category: Literal["linear", "inverse"]):
        assert category in ("linear", "inverse")
        path = {"linear": "/papi/v1/um/account", "inverse": "/papi/v1/cm/account"}[category]

        return await self.raw_request("GET", path, auth=True)

    @catch_it
    async def place_margin_order(
        self,
        symbol: str,
        side: Literal["BUY", "SELL"],
        type: Literal["LIMIT", "MARKET"],
        quantity: Optional[str] = None,
        price: Optional[str] = None,
        newClientOrderId: Optional[str] = None,
        timeInForce: Optional[Literal["GTC", "IOC", "FOK"]] = None,
        **kwargs,
    ):
        path = "/papi/v1/margin/order"
        params = {
            "symbol": symbol,
            "side": side,
            "type": type,
            "quantity": quantity,
            "price": price,
            "newClientOrderId": newClientOrderId,
            "timeInForce": timeInForce,
            **kwargs,
        }

        return await self.raw_request("POST", path, params=params, auth=True)

    @catch_it
    async def place_um_order(
        self,
        symbol: str,
        side: Literal["BUY", "SELL"],
        type: Literal["LIMIT", "MARKET"],
        positionSide: Literal["BOTH", "LONG", "SHORT"] = "BOTH",
        reduceOnly: Optional[bool] = None,
        quantity: Optional[str] = None,
        price: Optional[str] = None,
        newClientOrderId: Optional[str] = None,
        timeInForce: Optional[Literal["GTC", "IOC", "FOK"]] = None,
        **kwargs,
    ):
        path = "/papi/v1/um/order"
        params = {
            "symbol": symbol,
            "side": side,
            "type": type,
            "positionSide": positionSide,
            "reduceOnly": reduceOnly,
            "quantity": quantity,
            "newClientOrderId": newClientOrderId,
            **kwargs,
        }
        if type != "MARKET":
            params["timeInForce"] = timeInForce
            params["price"] = price

        return await self.raw_request("POST", path, params=params, auth=True)

    @catch_it
    async def place_cm_order(
        self,
        symbol: str,
        side: Literal["BUY", "SELL"],
        type: Literal["LIMIT", "MARKET"],
        positionSide: Literal["BOTH", "LONG", "SHORT"] = "BOTH",
        reduceOnly: Optional[bool] = None,
        quantity: Optional[str] = None,
        price: Optional[str] = None,
        newClientOrderId: Optional[str] = None,
        timeInForce: Optional[Literal["GTC", "IOC", "FOK"]] = None,
        **kwargs,
    ):
        path = "/papi/v1/cm/order"
        params = {
            "symbol": symbol,
            "side": side,
            "type": type,
            "positionSide": positionSide,
            "reduceOnly": reduceOnly,
            "quantity": quantity,
            "price": price,
            "newClientOrderId": newClientOrderId,
            "timeInForce": timeInForce,
            **kwargs,
        }

        return await self.raw_request("POST", path, params=params, auth=True)

    @catch_it
    async def get_margin_open_orders(self, symbol: str):
        path = "/papi/v1/margin/openOrders"
        params = {"symbol": symbol}

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def get_um_open_orders(self, symbol: str):
        path = "/papi/v1/um/openOrders"
        params = {"symbol": symbol}

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def get_cm_open_orders(self, symbol: Optional[str] = None, pair: Optional[str] = None):
        path = "/papi/v1/cm/openOrders"
        params = {"symbol": symbol, "pair": pair}

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def cancel_margin_order(self, symbol: str, orderId: Optional[int] = None, clientOid: Optional[str] = None):
        path = "/papi/v1/margin/order"
        params = {"symbol": symbol, "orderId": orderId, "origClientOrderId": clientOid}

        return await self.raw_request("DELETE", path, params=params, auth=True)

    @catch_it
    async def cancel_um_order(self, symbol: str, orderId: Optional[int] = None, clientOid: Optional[str] = None):
        path = "/papi/v1/um/order"
        params = {"symbol": symbol, "orderId": orderId, "origClientOrderId": clientOid}

        return await self.raw_request("DELETE", path, params=params, auth=True)

    @catch_it
    async def cancel_cm_order(self, symbol: str, orderId: Optional[int] = None, clientOid: Optional[str] = None):
        path = "/papi/v1/cm/order"
        params = {"symbol": symbol, "orderId": orderId, "origClientOrderId": clientOid}

        return await self.raw_request("DELETE", path, params=params, auth=True)

    @catch_it
    async def papi_v1_um_income(
        self,
        symbol: Optional[str] = None,
        incomeType: Optional[str] = None,
        startTime: Optional[int] = None,
        endTime: Optional[int] = None,
        limit: Optional[int] = 100,
    ):
        path = "/papi/v1/um/income"
        params = {
            "symbol": symbol,
            "incomeType": incomeType,
            "startTime": startTime,
            "endTime": endTime,
            "limit": limit,
        }

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def papi_v1_cm_income(
        self,
        symbol: Optional[str] = None,
        incomeType: Optional[str] = None,
        startTime: Optional[int] = None,
        endTime: Optional[int] = None,
        limit: Optional[int] = 100,
    ):
        path = "/papi/v1/cm/income"
        params = {
            "symbol": symbol,
            "incomeType": incomeType,
            "startTime": startTime,
            "endTime": endTime,
            "limit": limit,
        }

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def papi_v1_um_userTrades(
        self,
        symbol: str,
        startTime: Optional[int] = None,
        endTime: Optional[int] = None,
        orderId: Optional[str] = None,
        limit: Optional[int] = 1000,
    ):
        path = "/papi/v1/um/userTrades"
        params = {"symbol": symbol, "orderId": orderId, "startTime": startTime, "endTime": endTime, "limit": limit}

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def papi_v1_margin_myTrades(
        self,
        symbol: str,
        startTime: Optional[int] = None,
        endTime: Optional[int] = None,
        from_id: Optional[int] = None,
        limit: Optional[int] = None,
        order_id: Optional[int] = None,
    ):
        path = "/papi/v1/margin/myTrades"
        params = {
            "symbol": symbol,
            "startTime": startTime,
            "endTime": endTime,
            "limit": limit,
            "fromId": from_id,
            "orderId": order_id,
        }

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def papi_v1_cm_userTrades(
        self,
        symbol: Optional[str] = None,
        pair: Optional[str] = None,
        startTime: Optional[int] = None,
        endTime: Optional[int] = None,
        limit: Optional[int] = 1000,
    ):
        assert (symbol or pair) and not (symbol and pair), "Either Parameters `symbol` and `pair` is required."
        path = "/papi/v1/cm/userTrades"
        params = {"symbol": symbol, "pair": pair, "startTime": startTime, "endTime": endTime, "limit": limit}

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def papi_v1_um_allOrders(
        self,
        symbol: str,
        orderId: Optional[str] = None,
        startTime: Optional[int] = None,
        endTime: Optional[int] = None,
        limit: Optional[int] = 1000,
    ):
        path = "/papi/v1/um/allOrders"
        params = {"symbol": symbol, "orderId": orderId, "startTime": startTime, "endTime": endTime, "limit": limit}

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def papi_v1_cm_allOrders(
        self,
        symbol: Optional[str] = None,
        pair: Optional[str] = None,
        orderId: Optional[str] = None,
        startTime: Optional[int] = None,
        endTime: Optional[int] = None,
        limit: Optional[int] = 100,
    ):
        assert (symbol or pair) and not (symbol and pair), "Either Parameters `symbol` and `pair` is required."
        path = "/papi/v1/cm/allOrders"
        params = {
            "symbol": symbol,
            "pair": pair,
            "orderId": orderId,
            "startTime": startTime,
            "endTime": endTime,
            "limit": limit,
        }

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def papi_v1_margin_allOrders(
        self,
        symbol: str,
        orderId: Optional[str] = None,
        startTime: Optional[int] = None,
        endTime: Optional[int] = None,
        limit: Optional[int] = 500,
    ):
        path = "/papi/v1/margin/allOrders"
        params = {"symbol": symbol, "orderId": orderId, "startTime": startTime, "endTime": endTime, "limit": limit}

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def get_interest_history(
        self,
        asset: Optional[str] = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        current: Optional[int] = 1,
        size: Optional[int] = 100,
    ):
        path = "/papi/v1/margin/marginInterestHistory"
        params = {"asset": asset, "startTime": start_time, "endTime": end_time, "current": current, "size": size}

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def papi_v1_um_commission_rate(self, symbol: str):
        path = "/papi/v1/um/commissionRate"
        params = {"symbol": symbol}

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def papi_v1_cm_commission_rate(self, symbol: str):
        path = "/papi/v1/cm/commissionRate"
        params = {"symbol": symbol}

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def papi_v1_listen_key(self):
        path = "/papi/v1/listenKey"

        return await self.raw_request("POST", path)

    @catch_it
    async def papi_v1_delay_listen_key(self):
        path = "/papi/v1/listenKey"

        return await self.raw_request("PUT", path)

    @catch_it
    async def papi_auto_collection(self):
        path = "/papi/v1/auto-collection"

        return await self.raw_request("POST", path, auth=True)

    @catch_it
    async def papi_bnb_transfer(self, amount: Union[str, Decimal], transfer_side: Literal["TO_UM", "FROM_UM"]):
        path = "/papi/v1/bnb-transfer"
        params = {
            "amount": str(amount),
            "transferSide": transfer_side,
        }

        return await self.raw_request("POST", path, params=params, auth=True)

    @catch_it
    async def papi_repay_futures_negative_balance(self):
        path = "/papi/v1/repay-futures-negative-balance"

        return await self.raw_request("POST", path, auth=True)

    @catch_it
    async def change_repay_type(self, auto_repay: bool):
        path = "/papi/v1/repay-futures-switch"
        if auto_repay:
            params = {"autoRepay": "true"}
        else:
            params = {"autoRepay": "false"}

        return await self.raw_request("POST", path, params=params, auth=True)

    @catch_it
    async def query_repay_type(self):
        path = "/papi/v1/repay-futures-switch"

        return await self.raw_request("GET", path, auth=True)

    @catch_it
    async def get_collateral_rate(self):
        url = f"https://www.binance.com/bapi/margin/v1/public/margin/portfolio/collateral-rate"

        return await self.http_sess.request("GET", url)
