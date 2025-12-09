import hmac
import time
import json
import uuid
from typing import Optional, Any, Literal
from urllib.parse import urlencode

from ..data_type import AccountConfig, RestConfig
from ..base_client import BaseRestClient, catch_it
from ..utils import clean_none_value


class BybitRestClient(BaseRestClient):
    def __init__(
        self,
        account_config: AccountConfig,
        rest_config: RestConfig,
    ):
        rest_config.url = "https://api.bybit.com"
        super().__init__(account_config, rest_config)

    def _gen_header(self, http_method, url_path, params=None, payload_string=None):
        if not self.secret_key:
            raise ValueError("secret_key is required")

        timestamp = str(int(time.time() * 1000))
        recv_windows = str(5000)
        query_string = urlencode(params or {})
        payload_string = payload_string or ""
        s = f"{timestamp}{self.api_key}{recv_windows}{query_string}{payload_string}"
        hash_value = hmac.new(
            bytes(self.secret_key, "utf-8"),
            s.encode("utf-8"),
            digestmod="sha256",
        )

        signature = str(hash_value.hexdigest())
        headers = {
            "X-BAPI-API-KEY": self.api_key,
            "X-BAPI-SIGN": signature,
            "X-BAPI-SIGN-TYPE": "2",
            "X-BAPI-TIMESTAMP": timestamp,
            "X-BAPI-RECV-WINDOW": recv_windows,
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
        if auth:
            headers = self._gen_header(method, url, params, payload_string)

        return url, headers, params, payload_string

    """
    ###############
    # market data #
    ###############
    """

    @catch_it
    async def get_market_kline(
        self,
        category: Literal["spot", "linear", "inverse"],
        symbol: str,
        interval: Literal["1", "3", "5", "15", "30", "60", "120", "240", "360", "720", "D", "M", "W"],
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        limit: Optional[int] = None,
    ):
        path = "/v5/market/kline"
        params = {
            "category": category,
            "symbol": symbol,
            "interval": interval,
            "start": start_time,
            "end": end_time,
            "limit": limit,
        }

        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def get_market_tickers(
        self,
        category: Literal["spot", "linear", "inverse", "option"],
        symbol: Optional[str] = None,
        base_coin: Optional[str] = None,
        exp_date: Optional[str] = None,
    ):
        path = "/v5/market/tickers"
        params = {
            "category": category,
            "symbol": symbol,
            "baseCoin": base_coin,
            "expDate": exp_date,
        }

        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def get_orderbook(
        self, category: Literal["spot", "linear", "inverse", "option"], symbol: str, limit: int = 100
    ):
        path = "/v5/market/orderbook"
        params = {"symbol": symbol, "limit": limit, "category": category}

        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def get_announcement(
        self,
        locale: str,
        anno_type: Optional[str] = None,
        tag: Optional[str] = None,
        page: Optional[int] = None,
        limit: Optional[int] = None,
    ):
        path = "/v5/announcements/index"
        params = {"locale": locale, "type": anno_type, "tag": tag, "limit": limit, "page": page}

        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def get_premium_index(
        self,
        category: Literal["linear"],
        symbol: str,
        interval: Literal["1", "3", "5", "15", "30", "60", "120", "240", "360", "720", "D", "M", "W"],
        start: Optional[int] = None,
        end: Optional[int] = None,
        limit: Optional[int] = None,
    ):
        path = "/v5/market/premium-index-price-kline"
        params = {
            "category": category,
            "symbol": symbol,
            "interval": interval,
            "start": start,
            "end": end,
            "limit": limit,
        }

        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def get_instrument_info(
        self,
        category: Literal["spot", "linear", "inverse", "option"],
        symbol: Optional[str] = None,
        base_coin: Optional[str] = None,
        limit: Optional[int] = 1000,
        cursor: Optional[str] = None,
    ):
        path = "/v5/market/instruments-info"
        params = {
            "category": category,
            "symbol": symbol,
            "baseCoin": base_coin,
            "limit": limit,
            "cursor": cursor,
        }

        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def get_history_funding_rate(
        self,
        category: Literal["linear", "inverse"],
        symbol: str,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        limit: int = 200,
    ):
        path = "/v5/market/funding/history"
        params = {
            "category": category,
            "symbol": symbol,
            "startTime": start_time,
            "endTime": end_time,
            "limit": limit,
        }

        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def get_current_funding_rate(
        self, category: Literal["spot", "linear", "inverse", "option"], symbol: str | None = None
    ):
        path = "/v5/market/tickers"
        params = {
            "category": category,
            "symbol": symbol,
        }

        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def get_open_interest(
        self,
        category: Literal["linear", "inverse"],
        symbol: str,
        interval_time: Literal["5min", "15min", "30min", "1h", "4h", "1d"],
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        limit: Optional[int] = None,
        cursor: Optional[str] = None,
    ):
        path = "/v5/market/open-interest"
        params = {
            "category": category,
            "symbol": symbol,
            "intervalTime": interval_time,
            "start": start_time,
            "end": end_time,
            "limit": limit,
            "cursor": cursor,
        }

        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def get_market_risk_limit(self, category: Literal["linear", "inverse"], symbol: Optional[str] = None):
        path = "/v5/market/risk-limit"
        params = {"category": category, "symbol": symbol}

        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def get_cross_margin_pledge_info(self, coin: Optional[str] = None):
        path = "/v5/spot-cross-margin-trade/pledge-token"
        params = {"coin": coin}

        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def get_cross_margin_borrow_info(self, coin: Optional[str] = None):
        path = "/v5/spot-cross-margin-trade/borrow-token"
        params = {"coin": coin}

        return await self.raw_request("GET", path, params=params)

    """
    ################
    # account data #
    ################
    """

    @catch_it
    async def get_open_orders(
        self,
        category: Literal["spot", "linear", "inverse", "option"],
        symbol: Optional[str] = None,
        base_coin: Optional[str] = None,
        settle_coin: Optional[str] = None,
        order_id: Optional[str] = None,
        client_order_id: Optional[str] = None,
        limit: Optional[int] = 50,
        cursor: Optional[str] = None,
    ):
        if category == "linear" and (symbol is None and settle_coin is None):
            settle_coin = "USDT"
        path = "/v5/order/realtime"
        params = {
            "category": category,
            "symbol": symbol,
            "baseCoin": base_coin,
            "settleCoin": settle_coin,
            "order_id": order_id,
            "orderLinkId": client_order_id,
            "limit": limit,
            "cursor": cursor,
        }

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def get_balance(self, account_type: Literal["UNIFIED", "CONTRACT", "SPOT"], coin: Optional[str] = None):
        """
        :param account_type: UNIFIED or CONTRACT
        :param coin: optional, name of ccy
        :return:
        """
        path = "/v5/account/wallet-balance"
        params = {"accountType": account_type, "coin": coin}

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def get_cross_margin_loan_info(self, coin: str):
        path = "/v5/spot-cross-margin-trade/loan-info"
        params = {"coin": coin}

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def get_position(
        self,
        category: Literal["linear", "inverse"],
        symbol: Optional[str] = None,
        base_coin: Optional[str] = None,
        settle_coin: Optional[str] = None,
        limit: int = 200,
        cursor: Optional[str] = None,
    ):
        if not symbol and not settle_coin:
            if category == "inverse":
                settle_coin = None
            else:
                settle_coin = "USDT"

        path = "/v5/position/list"
        params = {
            "category": category,
            "symbol": symbol,
            "baseCoin": base_coin,
            "settleCoin": settle_coin,
            "limit": limit,
            "cursor": cursor,
        }

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def get_account_config(self):
        path = "/v5/account/info"

        return await self.raw_request("GET", path, auth=True)

    @catch_it
    async def get_borrow_history(
        self,
        currency: Optional[str] = None,
        startTime: Optional[int] = None,
        endTime: Optional[int] = None,
        limit: Optional[int] = None,
        cursor: Optional[str] = None,
    ):
        path = "/v5/account/borrow-history"
        params = {
            "currency": currency,
            "startTime": startTime,
            "endTime": endTime,
            "limit": limit,
            "cursor": cursor,
        }

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def execution_list(
        self,
        category: Literal["spot", "linear", "inverse", "option"],
        symbol: Optional[str] = None,
        orderId: Optional[str] = None,
        **kwargs,
    ):
        path = "/v5/execution/list"
        params = {"category": category, "symbol": symbol, "orderId": orderId, **kwargs}

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def get_order_history(
        self,
        category: Literal["spot", "linear", "inverse", "option"],
        symbol: Optional[str] = None,
        orderId: Optional[str] = None,
        orderStatus: Optional[str] = None,
        **kwargs,
    ):
        path = "/v5/order/history"
        params = {
            "category": category,
            "symbol": symbol,
            "orderId": orderId,
            "orderStatus": orderStatus,
            **kwargs,
        }

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def get_collateral_info(self, currency: Optional[str] = None):
        path = "/v5/account/collateral-info"
        params = {
            "currency": currency.upper() if currency else None,
        }

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def set_collateral_switch(self, coin: str, open: bool):
        path = "/v5/account/set-collateral-switch"
        params = {"switch": "ON" if open else "OFF", "coin": coin}

        return await self.raw_request("POST", path, auth=True, payload=params)

    @catch_it
    async def get_lending_info(self, coin: Optional[str] = None):
        path = "/v5/lending/info"
        params = {"coin": coin}

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def get_cross_margin_account_info(self):
        path = "/v5/spot-cross-margin-trade/account"

        return await self.raw_request("GET", path, auth=True)

    @catch_it
    async def get_cross_margin_orders(
        self,
        status: Literal[0, 1, 2] = 0,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        coin: Optional[str] = None,
        limit: Optional[int] = 500,
    ):
        path = "/v5/spot-cross-margin-trade/orders"
        params = {
            "status": status,
            "startTime": start_time,
            "endTime": end_time,
            "coin": coin,
            "limit": limit,
        }

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def cross_margin_loan(self, coin: str, qty: str):
        path = "/v5/spot-cross-margin-trade/loan"
        params = {"coin": coin, "qty": qty}

        return await self.raw_request("POST", path, auth=True, payload=params)

    @catch_it
    async def cross_margin_repay(self, coin: str, qty: Optional[str] = None, complete: Optional[int] = None):
        path = "/v5/spot-cross-margin-trade/repay"
        params = {"coin": coin, "qty": qty, "completeRepayment": complete}

        return await self.raw_request("POST", path, auth=True, payload=params)

    @catch_it
    async def cross_margin_switch(self, switch: int):
        path = "/v5/spot-cross-margin-trade/switch"
        params = {"switch": switch}

        return await self.raw_request("POST", path, auth=True, payload=params)

    @catch_it
    async def uta_spot_margin_switch(self, spotMarginMode: str):
        path = "/v5/spot-margin-trade/switch-mode"
        params = {"spotMarginMode": spotMarginMode}

        return await self.raw_request("POST", path, auth=True, payload=params)

    @catch_it
    async def uta_spot_margin_leverage(self, leverage: str):
        path = "/v5/spot-margin-trade/set-leverage"
        params = {"leverage": leverage}

        return await self.raw_request("POST", path, auth=True, payload=params)

    @catch_it
    async def set_leverage(
        self, category: Literal["linear", "inverse"], symbol: str, buy_leverage: str, sell_leverage: str
    ):
        path = "/v5/position/set-leverage"
        params = {
            "category": category,
            "symbol": symbol,
            "buyLeverage": str(buy_leverage),
            "sellLeverage": str(sell_leverage),
        }

        return await self.raw_request("POST", path, auth=True, payload=params)

    @catch_it
    async def set_risk_limit(
        self,
        category: Literal["linear", "inverse"],
        symbol: str,
        risk_id: int,
        position_idx: Optional[Literal[0, 1, 2]] = None,
    ):
        path = "/v5/position/set-risk-limit"
        params = {
            "category": category,
            "symbol": symbol,
            "riskId": risk_id,
            "positionIdx": position_idx,
        }

        return await self.raw_request("POST", path, auth=True, payload=params)

    @catch_it
    async def set_margin_mode(self, mode: Literal["REGULAR_MARGIN", "ISOLATED_MARGIN", "PORTFOLIO_MARGIN"]):
        """
        :param mode: margin mode, REGULAR_MARGIN or PORTFOLIO_MARGIN
        :return:
        """
        path = "/v5/account/set-margin-mode"
        params = {"setMarginMode": mode}

        return await self.raw_request("POST", path, auth=True, payload=params)

    @catch_it
    async def upgrade_to_uta(self):
        """ """
        path = "/v5/account/upgrade-to-uta"

        return await self.raw_request("POST", path, auth=True)

    @catch_it
    async def v5_order_create(
        self,
        category: Literal["spot", "linear", "inverse", "option"],
        symbol: str,
        side: Literal["Buy", "Sell"],
        orderType: Literal["Market", "Limit"],
        qty: str,
        price: Optional[str] = None,
        **kwargs,
    ):
        """
        qty:訂單數量. 若category=spot, 且是Market Buy單, 則qty表示為報價幣種金額
        """
        path = "/v5/order/create"
        params = {
            "category": category,
            "symbol": symbol,
            "side": side,
            "orderType": orderType,
            "qty": qty,
            "price": price,
            **kwargs,
        }

        return await self.raw_request("POST", path, auth=True, payload=params)

    @catch_it
    async def ensure_tokens_convert(self, product_id: Optional[str] = ""):
        """
        查询保证金币种信息
        """
        path = "/v5/ins-loan/ensure-tokens-convert"
        params = {"productId": product_id}

        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def cancel_order(
        self,
        category: Literal["spot", "linear", "inverse", "option"],
        symbol: str,
        orderId: Optional[str] = None,
        clientOid: Optional[str] = None,
    ):
        """
        撤单
        """
        path = "/v5/order/cancel"
        params = {"category": category, "symbol": symbol, "orderId": orderId, "orderLinkId": clientOid}

        return await self.raw_request("POST", path, auth=True, payload=params)

    @catch_it
    async def cancel_all_orders(
        self,
        category: Literal["spot", "linear", "inverse", "option"],
        symbol: Optional[str] = None,
        base_coin: Optional[str] = None,
        settle_coin: Optional[str] = None,
    ):
        """
        撤销全部订单
        """
        path = "/v5/order/cancel-all"
        params = {"category": category, "symbol": symbol, "baseCoin": base_coin, "settleCoin": settle_coin}

        return await self.raw_request("POST", path, auth=True, payload=params)

    @catch_it
    async def get_commission_rate(self, category: Literal["spot", "linear", "inverse", "option"], symbol: str):
        path = "/v5/account/fee-rate"
        params = {"symbol": symbol, "category": category}

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def get_long_short_ratio(
        self,
        category: Literal["linear", "inverse"],
        symbol: str,
        period: Literal["5min", "15min", "30min", "1h", "4h", "1d"],
        limit: Optional[int] = None,
    ):
        path = "/v5/market/account-ratio"
        params = {"category": category, "symbol": symbol, "period": period, "limit": limit}

        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def inter_transfer(
        self,
        from_acct_type: Literal["FUND", "CONTRACT", "UNIFIED"],
        to_acct_type: Literal["FUND", "CONTRACT", "UNIFIED"],
        ccy: str,
        amount: str,
    ):
        path = "/v5/asset/transfer/inter-transfer"
        params = {
            "transferId": str(uuid.uuid1()),
            "coin": ccy,
            "amount": amount,
            "fromAccountType": from_acct_type,
            "toAccountType": to_acct_type,
        }

        return await self.raw_request("POST", path=path, auth=True, payload=params)

    @catch_it
    async def subaccount_transfer(
        self,
        fromAccountType: Literal["FUND", "CONTRACT", "UNIFIED"],
        toAccountType: Literal["FUND", "CONTRACT", "UNIFIED"],
        coin: str,
        amount: str,
        fromMemberId: int,
        toMemberId: int,
    ):
        path = "/v5/asset/transfer/universal-transfer"
        params = {
            "transferId": str(uuid.uuid1()),
            "coin": coin,
            "amount": amount,
            "fromAccountType": fromAccountType,
            "toAccountType": toAccountType,
            "fromMemberId": fromMemberId,
            "toMemberId": toMemberId,
        }
        return await self.raw_request("POST", path=path, auth=True, payload=params)

    @catch_it
    async def withdraw(
        self,
        address: str,
        amount: str,
        coin: str,
        vaspEntityId: str,
        chain: Optional[str] = None,
        forceChain: Literal[0, 1, 2] = 0,
        clientOid: Optional[str] = None,
    ):
        """
        vaspEntityId: 接收方交易所id, 可用接口`/v5/asset/withdraw/vasp/list`查询
                    當提現至Upbit或者不在該列表內的平台時, 請使用vaspEntityId="others"
        """
        path = "/v5/asset/withdraw/create"
        params = {
            "coin": coin,
            "forceChain": forceChain,
            "address": address,
            "chain": chain,
            "amount": amount,
            "requestId": clientOid,
            "vaspEntityId": vaspEntityId,
            "timestamp": int(time.time() * 1000),
        }
        return await self.raw_request("POST", path, payload=params, auth=True)

    @catch_it
    async def withdraw_records(
        self,
        startTime: Optional[int] = None,
        endTime: Optional[int] = None,
        coin: Optional[str] = None,
        withdrawID: Optional[str] = None,
        txID: Optional[str] = None,
        limit: Optional[int] = 50,
        cursor: Optional[str] = None,
    ):
        path = "/v5/asset/withdraw/query-record"
        params = {
            "coin": coin,
            "withdrawID": withdrawID,
            "txID": txID,
            "startTime": startTime,
            "endTime": endTime,
            "limit": limit,
            "cursor": cursor,
        }
        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def get_subaccount_info(self):
        path = "/v5/user/query-sub-members"
        return await self.raw_request("GET", path, auth=True)

    @catch_it
    async def get_subaccount_assets(
        self,
        memberId: str,
        accountType: Literal["UNIFIED", "CONTRACT", "FUND", "SPOT"],
        coin: Optional[str] = None,
    ):
        path = "/v5/asset/transfer/query-account-coins-balance"
        if accountType == "UNIFIED" and not coin:
            raise ValueError("param `coin` is needed when accountType==UNIFIED")
        params = {
            "memberId": memberId,
            "accountType": accountType,
            "coin": coin,
        }
        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def get_vip_level(self):
        path = "/v5/user/query-api"

        return await self.raw_request("GET", path, auth=True)

    @catch_it
    async def get_ltv(self):
        # 获取风险率
        path = "/v5/ins-loan/ltv-convert"

        return await self.raw_request("GET", path, auth=True)
    
    @catch_it
    async def get_loanable_data(self, currency: str | None = None, vipLevel: str | None = None):
        path = "/v5/crypto-loan-common/loanable-data"
        params = {"currency": currency, "vipLevel": vipLevel}
        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def get_margin_trade_data(self, currency: str | None = None, vipLevel: str | None = None):
        path = "/v5/spot-margin-trade/data"
        params = {"vipLevel": vipLevel, "currency": currency}
        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def get_margin_interest_history(
        self,
        currency: str,
        vipLevel: str | None = None,
        startTime: int | None = None,
        endTime: int | None = None
    ):
        # 您可以查詢最多過去6個月的借貸利率數據
        # 請注意對於"No VIP", 需要傳入"No%20VIP"（实际应传入'No VIP'）, 若不傳, 則返回匹配您帳戶等級的數據
        path = "/v5/spot-margin-trade/interest-rate-history"
        params = {"currency": currency, "vipLevel": vipLevel, "startTime": startTime, "endTime": endTime}
        return await self.raw_request("GET", path, params=params, auth=True)
