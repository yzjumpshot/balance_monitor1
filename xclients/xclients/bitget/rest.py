import base64
import hmac
import json
import time
from typing import Any, Literal, Optional
from urllib.parse import urlencode

from ..base_client import BaseRestClient, catch_it
from ..utils import clean_none_value
from ..data_type import AccountConfig, RestConfig


class BitgetRestClient(BaseRestClient):
    def __init__(
        self,
        account_config: AccountConfig,
        rest_config: RestConfig,
    ):
        rest_config.url = "https://api.bitget.com"
        super().__init__(account_config, rest_config)

    def get_timestamp(self):
        return int(time.time() * 1000)

    def _sign(self, message):
        if not self.secret_key:
            raise ValueError("secret_key is required for signing")
        mac = hmac.new(bytes(self.secret_key, encoding="utf8"), bytes(message, encoding="utf-8"), digestmod="sha256")
        d = mac.digest()
        return base64.b64encode(d).decode("ascii")

    def _sort_params(self, params):
        params = [(key, val) for key, val in params.items() if val != None]
        params.sort(key=lambda x: x[0])
        return dict(params)

    def _gen_header(self, http_method, url_path, params=None, payload_string=None):
        if not self.secret_key:
            raise ValueError("secret_key is required")

        timestamp = str(int(time.time() * 1000))
        query_string = urlencode(params or {})  # actually also can use
        payload_string = payload_string or ""
        if query_string:
            s = f"{timestamp}{http_method.upper()}{url_path}?{query_string}{payload_string}"
        else:
            s = f"{timestamp}{http_method.upper()}{url_path}{payload_string}"
        signature = self._sign(s)

        headers = {
            "ACCESS-KEY": self.api_key,
            "ACCESS-SIGN": signature,
            "ACCESS-TIMESTAMP": timestamp,
            "ACCESS-PASSPHRASE": self.passphrase,
            "Content-Type": "application/json",
            "locale": "en-US",
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
        params = self._sort_params(params or {})
        payload = self._sort_params(payload or {})
        payload_string = json.dumps(payload) if payload else ""
        if auth:
            headers = self._gen_header(method, path, params, payload_string)

        return url, headers, params, payload_string

    @catch_it
    async def get_commission_rate(self, symbol: str, businessType: Literal["mix", "spot", "margin"] = "mix"):
        path = "/api/v2/common/trade-rate"
        params = {"symbol": symbol, "businessType": businessType}

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def get_all_account_balance(self):
        path = "/api/v2/account/all-account-balance"

        return await self.raw_request("GET", path, auth=True)


class BitgetSpotRestClient(BitgetRestClient):
    def __init__(
        self,
        account_config: AccountConfig,
        rest_config: RestConfig,
    ):
        rest_config.url = "https://api.bitget.com"
        super().__init__(account_config, rest_config)

    """
    ###############
    # market data #
    ###############
    """

    @catch_it
    async def get_symbols(self, symbol: Optional[str] = None):
        path = "/api/v2/spot/public/symbols"
        params = {"symbol": symbol}

        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def get_tickers(self, symbol: Optional[str] = None):
        path = "/api/v2/spot/market/tickers"
        params = {"symbol": symbol}

        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def get_orderbook(self, symbol: str, limit: int = 100):
        path = "/api/v2/spot/market/orderbook"
        params = {"symbol": symbol, "limit": limit}

        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def get_history_kline(
        self,
        symbol: str,
        granularity: str,
        endTime: Optional[str] = None,
        limit: Optional[str] = None,
    ):
        path = "/api/v2/spot/market/history-candles"
        params = {
            "symbol": symbol,
            "granularity": granularity,
            "endTime": endTime,
            "limit": limit,
        }
        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def get_loan_interest(
        self,
        loanCoin: str,
        pledgeCoin: str = "BTC",  # 质押币种
        daily: Literal[
            "SEVEN", "THIRTY", "FLEXIBLE"
        ] = "FLEXIBLE",  # 质押天数 SEVEN: 7天, THIRTY: 30天, FLEXIBLE: 活期
        pledgeAmount: str = "1",  # 质押数量
    ):
        # 网页上的数据区分有vip等级，是在VIP0基础上折扣，如需其它vip等级可通过网页接口getCoinInfoListV1--> loanVipRights查询折扣率
        path = "/api/v2/earn/loan/public/hour-interest"
        params = {"loanCoin": loanCoin, "pledgeCoin": pledgeCoin, "daily": daily, "pledgeAmount": pledgeAmount}
        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def get_loan_interest_history(self, loanCoin: str):
        """
        https://www.bitget.com/zh-CN/earning/loan/material
        getInterestRateHistoryList 最大可获取近一月活期历史数据
        """
        coin_path = "https://www.bitget.com/v1/spot/public/loan/getCoinInfoListV1"
        coin_resp = await self.http_sess.request("GET", coin_path)
        if not (isinstance(coin_resp, dict) and coin_resp.get("code") == "200"):
            raise ValueError(f"get coin_info unexpected response[{coin_resp}]")
        coin_id = None
        for info in coin_resp.get("data", {}).get("item", {}).get("loanList", []):
            coin_name = info["coinName"]
            if coin_name == loanCoin:
                coin_id = info["coinId"]
                break
        if not coin_id:
            raise ValueError("get coin_id by request `CoinInfoList` failed")
        path = f"https://www.bitget.com/v1/spot/public/loan/getInterestRateHistoryList?coinId={coin_id}"
        resp = await self.http_sess.request("GET", path)
        return resp

    @catch_it
    async def get_margin_interest_rate_cur(self, coin: str):
        path = "/api/v2/margin/interest-rate-record"
        return await self.raw_request("GET", path, params={"coin": coin}, auth=True)

    """
    ###############
    #    Trade    #
    ###############
    """

    @catch_it
    async def spot_order(
        self,
        symbol: str,
        side: Literal["buy", "sell"],
        orderType: Literal["limit", "market"],
        force: Literal["gtc", "post_only", "fok", "ioc"],
        size: str,
        price: Optional[str] = None,
        clientOid: Optional[str] = None,
        **kwargs,
    ):
        path = "/api/v2/spot/trade/place-order"
        params = clean_none_value(
            {
                "symbol": symbol,
                "side": side,
                "orderType": orderType,
                "force": force,
                "size": str(size),
                "price": str(price) if price is not None else None,
                "clientOid": str(clientOid) if clientOid is not None else None,
                **kwargs,
            }
        )
        return await self.raw_request("POST", path, payload=params, auth=True)

    @catch_it
    async def get_open_orders(self, symbol: Optional[str] = None):
        path = "/api/v2/spot/trade/unfilled-orders"
        params = {"symbol": symbol}
        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def cancel_order(self, symbol: str, orderId: Optional[str] = None, clientOid: Optional[str] = None):
        assert orderId or clientOid, "Either Parameters `orderId` or `clientOid` is needed"
        path = "/api/v2/spot/trade/cancel-order"
        params = {"symbol": symbol, "orderId": orderId, "clientOid": clientOid}
        return await self.raw_request("POST", path, payload=params, auth=True)

    @catch_it
    async def get_order_history(
        self,
        symbol: Optional[str],
        startTime: Optional[str] = None,
        endTime: Optional[str] = None,
        limit: Optional[str] = "100",
        idLessThan: Optional[str] = None,
        **kwargs,
    ):
        path = "/api/v2/spot/trade/history-orders"
        params = {
            "symbol": symbol,
            "startTime": startTime,
            "endTime": endTime,
            "limit": limit,
            "idLessThan": idLessThan,
            **kwargs,
        }
        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def get_fills_history(
        self,
        symbol: str,
        limit: int = 100,
        idLessThan: Optional[str] = None,
        startTime: Optional[str] = None,
        endTime: Optional[str] = None,
        orderId: Optional[str] = None,
    ):
        path = "/api/v2/spot/trade/fills"
        params = {
            "symbol": symbol,
            "orderId": orderId,
            "limit": limit,
            "idLessThan": idLessThan,
            "startTime": startTime,
            "endTime": endTime,
        }

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def query_spot_order(
        self,
        orderId: Optional[str] = None,
        clientOid: Optional[str] = None,
        requestTime: Optional[int] = None,
        receiveWindow: Optional[int] = None,
    ):
        assert orderId or clientOid

        path = "/api/v2/spot/trade/orderInfo"
        params = {
            "orderId": orderId,
            "clientOid": clientOid,
            "requestTime": requestTime,
            "receiveWindow": receiveWindow,
        }

        return await self.raw_request("GET", path, params=params, auth=True)

    """
    ###############
    #   Account   #
    ###############
    """

    @catch_it
    async def get_assets(self, coin: Optional[str] = None, assetType: Literal["hold_only", "any"] = "hold_only"):
        path = "/api/v2/spot/account/assets"
        params = {"coin": coin, "assetType": assetType}

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def transfer(
        self,
        fromType: Literal["spot", "usdt_futures", "crossed_margin", "coin_futures"],
        toType: Literal["spot", "usdt_futures", "crossed_margin", "coin_futures"],
        amount: str,
        coin: str,
        symbol: Optional[str] = None,
        clientOid: Optional[str] = None,
    ):
        path = "/api/v2/spot/wallet/transfer"
        params = {
            "fromType": fromType,
            "toType": toType,
            "amount": amount,
            "coin": coin,
            "symbol": symbol,
            "clientOid": clientOid,
        }

        return await self.raw_request("POST", path, payload=params, auth=True)

    @catch_it
    async def subaccount_transfer(
        self,
        fromType: str,
        toType: str,
        fromUserId: str,
        toUserId: str,
        amount: str,
        coin: str,
        symbol: Optional[str] = None,
        clientOid: Optional[str] = None,
    ):
        path = "/api/v2/spot/wallet/subaccount-transfer"
        params = {
            "fromType": fromType,
            "toType": toType,
            "amount": amount,
            "coin": coin,
            "symbol": symbol,
            "clientOid": clientOid,
            "fromUserId": fromUserId,
            "toUserId": toUserId,
        }
        return await self.raw_request("POST", path, payload=params, auth=True)

    @catch_it
    async def withdraw(
        self,
        transferType: Literal["on_chain", "internal_transfer"],
        address: str,
        size: str,
        coin: str,
        chain: Optional[str] = None,
        clientOid: Optional[str] = None,
        innerToType: Literal["email", "mobile", "uid"] = "uid",
    ):
        path = "/api/v2/spot/wallet/withdrawal"
        params = {
            "coin": coin,
            "transferType": transferType,
            "address": address,
            "chain": chain,
            "innerToType": innerToType,
            "size": size,
            "clientOid": clientOid,
        }
        return await self.raw_request("POST", path, payload=params, auth=True)

    @catch_it
    async def withdraw_records(
        self,
        startTime: Optional[int] = None,
        endTime: Optional[int] = None,
        coin: Optional[str] = None,
        clientOid: Optional[str] = None,
        orderId: Optional[str] = None,
        idLessThan: Optional[str] = None,
        limit: Optional[int] = 100,
    ):
        path = "/api/v2/spot/wallet/withdrawal-records"
        params = {
            "coin": coin,
            "clientOid": clientOid,
            "orderId": orderId,
            "startTime": startTime,
            "endTime": endTime,
            "idLessThan": idLessThan,
            "limit": limit,
        }
        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def get_sp_subaccount_assets(self):
        path = "/api/v2/spot/account/subaccount-assets"
        return await self.raw_request("GET", path=path, auth=True)

    @catch_it
    async def get_spot_account_info(self):
        path = "/api/v2/spot/account/info"
        return await self.raw_request("GET", path=path, auth=True)

    """
    ###################
    #   BGB Convert   #
    ###################
    """

    @catch_it
    async def get_bgb_convert_coin_list(self):
        path = "/api/v2/convert/bgb-convert-coin-list"
        return await self.raw_request("GET", path, auth=True)

    @catch_it
    async def bgb_convert(self, coinList: list[str]):
        path = "/api/v2/convert/bgb-convert"
        params = {"coinList": coinList}
        return await self.raw_request("POST", path, payload=params, auth=True)

    @catch_it
    async def create_virtual_subaccount(self, subAccountList: list[str]):
        """
        subAccountList: 需要创建的虚拟子账户列表(虚拟昵称 长度必须为8位的纯英文字母组合 全局唯一)
        """
        path = "/api/v2/user/create-virtual-subaccount"
        params = {
            "subAccountList": subAccountList,
        }
        return await self.raw_request("POST", path, payload=params, auth=True)

    @catch_it
    async def create_virtual_subaccount_api_key(
        self, subAccountUid: str, passphrase: str, label: str, permList: list[str], ipList: Optional[list[str]] = None
    ):
        """
        permList: spot_trade: 现货交易, margin_trade: 现货杠杆交易, contract_trade: 合约交易读写
                    transfer:钱包划转权限, read:读取权限
        """
        path = "/api/v2/user/create-virtual-subaccount-apikey"
        params = {
            "subAccountUid": subAccountUid,
            "passphrase": passphrase,
            "label": label,
            "permList": permList,
            "ipList": ipList,
        }
        return await self.raw_request("POST", path, payload=params, auth=True)

    @catch_it
    async def switch_deduct(self, deduct: bool):
        """
        开启或关闭BGB抵扣
        """
        path = "/api/v2/spot/account/switch-deduct"
        if deduct:
            params = {"deduct": "on"}
        else:
            params = {"deduct": "off"}
        return await self.raw_request("POST", path, payload=params, auth=True)


class BitgetFutureRestClient(BitgetRestClient):
    def __init__(
        self,
        account_config: AccountConfig,
        rest_config: RestConfig,
    ):
        rest_config.url = "https://api.bitget.com"
        super().__init__(account_config, rest_config)

    """
    ###############
    # market data #
    ###############
    """

    @catch_it
    async def get_contracts(self, productType: str, symbol: Optional[str] = None):
        path = "/api/v2/mix/market/contracts"
        params = {"productType": productType, "symbol": symbol}

        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def get_tickers(self, productType: str):
        path = "/api/v2/mix/market/tickers"
        params = {"productType": productType}

        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def get_ticker(self, productType: str, symbol: str):
        path = "/api/v2/mix/market/ticker"
        params = {"symbol": symbol, "productType": productType}

        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def get_orderbook(self, productType: str, symbol: str, limit: int = 100, precision: Optional[str] = None):
        path = "/api/v2/mix/market/merge-depth"
        params = {"symbol": symbol, "limit": limit, "productType": productType, "precision": precision}

        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def get_current_funding_rate(self, productType: str, symbol: str | None = None):
        path = "/api/v2/mix/market/current-fund-rate"
        params = {"symbol": symbol, "productType": productType}

        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def get_next_funding_time(self, productType: str, symbol: str):
        path = "/api/v2/mix/market/funding-time"
        params = {"symbol": symbol, "productType": productType}

        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def get_history_funding_rate(
        self,
        productType: str,
        symbol: str,
        pageSize: Optional[int] = 100,
        pageNo: Optional[int] = None,
    ):
        path = "/api/v2/mix/market/history-fund-rate"
        params = {
            "productType": productType,
            "symbol": symbol,
            "pageSize": pageSize,
            "pageNo": pageNo,
        }
        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def get_history_kline(
        self,
        productType: str,
        symbol: str,
        granularity: str,
        startTime: Optional[str] = None,
        endTime: Optional[str] = None,
        limit: Optional[str] = None,
    ):
        path = "/api/v2/mix/market/history-candles"
        params = {
            "productType": productType,
            "symbol": symbol,
            "granularity": granularity,
            "startTime": startTime,
            "endTime": endTime,
            "limit": limit,
        }
        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def get_long_short_ratio(
        self,
        symbol: str,
        period: Literal["5m", "15m", "30m", "1h", "2h", "4h", "6h", "12h", "1d"],
    ):
        path = "/api/v2/mix/market/account-long-short"
        params = {"symbol": symbol, "period": period}
        return await self.raw_request("GET", path, params=params)

    """
    ###############
    #   Account   #
    ###############
    """

    async def get_account(self, productType: str, symbol: str, marginCoin: str):
        path = "/api/v2/mix/account/account"
        params = {"marginCoin": marginCoin, "productType": productType, "symbol": symbol}

        return await self.raw_request("GET", path, params=params, auth=True)

    async def get_accounts(self, productType: str):
        path = "/api/v2/mix/account/accounts"
        params = {"productType": productType}

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def set_leverage(self, productType: str, marginCoin: str, symbol: str, leverage: str):
        path = "/api/v2/mix/account/set-leverage"
        params = {
            "productType": productType,
            "marginCoin": marginCoin.upper(),
            "symbol": symbol,
            "leverage": str(leverage),
        }
        return await self.raw_request("POST", path, payload=params, auth=True)

    @catch_it
    async def set_asset_mode(self, productType: str, assetMode: Literal["single", "union"]):
        """https://www.bitget.com/zh-CN/api-doc/contract/account/Set-Balance-Mode"""

        path = "/api/v2/mix/account/set-asset-mode"
        params = {
            "productType": productType,
            "assetMode": assetMode,
        }
        return await self.raw_request("POST", path, payload=params, auth=True)

    @catch_it
    async def set_margin_mode(
        self, productType: str, marginCoin: str, symbol: str, marginMode: Literal["isolated", "crossed"]
    ):
        """https://www.bitget.com/zh-CN/api-doc/contract/account/Change-Margin-Mode"""

        path = "/api/v2/mix/account/set-margin-mode"
        params = {
            "productType": productType,
            "marginCoin": marginCoin.upper(),
            "symbol": symbol,
            "marginMode": marginMode,
        }
        return await self.raw_request("POST", path, payload=params, auth=True)

    @catch_it
    async def set_position_mode(self, productType: str, posMode: Literal["one_way_mode", "hedge_mode"]):
        """https://www.bitget.com/zh-CN/api-doc/contract/account/Change-Hold-Mode

        CAVEAT: Will set one_way_mode by default!
        """

        path = "/api/v2/mix/account/set-position-mode"
        params = {
            "productType": productType,
            "posMode": posMode,
        }
        return await self.raw_request("POST", path, payload=params, auth=True)

    @catch_it
    async def get_lps_subaccount_assets(self, productType: str):
        path = "/api/v2/mix/account/sub-account-assets"
        return await self.raw_request("GET", path=path, params={"productType": productType}, auth=True)

    """
    ###############
    #  Position   #
    ###############
    """

    @catch_it
    async def get_positions(self, productType: str, marginCoin: str | None = None):
        path = "/api/v2/mix/position/all-position"
        params = {"productType": productType, "marginCoin": marginCoin}

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def get_position(self, productType: str, symbol: str, marginCoin: str):
        path = "/api/v2/mix/position/single-position"
        params = {"productType": productType, "symbol": symbol, "marginCoin": marginCoin}

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def get_account_bill(
        self,
        productType: str,
        businessType: str,
        startTime: Optional[str] = None,
        endTime: Optional[str] = None,
        limit: Optional[int] = 100,
        idLessThan: Optional[str] = None,
    ):
        path = "/api/v2/mix/account/bill"
        params = {
            "productType": productType,
            "businessType": businessType,
            "startTime": startTime,
            "endTime": endTime,
            "limit": limit,
            "idLessThan": idLessThan,
        }
        return await self.raw_request("GET", path, params=params, auth=True)

    """
    ###############
    #    Trade    #
    ###############
    """

    @catch_it
    async def get_fills_history(
        self,
        productType: str,
        symbol: str,
        limit: int = 100,
        idLessThan: Optional[str] = None,
        startTime: Optional[str] = None,
        endTime: Optional[str] = None,
    ):
        path = "/api/v2/mix/order/fills"
        params = {
            "productType": productType,
            "symbol": symbol,
            "limit": limit,
            "idLessThan": idLessThan,
            "startTime": startTime,
            "endTime": endTime,
        }

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def futures_order(
        self,
        symbol: str,
        productType: str,
        marginMode: Literal["crossed", "isolated"],
        marginCoin: str,
        side: Literal["buy", "sell"],
        orderType: Literal["limit", "market"],
        force: Literal["gtc", "post_only", "fok", "ioc"],
        size: str,
        price: Optional[str] = None,
        clientOid: Optional[str] = None,
        **kwargs,
    ):
        path = "/api/v2/mix/order/place-order"
        params = {
            "symbol": symbol,
            "productType": productType,
            "marginMode": marginMode,
            "marginCoin": marginCoin,
            "side": side,
            "orderType": orderType,
            "force": force,
            "size": size,
            "price": str(price) if price is not None else None,
            "clientOid": str(clientOid) if clientOid is not None else None,
            **kwargs,
        }
        return await self.raw_request("POST", path, payload=params, auth=True)

    @catch_it
    async def get_open_orders(self, productType: str, symbol: Optional[str] = None):
        path = "/api/v2/mix/order/orders-pending"
        params = {"productType": productType, "symbol": symbol}
        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def cancel_order(
        self, productType: str, symbol: str, orderId: Optional[str] = None, clientOid: Optional[str] = None
    ):
        assert orderId or clientOid, "Either Parameters `orderId` or `clientOid` is needed"
        path = "/api/v2/mix/order/cancel-order"
        params = {"productType": productType, "symbol": symbol, "orderId": orderId, "clientOid": clientOid}
        return await self.raw_request("POST", path, payload=params, auth=True)

    @catch_it
    async def get_order_history(
        self,
        productType: str,
        symbol: Optional[str],
        startTime: Optional[str] = None,
        endTime: Optional[str] = None,
        limit: Optional[str] = "100",
        idLessThan: Optional[str] = None,
        **kwargs,
    ):
        path = "/api/v2/mix/order/orders-history"
        params = {
            "productType": productType,
            "symbol": symbol,
            "startTime": startTime,
            "endTime": endTime,
            "limit": limit,
            "idLessThan": idLessThan,
            **kwargs,
        }

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def query_future_order(
        self,
        symbol: str,
        productType: str,
        orderId: Optional[str] = None,
        clientOid: Optional[str] = None,
    ):
        assert orderId or clientOid

        path = "/api/v2/mix/order/detail"
        params = {
            "symbol": symbol,
            "productType": productType,
            "orderId": orderId,
            "clientOid": clientOid,
        }

        return await self.raw_request("GET", path, params=params, auth=True)
