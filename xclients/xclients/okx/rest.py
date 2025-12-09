# !/usr/bin/env python
# -*-coding:utf-8 -*-

"""
Author     : Kevin Leung
File       : okx.py
time       : 2023/2/24 14:16
Description: None
IDE        : PyCharm
"""


import json
from urllib.parse import urlencode
import time
import hmac
import base64
import hashlib
import datetime
from typing import Optional, Any, Literal, Union
from urllib.parse import urlencode

from ..data_type import AccountConfig, RestConfig
from ..base_client import BaseRestClient, catch_it
from ..utils import clean_none_value


class OKXRestClient(BaseRestClient):
    def __init__(
        self,
        account_config: AccountConfig,
        rest_config: RestConfig,
    ):
        rest_config.url = "https://www.okx.com"
        super().__init__(account_config, rest_config)

    def _sign_request(self, http_method, endpoint, params=None, payload_string=None):
        """
        :param path: endpoint
        :param method: GET, POST or PUT
        :param body: dict or list params, will be given symbol in post requests
        :param params: params following path
        :return:
        """
        if not self.secret_key:
            raise ValueError("secret_key is required")
        ts = str(datetime.datetime.utcnow().replace().isoformat()[:-3] + "Z")
        query_string = urlencode(params or {})
        payload_string = payload_string or ""
        if http_method in ("GET", "DELETE") and query_string:
            str_to_sign = f"{ts}{http_method}{endpoint}?{query_string}{payload_string}"
        else:
            str_to_sign = f"{ts}{http_method}{endpoint}{payload_string}"

        hmac_digest = hmac.new(self.secret_key.encode("utf-8"), str_to_sign.encode("utf-8"), hashlib.sha256).digest()
        sign = base64.b64encode(hmac_digest).decode("ascii")

        headers = {
            "OK-ACCESS-TIMESTAMP": ts,
            "Content-Type": "application/json",
            "OK-ACCESS-KEY": self.api_key,
            "OK-ACCESS-SIGN": sign,
            "OK-ACCESS-PASSPHRASE": self.passphrase,
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
        payload = payload if isinstance(payload, list) else clean_none_value(payload or {})
        payload_string = json.dumps(payload) if payload else ""
        if auth == True:
            headers = self._sign_request(method, path, params, payload_string)

        return url, headers, params, payload_string

    """
	###################
	# account request #
	###################
	"""

    # 获取账户余额信息, 10次/2s
    @catch_it
    async def get_balance(self, ccy: Optional[str] = None):
        path = "/api/v5/account/balance"
        params = {"ccy": ccy}

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def get_max_size(
        self,
        inst_id: str,
        td_mode: Literal["cross", "isolated", "cash", "spot_isolated"] = "cross",
        ccy: Optional[str] = None,
        px: Optional[str] = None,
        leverage: Optional[str] = None,
        un_spot_offset: Optional[bool] = None,
    ):
        path = "/api/v5/account/max-size"
        params = {
            "instId": inst_id,
            "tdMode": td_mode,
            "ccy": ccy,
            "px": px,
            "leverage": leverage,
            "unSpotOffset": un_spot_offset,
        }

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def get_funding_balance(self, ccy: Optional[str] = None):
        path = "/api/v5/asset/balances"
        params = {"ccy": ccy}

        return await self.raw_request("GET", path, params=params, auth=True)

    # 用这个来查持仓情况, 10次/2s
    @catch_it
    async def get_position(
        self,
        inst_type: Optional[Literal["MARGIN", "SWAP", "FUTURES", "OPTION"]] = None,
        inst_id: Optional[str] = None,
        pos_id: Optional[str] = None,
    ):
        path = "/api/v5/account/positions"
        params = {"instType": inst_type, "instId": inst_id, "posId": pos_id}

        return await self.raw_request("GET", path, params=params, auth=True)

    # 获取最大可借贷量, 20 次/2s
    @catch_it
    async def get_account_max_loan(
        self, symbol: str, mgn_mode: Literal["cross", "isolated"] = "cross", mgn_ccy: Optional[str] = None
    ):
        path = "/api/v5/account/max-loan"
        params = {"instId": symbol, "mgnMode": mgn_mode, "mgnCcy": mgn_ccy}

        return await self.raw_request("GET", path, params=params, auth=True)

    # 获取交易手续费率, 5次/2s
    @catch_it
    async def get_account_fee(
        self,
        inst_type: Literal["SPOT", "MARGIN", "SWAP", "FUTURES", "OPTION"],
        inst_id: Optional[str] = None,
        uly: Optional[str] = None,
        inst_family: Optional[str] = None,
    ):
        path = "/api/v5/account/trade-fee"
        params = {"instType": inst_type, "instId": inst_id, "uly": uly, "instFamily": inst_family}

        return await self.raw_request("GET", path, params=params, auth=True)

    # 设置账户/品种杠杆, 20次/2s
    @catch_it
    async def set_leverage(
        self,
        leverage: str,
        mgn_mode: Literal["cross", "isolated"],
        pos_side: Optional[Literal["long", "short"]] = None,
        inst_id: Optional[str] = None,
        ccy: Optional[str] = None,
    ):
        assert inst_id or ccy
        path = "/api/v5/account/set-leverage"
        params = {"lever": leverage, "mgnMode": mgn_mode, "posSide": pos_side, "instId": inst_id, "ccy": ccy}

        return await self.raw_request("POST", path, auth=True, payload=params)

    @catch_it
    async def get_leverage(self, inst_id: str, mgn_mode: Literal["cross", "isolated"]):
        path = "/api/v5/account/leverage-info"
        params = {"instId": inst_id, "mgnMode": mgn_mode}

        return await self.raw_request("GET", path, params=params, auth=True)

    # 获取借币利率, 5次/2s
    @catch_it
    async def get_loan_rate(self, type: Optional[Literal[1, 2]] = None, ccy: Optional[str] = None):
        path = "/api/v5/account/interest-limits"
        params = {"type": str(type), "ccy": ccy}

        return await self.raw_request("GET", path, params=params, auth=True)

    # 杠杆借币利率, 5次/2s
    @catch_it
    async def get_margin_loan_rate(self, ccy: Optional[str] = None):
        path = "/api/v5/account/interest-rate"
        params = {"ccy": ccy}

        return await self.raw_request("GET", path, params=params, auth=True)

    # 借币/还币, 6次/s
    @catch_it
    async def borrow_operation(
        self, ccy: str, side: Literal["borrow", "repay"], amt: str, ord_id: Optional[str] = None
    ):
        assert side in ("borrow", "repay"), "side should be either borrow or repay"
        assert side != "repay" or ord_id is not None, "ord_id is required for repay"
        path = "/api/v5/account/borrow-repay"
        params = {"ccy": ccy, "side": side, "amt": str(amt), "ordId": ord_id}

        return await self.raw_request("POST", path, auth=True, payload=params)

    @catch_it
    async def vip_loan_detail(
        self,
        order_id: str,
        ccy: Optional[str] = None,
        start: Optional[int] = None,
        end: Optional[int] = None,
        limit: Optional[int] = None,
    ):
        path = "/api/v5/account/vip-loan-order-detail"
        params = {
            "ordId": order_id,
            "ccy": ccy,
            "before": start,
            "after": end,
            "limit": limit,
        }

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def vip_loan_history(
        self,
        order_id: Optional[str] = None,
        state: Optional[Literal["1", "2", "3", "4", "5"]] = None,
        ccy: Optional[str] = None,
        start: Optional[int] = None,
        end: Optional[int] = None,
        limit: Optional[int] = None,
    ):
        # TODO: check this func valid
        path = "/api/v5/account/vip-loan-order-list"
        params = {
            "ordId": order_id,
            "state": state,
            "ccy": ccy,
            "before": start,
            "after": end,
            "limit": limit,
        }

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def vip_borrow_repay_history(
        self,
        ccy: Optional[str] = None,
        start: Optional[int] = None,
        end: Optional[int] = None,
        limit: Optional[int] = None,
    ):
        path = "/api/v5/account/borrow-repay-history"
        params = {
            "ccy": ccy,
            "before": start,
            "after": end,
            "limit": limit,
        }

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def get_account_config(self):
        path = "/api/v5/account/config"

        return await self.raw_request("GET", path, auth=True)

    @catch_it
    async def set_position_mode(self, pos_mode: Literal["long_short_mode", "net_mode"]):
        path = "/api/v5/account/set-position-mode"
        params = {"posMode": pos_mode}

        return await self.raw_request("POST", path, auth=True, payload=params)

    @catch_it
    async def get_max_withdraw(self, ccy: Optional[Union[str, list]] = None):
        if ccy and not isinstance(ccy, str):
            assert len(ccy) <= 20
            ccy = ",".join(ccy)

        path = "/api/v5/account/max-withdrawal"
        params = {"ccy": ccy}

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def get_account_bill(
        self,
        inst_type: Optional[Literal["SPOT", "MARGIN", "SWAP", "FUTURES", "OPTION"]] = None,
        ccy: Optional[str] = None,
        mgn_mode: Optional[Literal["isolated", "cross"]] = None,
        ct_type: Optional[Literal["linear", "inverse"]] = None,
        bill_type: Optional[str] = None,
        start_ts: Optional[int] = None,
        end_ts: Optional[int] = None,
        before: Optional[str] = None,
        after: Optional[str] = None,
        limit: Optional[int] = None,
    ):
        path = "/api/v5/account/bills"
        params = {
            "instType": inst_type,
            "ccy": ccy,
            "mgnMode": mgn_mode,
            "ctType": ct_type,
            "type": bill_type,
            "begin": start_ts,
            "end": end_ts,
            "before": before,
            "after": after,
            "limit": limit,
        }

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def get_order_history(
        self,
        inst_type: Literal["SPOT", "MARGIN", "SWAP", "FUTURES", "OPTION"],
        inst_id: Optional[str] = None,
        uly: Optional[str] = None,
        inst_family: Optional[str] = None,
        state: Optional[Literal["canceled", "filled", "mmp_canceled"]] = "filled",
        category: Optional[
            Literal["twap", "adl", "full_liquidation", "partial_liquidation", "delivery", "ddh"]
        ] = None,
        order_type: Optional[
            Literal[
                "market", "limit", "post_only", "fok", "ioc", "optimal_limit_ioc", "mmp", "mmp_and_post_only", "op_fok"
            ]
        ] = None,
        start_ts: Optional[int] = None,
        end_ts: Optional[int] = None,
        end_order_id: Optional[str] = None,
        start_order_id: Optional[str] = None,
        limit: Optional[int] = 100,
    ):
        path = "/api/v5/trade/orders-history"
        params = {
            "instType": inst_type,
            "instId": inst_id,
            "uly": uly,
            "instFamily": inst_family,
            "state": state,
            "category": category,
            "ordType": order_type,
            "begin": start_ts,
            "end": end_ts,
            "after": end_order_id,
            "before": start_order_id,
            "limit": limit,
        }

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def account_position_tier(
        self,
        inst_type: Literal["SWAP", "FUTURES", "OPTION"],
        uly: Optional[Union[str, list]] = None,
        inst_family: Optional[Union[str, list]] = None,
    ):
        assert uly or inst_family
        if uly and not isinstance(uly, str):
            assert len(uly) <= 3
            uly = ",".join(uly)

        if inst_family and not isinstance(inst_family, str):
            assert len(inst_family) <= 5
            inst_family = ",".join(inst_family)

        path = "/api/v5/account/position-tiers"
        params = {"instType": inst_type, "uly": uly, "instFamily": inst_family}

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def get_accrued_interest(
        self,
        loan_type: Optional[Literal[1, 2]] = 1,
        ccy: Optional[str] = None,
        inst_id: Optional[str] = None,
        mgn_mode: Optional[Literal["cross", "isolated"]] = None,
        limit: Optional[int] = 100,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
    ):
        path = "/api/v5/account/interest-accrued"
        params = {
            "type": loan_type,
            "ccy": ccy,
            "instId": inst_id,
            "mgnMode": mgn_mode,
            "before": start_time,
            "after": end_time,
            "limit": limit,
        }

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def get_pending_order(
        self,
        inst_type: Optional[Literal["SPOT", "MARGIN", "SWAP", "FUTURES", "OPTION"]] = None,
        uly: Optional[str] = None,
        inst_family: Optional[str] = None,
        inst_id: Optional[str] = None,
        order_type: Optional[
            Literal[
                "market", "limit", "post_only", "fok", "ioc", "optimal_limit_ioc", "mmp", "mmp_and_post_only", "op_fok"
            ]
        ] = None,
        state: Optional[Literal["live", "partially_filled"]] = None,
        start_order_id: Optional[str] = None,
        end_order_id: Optional[str] = None,
        limit: Optional[int] = 100,
    ):
        path = "/api/v5/trade/orders-pending"
        params = {
            "instType": inst_type,
            "uly": uly,
            "instFamily": inst_family,
            "instId": inst_id,
            "ordType": order_type,
            "state": state,
            "after": end_order_id,
            "before": start_order_id,
            "limit": limit,
        }

        return await self.raw_request("GET", path, params=params, auth=True)

    """
	###########
	# trading #
	###########
	"""

    # 下单, 60次/2s
    @catch_it
    async def place_order(
        self,
        inst_id: str,
        td_mode: Literal["isolated", "cross", "cash", "spot_isolated"],
        side: Literal["buy", "sell"],
        ord_type: Literal[
            "market", "limit", "post_only", "fok", "ioc", "optimal_limit_ioc", "mmp", "mmp_and_post_only"
        ],
        size: str,
        px: Optional[str] = None,
        ccy: Optional[str] = None,
        clOrdId: Optional[str] = None,
        tag: Optional[str] = None,
        pos_side: Optional[Literal["long", "short"]] = None,
        reduce_only: Optional[bool] = None,
        tgtCcy: Optional[Literal["base_ccy", "quote_ccy"]] = None,
        **kwargs,
    ):
        assert td_mode in ("isolated", "cross", "cash", "spot_isolated")
        path = "/api/v5/trade/order"
        params = {
            "instId": inst_id,
            "tdMode": td_mode,
            "side": side,
            "ordType": ord_type,
            "sz": size,
            "px": px,
            "ccy": ccy,
            "clOrdId": clOrdId or str(int(time.time() * 1000)),
            "tag": tag,
            "posSide": pos_side,
            "reduceOnly": reduce_only,
            "tgtCcy": tgtCcy,
            **kwargs,
        }

        return await self.raw_request("POST", path, auth=True, payload=params)

    # 查单, 300次/2s
    @catch_it
    async def get_order(self, inst_id: str, ord_id: Optional[str] = None, cl_ord_id: Optional[str] = None):
        assert ord_id or cl_ord_id
        path = "/api/v5/trade/order"
        params = {"instId": inst_id, "ordId": ord_id, "clOrdId": cl_ord_id}

        return await self.raw_request("GET", path, params=params, auth=True)

    # 查最近三天成交明细, 60次/2s
    @catch_it
    async def get_trades(
        self,
        inst_type: Optional[Literal["SPOT", "MARGIN", "SWAP", "FUTURES", "OPTION"]] = None,
        inst_id: Optional[str] = None,
        ord_id: Optional[str] = None,
        start_ts: Optional[int] = None,
        end_ts: Optional[int] = None,
        start_order_id: Optional[str] = None,
        end_order_id: Optional[str] = None,
        limit: Optional[int] = None,
        uly: Optional[str] = None,
        inst_family: Optional[str] = None,
        sub_type: Optional[str] = None,
    ):
        path = "/api/v5/trade/fills"
        params = {
            "instType": inst_type,
            "instId": inst_id,
            "ordId": ord_id,
            "after": end_order_id,
            "before": start_order_id,
            "begin": start_ts,
            "end": end_ts,
            "limit": limit,
            "uly": uly,
            "instFamily": inst_family,
            "subType": sub_type,
        }

        return await self.raw_request("GET", path, params=params, auth=True)

    # 撤单, 60次/2s
    @catch_it
    async def cancel_order(self, inst_id: str, ord_id: Optional[str] = None, cl_ord_id: Optional[str] = None):
        assert ord_id or cl_ord_id
        path = "/api/v5/trade/cancel-order"
        params = {"instId": inst_id, "ordId": ord_id, "clOrdId": cl_ord_id}

        return await self.raw_request("POST", path, auth=True, payload=params)

    @catch_it
    async def cancel_batch_order(
        self, inst_id: str, order_id_list: Optional[list] = None, client_oid_list: Optional[list] = None
    ):
        assert order_id_list or client_oid_list
        path = "/api/v5/trade/cancel-batch-orders"
        params = []
        if order_id_list:
            for order_id in order_id_list:
                params.append({"instId": inst_id, "ordId": order_id})
        elif client_oid_list:
            for client_oid in client_oid_list:
                params.append({"instId": inst_id, "clOrdId": client_oid})

        return await self.raw_request("POST", path, auth=True, payload=params)

    @catch_it
    async def get_easy_convert_currency(self):
        path = "/api/v5/trade/easy-convert-currency-list"

        return await self.raw_request("GET", path, auth=True)

    @catch_it
    async def easy_convert(self, from_ccy: list, to_ccy: str):
        path = "/api/v5/trade/easy-convert"
        params = {"fromCcy": from_ccy, "toCcy": to_ccy}

        return await self.raw_request("POST", path, auth=True, payload=params)

    @catch_it
    async def get_one_click_repay_currency(self, debt_type: Optional[Literal["cross", "isolated"]] = "cross"):
        path = "/api/v5/trade/one-click-repay-currency-list"
        params = {"debtType": debt_type}

        return await self.raw_request("GET", path, params=params, auth=True)

    @catch_it
    async def one_click_repay(self, debt_ccy: list, repay_ccy: str):
        path = "/api/v5/trade/one-click-repay"
        params = {"debtCcy": debt_ccy, "repayCcy": repay_ccy}

        return await self.raw_request("POST", path, auth=True, payload=params)

    """
	###############
	# public data #
	###############
	"""

    # original rest API func
    @catch_it
    async def get_instrument_info(
        self,
        inst_type: Literal["SPOT", "MARGIN", "SWAP", "FUTURES", "OPTION"],
        uly: Optional[str] = None,
        inst_family: Optional[str] = None,
        inst_id: Optional[str] = None,
    ):
        path = "/api/v5/public/instruments"
        params = {"instType": inst_type, "uly": uly, "instFamily": inst_family, "instId": inst_id}

        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def get_funding_rate(self, inst_id: str):
        path = "/api/v5/public/funding-rate"
        params = {"instId": inst_id}

        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def get_tickers(self, inst_type: Literal["SPOT", "SWAP", "FUTURES"]):
        path = "/api/v5/market/tickers"
        params = {"instType": inst_type}

        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def get_ticker(self, inst_id: str):
        path = "/api/v5/market/ticker"
        params = {"instId": inst_id}

        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def get_history_funding_rate(
        self,
        inst_id: str = "BTC-USDT-SWAP",
        start: Optional[int] = None,
        end: Optional[int] = None,
        limit: int = 400,
    ):
        path = "/api/v5/public/funding-rate-history"
        params = {"instId": inst_id, "before": start, "after": end, "limit": str(limit)}

        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def get_current_funding_rate(self, inst_id: str):
        path = "/api/v5/public/funding-rate"
        params = {"instId": inst_id}

        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def get_kline(
        self,
        inst_id: str,
        start: Optional[int] = None,
        end: Optional[int] = None,
        interval: Optional[
            Literal[
                "1s", "1m", "3m", "5m", "15m", "30m", "1H", "2H", "4H", "6H", "12H", "1D", "2D", "3D", "1W", "1M", "3M"
            ]
        ] = "1m",
        limit: Optional[int] = None,
    ):
        path = "/api/v5/market/history-candles"
        params = {
            "instId": inst_id,
            "before": start,
            "after": end,
            "bar": interval,
            "limit": limit,
        }

        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def get_index_price(self, quote_ccy: Optional[str] = None, inst_id: Optional[str] = None):
        assert quote_ccy or inst_id, "you should input either quote_ccy or nist_id"
        path = "/api/v5/market/index-tickers"
        params = {"quoteCcy": quote_ccy, "instId": inst_id}

        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def public_mark_price(
        self,
        inst_type: Literal["MARGIN", "SWAP", "FUTURES", "OPTION"],
        uly: Optional[str] = None,
        inst_family: Optional[str] = None,
        inst_id: Optional[str] = None,
    ):
        path = "/api/v5/public/mark-price"
        params = {
            "instType": inst_type,
            "uly": uly,
            "instFamily": inst_family,
            "instId": inst_id,
        }

        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def get_vip_loan_info(self):
        path = "/api/v5/public/vip-interest-rate-loan-quota"

        return await self.raw_request("GET", path)

    @catch_it
    async def get_position_tiers(
        self,
        inst_type: Literal["MARGIN", "SWAP", "FUTURES", "OPTION"],
        td_mode: Literal["cross", "isolated"] = "cross",
        inst_family: Optional[str] = None,
        uly: Optional[str] = None,
        inst_id: Optional[str] = None,
        ccy: Optional[str] = None,
        tier: Optional[str] = None,
    ):
        path = "/api/v5/public/position-tiers"
        params = {
            "instType": inst_type,
            "tdMode": td_mode,
            "uly": uly,
            "instFamily": inst_family,
            "instId": inst_id,
            "ccy": ccy,
            "tier": tier,
        }

        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def get_open_interest(
        self,
        inst_type: Literal["SWAP", "FUTURES", "OPTION"],
        inst_id: Optional[str] = None,
        uly: Optional[str] = None,
        inst_family: Optional[str] = None,
    ):
        path = "/api/v5/public/open-interest"
        params = {
            "instType": inst_type,
            "uly": uly,
            "instFamily": inst_family,
            "instId": inst_id,
        }

        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def get_discount_rate_interest_free_quota(
        self, ccy: Optional[str] = None, discount_lv: Optional[Literal[1, 2, 3, 4, 5]] = None
    ):
        path = "/api/v5/public/discount-rate-interest-free-quota"
        params = {"ccy": ccy, "discountLv": discount_lv}

        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def sapi_vip_loanable_asset(self):
        path = "/api/v5/public/vip-interest-rate-loan-quota"

        return await self.raw_request("GET", path)

    @catch_it
    async def sapi_nonvip_loanable_asset(self):
        path = "/api/v5/public/interest-rate-loan-quota"

        return await self.raw_request("GET", path)

    @catch_it
    async def get_long_short_ratio(
        self,
        ccy: str,
        period: Literal["5m", "1H", "1D"],
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
    ):
        path = "/api/v5/rubik/stat/margin/loan-ratio"
        params = {"ccy": ccy, "period": period, "begin": start_time, "end": end_time}

        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def get_insurance_fund(
        self,
        inst_type: Literal["MARGIN", "SWAP", "FUTURES", "OPTION"],
        uly: Optional[str] = None,
        inst_family: Optional[str] = None,
        ccy: Optional[str] = None,
        start_ts: Optional[str] = None,
        end_ts: Optional[str] = None,
        limit: Optional[int] = None,
        risk_reserve_type: Optional[
            Literal["regular_update", "liquidation_balance_deposit", "bankruptcy_loss", "platform_revenue", "adl"]
        ] = None,
    ):
        """
        uly: 标的指数(e.g. BTC-USD)
        inst_family: 交易品种
        ccy: 仅适用于MARGIN, 且此时必填
        (uly和inst_family必须传一个, 若传两个, 以instFamily为主)
        10次2秒, 容易限频
        """
        path = "/api/v5/public/insurance-fund"
        params = {
            "instType": inst_type,
            "uly": uly,
            "instFamily": inst_family,
            "ccy": ccy,
            "before": start_ts,
            "after": end_ts,
            "limit": limit,
            "risk_reserve_type": risk_reserve_type,
        }

        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def get_index_tickers(self, quote_ccy: Optional[str] = None, inst_id: Optional[str] = None):
        path = "/api/v5/market/index-tickers"
        params = {"quoteCcy": quote_ccy, "instId": inst_id}

        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def get_orderbook(self, inst_id: str, limit: Optional[int] = 20):
        path = "/api/v5/market/books"
        params = {"instId": inst_id, "sz": limit}

        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def get_lending_rate_summary(self, ccy: Optional[str] = None):
        # 6次/s
        path = "/api/v5/finance/savings/lending-rate-summary"
        params = {"ccy": ccy if ccy else None}
        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def get_lending_rate_history(
        self,
        ccy: Optional[str] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        limit: Optional[int] = 100,
    ):
        # 6次/s
        path = "/api/v5/finance/savings/lending-rate-history"
        params = {
            "ccy": ccy if ccy else None,
            "before": start_time,
            "after": end_time,
            "limit": limit,
        }
        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def get_staking_sol_rate_history(self, days: Optional[int] = None):
        path = "/api/v5/finance/staking-defi/sol/apy-history"
        params = {"days": days}
        return await self.raw_request("GET", path, params=params)

    @catch_it
    async def get_staking_eth_rate_history(self, days: Optional[int] = None):
        path = "/api/v5/finance/staking-defi/eth/apy-history"
        params = {"days": days}
        return await self.raw_request("GET", path, params=params)
