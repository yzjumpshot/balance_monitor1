import json
import os
import pathlib
import tomllib

import redis


class AccountCredentialManager:
    """AccountCredentialManager用于获取账户凭证."""

    def __init__(self, redis_acc_url: str | None = None):
        """初始化账户凭证管理器.

        Args:
            redis_acc_url (str | None, optional): Redis 连接 URL. Defaults to None.

        Raises:
            ValueError: 如果未提供 redis_acc_url 且环境变量 REDIS_ACC_URL 不可用.

        Usage:
            acm = AccountCredentialManager(redis_acc_url="redis://localhost:6379/0")
            # or
            acm = AccountCredentialManager()  # will use the environment variable REDIS_ACC_URL
            access_key, secret_key, passphrase, uid = acm.get_credential('account_name', 'exchange_name', 'market_type', 'account_type')
        """
        if redis_acc_url:
            self._redis_acc_url = redis_acc_url
        else:
            self._redis_acc_url = os.getenv("REDIS_ACC_URL")

        if self._redis_acc_url is None:
            raise ValueError("redis_acc_url not set! please add `redis_acc_url` or set env var `REDIS_ACC_URL`")

    def get_credential(
        self, account: str, exchange: str, market_type: str, account_type: str | None = None
    ) -> tuple[str, str, str, str]:
        """从 Redis 中获取账户凭证.

        Args:
            account (str): 账户名.
            exchange (str): 交易所名称.
            market_type (str): 市场类型.
            account_type (str | None, optional): 账户类型. Defaults to None. 如果是统一账户, 设置为 "unified".

        Raises:
            ValueError: 如果账户凭证没找到.

        Returns:
            tuple[str, str, str, str]: access key, secret key, passphrase, uid
        """

        if self._redis_acc_url is None:
            raise ValueError("redis_acc_url not set! please add `redis_acc_url` or set env var `REDIS_ACC_URL`")

        exchange = exchange.lower()
        market_type = market_type.lower()
        if account_type is not None:
            account_type = account_type.lower()

        match (exchange, market_type, account_type):
            case "okx", _, _:
                exchange = "okex"
            case "binance", _, "unified":
                exchange = f"{exchange}pm"
            case "gate", _, "unified":
                exchange = f"{exchange}pm"
            case "kucoin", mt, _ if mt != "spot":
                exchange = "kucoinswap"
            case _:
                pass

        key = f"{exchange}_{account}"

        r = redis.Redis.from_url(self._redis_acc_url)
        raw_data = r.hget(name="account", key=key)
        if not raw_data:
            return self.get_credential_from_file(account)

        res = json.loads(raw_data.replace(b"'", b'"'))  # type: ignore
        access_key = res["ACCESS_KEY"]
        secret_key = res["SECRET_KEY"]
        passphrase = res.get("PASSPHRASE", "")
        uid = res.get("UID", "0")
        return access_key, secret_key, passphrase, uid

    def get_credential_from_file(self, account: str) -> tuple[str, str, str, str]:
        """从文件中获取账户凭证.

        Args:
            file_path (str): 凭证文件路径.
            account (str): 账户名.
            exchange (str): 交易所名称.
            market_type (str): 市场类型.
            account_type (str | None, optional): 账户类型. Defaults to None. 如果是统一账户, 设置为 "unified".

        Raises:
            ValueError: 如果账户凭证没找到.

        Returns:
            tuple[str, str, str, str]: access key, secret key, passphrase, uid
        """

        credential_path = pathlib.Path.home() / ".credential" / "account" / f"{account}.toml"
        if credential_path.exists():

            with open(credential_path, "rb") as f:
                credential = tomllib.load(f)
            api_key = credential.pop("api_key", "")
            api_secret = credential.pop("api_secret", credential.pop("secret_key", ""))
            passphrase = credential.pop("passphrase", "")
            uid = credential.pop("uid", "0")
            return api_key, api_secret, passphrase, uid
        else:
            raise ValueError(f"Credential of {account} not found")
