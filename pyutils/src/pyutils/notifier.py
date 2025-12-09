import os
import aiohttp
from typing import Literal, Optional, List, Union, Dict, Any, Self


class Notifier:
    """Notifier是一个通知模块,可以新增，删除 channel,发送通知."""

    def __init__(self, *, url: str, user_id: str, **kwargs) -> None:
        """初始化 Notifier

        Args:
            url (str): 通知服务的 URL.
            user_id (str): 用户 ID.
        """
        self._url = url
        self._user_id = user_id

    @classmethod
    def from_env(cls, **kwargs) -> Self:
        url = os.getenv("NOTIFIER_URL")
        user_id = os.getenv("NOTIFIER_USER_ID")

        if not url or not user_id:
            raise ValueError("Notifier URL and User ID must be provided either as kwargs or environment variables")

        return cls(url=url, user_id=user_id, **kwargs)

    @classmethod
    def from_config(cls, config_dir: str, config_name: str, key: str = "notifier", **kwargs) -> Self:
        from .config import ConfigLoader

        cl = ConfigLoader(config_dir=config_dir, config_name=config_name)
        if key:
            config = cl.load_config()[key]
        else:
            config = cl.load_config()

        return cls(**config, **kwargs)

    @property
    def url(self) -> str:
        return self._url

    @property
    def user_id(self) -> str:
        return self._user_id

    async def add_channel(
        self,
        channel: str,
        bot_type: Literal["lark", "slack"],
        bot_url: str,
        bot_note: str = "",
    ):
        """添加一个通知channel.

        Args:
            channel (str): channel名称.
            bot_type (Literal["lark", "slack"]): 类型.
            bot_url (str): Webhook URL.
            bot_note (str, optional): 备注. Defaults to "".

        Raises:
            Exception: 当请求失败或者回报msgCode不为0时抛出异常.
        """
        endpoint = f"{self._url}/api/channel/"
        body = {
            "userId": self._user_id,
            "channel": channel,
            "bots": [{"bot_type": bot_type, "bot_url": bot_url, "bot_note": bot_note}],
        }
        async with aiohttp.ClientSession() as sess:
            async with sess.post(endpoint, json=body) as resp:
                if not resp.ok:
                    raise Exception(f"add channel<{channel}> failed - {resp}")

                result = await resp.json()

                if result["msgCode"] != 0:
                    raise Exception(f"add channel<{channel}> failed - {resp} - {result}")

    async def del_channel(self, channel: str):
        """删除 channel

        Args:
            channel (str): channel名称.

        Raises:
            Exception: 当请求失败或者回报msgCode不为0时抛出异常.
        """
        endpoint = f"{self._url}/api/channel/"
        body = {
            "userId": self._user_id,
            "channel": channel,
            "bots": [],
        }
        async with aiohttp.ClientSession() as sess:
            async with sess.post(endpoint, json=body) as resp:
                if not resp.ok:
                    raise Exception(f"del channel<{channel}> failed - {resp}")

                result = await resp.json()

                if result["msgCode"] != 0:
                    raise Exception(f"del channel<{channel}> failed - {resp} - {result}")

    async def get_channel(self, channel: str) -> List[Dict[str, Any]]:
        """获取 channel 信息

        Args:
            channel (str): channel名称.

        Raises:
            Exception: 当请求失败或者回报msgCode不为0时抛出异常.

        Returns:
            List[Dict[str, Any]]: channel信息.
        """
        endpoint = f"{self._url}/api/channel_info/"
        body = {
            "userId": self._user_id,
            "channel": channel,
        }
        async with aiohttp.ClientSession() as sess:
            async with sess.get(endpoint, json=body) as resp:
                if not resp.ok:
                    raise Exception(f"get channel<{channel}> failed - {resp}")

                result = await resp.json()

                if result["msgCode"] != 0:
                    raise Exception(f"get channel<{channel}> failed - {resp} - {result}")

                return result["bots"]

    async def notify(
        self,
        channel: str,
        msg: Union[str, Dict[str, Any]],
        at: Optional[List[str]] = None,
        timeout: int = 3,
        msg_type: str | None = None,
        **kwargs,
    ):
        """发送通知到指定的 channel.

        Args:
            channel (str): channel名称.
            msg (Union[str, Dict[str, Any]]): 消息内容.
            at (Optional[List[str]], optional): @的人. Defaults to None.
            timeout (int, optional): 超时时间. Defaults to 3.
            msg_type (str | None, optional): 消息类型. Defaults to None.

        Raises:
            Exception: 当请求失败或者回报msgCode不为0时抛出异常.
        """
        endpoint = f"{self._url}/api/notify/"
        body = {
            "userId": self._user_id,
            "channel": channel,
            "at": at or [],
            "params": kwargs,
        }

        if isinstance(msg, str):
            body["msgType"] = msg_type or "text"
            body["data"] = msg
        elif isinstance(msg, dict):
            body["msgType"] = "lark_raw"
            body["data"] = msg
        else:
            raise Exception(f"unsupported msg type - {type(msg)}")

        async with aiohttp.ClientSession() as sess:
            async with sess.post(endpoint, json=body, timeout=aiohttp.ClientTimeout(total=timeout)) as resp:
                if not resp.ok:
                    raise Exception(f"notify failed - {resp}")

                result = await resp.json()

                if result["msgCode"] != 0:
                    raise Exception(f"notify failed - {resp} - {result}")
