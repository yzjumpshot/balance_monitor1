from typing import Dict, Any, Optional
from pathlib import Path
import orjson
import re
import tomllib
import os


def merge_config(*configs) -> Dict[str, Any]:
    """merge several configs into one config, the latter would replace the former

    Returns:
        Dict[str, Any]: merged config
    """
    config = {}
    for c in configs:
        for k, v in c.items():
            if isinstance(v, dict):
                v2 = v
                v1 = config.setdefault(k, {})
                v = merge_config(v1, v2)
                if not v:
                    config.pop(k, None)
                else:
                    config[k] = v
            else:
                if v is None:
                    config.pop(k, None)
                else:
                    config[k] = v

    return config


def to_snake_case(s):
    return re.sub(r"(?<=[a-z])[A-Z]|(?<!^)[A-Z](?=[a-z])", r"_\g<0>", s).lower()


class ConfigLoader:
    """ConfigLoader是一个配置加载类,以 toml 格式加载所需配置."""

    def __init__(
        self,
        config_dir: str,
        config_name: str,
        env_prefix: Optional[str] = None,
    ) -> None:
        """
        - 一般 config_dir 配置为 `.project_name`
        - 一般 config_name 配置为 `config.toml`
        - env_prefix 可选配置为 `PROJECT_NAME`
        - 配置按顺序加载，后面的会覆盖前面的配置:
            - 用户根目录的config_dir/config_name
            - 当前工作目录的config_dir/config_name
            - 环境变量中以 env_prefix 开头的配置(若env_prefix不为 None )
            - load_config传入的 config 参数

        Args:
            config_dir (Optional[str], optional): file f'{config_dir}/{config_name}' at home-dir or cur-dir would be loaded.
            config_name (Optional[str], optional): file f'{config_dir}/{config_name}' at home-dir or cur-dir would be loaded.
            env_prefix (Optional[str], optional): env variable with env_prefix would be loaded.
        """
        self._config_dir = config_dir
        self._config_name = config_name
        self._env_prefix = env_prefix

    def load_config(self, config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """load config from env-variable, home-dir, current-dir, config params and merge all into one config

        Args:
            config (Optional[Dict[str, Any]], optional): config params. Defaults to None.

        Returns:
            Dict[str, Any]: merged config
        """
        home_config = self.load_config_from_home()
        cwd_config = self.load_config_from_cwd()
        env_config = self.load_config_from_env()
        config = config or {}
        merged_config = merge_config(home_config, cwd_config, env_config, config)

        return merged_config

    def load_config_from_env(self) -> Dict[str, Any]:
        config = {}
        for k, v in os.environ.items():
            if len(l := k.split("__")) != 2:
                continue

            p, c = l

            if p != self._env_prefix:
                continue

            sc = config
            if len(keys := c.split("_")) == 1:
                k = keys[0]
            else:
                for k in keys[:-1]:
                    sc = sc.setdefault(to_snake_case(k), {})

                k = keys[-1]

            try:
                v = orjson.loads(v)
            except:
                pass
            finally:
                sc[to_snake_case(k)] = v

        return config

    def load_config_from_home(self) -> Dict[str, Any]:
        config = {}
        path = Path.home().joinpath(self._config_dir).joinpath(self._config_name)

        if not path.exists():
            return config

        with path.open("rb") as f:
            config = tomllib.load(f)

        return config

    def load_config_from_cwd(self) -> Dict[str, Any]:
        config = {}
        path = Path.cwd().joinpath(self._config_dir).joinpath(self._config_name)

        if not path.exists():
            return config

        with path.open("rb") as f:
            config = tomllib.load(f)

        return config
