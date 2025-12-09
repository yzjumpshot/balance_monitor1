from pyutils import ConfigLoader

XCLIENTS_CONFIG_LOADER = ConfigLoader(config_dir=".xclients", config_name="config.toml", env_prefix="XCLIENTS")

IS_DEBUG = bool(XCLIENTS_CONFIG_LOADER.load_config().get("debug", False))
