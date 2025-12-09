import asyncio
import importlib.machinery
import json
import os
import pathlib
import sys
import types
from typing import Any, Dict, Sequence, Tuple

# Ensure repo paths are configured so `import xclients` works when running directly.
ROOT = pathlib.Path(__file__).resolve().parent
PROJECT_ROOT = ROOT.parents[0]
DEFAULT_XCLIENTS_DIRS = [
    PROJECT_ROOT / "xclients",
    ROOT / "xclients",
]
ENV_XCLIENTS = os.getenv("BALANCE_MONITOR_XCLIENTS") or os.getenv("XCLIENTS_PATH")
if ENV_XCLIENTS:
    DEFAULT_XCLIENTS_DIRS.insert(0, pathlib.Path(ENV_XCLIENTS))

for path in (ROOT,):
    p = str(path)
    if p not in sys.path:
        sys.path.insert(0, p)


def _ensure_xclients_package() -> None:
    if "xclients" in sys.modules:
        return
    for candidate in DEFAULT_XCLIENTS_DIRS:
        if not candidate:
            continue
        pkg_path = candidate
        if (candidate / "xclients").is_dir():
            pkg_path = candidate / "xclients"
        if not pkg_path.is_dir():
            continue
        spec = importlib.machinery.ModuleSpec("xclients", loader=None, is_package=True)
        spec.submodule_search_locations = [str(pkg_path)]
        package = types.ModuleType("xclients")
        package.__spec__ = spec
        package.__path__ = [str(pkg_path)]
        sys.modules["xclients"] = package
        return
    raise ImportError("xclients package path not found; set BALANCE_MONITOR_XCLIENTS")


_ensure_xclients_package()

from xclients.enum_type import AccountType, ExchangeName, MarketType
from xclients.get_wrapper import get_rest_wrapper

# 按交易所分组的账号与要扫描的市场/账户类型组合
ACCOUNT_GROUPS = [
    {
        "exchange": ExchangeName.BINANCE,
        "accounts": ("newmpflyotterbn18", "mpflyotterbn81", "mpflyotterbnvip4"),
        # 常用：现货普通 + U 本位合约（统一账户）
        "combos": (
            (MarketType.SPOT, AccountType.NORMAL),
            (MarketType.UPERP, AccountType.UNIFIED),
        ),
    },
    {
        "exchange": ExchangeName.OKX,
        "accounts": ("mpokexflyotter28",),
        "combos": (
            (MarketType.SPOT, AccountType.UNIFIED),
            (MarketType.UPERP, AccountType.UNIFIED),
        ),
    },
    {
        "exchange": ExchangeName.GATE,
        "accounts": ("mpflyottergate28",),
        "combos": (
            (MarketType.SPOT, AccountType.NORMAL),
            (MarketType.UPERP, AccountType.UNIFIED),
        ),
    },
    {
        "exchange": ExchangeName.BYBIT,
        "accounts": ("mpotctrade01",),
        "combos": (
            (MarketType.SPOT, AccountType.UNIFIED),
            (MarketType.UPERP, AccountType.UNIFIED),
        ),
    },
]


def _to_jsonable(obj: Any) -> Any:
    """Best-effort convert Balance/Balance dicts to JSON-friendly structures."""
    if hasattr(obj, "model_dump"):
        return obj.model_dump()
    if hasattr(obj, "__dict__"):
        return obj.__dict__
    if isinstance(obj, (list, tuple)):
        return [_to_jsonable(i) for i in obj]
    if isinstance(obj, dict):
        return {k: _to_jsonable(v) for k, v in obj.items()}
    return obj


async def fetch_balances_for_account(
    exch: ExchangeName, account_name: str, combos: Sequence[Tuple[MarketType, AccountType]]
) -> Dict[str, Any]:
    result: Dict[str, Any] = {"account": account_name, "exchange": exch.name, "markets": {}}
    for mt, at in combos:
        key = f"{mt.name}-{at.name}"
        try:
            async with get_rest_wrapper(
                exch_name=exch,
                market_type=mt,
                account_type=at,
                account_name=account_name,
            ) as rest:
                resp = await rest.get_assets()
                if resp.get("status") == 0:
                    result["markets"][key] = {"status": 0, "data": _to_jsonable(resp["data"])}
                else:
                    result["markets"][key] = {"status": resp.get("status", -1), "msg": resp.get("msg", "unknown")}
        except Exception as exc:  # noqa: BLE001
            result["markets"][key] = {"status": -1, "msg": str(exc)}
    return result


async def main():
    results = []
    for group in ACCOUNT_GROUPS:
        exch = group["exchange"]
        combos = group["combos"]
        for acc in group["accounts"]:
            results.append(await fetch_balances_for_account(exch, acc, combos))

    print(json.dumps(results, ensure_ascii=False, default=_to_jsonable, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
