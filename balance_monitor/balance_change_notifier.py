#!/usr/bin/env python3
"""
Utility helpers to detect OTC account balance movements and
prepare a concise summary so it can be delivered to a Lark chat.
"""

from __future__ import annotations

import asyncio
import argparse
import json
import math
import os
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple
from urllib.error import URLError
from urllib.request import Request, urlopen

MODULE_DIR = Path(__file__).resolve().parent
DEFAULT_BALANCE_FILE = Path("balance_monitor/account_balance.json")
DEFAULT_SNAPSHOT_PATH = MODULE_DIR / ".balance_snapshot.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Detect changes between the latest OTC account balance snapshot and the current value."
    )
    parser.add_argument(
        "--balance-file",
        type=Path,
        default=DEFAULT_BALANCE_FILE,
        help=f"Path to the JSON file that contains current balances (default: {DEFAULT_BALANCE_FILE}).",
    )
    parser.add_argument(
        "--snapshot",
        type=Path,
        default=DEFAULT_SNAPSHOT_PATH,
        help=f"Path to the balance snapshot cache (default: {DEFAULT_SNAPSHOT_PATH}).",
    )
    parser.add_argument(
        "--webhook",
        type=str,
        help="Optional Lark webhook URL to deliver the summary. If omitted the message is printed only.",
    )
    parser.add_argument(
        "--mode",
        choices=("file", "xclient"),
        default="file",
        help="Balance source to use: 'file' reads JSON, 'xclient' pulls via xclients (default: file).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute the diff without updating the snapshot file.",
    )
    parser.add_argument(
        "--fail-on-change",
        action="store_true",
        help="Exit with non-zero status if changes are detected.",
    )
    return parser.parse_args()


def read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def flatten_balances(root: Any, prefix: Tuple[str, ...] = ()) -> Dict[str, float]:
    """
    Walk a nested JSON object and pick numeric leaves (ignoring booleans).
    Returns a mapping that uses slash-delimited paths to describe nodes.
    """
    items: Dict[str, float] = {}
    if isinstance(root, dict):
        for key, value in root.items():
            items.update(flatten_balances(value, prefix + (str(key),)))
    elif isinstance(root, list):
        for index, value in enumerate(root):
            items.update(flatten_balances(value, prefix + (str(index),)))
    elif isinstance(root, (int, float)) and not isinstance(root, bool):
        path = "/".join(prefix)
        items[path] = float(root)
    return items


def _collect_from_file(balance_file: Path) -> Dict[str, float]:
    if not balance_file.exists():
        raise FileNotFoundError(f"Missing balance file: {balance_file}")
    parsed = read_json(balance_file)
    balances = flatten_balances(parsed)
    if not balances:
        raise ValueError(f"No numeric balances found in {balance_file}")
    return balances


def _collect_from_xclient() -> Dict[str, float]:
    try:
        from balance_monitor import get_balance as balance_fetcher
    except ImportError as exc:  # pragma: no cover - optional dependency
        raise RuntimeError("xclient balance fetcher is unavailable") from exc

    async def _fetch_all() -> List[Dict[str, Any]]:
        tasks = []
        for group in balance_fetcher.ACCOUNT_GROUPS:
            exch = group["exchange"]
            combos = group["combos"]
            for acc in group["accounts"]:
                tasks.append(balance_fetcher.fetch_balances_for_account(exch, acc, combos))
        if not tasks:
            return []
        return await asyncio.gather(*tasks)

    entries = asyncio.run(_fetch_all())
    items: Dict[str, float] = {}
    for entry in entries:
        exchange = str(entry.get("exchange") or "").upper()
        account = str(entry.get("account") or "")
        markets = entry.get("markets") or {}
        for market_name, market_info in markets.items():
            if not isinstance(market_info, dict) or int(market_info.get("status", -1)) != 0:
                continue
            assets = market_info.get("data") or {}
            for asset, details in assets.items():
                if not isinstance(details, dict):
                    continue
                balance_value = details.get("balance")
                if balance_value is None:
                    continue
                key_parts = [exchange, account, market_name, asset]
                key = "/".join(part for part in key_parts if part)
                items[key] = float(balance_value)
    if not items:
        raise ValueError("xclient balance fetch returned no balances")
    return items


def collect_balances(
    balance_file: Path = DEFAULT_BALANCE_FILE,
    *,
    mode: str = "file",
) -> Dict[str, float]:
    mode = (mode or "file").lower()
    if mode == "xclient":
        return _collect_from_xclient()
    return _collect_from_file(balance_file)


def load_snapshot(path: Path) -> Dict[str, float]:
    if not path.exists():
        return {}
    try:
        with path.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
    except json.JSONDecodeError:
        return {}
    return {key: float(value) for key, value in data.items()}


def save_snapshot(path: Path, data: Dict[str, float]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(data, handle, indent=2, sort_keys=True)
        handle.write("\n")


def diff_balances(
    old: Dict[str, float], new: Dict[str, float]
) -> List[Tuple[str, str, float | None, float | None]]:
    changes: List[Tuple[str, str, float | None, float | None]] = []
    keys = set(old) | set(new)
    for key in sorted(keys):
        if key not in old:
            changes.append(("added", key, None, new[key]))
        elif key not in new:
            changes.append(("removed", key, old[key], None))
        else:
            if not math.isclose(old[key], new[key], rel_tol=1e-9, abs_tol=1e-9):
                changes.append(("changed", key, old[key], new[key]))
    return changes


def _fmt_amount(value: float | None) -> str:
    if value is None:
        return "-"
    return f"{value:,.2f}"


def _fmt_delta(old: float | None, new: float | None) -> str:
    if old is None or new is None:
        return ""
    delta = new - old
    sign = "+" if delta >= 0 else ""
    return f" (Δ {sign}{delta:,.2f})"


def format_changes(changes: Iterable[Tuple[str, str, float | None, float | None]]) -> str:
    changes = list(changes)
    if not changes:
        return "未检测到余额变化。"

    lines: List[str] = ["[OTC 账户余额变动]"]
    for change_type, key, old_value, new_value in changes:
        human_key = key.replace("/", " -> ")
        if change_type == "added":
            lines.append(f"新增 {human_key} = {_fmt_amount(new_value)}")
        elif change_type == "removed":
            lines.append(f"删除 {human_key} (原值 {_fmt_amount(old_value)})")
        else:
            lines.append(
                f"{human_key}: {_fmt_amount(old_value)} -> {_fmt_amount(new_value)}{_fmt_delta(old_value, new_value)}"
            )
    return "\n".join(lines)


def generate_report(
    balance_file: Path = DEFAULT_BALANCE_FILE,
    snapshot_path: Path = DEFAULT_SNAPSHOT_PATH,
    *,
    update_snapshot: bool = True,
    source_mode: str = "file",
) -> Tuple[str, bool]:
    current = collect_balances(balance_file, mode=source_mode)
    snapshot = load_snapshot(snapshot_path)
    changes = diff_balances(snapshot, current)
    message = format_changes(changes)
    changed = bool(changes)

    if changed and update_snapshot:
        save_snapshot(snapshot_path, current)

    return message, changed


def send_to_lark(webhook: str, message: str) -> None:
    payload = {"msg_type": "text", "content": {"text": message}}
    body = json.dumps(payload).encode("utf-8")
    request = Request(
        webhook,
        data=body,
        headers={"Content-Type": "application/json; charset=utf-8"},
        method="POST",
    )
    try:
        with urlopen(request, timeout=5):
            pass
    except URLError as exc:
        raise RuntimeError(f"Failed to send message to Lark: {exc}") from exc


def main() -> int:
    args = parse_args()
    try:
        message, changed = generate_report(
            balance_file=args.balance_file,
            snapshot_path=args.snapshot,
            update_snapshot=not args.dry_run,
            source_mode=args.mode,
        )
    except Exception as exc:
        print(f"balance diff failed: {exc}")
        return 2

    print(message)

    if args.webhook:
        try:
            send_to_lark(args.webhook, message)
        except RuntimeError as error:
            print(error)
            if args.fail_on_change:
                return 2

    if changed and args.fail_on_change:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
