#!/usr/bin/env python3
"""
Background worker that snapshots OTC balances periodically and
pushes the diff to a configured Lark destination. Can optionally
trigger phone calls during late-night windows.
"""

from __future__ import annotations

from datetime import datetime, time, timedelta
import logging
import os
import sys
import time
from pathlib import Path

MODULE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = MODULE_DIR.parents[0]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from balance_monitor.balance_change_notifier import (
    DEFAULT_BALANCE_FILE,
    DEFAULT_SNAPSHOT_PATH,
    generate_report,
    send_to_lark,
)
from balance_monitor.lark_client import send_text_to_chat
from utils.sms_n_call import AutoPhone

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DEFAULT_INTERVAL = float(os.getenv("BALANCE_MONITOR_INTERVAL", "15"))
BALANCE_MODE = (os.getenv("BALANCE_MONITOR_MODE") or "file").lower()
BALANCE_FILE = Path(os.getenv("BALANCE_MONITOR_SOURCE", str(DEFAULT_BALANCE_FILE)))
SNAPSHOT_PATH = Path(os.getenv("BALANCE_MONITOR_SNAPSHOT", str(DEFAULT_SNAPSHOT_PATH)))
CHAT_ID = os.getenv("BALANCE_MONITOR_CHAT_ID") 
WEBHOOK_URL = os.getenv("LARK_WEBHOOK_URL")
CALL_NUMBERS = [number.strip() for number in os.getenv("BALANCE_MONITOR_CALLS", "").split(",") if number.strip()]
_CALLER: AutoPhone | None = AutoPhone() if CALL_NUMBERS else None
HK_OFFSET = timedelta(hours=8)
NIGHT_START = time(22, 0)
MORNING_END = time(9, 30)


def _deliver(message: str) -> None:
    if CHAT_ID:
        send_text_to_chat(CHAT_ID, message)
        logger.info("pushed balance update to chat %s", CHAT_ID)
        return
    if WEBHOOK_URL:
        send_to_lark(WEBHOOK_URL, message)
        logger.info("pushed balance update via webhook")
        return
    logger.warning("no push channel configured for balance monitor; printing message\n%s", message)


def _is_in_night_window(now: datetime | None = None) -> bool:
    now = now or datetime.utcnow() + HK_OFFSET
    current = now.time()
    return current >= NIGHT_START or current < MORNING_END


def _normalize_number(number: str) -> str:
    return number.replace(" ", "").replace("-", "")


def _trigger_calls() -> None:
    if not _CALLER or not CALL_NUMBERS:
        return
    if not _is_in_night_window():
        logger.debug("call window closed; skipping phone alerts")
        return

    mainland = []
    overseas = []
    for raw in CALL_NUMBERS:
        normalized = _normalize_number(raw)
        if normalized.startswith("+86"):
            mainland.append(normalized)
        else:
            overseas.append(normalized)

    try:
        if mainland:
            _CALLER.make_mainland_call(mainland)
            logger.info("triggered mainland phone alerts count=%s", len(mainland))
        for number in overseas:
            _CALLER.make_call(number)
            logger.info("triggered overseas phone alert number=%s", number)
    except Exception:
        logger.exception("failed to trigger phone alerts")


def watch(
    *,
    interval: float = DEFAULT_INTERVAL,
    balance_file: Path = BALANCE_FILE,
    snapshot_path: Path = SNAPSHOT_PATH,
) -> None:
    source_desc = balance_file if BALANCE_MODE == "file" else f"{BALANCE_MODE}"
    logger.info(
        "starting balance watcher interval=%ss source=%s mode=%s snapshot=%s",
        interval,
        source_desc,
        BALANCE_MODE,
        snapshot_path,
    )
    while True:
        try:
            message, changed = generate_report(
                balance_file=balance_file,
                snapshot_path=snapshot_path,
                update_snapshot=True,
                source_mode=BALANCE_MODE,
            )
            if changed:
                _deliver(message)
                _trigger_calls()
            else:
                logger.debug("no balance changes detected")
        except Exception:
            logger.exception("balance watcher iteration failed")
        time.sleep(interval)


def main() -> None:
    watch()


if __name__ == "__main__":
    main()
