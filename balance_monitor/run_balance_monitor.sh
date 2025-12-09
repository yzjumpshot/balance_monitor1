#!/usr/bin/env bash

set -euo pipefail

APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

: "${LARK_APP_ID:=cli_a9b8c27110781ed4}"
: "${LARK_APP_SECRET:=vwUd8YPLxXjhc6s61p14HbSkpb4Alrac}"
: "${BALANCE_MONITOR_CHAT_ID:=oc_5696f2dfdf3e23c41c590e01149ac5d5}"
: "${BALANCE_MONITOR_INTERVAL:=15}"
: "${BALANCE_MONITOR_SOURCE:=${APP_DIR}/balance_monitor/account_balance.json}"
: "${BALANCE_MONITOR_SNAPSHOT:=${APP_DIR}/balance_monitor/.balance_snapshot.json}"
#mode:xclient or file，实盘选xclient
: "${BALANCE_MONITOR_MODE:=file}"
: "${BALANCE_MONITOR_CALLS:=+6582789249}"
: "${REDIS_ACC_URL:=redis://mp-data-prod-jp.rqo9pb.ng.0001.apne1.cache.amazonaws.com:6379/0}"
: "${REDIS_RMX_URL:=}"
: "${REDIS_KIT_URL:=}"

export LARK_APP_ID
export LARK_APP_SECRET
export BALANCE_MONITOR_CHAT_ID
export BALANCE_MONITOR_INTERVAL
export BALANCE_MONITOR_SOURCE
export BALANCE_MONITOR_SNAPSHOT
export BALANCE_MONITOR_MODE
export BALANCE_MONITOR_CALLS
export REDIS_ACC_URL
export REDIS_RMX_URL
export REDIS_KIT_URL

cd "${APP_DIR}"
python balance_monitor/watch_balance_changes.py
