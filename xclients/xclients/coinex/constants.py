from ..enum_type import Interval, WithdrawStatus, TimeInForce
from typing import Literal

INTERVAL_MAP = {
    Interval._1m: "1min",
    Interval._3m: "3min",
    Interval._5m: "5min",
    Interval._15m: "15min",
    Interval._30m: "30min",
    Interval._1h: "1hour",
    Interval._2h: "2hour",
    Interval._4h: "4hour",
    Interval._6h: "6hour",
    Interval._12h: "12hour",
    Interval._1d: "1day",
    Interval._3d: "3day",
    Interval._1w: "1week",
}


# withdraw_status:
#     created: '待用户确认'
#     audit_required: '待审核'
#     audited: '已审核'
#     processing: '处理中'
#     confirming: '确认中'
#     finished: '已完成'
#     cancelled: '已取消'
#     cancellation_failed: '取消失败'
#     failed: '失败'
WITHDRAW_STATUS_MAP = {
    "created": WithdrawStatus.PENDING,
    "audit_required": WithdrawStatus.PENDING,
    "audit": WithdrawStatus.PENDING,  # 文档枚举中无，但文档响应示例中有.
    "audited": WithdrawStatus.PENDING,
    "processing": WithdrawStatus.PENDING,
    "confirming": WithdrawStatus.PENDING,
    "failed": WithdrawStatus.FAIL,
    "finished": WithdrawStatus.SUCCESS,
    "cancelled": WithdrawStatus.CANCELED,
    "cancellation_failed": WithdrawStatus.UNKNOWN,  # TODO 提币的 取消失败 状态后续再次查询最终状态？
}

TIF_MAP: dict[TimeInForce, Literal["limit", "market", "maker_only", "ioc", "fok"]] = {
    TimeInForce.GTC: "limit",
    TimeInForce.GTX: "maker_only",
    TimeInForce.IOC: "ioc",
    TimeInForce.FOK: "fok",
}
