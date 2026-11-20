"""待补验记录领域服务：承接共享版放行后仍缺字段的接入。

同一通道可以同时拥有私有版与共享版结构：私有版要求币种，共享版允许缺省。
一笔不带币种的订单走共享版放行后，会在这里留下一条待补验记录，
供归属通道补齐字段后复核；记录同时回指接入清单与落湖分区。
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.services.json_module import JsonModuleService

PENDING_MODULE = "pending_revalidation"


class PendingRevalidationService(JsonModuleService):
    MODULE = PENDING_MODULE
    ENTITY = "待补验记录"
    OPERATION_TERM = "复核"
    LIST_FIELDS = [
        "补验编号", "关联清单", "通道", "事件类型", "实际使用归属",
        "Schema版本", "缺失字段", "补验说明",
    ]
    REQUIRED_FIELDS = ["关联清单", "通道"]
    STATUS_ORDER = ["待补验", "已补验", "已忽略"]
    ACTION_RULES = {"确认补验": "已补验", "忽略补验": "已忽略"}
    NEGATIVE_ACTIONS = []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = self.get_entry(entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
        target = self.ACTION_RULES.get(action)
        if target is None:
            return None, f"动作「{action}」不属于{self.MODULE}可执行范围"
        updated = self._store().update(self.MODULE, entry_id, {
            "status": target,
            "pending": False,
            "abnormal": False,
            "复核时间": datetime.now(timezone.utc).isoformat(),
            "复核结论": "字段已补齐，原校验结果维持" if target == "已补验" else "确认忽略缺口",
        })
        manifest_id = entry.get("关联清单")
        if manifest_id and target == "已补验":
            self._store().update("ingest_manifest", int(manifest_id), {"待补验": False})
        return updated, f"待补验记录已{action}"

    def list_for_manifest(self, manifest_id: int) -> list[dict[str, Any]]:
        return [
            row for row in self._store().get_all(self.MODULE)
            if str(row.get("关联清单")) == str(manifest_id)
        ]

    def domain_snapshot(self) -> dict[str, Any]:
        snapshot = super().domain_snapshot()
        rows = self.rows()
        snapshot.update({
            "waiting": sum(1 for row in rows if row.get("status") == "待补验"),
        })
        return snapshot
