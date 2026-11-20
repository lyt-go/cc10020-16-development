"""接入清单领域服务：记录每笔接入实际使用的结构归属与校验结论。

接入清单是"接入 → 结构归属 → 落湖记录 → 待补验"之间的连接凭证：
- 每笔接入一行，固化当时使用的字段快照与事件快照；
- 授权切换后重新投递历史订单时，可沿清单回查原校验结果；
- 落湖记录通过清单编号回指，正式分区数据能反查归属。
"""
from __future__ import annotations

from typing import Any

from app.services.json_module import JsonModuleService

MANIFEST_MODULE = "ingest_manifest"


class IngestManifestService(JsonModuleService):
    MODULE = MANIFEST_MODULE
    ENTITY = "接入记录"
    OPERATION_TERM = "追溯"
    LIST_FIELDS = [
        "清单编号", "通道", "事件类型", "归属编号", "Schema版本",
        "发布范围", "校验结论", "落湖分区",
    ]
    REQUIRED_FIELDS = ["通道", "事件类型"]
    STATUS_ORDER = ["通过", "待补验", "拒绝"]
    ACTION_RULES = {"复核通过": "通过", "标记拒绝": "拒绝"}
    NEGATIVE_ACTIONS = []

    def update_lake_result(self, manifest_id: int, sink_result: dict[str, Any],
                           dispatch_targets: list[str] | None = None,
                           dedup_outcome: str = "",
                           lake_event_id: str | None = None) -> dict[str, Any] | None:
        """把落湖结果与追踪结论回写到接入清单。"""
        updates: dict[str, Any] = {
            "落湖分区": sink_result.get("partition", ""),
            "落湖文件": sink_result.get("file_path", ""),
        }
        if lake_event_id:
            updates["落湖事件标识"] = lake_event_id
        if dispatch_targets is not None:
            updates["分发目标"] = dispatch_targets
        if dedup_outcome:
            updates["去重结果"] = dedup_outcome
        entry = self._store().get_by_id(self.MODULE, manifest_id)
        if entry is None:
            return None
        owner = {}
        if entry.get("归属ID"):
            from app.services.schema_ownership import OWNERSHIP_MODULE
            owner_row = self._store().get_by_id(OWNERSHIP_MODULE, int(entry["归属ID"]))
            if owner_row:
                owner = {
                    "归属编号": owner_row.get("归属编号"),
                    "字段指纹": owner_row.get("字段指纹"),
                    "归属通道": owner_row.get("归属通道"),
                    "发布范围": owner_row.get("发布范围"),
                }
        updates["落湖归属"] = owner
        return self._store().update(self.MODULE, manifest_id, updates)

    def trace(self, manifest_id: int | None = None,
              event_id: str | None = None) -> dict[str, Any] | None:
        """按清单编号或事件标识回查一笔接入的完整链路。"""
        from app.services.schema_ownership import SchemaOwnershipService
        return SchemaOwnershipService().trace_intake(manifest_id=manifest_id, event_id=event_id)

    def pending_revalidations(self) -> list[dict[str, Any]]:
        """列出仍待补验的接入记录。"""
        return [
            row for row in self._store().get_all(self.MODULE)
            if row.get("待补验") and row.get("status") != "通过"
        ]
