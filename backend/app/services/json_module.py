"""JSON持久化模块服务基类。

通用资源路由既服务于内存示例数据的模块，也服务于完全落在 JSON 存储里的
模块（结构归属、接入清单、待补验记录）。这里把后者的查询/登记/动作统一起来，
并动态解析 `app.persistence.json_store`，保证测试替换存储后立刻生效。
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService


class JsonModuleService(BaseModuleService):
    def _store(self):
        from app import persistence
        return persistence.json_store

    def rows(self) -> list[dict[str, Any]]:
        return self._store().get_all(self.MODULE)

    def list_entries(
        self,
        *,
        filters: dict[str, str] | None = None,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = self.rows()
        for field, expected in (filters or {}).items():
            if field in self.LIST_FIELDS:
                rows = [
                    row for row in rows
                    if expected.casefold() in str(row.get(field, "")).casefold()
                ]
        if keyword and self.LIST_FIELDS:
            field = self.LIST_FIELDS[0]
            rows = [row for row in rows if keyword.casefold() in str(row.get(field, "")).casefold()]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = (page - 1) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return self._store().get_by_id(self.MODULE, entry_id)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [
            field for field in self.REQUIRED_FIELDS
            if not str(values.get(field) or "").strip()
        ]
        if missing:
            return None, missing
        entry: dict[str, Any] = {}
        entry.update({field: values.get(field, "") for field in self.LIST_FIELDS})
        entry["status"] = self.STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        entry["created_at"] = datetime.now(timezone.utc).isoformat()
        self._store().insert(self.MODULE, entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = self.get_entry(entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
        target = self.ACTION_RULES.get(action)
        if target is None:
            return None, f"动作「{action}」不属于{self.MODULE}可执行范围"
        updated = self._store().update(self.MODULE, entry_id, {
            "status": target,
            "pending": target != self.STATUS_ORDER[-1],
            "abnormal": action in self.NEGATIVE_ACTIONS,
        })
        return updated, f"{self.ENTITY}已{action}"

    def domain_snapshot(self) -> dict[str, Any]:
        rows = self.rows()
        by_status = {status: 0 for status in self.STATUS_ORDER}
        for row in rows:
            value = row.get("status", "")
            if value in by_status:
                by_status[value] += 1
        return {
            "module": self.MODULE,
            "entity": self.ENTITY,
            "artifact": f"{self.ENTITY}数据集",
            "count": len(rows),
            "by_status": by_status,
            "abnormal": sum(1 for row in rows if row.get("abnormal")),
        }
