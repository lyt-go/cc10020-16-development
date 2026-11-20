"""事件结构定义领域服务：Schema定义、字段校验、版本演进与兼容性检查。

提供事件结构版本管理、字段级Schema校验和跨版本兼容性比对能力。
关联模块：webhook_channel（通道关联校验）、data_quality_rule（质量规则校验）。
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from app.persistence import json_store
from app.services.json_module import JsonModuleService
from app.services.schema_validation import validate_field_snapshot


class EventSchemaService(JsonModuleService):
    MODULE = "event_schema"
    ENTITY = "事件结构"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["结构编号", "事件类型", "字段名称", "字段类型", "必填标识", "默认值", "Schema版本", "所属通道"]
    REQUIRED_FIELDS = ["结构编号", "事件类型", "字段名称"]
    STATUS_ORDER = ["草稿", "待校验", "已生效", "已废弃"]
    ACTION_RULES = {"提交校验": "待校验", "标记生效": "已生效", "标记废弃": "已废弃"}
    NEGATIVE_ACTIONS = []

    _TYPE_MAP = {
        "string": str, "integer": int, "float": float, "boolean": bool,
        "array": list, "object": dict, "number": (int, float),
        "datetime": str, "json": (dict, list), "any": object,
    }

    def validate_event(self, channel_id: int, event_type: str,
                       payload: dict[str, Any]) -> dict[str, Any]:
        """对接收的事件Payload执行Schema校验。

        校验流程：
        0. 若该事件类型存在已发布的结构归属明细，改按归属校验（见
           `validate_by_ownership`），避免通道借道未授权的共享结构；
        1. 查找该通道/事件类型下所有已生效的Schema字段定义
        2. 逐一检查必填字段是否存在、类型是否匹配
        3. 对缺失的必填字段返回具体错误

        Args:
            channel_id: Webhook通道ID
            event_type: 事件类型名称
            payload: 事件数据体

        Returns:
            校验结果:{ ok: bool, message: str, missing_fields: list, type_errors: list }
        """
        ownership_result = self.validate_by_ownership(channel_id, event_type, payload)
        if ownership_result is not None:
            return ownership_result

        # 查找关联通道的已生效schema字段
        schemas = json_store.get_all(self.MODULE)
        matching = [
            s for s in schemas
            if s.get("status") == "已生效"
            and (str(s.get("事件类型", "")) == event_type or s.get("事件类型", "") == "通用")
            and str(s.get("所属通道", "")) in (str(channel_id), "", "全通道")
        ]

        result = {
            "ok": True,
            "message": "结构校验通过",
            "missing_fields": [],
            "type_errors": [],
            "checked_fields": len(matching),
        }

        if not matching:
            # 没有Schema定义时默认通过（宽松校验策略）
            return result

        for schema_field in matching:
            field_name = str(schema_field.get("字段名称", ""))
            field_type = str(schema_field.get("字段类型", "any"))
            is_required = str(schema_field.get("必填标识", "否")) in ("是", "true", "True")

            if field_name not in payload:
                if is_required:
                    result["ok"] = False
                    result["missing_fields"].append(field_name)
                continue

            expected_type = self._TYPE_MAP.get(field_type, object)
            actual_value = payload[field_name]

            # 类型检查
            if isinstance(expected_type, tuple):
                if not isinstance(actual_value, expected_type):
                    result["ok"] = False
                    result["type_errors"].append({
                        "field": field_name,
                        "expected": field_type,
                        "actual": type(actual_value).__name__,
                    })
            elif expected_type is object:
                continue
            elif not isinstance(actual_value, expected_type):
                result["ok"] = False
                result["type_errors"].append({
                    "field": field_name,
                    "expected": field_type,
                    "actual": type(actual_value).__name__,
                })

        if not result["ok"]:
            messages = []
            if result["missing_fields"]:
                messages.append(f"缺少必填字段: {', '.join(result['missing_fields'])}")
            if result["type_errors"]:
                type_msgs = [f"{te['field']}(期望{te['expected']},实际{te['actual']})" for te in result["type_errors"]]
                messages.append(f"字段类型不匹配: {'; '.join(type_msgs)}")
            result["message"] = " | ".join(messages)

        return result

    def validate_by_ownership(self, channel_id: int, event_type: str,
                              payload: dict[str, Any]) -> dict[str, Any] | None:
        """按事件结构归属明细校验通道是否有权使用结构。

        返回 None 表示该事件类型尚未发布归属，调用方回落到既有通道 Schema 口径，
        这样历史数据重放时字段校验结论保持不变。
        """
        try:
            from app.services.schema_ownership import SchemaOwnershipService
            ownership_svc = SchemaOwnershipService()
            resolution = ownership_svc.resolve_candidates(channel_id, event_type)
        except Exception:
            return None

        if not resolution["published"]:
            return None

        if resolution["candidates"]:
            row = resolution["candidates"][0]
            result = validate_field_snapshot(row.get("字段快照") or [], payload)
            result.update({
                "ownership_id": row.get("id"),
                "归属编号": row.get("归属编号"),
                "Schema版本": row.get("Schema版本"),
                "发布范围": row.get("发布范围"),
                "legacy": False,
            })
            return result

        if resolution["version_mismatch"]:
            blocked = resolution["version_mismatch"][0]
            message = (
                f"结构版本确认失败：通道版本 {resolution['channel_version']} 与结构适用版本"
                f" {blocked.get('适用通道版本')} 不一致"
            )
        elif resolution["blocked_shares"]:
            blocked = resolution["blocked_shares"][0]
            message = (
                f"通道 {channel_id} 未获共享授权，不能借用结构 {blocked.get('归属编号')}（归属通道 "
                f"{blocked.get('归属通道')}）放行"
            )
        elif resolution.get("revoked_shares"):
            revoked = resolution["revoked_shares"][0]
            message = (
                f"通道 {channel_id} 的共享授权已撤销：结构 {revoked.get('归属编号')}"
                f"（归属通道 {revoked.get('归属通道')}）退回私有，不再放行新订单"
            )
        elif resolution.get("readonly_others"):
            readonly = resolution["readonly_others"][0]
            message = (
                f"通道 {channel_id} 未获共享授权，结构 {readonly.get('归属编号')}"
                f"（归属通道 {readonly.get('归属通道')}）只读查看，不能用于放行"
            )
        else:
            message = f"通道 {channel_id} 没有事件类型 {event_type} 的可用结构归属"
        return {
            "ok": False,
            "message": message,
            "missing_fields": [],
            "type_errors": [],
            "checked_fields": 0,
            "blocked": True,
            "ownership_id": (
                resolution["blocked_shares"] or resolution["version_mismatch"]
                or resolution.get("revoked_shares") or resolution.get("readonly_others") or [{}]
            )[0].get("id"),
            "legacy": False,
        }

    def publish_ownership(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        """登记并发布一份事件结构归属明细（委托归属服务）。"""
        from app.services.schema_ownership import SchemaOwnershipService
        ownership_svc = SchemaOwnershipService()
        entry, missing = ownership_svc.create_entry(values)
        if entry is None:
            return None, missing
        published, message = ownership_svc.run_action(int(entry["id"]), "发布归属")
        if published is None:
            return None, [message]
        return published, []

    def ownership_detail(self, channel_id: int | None = None) -> list[dict[str, Any]]:
        """查看事件结构归属明细；未授权通道看到的是只读视图。"""
        from app.services.schema_ownership import SchemaOwnershipService
        return SchemaOwnershipService().list_ownership_detail(channel_id=channel_id)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        """创建Schema结构定义，附带版本管理和字段去重检查。"""
        missing = [
            field for field in self.REQUIRED_FIELDS
            if not str(values.get(field) or "").strip()
        ]
        if missing:
            return None, missing

        # 检查同一结构版本下是否已存在同名生效字段。
        # 结构编号是结构的身份：同一事件类型可以并存私有版/共享版两套结构，
        # 只要它们属于不同的结构编号，同名字段是正常的。
        existing = json_store.get_all(self.MODULE)
        duplicate = [
            e for e in existing
            if e.get("事件类型") == values.get("事件类型")
            and e.get("字段名称") == values.get("字段名称")
            and str(e.get("结构编号", "")) == str(values.get("结构编号", ""))
            and e.get("status") in ("已生效", "待校验")
        ]
        if duplicate:
            return None, ["同事件类型下已存在同名字段定义，请先废弃旧版本或修改字段名称"]

        max_id = max((int(e.get("id", 0)) for e in existing), default=0)
        entry = {"id": max_id + 1}
        entry.update({field: values.get(field, "") for field in self.LIST_FIELDS})
        entry["status"] = self.STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        entry["created_at"] = datetime.now(timezone.utc).isoformat()
        entry["parent_schema_id"] = values.get("parent_schema_id")

        # Schema版本自动递增
        schema_version = str(values.get("Schema版本", "v1"))
        entry["Schema版本"] = schema_version

        json_store.insert(self.MODULE, entry)
        return entry, []

    def get_schema_for_channel(self, channel_id: int, event_type: str | None = None) -> dict[str, Any]:
        """获取通道的完整Schema定义（聚合所有已生效的字段定义）。"""
        schemas = json_store.get_all(self.MODULE)
        channel_schemas = [
            s for s in schemas
            if s.get("status") == "已生效"
            and str(s.get("所属通道", "")) in (str(channel_id), "", "全通道")
        ]
        if event_type:
            channel_schemas = [
                s for s in channel_schemas
                if s.get("事件类型") == event_type or s.get("事件类型") == "通用"
            ]

        fields_by_type: dict[str, list[dict[str, Any]]] = {}
        for s in channel_schemas:
            et = str(s.get("事件类型", "通用"))
            if et not in fields_by_type:
                fields_by_type[et] = []
            fields_by_type[et].append({
                "field_name": s.get("字段名称"),
                "field_type": s.get("字段类型"),
                "required": str(s.get("必填标识", "否")) in ("是", "true", "True"),
                "default": s.get("默认值"),
                "version": s.get("Schema版本"),
            })

        return {
            "channel_id": channel_id,
            "event_types": list(fields_by_type.keys()),
            "schemas": fields_by_type,
            "total_fields": len(channel_schemas),
        }

    def check_version_compatibility(self, schema_id: int, new_fields: dict[str, Any]) -> dict[str, Any]:
        """检查Schema版本演进的兼容性（新增可选字段兼容，删除必填字段不兼容）。"""
        existing = json_store.get_by_id(self.MODULE, schema_id)
        if existing is None:
            return {"compatible": False, "reason": "Schema不存在"}

        issues = []
        # 检查是否删除了已有的必填字段
        existing_required = str(existing.get("必填标识", "否")) in ("是", "true", "True")
        new_required = str(new_fields.get("必填标识", "否")) in ("是", "true", "True")

        if existing_required and not new_required:
            issues.append("警告：将必填字段改为非必填可能影响下游消费者")

        type_changed = str(existing.get("字段类型", "")) != str(new_fields.get("字段类型", ""))
        if type_changed:
            issues.append(f"警告：字段类型从 {existing.get('字段类型')} 变更为 {new_fields.get('字段类型')}，历史数据需要迁移")

        compatible = len([i for i in issues if i.startswith("错误")]) == 0
        return {
            "compatible": compatible,
            "issues": issues,
            "recommendation": "建议创建新Schema版本而非修改已有定义" if not compatible else "变更兼容，可安全升级",
        }

    def domain_snapshot(self) -> dict[str, Any]:
        """输出事件结构定义的数据质量快照。"""
        rows = json_store.get_all(self.MODULE)
        by_status = {status: 0 for status in self.STATUS_ORDER}
        for row in rows:
            s = row.get("status", "")
            if s in by_status:
                by_status[s] += 1

        event_types = set(r.get("事件类型") for r in rows if r.get("事件类型"))
        return {
            "module": self.MODULE,
            "entity": self.ENTITY,
            "artifact": "事件结构定义数据集",
            "count": len(rows),
            "by_status": by_status,
            "abnormal": sum(1 for r in rows if r.get("abnormal")),
            "unique_event_types": len(event_types),
            "required_fields_count": sum(1 for r in rows if str(r.get("必填标识")) in ("是", "true", "True")),
            "operation": self.OPERATION_TERM,
            "ownership_published": sum(
                1 for r in json_store.get_all("event_schema_ownership")
                if r.get("status") == "已发布"
            ),
        }