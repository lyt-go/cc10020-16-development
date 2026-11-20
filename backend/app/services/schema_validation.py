"""事件结构字段校验共享工具。

归属明细在发布时会把当时的字段定义固化成字段快照；接入校验、待补验重放和
历史订单回查都复用这里的同一套口径，确保"当时怎么校验、回查就怎么解释"。

这里的类型判定与 event_schema.EventSchemaService 既有口径保持一致：
- 必填标识取 "是"/"true"/"True"/"Y"/"1"
- 字段缺失时只有必填字段报错
- 类型不匹配记录 expected/actual
"""
from __future__ import annotations

import hashlib
import json
from typing import Any

# 与既有事件结构定义保持相同类型映射（datetime/json 按字符串或容器处理）。
TYPE_MAP: dict[str, Any] = {
    "string": str, "integer": int, "float": float, "boolean": bool,
    "array": list, "object": dict, "number": (int, float),
    "datetime": str, "json": (dict, list), "any": object,
}

REQUIRED_TRUE = ("是", "true", "True", "Y", "1", "yes")


def truthy_flag(value: Any) -> bool:
    """统一判定必填标识等布尔字段。"""
    return str(value) in REQUIRED_TRUE


def normalize_field_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """把事件结构字段行整理成字段快照（按字段行 ID 稳定排序）。"""
    ordered = sorted(rows, key=lambda row: int(row.get("id", 0)))
    seen: dict[str, dict[str, Any]] = {}
    for row in ordered:
        field_name = str(row.get("字段名称", "")).strip()
        if not field_name:
            continue
        seen[field_name] = {
            "field_name": field_name,
            "field_type": str(row.get("字段类型", "any") or "any"),
            "required": truthy_flag(row.get("必填标识", "否")),
            "default": row.get("默认值", ""),
            "schema_row_id": row.get("id"),
            "schema_no": row.get("结构编号"),
            "schema_version": row.get("Schema版本"),
        }
    return list(seen.values())


def snapshot_hash(fields: list[dict[str, Any]]) -> str:
    """字段快照指纹：发布后快照不可变，用指纹核对版本。"""
    raw = json.dumps(fields, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def validate_field_snapshot(
    fields: list[dict[str, Any]],
    payload: dict[str, Any],
) -> dict[str, Any]:
    """按字段快照校验事件数据。

    返回值刻意复用 EventSchemaService.validate_event 的字段结构
    （ok/message/missing_fields/type_errors/checked_fields），
    便于既有接入、重放逻辑无感切换。
    """
    result: dict[str, Any] = {
        "ok": True,
        "message": "结构校验通过",
        "missing_fields": [],
        "type_errors": [],
        "defaulted_fields": [],
        "applied_defaults": {},
        "checked_fields": len(fields),
        "profile_kind": "ownership",
    }

    for field in fields:
        field_name = str(field.get("field_name", ""))
        field_type = str(field.get("field_type", "any"))
        is_required = bool(field.get("required"))
        default_value = field.get("default")

        if field_name not in payload:
            if is_required:
                result["ok"] = False
                result["missing_fields"].append(field_name)
            elif default_value not in (None, ""):
                # 共享版允许缺省的字段：记录可回查的默认值口径，但不改写原始 Payload。
                result["defaulted_fields"].append(field_name)
                result["applied_defaults"][field_name] = default_value
            continue

        expected_type = TYPE_MAP.get(field_type, object)
        actual_value = payload[field_name]

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
            type_msgs = [
                f"{error['field']}(期望{error['expected']},实际{error['actual']})"
                for error in result["type_errors"]
            ]
            messages.append(f"字段类型不匹配: {'; '.join(type_msgs)}")
        result["message"] = " | ".join(messages)

    return result
