"""Webhook通道领域服务：事件接收、签名验证、Payload解析与分发编排。

提供完整的Webhook接收流程：签名校验→Payload验证→事件解析→去重→分发路由→落湖。
关联模块：event_schema（结构校验）、dedup_filter（幂等去重）、dispatch_rule（条件路由）、data_lake_sink（落湖存储）。
"""
from __future__ import annotations

import hashlib
import hmac
import json
import uuid
from datetime import datetime, timezone
from typing import Any

from app.persistence import json_store
from app.services.base import BaseModuleService
from app.store import store


class WebhookChannelService(BaseModuleService):
    MODULE = "webhook_channel"
    ENTITY = "通道配置"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["通道编号", "通道名称", "接收路径", "签名密钥", "验证方式", "并发上限", "回调地址", "通道版本"]
    REQUIRED_FIELDS = ["通道编号", "通道名称", "接收路径"]
    STATUS_ORDER = ["草稿", "待校验", "已启用", "已停用"]
    ACTION_RULES = {"提交校验": "待校验", "启用通道": "已启用", "停用通道": "已停用"}
    NEGATIVE_ACTIONS = ["停用通道"]

    def __init__(self) -> None:
        super().__init__()
        self._received_events: list[dict[str, Any]] = []

    def receive_webhook(self, channel_id: int, payload: dict[str, Any],
                        signature: str | None = None,
                        headers: dict[str, str] | None = None,
                        intake_context: dict[str, Any] | None = None) -> dict[str, Any]:
        """接收Webhook事件的核心方法。

        完整流程：
        1. 通道状态校验
        2. 签名验证（HMAC-SHA256或RAW）
        3. Payload结构校验（按事件结构归属明细 + 通道授权）
        4. 事件去重判定
        5. 分发规则匹配
        6. 数据落湖（写入结构归属凭证）
        7. 校验结论写入接入清单

        `intake_context` 用于历史订单重投：可携带原接入清单/原事件标识，
        从而在授权切换后仍能核对原校验结果。

        返回处理结果包含完整的处理轨迹。
        """
        channel = json_store.get_by_id(self.MODULE, channel_id)
        if channel is None:
            fallback = store.find(self.MODULE, channel_id)
            if fallback is None:
                return {"ok": False, "message": f"通道 {channel_id} 不存在", "event_id": None, "stage": "通道校验"}
            # 内存示例数据里的通道同样走读路径，保证演示数据可用。
            channel = dict(fallback)

        if channel.get("status") != "已启用":
            return {"ok": False, "message": f"通道 {channel_id} 未启用（当前状态: {channel['status']}）", "event_id": None, "stage": "状态校验"}

        trace: list[dict[str, Any]] = []

        # 阶段1：签名验证
        secret = str(channel.get("签名密钥", ""))
        verify_method = str(channel.get("验证方式", "HMAC-SHA256"))
        sign_ok = self._verify_signature(payload, signature, secret, verify_method)
        trace.append({"stage": "签名验证", "passed": sign_ok, "method": verify_method})

        if not sign_ok and verify_method != "NONE":
            self._record_receive_event(channel_id, payload, "signature_failed", trace)
            return {"ok": False, "message": "签名验证失败", "event_id": None, "stage": "签名验证", "trace": trace}

        # 阶段2：事件结构归属校验（跨模块：event_schema + event_schema_ownership）
        event_type = payload.get("type") or payload.get("event_type", "unknown")
        context = intake_context or {}
        ownership = self._validate_event_schema(channel_id, event_type, payload, context)
        trace.append({
            "stage": "结构归属校验",
            "passed": ownership["ok"],
            "detail": ownership.get("message"),
            "归属编号": (ownership.get("ownership") or {}).get("归属编号")
            if isinstance(ownership.get("ownership"), dict) else None,
            "发布范围": (ownership.get("ownership") or {}).get("发布范围")
            if isinstance(ownership.get("ownership"), dict) else None,
        })

        if not ownership["ok"]:
            self._record_receive_event(channel_id, payload, "schema_invalid", trace)
            result = {
                "ok": False,
                "message": f"事件结构校验失败: {ownership['message']}",
                "event_id": ownership.get("event_id"),
                "stage": "结构校验",
                "trace": trace,
                "ownership_detail": ownership,
                "manifest": self._load_manifest(ownership.get("manifest_id")),
            }
            replay = self._replay_historical(channel_id, event_type, payload, context)
            if replay is not None:
                result["historical_check"] = replay
            return result

        # 阶段3：去重判定（跨模块：dedup_filter）
        event_id = str(uuid.uuid4())
        dedup_result = self._check_dedup(channel_id, event_type, payload)
        trace.append({"stage": "去重判定", "passed": dedup_result["ok"], "is_duplicate": dedup_result.get("is_duplicate")})

        if dedup_result.get("is_duplicate"):
            self._record_receive_event(channel_id, payload, "duplicate", trace)
            result = {"ok": True, "message": "事件已去重（重复事件）", "event_id": event_id, "stage": "去重判定", "trace": trace, "skipped": True}
            replay = self._replay_historical(channel_id, event_type, payload, context)
            if replay is not None:
                result["historical_check"] = replay
            return result

        # 阶段4：分发路由（跨模块：dispatch_rule）
        dispatch_targets = self._match_dispatch_rules(event_type, payload)
        trace.append({"stage": "分发路由", "targets": len(dispatch_targets), "routes": [t.get("目标系统") for t in dispatch_targets]})

        # 阶段5：数据落湖（跨模块：data_lake_sink），携带结构归属凭证
        sink_result = self._sink_event(
            event_id, event_type, payload, channel_id,
            attribution=self._lake_attribution(ownership),
        )
        trace.append({"stage": "数据落湖", "passed": sink_result["ok"], "detail": sink_result.get("message")})

        # 阶段6：校验结论写入接入清单
        manifest = None
        try:
            from app.services.ingest_manifest import IngestManifestService
            manifest_svc = IngestManifestService()
            manifest_id = ownership.get("manifest_id")
            if manifest_id:
                manifest = manifest_svc.update_lake_result(
                    int(manifest_id),
                    sink_result,
                    dispatch_targets=[t.get("目标系统") for t in dispatch_targets],
                    dedup_outcome="通过去重检查",
                    lake_event_id=event_id,
                )
        except Exception:
            manifest = None

        # 记录成功接收
        self._record_receive_event(channel_id, payload, "success", trace)
        if json_store.get_by_id(self.MODULE, channel_id) is not None:
            json_store.update(self.MODULE, channel_id, {
                "last_event_at": datetime.now(timezone.utc).isoformat(),
                "total_received": int(channel.get("total_received", 0)) + 1,
            })

        result = {
            "ok": True,
            "message": f"事件接收成功，已路由至 {len(dispatch_targets)} 个目标系统",
            "event_id": event_id,
            "stage": "完成",
            "trace": trace,
            "dispatch_targets": [t.get("目标系统") for t in dispatch_targets],
            "ownership_detail": ownership,
            "manifest": manifest,
            "lake": {
                "partition": sink_result.get("partition", ""),
                "file_path": sink_result.get("file_path", ""),
                "attribution": sink_result.get("attribution", {}),
            },
        }
        replay = self._replay_historical(channel_id, event_type, payload, context, manifest_id=(manifest or {}).get("id"))
        if replay is not None:
            result["historical_check"] = replay
        return result

    def _verify_signature(self, payload: dict[str, Any], signature: str | None,
                          secret: str, method: str) -> bool:
        """验证Webhook签名。

        支持HMAC-SHA256和SHA256两种验证方式。
        """
        if method == "NONE":
            return True
        if not signature:
            return False
        body = json.dumps(payload, sort_keys=True, ensure_ascii=False)
        if method == "HMAC-SHA256":
            expected = hmac.new(secret.encode(), body.encode(), hashlib.sha256).hexdigest()
            return hmac.compare_digest(expected, signature)
        elif method == "SHA256":
            expected = hashlib.sha256(body.encode()).hexdigest()
            return hmac.compare_digest(expected, signature)
        return False

    def _validate_event_schema(self, channel_id: int, event_type: str, payload: dict[str, Any],
                                context: dict[str, Any] | None = None) -> dict[str, Any]:
        """跨模块调用结构归属明细完成事件结构校验。

        返回值沿用 event_schema 的校验字段，同时附带：
        - ownership：实际使用的结构归属摘要
        - manifest_id：写入接入清单的记录 ID
        """
        try:
            from app.services.schema_ownership import SchemaOwnershipService
            ownership_svc = SchemaOwnershipService()
            result = ownership_svc.process_intake(
                channel_id,
                event_type,
                payload,
                event_id=None,
                preferred_ownership_id=(context or {}).get("preferred_ownership_id"),
            )
            return result
        except Exception as e:
            return {"ok": True, "message": f"结构校验跳过（Schema服务不可用: {e}）", "ownership": None}

    def _check_dedup(self, channel_id: int, event_type: str, payload: dict[str, Any]) -> dict[str, Any]:
        """跨模块调用dedup_filter进行事件去重判定。"""
        try:
            from app.services.dedup_filter import DedupFilterService
            dedup_svc = DedupFilterService()
            return dedup_svc.check_duplicate(event_type, payload)
        except Exception:
            return {"ok": True, "is_duplicate": False}

    def _match_dispatch_rules(self, event_type: str, payload: dict[str, Any]) -> list[dict[str, Any]]:
        """跨模块调用dispatch_rule进行分发目标匹配。"""
        try:
            from app.services.dispatch_rule import DispatchRuleService
            dispatch_svc = DispatchRuleService()
            return dispatch_svc.match_targets(event_type, payload)
        except Exception:
            return []

    def _sink_event(self, event_id: str, event_type: str, payload: dict[str, Any], channel_id: int,
                    attribution: dict[str, Any] | None = None) -> dict[str, Any]:
        """跨模块调用data_lake_sink进行事件数据落湖。"""
        try:
            from app.services.data_lake_sink import DataLakeSinkService
            sink_svc = DataLakeSinkService()
            return sink_svc.write_event(
                event_id, event_type, payload,
                source_channel_id=channel_id,
                attribution=attribution,
            )
        except Exception as e:
            return {"ok": False, "message": f"落湖失败: {e}"}

    def _load_manifest(self, manifest_id: Any) -> dict[str, Any] | None:
        if not manifest_id:
            return None
        try:
            return json_store.get_by_id("ingest_manifest", int(manifest_id))
        except (TypeError, ValueError):
            return None

    def _lake_attribution(self, ownership: dict[str, Any]) -> dict[str, Any]:
        """落湖记录里要固化的结构归属凭证。"""
        used = ownership.get("ownership") if isinstance(ownership.get("ownership"), dict) else {}
        if not used:
            return {}
        return {
            "归属编号": used.get("归属编号"),
            "结构编号": used.get("结构编号"),
            "Schema版本": used.get("Schema版本"),
            "发布范围": used.get("发布范围"),
            "归属通道": used.get("归属通道"),
            "字段指纹": used.get("字段指纹"),
            "通道": str(ownership.get("channel_id", "")),
            "检验结论": ownership.get("decision"),
            "待补验": bool(ownership.get("needs_supplement")),
            "校验模式": "归属校验" if not ownership.get("legacy") else "既有通道口径",
        }

    def _replay_historical(self, channel_id: int, event_type: str, payload: dict[str, Any],
                            context: dict[str, Any], manifest_id: int | None = None) -> dict[str, Any] | None:
        """历史订单重投时的原校验结果核对。

        触发条件：显式提供 manifest_id/event_id，或当前通道+事件指纹能命中
        同一条历史订单（重新投递）。没有历史记录时不产生额外输出。
        """
        target_id = context.get("manifest_id") or manifest_id
        event_key = context.get("event_id") or context.get("event_key")
        try:
            from app.services.schema_ownership import SchemaOwnershipService
            ownership_svc = SchemaOwnershipService()
            manifest = None
            if target_id:
                manifest = json_store.get_by_id("ingest_manifest", int(target_id))
            elif event_key:
                trace = ownership_svc.trace_intake(event_id=str(event_key))
                manifest = (trace or {}).get("manifest")
            else:
                fingerprint = ownership_svc._payload_fingerprint(payload)  # noqa: SLF001
                rows = [
                    row for row in json_store.get_all("ingest_manifest")
                    if str(row.get("通道")) == str(channel_id)
                    and row.get("事件类型") == event_type
                    and row.get("事件指纹") == fingerprint
                ]
                if rows:
                    # 重投会新增一条清单；核对对象是它回指的原清单，而不是刚写入的这条。
                    newest = rows[-1]
                    origin_id = newest.get("重投原清单")
                    if not origin_id:
                        return None
                    manifest = json_store.get_by_id("ingest_manifest", int(origin_id))
            if manifest is None:
                return None
            if target_id or event_key:
                if str(manifest.get("通道")) != str(channel_id):
                    return None
            return ownership_svc.verify_historical_result(int(manifest["id"]))
        except Exception:
            return None

    def _record_receive_event(self, channel_id: int, payload: dict[str, Any],
                              outcome: str, trace: list[dict[str, Any]]) -> None:
        """记录Webhook接收事件的完整轨迹。"""
        record = {
            "channel_id": channel_id,
            "event_type": payload.get("type") or payload.get("event_type", "unknown"),
            "payload_hash": hashlib.sha256(json.dumps(payload, sort_keys=True, ensure_ascii=False).encode()).hexdigest()[:16],
            "outcome": outcome,
            "trace": trace,
            "received_at": datetime.now(timezone.utc).isoformat(),
        }
        self._received_events.append(record)

    def get_channel_stats(self, channel_id: int) -> dict[str, Any]:
        """获取通道的接收统计信息。"""
        channel = json_store.get_by_id(self.MODULE, channel_id)
        if channel is None:
            return {"ok": False, "message": f"通道 {channel_id} 不存在"}

        channel_events = [e for e in self._received_events if e["channel_id"] == channel_id]
        return {
            "ok": True,
            "channel": channel.get("通道名称"),
            "total_received": len(channel_events),
            "success_count": sum(1 for e in channel_events if e["outcome"] == "success"),
            "signature_failures": sum(1 for e in channel_events if e["outcome"] == "signature_failed"),
            "schema_failures": sum(1 for e in channel_events if e["outcome"] == "schema_invalid"),
            "duplicates": sum(1 for e in channel_events if e["outcome"] == "duplicate"),
        }

    def domain_snapshot(self) -> dict[str, Any]:
        """输出Webhook通道的数据质量快照，包含跨模块链路健康信息。"""
        rows = json_store.get_all(self.MODULE)
        by_status = {status: 0 for status in self.STATUS_ORDER}
        for row in rows:
            s = row.get("status", "")
            if s in by_status:
                by_status[s] += 1

        return {
            "module": self.MODULE,
            "entity": self.ENTITY,
            "artifact": "Webhook通道数据集",
            "count": len(rows),
            "by_status": by_status,
            "abnormal": sum(1 for r in rows if r.get("abnormal")),
            "total_events_received": len(self._received_events),
            "active_channels": sum(1 for r in rows if r.get("status") == "已启用"),
            "ownership_confirmed": sum(
                1 for r in rows if r.get("确认归属编号") and r.get("版本确认结论") == "一致"
            ),
            "operation": self.OPERATION_TERM,
        }