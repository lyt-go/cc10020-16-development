"""跨模块数据流管道API：暴露完整业务链路端点。

将领域服务的能力串联为可测试的数据流管道，每条管道对应
一个跨模块数据流链路。
"""
from __future__ import annotations

import json
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

router = APIRouter(prefix="/api/pipeline", tags=["跨模块数据流"])


class WebhookPayload(BaseModel):
    event_type: str = "default_event"
    data: dict[str, Any] = {}
    headers: dict[str, str] | None = None
    signature: str | None = None
    # 历史订单重投时可带上原接入清单/原事件标识，用于核对原校验结果。
    manifest_id: int | None = None
    event_id: str | None = None
    preferred_ownership_id: int | None = None


class BatchExecuteRequest(BaseModel):
    task_id: int
    max_pages: int = 5


class DlqReplayRequest(BaseModel):
    dlq_ids: list[int]


class BackfillExecuteRequest(BaseModel):
    job_id: int


class EndpointTestRequest(BaseModel):
    endpoint_id: int


class TransformRequest(BaseModel):
    rule_id: int
    source_data: list[dict[str, Any]]


class ValidationRequest(BaseModel):
    records: list[dict[str, Any]]
    source_module: str = ""


@router.post("/webhook-to-lake")
def pipeline_webhook_to_lake(channel_id: int, payload: WebhookPayload) -> dict[str, Any]:
    """链路1: Webhook接收 → 结构归属校验 → 去重 → 分发路由 → 数据落湖 → 接入清单。

    完整覆盖：webhook_channel → event_schema_ownership → event_schema
    → dedup_filter → dispatch_rule → data_lake_sink → ingest_manifest。
    """
    from app.services.webhook_channel import WebhookChannelService
    svc = WebhookChannelService()
    result = svc.receive_webhook(
        channel_id=channel_id,
        payload={"type": payload.event_type, **payload.data},
        signature=payload.signature,
        headers=payload.headers,
        intake_context={
            "manifest_id": payload.manifest_id,
            "event_id": payload.event_id,
            "preferred_ownership_id": payload.preferred_ownership_id,
        },
    )
    return result


@router.post("/batch-ingest-to-lake")
def pipeline_batch_ingest_to_lake(request: BatchExecuteRequest) -> dict[str, Any]:
    """链路2: 批量拉取 → 数据转换 → 质量校验 → 数据落湖。

    完整覆盖：batch_ingest → data_transform → data_quality_rule → data_lake_sink。
    """
    from app.services.batch_ingest import BatchIngestService
    svc = BatchIngestService()
    result = svc.execute_batch(task_id=request.task_id, max_pages=request.max_pages)
    return result


@router.post("/dlq-replay")
def pipeline_dlq_replay(request: DlqReplayRequest) -> dict[str, Any]:
    """链路3: 死信队列 → 分析 → 重放 → 追踪。

    完整覆盖：dlq_handler → mq_consumer → call_trace_log。
    """
    from app.services.dlq_handler import DlqHandlerService
    svc = DlqHandlerService()

    if len(request.dlq_ids) == 1 and request.dlq_ids[0] == -1:
        # 全部待处置记录重放
        analysis = svc.analyze_failure_reasons()
        if analysis["total"] == 0:
            return {"ok": False, "message": "无死信记录", "replayed": 0}
        from app.persistence import json_store
        all_dlq = json_store.get_all("dlq_handler")
        pending_ids = [int(r["id"]) for r in all_dlq if r.get("status") in ("草稿", "待处置")]
        return svc.batch_replay(pending_ids)

    return svc.batch_replay(request.dlq_ids)


@router.post("/backfill-execute")
def pipeline_backfill_execute(request: BackfillExecuteRequest) -> dict[str, Any]:
    """链路4: 历史回补 → 水位查询 → 批量拉取 → 数据落湖。

    完整覆盖：backfill_job → incre_cursor → batch_ingest → data_lake_sink。
    """
    from app.services.backfill_job import BackfillJobService
    svc = BackfillJobService()
    result = svc.execute_backfill(job_id=request.job_id)
    return result


@router.post("/endpoint-test")
def pipeline_endpoint_test(request: EndpointTestRequest) -> dict[str, Any]:
    """链路5: 端点连接测试 → 认证验证 → 调用追踪。

    完整覆盖：api_endpoint → api_auth_store → call_trace_log。
    """
    from app.services.api_endpoint import ApiEndpointService
    svc = ApiEndpointService()
    result = svc.test_connection(endpoint_id=request.endpoint_id)
    return result


@router.post("/transform-validate")
def pipeline_transform_validate(request: TransformRequest) -> dict[str, Any]:
    """链路6: 数据转换 → 质量校验 → 结果追踪。

    完整覆盖：data_transform → data_quality_rule → data_lake_sink。
    """
    from app.services.data_transform import DataTransformService
    transform_svc = DataTransformService()
    batch_result = transform_svc.batch_transform(rule_id=request.rule_id, source_records=request.source_data)
    return batch_result


@router.post("/batch-validate")
def pipeline_validate_batch(request: ValidationRequest) -> dict[str, Any]:
    """链路7: 质量批量校验 → 统计报告。

    完整覆盖：data_quality_rule → integration_monitor。
    """
    from app.services.data_quality_rule import DataQualityRuleService
    svc = DataQualityRuleService()
    result = svc.validate_batch(records=request.records, source_module=request.source_module)
    return result


@router.get("/health-summary")
def pipeline_health_summary() -> dict[str, Any]:
    """链路8: 全局监控指标聚合 → 健康度评分。

    完整覆盖：integration_monitor → call_trace_log → dlq_handler → data_lake_sink。
    """
    from app.services.integration_monitor import IntegrationMonitorService
    svc = IntegrationMonitorService()
    return svc.collect_global_metrics()


@router.get("/dedup-stats")
def pipeline_dedup_stats() -> dict[str, Any]:
    """链路9: 去重统计查询。"""
    from app.services.dedup_filter import DedupFilterService
    svc = DedupFilterService()
    return svc.get_dedup_stats()


@router.post("/archive-execute")
def pipeline_archive_execute(policy_id: int) -> dict[str, Any]:
    """链路10: 归档策略执行 → 压缩 → 过期清理。"""
    from app.services.event_archive import EventArchiveService
    svc = EventArchiveService()
    return svc.execute_archive(policy_id=policy_id)


class OwnershipPublishRequest(BaseModel):
    ownership: dict[str, Any]


class OwnershipShareRequest(BaseModel):
    ownership_id: int
    channel_ids: list[str | int] | None = None


class OwnershipVersionConfirmRequest(BaseModel):
    ownership_id: int
    channel_id: int


class OwnershipIntakeRequest(BaseModel):
    channel_id: int
    event_type: str
    data: dict[str, Any]
    preferred_ownership_id: int | None = None


@router.get("/schema-ownership")
def pipeline_schema_ownership(channel_id: int | None = None,
                              event_type: str | None = None) -> dict[str, Any]:
    """链路11: 事件结构归属明细（指定通道时返回只读/可用结论）。"""
    from app.services.schema_ownership import SchemaOwnershipService
    svc = SchemaOwnershipService()
    items = svc.list_ownership_detail(channel_id=channel_id, event_type=event_type)
    return {"ok": True, "items": items, "total": len(items)}


@router.post("/schema-ownership/publish")
def pipeline_schema_ownership_publish(request: OwnershipPublishRequest) -> dict[str, Any]:
    """链路11: 结构登记并发布归属（发布时固化字段快照与适用通道版本）。"""
    from app.services.schema_ownership import SchemaOwnershipService
    svc = SchemaOwnershipService()
    entry, missing = svc.create_entry(request.ownership)
    if entry is None:
        return {"ok": False, "message": f"归属登记失败：{'、'.join(missing)}", "entry": None}
    published, message = svc.run_action(int(entry["id"]), "发布归属")
    return {"ok": published is not None, "message": message, "entry": published}


@router.post("/schema-ownership/share")
def pipeline_schema_ownership_share(request: OwnershipShareRequest) -> dict[str, Any]:
    """链路11: 共享授权切换（授权通道发生变化后，新接入按新范围校验）。"""
    from app.services.schema_ownership import SchemaOwnershipService
    svc = SchemaOwnershipService()
    entry, message = svc.share_with(request.ownership_id, request.channel_ids)
    return {"ok": entry is not None, "message": message, "entry": entry}


@router.post("/schema-ownership/revoke-share")
def pipeline_schema_ownership_revoke(ownership_id: int) -> dict[str, Any]:
    """链路11: 撤销共享授权，结构退回私有只读范围。"""
    from app.services.schema_ownership import SchemaOwnershipService
    svc = SchemaOwnershipService()
    entry, message = svc.revoke_share(ownership_id)
    return {"ok": entry is not None, "message": message, "entry": entry}


@router.post("/schema-ownership/confirm-version")
def pipeline_schema_ownership_confirm_version(
        request: OwnershipVersionConfirmRequest) -> dict[str, Any]:
    """链路11: Webhook通道与结构发布的跨模块版本确认。"""
    from app.services.schema_ownership import SchemaOwnershipService
    svc = SchemaOwnershipService()
    return svc.confirm_channel_version(request.ownership_id, request.channel_id)


@router.post("/schema-ownership/intake")
def pipeline_schema_ownership_intake(request: OwnershipIntakeRequest) -> dict[str, Any]:
    """链路11: 单笔接入的结构归属校验（结论写入接入清单/待补验记录）。"""
    from app.services.schema_ownership import SchemaOwnershipService
    svc = SchemaOwnershipService()
    return svc.process_intake(
        request.channel_id,
        request.event_type,
        {"type": request.event_type, **request.data},
        preferred_ownership_id=request.preferred_ownership_id,
    )


@router.get("/ingest-manifest/{manifest_id}/trace")
def pipeline_ingest_trace(manifest_id: int, event_id: str | None = None) -> dict[str, Any]:
    """链路12: 沿接入清单回查实际使用的结构、落湖分区与待补验记录。"""
    from app.services.schema_ownership import SchemaOwnershipService
    svc = SchemaOwnershipService()
    trace = svc.trace_intake(manifest_id=manifest_id, event_id=event_id)
    if trace is None:
        raise HTTPException(status_code=404, detail=f"接入清单 {manifest_id} 不存在")
    return {"ok": True, **trace}


@router.get("/ingest-manifest/{manifest_id}/verify")
def pipeline_ingest_verify(manifest_id: int) -> dict[str, Any]:
    """链路12: 授权切换后核对历史接入的原校验结果。"""
    from app.services.schema_ownership import SchemaOwnershipService
    svc = SchemaOwnershipService()
    result = svc.verify_historical_result(manifest_id)
    if result is None:
        raise HTTPException(status_code=404, detail=f"接入清单 {manifest_id} 不存在")
    return {"ok": True, **result}