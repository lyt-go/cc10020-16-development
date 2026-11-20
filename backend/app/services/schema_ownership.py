"""事件结构归属领域服务：结构发布、通道授权、字段归属校验与接入结论编排。

背景：事件结构在多个接入通道之间发布/共享时，原来只有"字段定义"，
没有"这份结构归谁、可以被哪些通道使用"的凭证，导致通道 A 的共享结构
可能被未授权通道借道放行，也无法回查某笔接入当时实际用了哪份结构。

本服务提供：
- 事件结构归属明细（私有/共享、归属通道、授权通道、字段快照、版本基线）
- 通道字段归属校验（按发布归属判定通道是否有权使用结构）
- 共享授权切换（授权/撤销）与只读查看口径
- 跨模块版本确认（Webhook通道版本 vs 结构发布版本）
- 接入校验编排：校验结论写入接入清单，必要时生成待补验记录

校验口径与 `event_schema.EventSchemaService` 既有实现保持一致，
历史数据重放时直接使用发布时固化的字段快照，不再按当前授权重新解析。
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any

from app.services.json_module import JsonModuleService
from app.services.schema_validation import (
    normalize_field_rows,
    snapshot_hash,
    validate_field_snapshot,
)

OWNERSHIP_MODULE = "event_schema_ownership"
MANIFEST_MODULE = "ingest_manifest"
PENDING_MODULE = "pending_revalidation"

SCOPE_PRIVATE = "私有"
SCOPE_SHARED = "共享"

DECISION_PASS = "通过"
DECISION_PENDING = "待补验"
DECISION_REJECT = "拒绝"


class SchemaOwnershipService(JsonModuleService):
    MODULE = OWNERSHIP_MODULE
    ENTITY = "结构归属"
    OPERATION_TERM = "归属校验"
    LIST_FIELDS = [
        "归属编号", "结构编号", "事件类型", "归属通道", "发布范围",
        "授权通道", "Schema版本", "适用通道版本",
    ]
    REQUIRED_FIELDS = ["结构编号", "事件类型", "归属通道"]
    STATUS_ORDER = ["草稿", "已发布", "已停用"]
    ACTION_RULES = {
        "发布归属": "已发布",
        "授权共享": "已发布",
        "撤销共享": "已发布",
        "停用归属": "已停用",
    }
    NEGATIVE_ACTIONS = ["停用归属"]

    # ── 登记与动作 ──────────────────────────────────────────────
    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [
            field for field in self.REQUIRED_FIELDS
            if not str(values.get(field) or "").strip()
        ]
        if missing:
            return None, missing

        channel_id = self._to_channel_id(values.get("归属通道"))
        channel = self._get_channel(channel_id)
        if channel is None:
            return None, [f"归属通道 {values.get('归属通道')} 不存在"]

        structure_no = str(values.get("结构编号") or "").strip()
        event_type = str(values.get("事件类型") or "").strip()
        schema_version = str(values.get("Schema版本") or "").strip()
        scope = str(values.get("发布范围") or SCOPE_PRIVATE).strip()
        if scope not in (SCOPE_PRIVATE, SCOPE_SHARED):
            return None, [f"发布范围只能是「{SCOPE_PRIVATE}」或「{SCOPE_SHARED}」"]

        fields = self._resolve_field_snapshot(values, structure_no, event_type, channel_id)
        if not fields:
            return None, [f"结构 {structure_no} 下没有可固化的已生效字段定义"]

        rows = self.rows()
        if any(
            row.get("结构编号") == structure_no
            and row.get("事件类型") == event_type
            and str(row.get("归属通道")) == str(channel_id)
            and row.get("status") == "已发布"
            for row in rows
        ):
            return None, [f"通道 {channel_id} 已发布结构 {structure_no} 的归属明细，请先停用旧归属"]

        authorized = self._normalize_channels(values.get("授权通道"))
        if scope == SCOPE_PRIVATE:
            authorized = [str(channel_id)]
        else:
            unknown = [
                cid for cid in authorized
                if cid != str(channel_id) and self._get_channel(int(cid)) is None
            ]
            if unknown:
                return None, [f"授权通道不存在：{'、'.join(unknown)}"]
            authorized = sorted({str(channel_id), *authorized}, key=str)

        max_id = max((int(row.get("id", 0)) for row in rows), default=0)
        entry: dict[str, Any] = {
            "归属编号": str(values.get("归属编号") or "").strip() or f"OWN-{max_id + 1:04d}",
            "结构编号": structure_no,
            "事件类型": event_type,
            "归属通道": str(channel_id),
            "归属通道名称": channel.get("通道名称", ""),
            "发布范围": scope,
            "授权通道": ",".join(authorized),
            "Schema版本": schema_version
            or str(max((f.get("schema_version") or "" for f in fields), default="")),
            "适用通道版本": str(values.get("适用通道版本") or channel.get("通道版本", "") or ""),
            "字段快照": fields,
            "字段指纹": snapshot_hash(fields),
            "版本确认": [],
            "发布状态": self.STATUS_ORDER[0],
            "只读": False,
            "status": self.STATUS_ORDER[0],
            "pending": True,
            "abnormal": False,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        self._store().insert(self.MODULE, entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = self.get_entry(entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
        if action not in self.ACTION_RULES:
            return None, f"动作「{action}」不属于{self.MODULE}可执行范围"

        if action == "发布归属":
            if entry.get("status") == "已停用":
                return None, "已停用的归属不能直接发布，请重新登记"
            if not entry.get("字段快照"):
                return None, "归属明细缺少字段快照，无法发布"
            channel = self._get_channel(int(entry.get("归属通道") or 0))
            if channel is None:
                return None, f"归属通道 {entry.get('归属通道')} 不存在"
            updated = self._store().update(self.MODULE, entry_id, {
                "发布状态": "已发布",
                "status": "已发布",
                "pending": False,
                "abnormal": False,
                "effective_at": datetime.now(timezone.utc).isoformat(),
            })
            return updated, "结构归属已发布"
        if action == "授权共享":
            return self.share_with(entry_id, self._normalize_channels(entry.get("授权通道")))
        if action == "撤销共享":
            return self.revoke_share(entry_id)
        # 停用归属
        updated = self._store().update(self.MODULE, entry_id, {
            "发布状态": "已停用",
            "status": "已停用",
            "pending": False,
            "abnormal": True,
        })
        return updated, "结构归属已停用"

    # ── 共享授权切换 ────────────────────────────────────────────
    def share_with(self, ownership_id: int, channel_ids: list[int | str] | None = None) -> tuple[dict[str, Any] | None, str]:
        entry = self.get_entry(ownership_id)
        if entry is None:
            return None, f"{self.ENTITY} {ownership_id} 不存在或已归档"
        authorized = self._normalize_channels(channel_ids if channel_ids is not None else entry.get("授权通道"))
        owner = str(entry.get("归属通道"))
        authorized = [owner, *[cid for cid in authorized if cid != owner]]
        for cid in authorized:
            if str(cid) != owner and self._get_channel(int(cid)) is None:
                return None, f"授权通道 {cid} 不存在"
        updated = self._store().update(self.MODULE, ownership_id, {
            "发布范围": SCOPE_SHARED,
            "授权通道": ",".join(dict.fromkeys(authorized)),
            # 重新授权后清掉撤销留痕，避免下次拒单提示指向已恢复的共享版。
            "原授权通道": "",
            "status": "已发布" if entry.get("status") == "已发布" else entry.get("status"),
            "发布状态": "已发布" if entry.get("status") == "已发布" else entry.get("发布状态"),
        })
        return updated, f"已切换为共享，授权通道：{','.join(dict.fromkeys(authorized))}"

    def revoke_share(self, ownership_id: int) -> tuple[dict[str, Any] | None, str]:
        entry = self.get_entry(ownership_id)
        if entry is None:
            return None, f"{self.ENTITY} {ownership_id} 不存在或已归档"
        owner = str(entry.get("归属通道"))
        # 撤销后的原授权通道留痕：新订单被拒时可以指向"哪份共享版被收回"。
        revoked = [
            cid for cid in self._normalize_channels(entry.get("授权通道"))
            if cid != owner
        ]
        history = list(entry.get("共享变更记录") or [])
        history.append({
            "action": "撤销共享",
            "channel_ids": revoked,
            "at": datetime.now(timezone.utc).isoformat(),
        })
        updated = self._store().update(self.MODULE, ownership_id, {
            "发布范围": SCOPE_PRIVATE,
            "授权通道": owner,
            "原授权通道": ",".join(revoked),
            "共享变更记录": history,
            "status": "已发布" if entry.get("status") == "已发布" else entry.get("status"),
            "发布状态": "已发布" if entry.get("status") == "已发布" else entry.get("发布状态"),
        })
        return updated, f"已撤销共享授权，结构仅归属通道 {owner} 可使用"

    # ── 归属明细与只读口径 ──────────────────────────────────────
    def list_ownership_detail(
        self,
        channel_id: int | None = None,
        event_type: str | None = None,
    ) -> list[dict[str, Any]]:
        """输出事件结构归属明细。

        未列进共享范围的结构对其他通道只读：可查看归属与字段快照，
        但 `可使用` 为 False，且不能用于接入校验。
        """
        rows = self.rows()
        if event_type:
            rows = [row for row in rows if str(row.get("事件类型")) == event_type]
        return [self.describe_ownership(row, channel_id) for row in rows]

    def _store(self):
        from app import persistence
        return persistence.json_store

    def describe_ownership(self, entry: dict[str, Any], viewer_channel_id: int | None = None) -> dict[str, Any]:
        detail = dict(entry)
        owner = str(entry.get("归属通道"))
        if viewer_channel_id is None:
            detail["可使用"] = entry.get("status") == "已发布"
            detail["只读"] = False
            detail["授权结论"] = "未指定查看通道，仅展示明细"
            return detail

        viewer = str(viewer_channel_id)
        authorized = self._normalize_channels(entry.get("授权通道"))
        is_owner = viewer == owner
        can_use = (
            entry.get("status") == "已发布"
            and (is_owner or (entry.get("发布范围") == SCOPE_SHARED and viewer in authorized))
        )
        detail["可使用"] = can_use
        detail["只读"] = not can_use
        if is_owner:
            detail["授权结论"] = "归属通道，私有/共享范围均可用"
        elif entry.get("发布范围") == SCOPE_SHARED and viewer in authorized:
            detail["授权结论"] = "在共享授权范围内"
        elif entry.get("发布范围") == SCOPE_SHARED:
            detail["授权结论"] = "结构为共享版，但当前通道未获授权，只读查看"
        else:
            detail["授权结论"] = "私有结构，仅归属通道可用，其他通道只读查看"
        return detail

    def get_ownership_detail(self, ownership_id: int, viewer_channel_id: int | None = None) -> dict[str, Any] | None:
        entry = self.get_entry(ownership_id)
        if entry is None:
            return None
        return self.describe_ownership(entry, viewer_channel_id)

    # ── 解析候选结构 ────────────────────────────────────────────
    def resolve_candidates(self, channel_id: int, event_type: str) -> dict[str, Any]:
        """按发布归属解析通道可使用的结构。

        返回：
        - candidates：可用候选（归属通道私有版优先，然后共享授权版）
        - blocked_shares：同事件类型的共享版，但当前通道未获授权
        - version_mismatch：有权使用但通道版本不一致
        """
        rows = self.rows()
        published = [
            row for row in rows
            if row.get("status") == "已发布"
            and str(row.get("事件类型")) == event_type
        ]
        viewer = str(channel_id)
        channel = self._get_channel(channel_id)
        channel_version = str((channel or {}).get("通道版本", "") or "")

        own: list[dict[str, Any]] = []
        shared: list[dict[str, Any]] = []
        blocked: list[dict[str, Any]] = []
        revoked: list[dict[str, Any]] = []
        other_private: list[dict[str, Any]] = []
        version_blocked: list[dict[str, Any]] = []

        for row in published:
            owner = str(row.get("归属通道"))
            authorized = self._normalize_channels(row.get("授权通道"))
            if owner == viewer:
                own.append(row)
                continue
            if row.get("发布范围") == SCOPE_SHARED and viewer in authorized:
                shared.append(row)
                continue
            if row.get("发布范围") == SCOPE_SHARED:
                blocked.append(row)
                continue
            previously_shared_with_viewer = viewer in self._normalize_channels(row.get("原授权通道"))
            if previously_shared_with_viewer:
                revoked.append(row)
                continue
            other_private.append(row)

        blocked.sort(key=lambda item: int(item.get("id", 0)))

        def _version_ok(row: dict[str, Any]) -> bool:
            expected = str(row.get("适用通道版本", "") or "")
            return not expected or not channel_version or expected == channel_version

        usable: list[dict[str, Any]] = []
        for row in sorted(own, key=lambda item: (item.get("发布范围") != SCOPE_PRIVATE, int(item.get("id", 0)))):
            if _version_ok(row):
                usable.append(row)
            else:
                version_blocked.append(row)
        for row in sorted(shared, key=lambda item: int(item.get("id", 0))):
            if _version_ok(row):
                usable.append(row)
            else:
                version_blocked.append(row)

        return {
            "channel_id": channel_id,
            "event_type": event_type,
            "channel_version": channel_version,
            "candidates": usable,
            "blocked_shares": blocked,
            "revoked_shares": revoked,
            "readonly_others": other_private,
            "version_mismatch": version_blocked,
            "published": published,
        }

    # ── 接入校验编排 ────────────────────────────────────────────
    def process_intake(
        self,
        channel_id: int,
        event_type: str,
        payload: dict[str, Any],
        *,
        event_id: str | None = None,
        preferred_ownership_id: int | None = None,
    ) -> dict[str, Any]:
        """对一笔接入执行结构归属校验，并写入接入清单/待补验记录。

        兼容口径：该事件类型没有任何"已发布归属"时，回落到既有
        `EventSchemaService.validate_event` 的通道 Schema 校验。
        """
        resolution = self.resolve_candidates(channel_id, event_type)
        published = resolution["published"]

        if not published:
            return self._legacy_intake(channel_id, event_type, payload, event_id)

        candidates = list(resolution["candidates"])
        rejected_candidates: list[dict[str, Any]] = []

        if preferred_ownership_id is not None:
            preferred = next(
                (row for row in candidates if int(row.get("id", 0)) == int(preferred_ownership_id)),
                None,
            )
            if preferred is None:
                reason = self._explain_unavailable(preferred_ownership_id, resolution)
                return self._reject_intake(
                    channel_id, event_type, payload, event_id,
                    reason=reason, attempts=[], resolution=resolution,
                )
            candidates = [preferred]

        if not candidates:
            if resolution["version_mismatch"]:
                reason = (
                    f"结构版本确认失败：通道版本 {resolution['channel_version']} 与结构适用版本"
                    f" {resolution['version_mismatch'][0].get('适用通道版本')} 不一致"
                )
            elif resolution["blocked_shares"]:
                reason = (
                    f"通道 {channel_id} 未获共享授权，不能借用其他通道的结构 "
                    f"{resolution['blocked_shares'][0].get('归属编号')} 放行"
                )
            elif resolution.get("revoked_shares"):
                revoked = resolution["revoked_shares"][0]
                reason = (
                    f"通道 {channel_id} 的共享授权已撤销：结构 {revoked.get('归属编号')}"
                    f"（归属通道 {revoked.get('归属通道')}）退回私有，不再放行新订单"
                )
            elif resolution.get("readonly_others"):
                readonly = resolution["readonly_others"][0]
                reason = (
                    f"通道 {channel_id} 未获共享授权，结构 {readonly.get('归属编号')}"
                    f"（归属通道 {readonly.get('归属通道')}）只读查看，不能用于放行"
                )
            else:
                reason = f"通道 {channel_id} 没有事件类型 {event_type} 的可用结构归属"
            return self._reject_intake(
                channel_id, event_type, payload, event_id,
                reason=reason, attempts=[], resolution=resolution,
            )

        attempts: list[dict[str, Any]] = []
        used: dict[str, Any] | None = None
        used_validation: dict[str, Any] | None = None
        for row in candidates:
            validation = validate_field_snapshot(row.get("字段快照") or [], payload)
            attempt = {
                "ownership_id": row.get("id"),
                "归属编号": row.get("归属编号"),
                "结构编号": row.get("结构编号"),
                "发布范围": row.get("发布范围"),
                "Schema版本": row.get("Schema版本"),
                "ok": validation["ok"],
                "message": validation["message"],
                "missing_fields": validation["missing_fields"],
                "type_errors": validation["type_errors"],
                "validation": validation,
            }
            attempts.append(attempt)
            if validation["ok"]:
                used = row
                used_validation = validation
                break
            rejected_candidates.append(attempt)

        if used is None:
            missing_only = [
                attempt for attempt in rejected_candidates
                if attempt["missing_fields"] and not attempt["type_errors"]
            ]
            if missing_only:
                chosen = missing_only[0]
                reason = f"结构字段缺失，进入待补验：{chosen['message']}"
                return self._pending_intake(
                    channel_id, event_type, payload, event_id,
                    reason=reason, attempt=chosen, resolution=resolution,
                )
            chosen = rejected_candidates[0]
            return self._reject_intake(
                channel_id, event_type, payload, event_id,
                reason=f"结构校验失败：{chosen['message']}",
                attempts=attempts, resolution=resolution, used_failure=chosen,
            )

        # 校验通过：如果归属通道的私有版对同一笔数据仍有缺口，生成待补验记录。
        supplement_sources = [
            attempt for attempt in rejected_candidates
            if attempt["missing_fields"] and not attempt["type_errors"]
            and self._is_private_owner_attempt(attempt, channel_id)
        ]

        version_confirmation = self.confirm_channel_version(int(used.get("id", 0)), channel_id)
        manifest = self._write_manifest(
            channel_id=channel_id,
            event_type=event_type,
            payload=payload,
            event_id=event_id,
            decision=DECISION_PASS,
            admitted=True,
            ownership=used,
            validation=used_validation or {},
            attempts=attempts,
            version_confirmation=version_confirmation,
            needs_supplement=bool(supplement_sources),
            note=(
                f"共享结构 {used.get('归属编号')} 放行，私有版字段缺口待补验"
                if supplement_sources else "结构归属校验通过"
            ),
        )
        if supplement_sources:
            self._write_pending(
                manifest=manifest,
                channel_id=channel_id,
                event_type=event_type,
                ownership=used,
                supplement_sources=supplement_sources,
                event_id=event_id,
            )

        return {
            "ok": True,
            "admitted": True,
            "decision": DECISION_PASS,
            "message": "结构归属校验通过",
            "legacy": False,
            "ownership": self._ownership_summary(used),
            "ownership_id": used.get("id"),
            "validation": used_validation,
            "attempts": attempts,
            "blocked_shares": [
                self._ownership_summary(row) for row in resolution["blocked_shares"]
            ],
            "needs_supplement": bool(supplement_sources),
            "manifest_id": manifest["id"],
            "attribution": self._attribution(manifest, used),
        }

    def _legacy_intake(self, channel_id: int, event_type: str, payload: dict[str, Any],
                       event_id: str | None) -> dict[str, Any]:
        from app.services.event_schema import EventSchemaService
        validation = EventSchemaService().validate_event(channel_id, event_type, payload)
        manifest = self._write_manifest(
            channel_id=channel_id,
            event_type=event_type,
            payload=payload,
            event_id=event_id,
            decision=DECISION_PASS if validation["ok"] else DECISION_REJECT,
            admitted=bool(validation["ok"]),
            ownership=None,
            validation=validation,
            attempts=[],
            version_confirmation=None,
            needs_supplement=False,
            note="未发布结构归属，沿用既有通道 Schema 校验口径",
            extra={"校验模式": "既有通道口径"},
        )
        return {
            "ok": bool(validation["ok"]),
            "admitted": bool(validation["ok"]),
            "decision": DECISION_PASS if validation["ok"] else DECISION_REJECT,
            "message": validation.get("message", ""),
            "legacy": True,
            "ownership": None,
            "ownership_id": None,
            "validation": validation,
            "attempts": [],
            "blocked_shares": [],
            "needs_supplement": False,
            "manifest_id": manifest["id"],
            "attribution": None,
        }

    def _reject_intake(self, channel_id: int, event_type: str, payload: dict[str, Any],
                       event_id: str | None, *, reason: str,
                       attempts: list[dict[str, Any]],
                       resolution: dict[str, Any],
                       used_failure: dict[str, Any] | None = None) -> dict[str, Any]:
        manifest = self._write_manifest(
            channel_id=channel_id,
            event_type=event_type,
            payload=payload,
            event_id=event_id,
            decision=DECISION_REJECT,
            admitted=False,
            ownership=None,
            validation=(used_failure or {}).get("validation", {}),
            attempts=attempts,
            version_confirmation=None,
            needs_supplement=False,
            note=reason,
            extra={
                "拒绝原因": reason,
                "未授权共享": [
                    self._ownership_summary(row) for row in resolution.get("blocked_shares", [])
                ],
            },
        )
        return {
            "ok": False,
            "admitted": False,
            "decision": DECISION_REJECT,
            "message": reason,
            "legacy": False,
            "ownership": None,
            "ownership_id": (used_failure or {}).get("ownership_id"),
            "validation": (used_failure or {}).get("validation", {}),
            "attempts": attempts,
            "blocked_shares": [
                self._ownership_summary(row) for row in resolution.get("blocked_shares", [])
            ],
            "needs_supplement": False,
            "manifest_id": manifest["id"],
            "attribution": None,
        }

    def _pending_intake(self, channel_id: int, event_type: str, payload: dict[str, Any],
                        event_id: str | None, *, reason: str,
                        attempt: dict[str, Any],
                        resolution: dict[str, Any]) -> dict[str, Any]:
        ownership_id = attempt.get("ownership_id")
        ownership = self.get_entry(int(ownership_id)) if ownership_id else None
        manifest = self._write_manifest(
            channel_id=channel_id,
            event_type=event_type,
            payload=payload,
            event_id=event_id,
            decision=DECISION_PENDING,
            admitted=False,
            ownership=ownership,
            validation=attempt.get("validation", {}),
            attempts=[attempt],
            version_confirmation=None,
            needs_supplement=True,
            note=reason,
            extra={"待补验字段": attempt.get("missing_fields", [])},
        )
        self._write_pending(
            manifest=manifest,
            channel_id=channel_id,
            event_type=event_type,
            ownership=ownership,
            supplement_sources=[attempt],
            event_id=event_id,
        )
        return {
            "ok": False,
            "admitted": False,
            "decision": DECISION_PENDING,
            "message": reason,
            "legacy": False,
            "ownership": self._ownership_summary(ownership) if ownership else None,
            "ownership_id": ownership_id,
            "validation": attempt.get("validation", {}),
            "attempts": [attempt],
            "blocked_shares": [
                self._ownership_summary(row) for row in resolution.get("blocked_shares", [])
            ],
            "needs_supplement": True,
            "manifest_id": manifest["id"],
            "attribution": None,
        }

    # ── 跨模块版本确认 ──────────────────────────────────────────
    def confirm_channel_version(self, ownership_id: int, channel_id: int) -> dict[str, Any]:
        """Webhook通道与结构发布的跨模块版本确认。

        结论写入归属明细的版本确认记录，并回写到通道配置，
        供接入清单与落湖记录引用。
        """
        ownership = self.get_entry(ownership_id)
        channel = self._get_channel(channel_id)
        if ownership is None:
            return {"ok": False, "message": f"结构归属 {ownership_id} 不存在"}
        if channel is None:
            return {"ok": False, "message": f"通道 {channel_id} 不存在"}
        if ownership.get("status") != "已发布":
            return {"ok": False, "message": f"结构归属未发布（当前 {ownership.get('status')}）"}

        expected = str(ownership.get("适用通道版本", "") or "")
        actual = str(channel.get("通道版本", "") or "")
        matched = not expected or not actual or expected == actual
        conclusion = {
            "channel_id": str(channel_id),
            "通道版本": actual,
            "结构版本": ownership.get("Schema版本", ""),
            "适用通道版本": expected,
            "结论": "一致" if matched else "版本不一致",
            "confirmed_at": datetime.now(timezone.utc).isoformat(),
        }
        confirmations = [
            item for item in (ownership.get("版本确认") or [])
            if str(item.get("channel_id")) != str(channel_id)
            or item.get("适用通道版本") != expected
        ]
        confirmations.append(conclusion)
        self._store().update(self.MODULE, ownership_id, {"版本确认": confirmations})

        if self._channel_uses_json_store(channel_id):
            self._store().update("webhook_channel", channel_id, {
                "确认归属编号": ownership.get("归属编号"),
                "确认结构版本": ownership.get("Schema版本"),
                "版本确认时间": conclusion["confirmed_at"],
                "版本确认结论": conclusion["结论"],
            })

        if not matched:
            return {
                "ok": False,
                "message": f"通道版本 {actual} 与结构适用版本 {expected} 不一致",
                "conclusion": conclusion,
            }
        return {"ok": True, "message": "通道与结构版本确认一致", "conclusion": conclusion}

    # ── 回查与重放 ──────────────────────────────────────────────
    def trace_intake(self, manifest_id: int | None = None,
                     event_id: str | None = None) -> dict[str, Any] | None:
        """沿一笔接入回查：接入清单 → 实际使用的结构归属 → 落湖落点 → 待补验。"""
        manifest = None
        if manifest_id is not None:
            manifest = self._store().get_by_id(MANIFEST_MODULE, manifest_id)
        elif event_id:
            rows = self._store().get_all(MANIFEST_MODULE)
            matches = [row for row in rows if str(row.get("事件标识")) == str(event_id)]
            manifest = matches[-1] if matches else None
        if manifest is None:
            return None

        ownership = None
        if manifest.get("归属ID"):
            ownership = self.get_entry(int(manifest["归属ID"]))

        pending = [
            row for row in self._store().get_all(PENDING_MODULE)
            if str(row.get("关联清单")) == str(manifest.get("id"))
        ]

        lake_records: list[dict[str, Any]] = []
        try:
            from app.services.data_lake_sink import DataLakeSinkService
            lake_event_id = str(
                manifest.get("落湖事件标识") or manifest.get("事件标识") or ""
            )
            if lake_event_id:
                lake_records = DataLakeSinkService().find_events(
                    event_id=lake_event_id,
                    event_type=str(manifest.get("事件类型") or ""),
                )
        except Exception:
            lake_records = []

        return {
            "manifest": manifest,
            "ownership": self.describe_ownership(ownership, self._to_channel_id(manifest.get("通道"))) if ownership else None,
            "pending_revalidations": pending,
            "lake_records": lake_records,
            "conclusion": {
                "校验结论": manifest.get("校验结论"),
                "实际使用结构": ownership.get("归属编号") if ownership else manifest.get("归属编号", ""),
                "Schema版本": manifest.get("Schema版本", ""),
                "发布范围": manifest.get("发布范围", ""),
                "字段指纹": manifest.get("字段指纹", ""),
                "落湖分区": manifest.get("落湖分区", ""),
                "待补验": bool(pending),
            },
        }

    def verify_historical_result(self, manifest_id: int) -> dict[str, Any] | None:
        """核对历史接入的原校验结果。

        直接使用接入清单里固化的字段快照与事件快照重新执行同一套校验口径，
        不依赖当前共享授权状态，因此授权切换后仍能回查原结论。
        """
        manifest = self._store().get_by_id(MANIFEST_MODULE, manifest_id)
        if manifest is None:
            return None

        snapshot = manifest.get("字段快照")
        payload = manifest.get("事件快照", {})
        original = str(manifest.get("校验结论", ""))
        if snapshot:
            recheck = validate_field_snapshot(snapshot, payload)
            recheck_conclusion = DECISION_PASS if recheck["ok"] else DECISION_PENDING
        else:
            recheck = None
            recheck_conclusion = original

        ownership = self.get_entry(int(manifest["归属ID"])) if manifest.get("归属ID") else None
        current_usable = None
        if ownership is not None:
            detail = self.describe_ownership(ownership, self._to_channel_id(manifest.get("通道")))
            current_usable = detail["可使用"]

        consistent = True
        if original == DECISION_PASS:
            consistent = recheck is None or recheck["ok"]
        elif original in (DECISION_REJECT, DECISION_PENDING):
            consistent = recheck is None or not recheck["ok"]

        return {
            "manifest_id": manifest_id,
            "事件标识": manifest.get("事件标识"),
            "原校验结论": original,
            "原结论时间": manifest.get("结论时间"),
            "原归属编号": manifest.get("归属编号"),
            "原Schema版本": manifest.get("Schema版本"),
            "原发布范围": manifest.get("发布范围"),
            "原字段指纹": manifest.get("字段指纹"),
            "字段快照": snapshot,
            "复核校验": recheck,
            "复核结论": recheck_conclusion,
            "与原结论一致": consistent,
            "当前授权仍可使用": current_usable,
            "授权已变更": current_usable is False,
            "落湖分区": manifest.get("落湖分区", ""),
        }

    # ── 内部：清单/待补验 ──────────────────────────────────────
    def _write_manifest(
        self,
        *,
        channel_id: int,
        event_type: str,
        payload: dict[str, Any],
        event_id: str | None,
        decision: str,
        admitted: bool,
        ownership: dict[str, Any] | None,
        validation: dict[str, Any],
        attempts: list[dict[str, Any]],
        version_confirmation: dict[str, Any] | None,
        needs_supplement: bool,
        note: str,
        extra: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        rows = self._store().get_all(MANIFEST_MODULE)
        max_id = max((int(row.get("id", 0)) for row in rows), default=0)
        event_key = event_id or self._payload_fingerprint(payload)
        fingerprint = self._payload_fingerprint(payload)
        original = next(
            (
                row for row in rows
                if str(row.get("通道")) == str(channel_id)
                and row.get("事件类型") == event_type
                and row.get("事件指纹") == fingerprint
            ),
            None,
        )
        row: dict[str, Any] = {
            "清单编号": f"ING-{max_id + 1:05d}",
            "通道": str(channel_id),
            "事件类型": event_type,
            "事件标识": event_key,
            "事件指纹": fingerprint,
            "事件快照": payload,
            "归属ID": ownership.get("id") if ownership else None,
            "归属编号": ownership.get("归属编号", "") if ownership else "",
            "结构编号": ownership.get("结构编号", "") if ownership else "",
            "Schema版本": ownership.get("Schema版本", "") if ownership else "",
            "发布范围": ownership.get("发布范围", "") if ownership else "",
            "字段快照": ownership.get("字段快照", []) if ownership else [],
            "字段指纹": ownership.get("字段指纹", "") if ownership else "",
            "校验结论": decision,
            "待补验": needs_supplement,
            "通道版本": (self._get_channel(channel_id) or {}).get("通道版本", ""),
            "版本确认": (version_confirmation or {}).get("conclusion"),
            "校验明细": attempts,
            "缺失字段": validation.get("missing_fields", []),
            "类型错误": validation.get("type_errors", []),
            "结论说明": note,
            "结论时间": datetime.now(timezone.utc).isoformat(),
            "去重结果": "",
            "落湖分区": "",
            "落湖文件": "",
            "重投原清单": original.get("id") if original else None,
            "status": decision,
            "pending": decision != DECISION_PASS,
            "abnormal": decision == DECISION_REJECT,
        }
        if extra:
            row.update(extra)
        self._store().insert(MANIFEST_MODULE, row)
        return row

    def _write_pending(
        self,
        *,
        manifest: dict[str, Any],
        channel_id: int,
        event_type: str,
        ownership: dict[str, Any] | None,
        supplement_sources: list[dict[str, Any]],
        event_id: str | None,
    ) -> dict[str, Any]:
        rows = self._store().get_all(PENDING_MODULE)
        max_id = max((int(row.get("id", 0)) for row in rows), default=0)
        missing: list[str] = []
        for attempt in supplement_sources:
            for field in attempt.get("missing_fields", []):
                if field not in missing:
                    missing.append(field)
        required_by = [
            {
                "归属编号": attempt.get("归属编号"),
                "结构编号": attempt.get("结构编号"),
                "Schema版本": attempt.get("Schema版本"),
                "缺失字段": attempt.get("missing_fields", []),
            }
            for attempt in supplement_sources
        ]
        row: dict[str, Any] = {
            "补验编号": f"PEND-{max_id + 1:05d}",
            "关联清单": str(manifest.get("id")),
            "通道": str(channel_id),
            "事件类型": event_type,
            "事件标识": event_id or manifest.get("事件标识", ""),
            "实际使用归属": ownership.get("归属编号", "") if ownership else "",
            "实际使用范围": ownership.get("发布范围", "") if ownership else "",
            "结构编号": ownership.get("结构编号", "") if ownership else "",
            "Schema版本": ownership.get("Schema版本", "") if ownership else "",
            "缺失字段": ",".join(missing),
            "私有版要求": required_by,
            "补验说明": f"共享结构已放行，归属通道私有版要求字段：{','.join(missing) or '无'}",
            "status": "待补验",
            "pending": True,
            "abnormal": True,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        self._store().insert(PENDING_MODULE, row)
        return row

    # ── 内部：辅助 ──────────────────────────────────────────────
    def _resolve_field_snapshot(
        self,
        values: dict[str, Any],
        structure_no: str,
        event_type: str,
        channel_id: int,
    ) -> list[dict[str, Any]]:
        raw = values.get("字段快照")
        if isinstance(raw, str) and raw.strip():
            try:
                raw = json.loads(raw)
            except json.JSONDecodeError:
                raw = None
        if isinstance(raw, list) and raw:
            normalized = []
            for item in raw:
                if isinstance(item, dict) and item.get("字段名称"):
                    normalized.append(item)
            if normalized:
                return normalize_field_rows(normalized)
            return normalize_field_rows(raw)

        schema_rows = self._store().get_all("event_schema")
        matched = [
            row for row in schema_rows
            if row.get("status") == "已生效"
            and str(row.get("事件类型")) == event_type
            and (
                str(row.get("结构编号")) == structure_no
                and str(row.get("所属通道")) in (str(channel_id), "", "全通道")
                or (not structure_no and str(row.get("所属通道")) == str(channel_id))
            )
        ]
        if not matched:
            matched = [
                row for row in schema_rows
                if row.get("status") == "已生效"
                and str(row.get("事件类型")) == event_type
                and str(row.get("所属通道")) == str(channel_id)
            ]
        return normalize_field_rows(matched)

    def _is_private_owner_attempt(self, attempt: dict[str, Any], channel_id: int) -> bool:
        if attempt.get("发布范围") != SCOPE_PRIVATE:
            return False
        row = self.get_entry(int(attempt.get("ownership_id") or 0))
        return row is not None and str(row.get("归属通道")) == str(channel_id)

    def _explain_unavailable(self, ownership_id: int, resolution: dict[str, Any]) -> str:
        entry = self.get_entry(ownership_id)
        if entry is None:
            return f"结构归属 {ownership_id} 不存在"
        if entry.get("status") != "已发布":
            return f"结构归属 {entry.get('归属编号')} 未发布（当前 {entry.get('status')}）"
        for blocked in resolution["blocked_shares"]:
            if int(blocked.get("id", 0)) == ownership_id:
                return (
                    f"通道 {resolution['channel_id']} 未获结构 {blocked.get('归属编号')} 的共享授权，只读查看"
                )
        for blocked in resolution.get("revoked_shares", []):
            if int(blocked.get("id", 0)) == ownership_id:
                return (
                    f"结构 {blocked.get('归属编号')} 的共享授权已撤销，通道 "
                    f"{resolution['channel_id']} 只读查看"
                )
        for blocked in resolution.get("readonly_others", []):
            if int(blocked.get("id", 0)) == ownership_id:
                return (
                    f"结构 {blocked.get('归属编号')} 为通道 {blocked.get('归属通道')} 的私有版，"
                    f"通道 {resolution['channel_id']} 只读查看"
                )
        for blocked in resolution["version_mismatch"]:
            if int(blocked.get("id", 0)) == ownership_id:
                return (
                    f"结构版本确认失败：通道版本 {resolution['channel_version']} 与结构适用版本"
                    f" {blocked.get('适用通道版本')} 不一致"
                )
        return f"结构归属 {entry.get('归属编号')} 当前不可用于通道 {resolution['channel_id']}"

    def _ownership_summary(self, entry: dict[str, Any] | None) -> dict[str, Any] | None:
        if entry is None:
            return None
        return {
            "id": entry.get("id"),
            "归属编号": entry.get("归属编号"),
            "结构编号": entry.get("结构编号"),
            "事件类型": entry.get("事件类型"),
            "归属通道": entry.get("归属通道"),
            "发布范围": entry.get("发布范围"),
            "授权通道": self._normalize_channels(entry.get("授权通道")),
            "Schema版本": entry.get("Schema版本"),
            "适用通道版本": entry.get("适用通道版本"),
            "字段指纹": entry.get("字段指纹"),
        }

    def _attribution(self, manifest: dict[str, Any], ownership: dict[str, Any]) -> dict[str, Any]:
        return {
            "归属编号": ownership.get("归属编号"),
            "结构编号": ownership.get("结构编号"),
            "Schema版本": ownership.get("Schema版本"),
            "发布范围": ownership.get("发布范围"),
            "归属通道": ownership.get("归属通道"),
            "字段指纹": ownership.get("字段指纹"),
            "清单编号": manifest.get("清单编号"),
            "校验结论": manifest.get("校验结论"),
            "待补验": bool(manifest.get("待补验")),
        }

    def _payload_fingerprint(self, payload: dict[str, Any]) -> str:
        raw = json.dumps(payload, sort_keys=True, ensure_ascii=False, default=str)
        return hashlib.sha256(raw.encode()).hexdigest()[:16]

    def _normalize_channels(self, value: Any) -> list[str]:
        if value is None:
            return []
        if isinstance(value, (list, tuple, set)):
            items = list(value)
        else:
            text = str(value).strip()
            if not text:
                return []
            items = [part for part in text.replace("，", ",").replace("、", ",").split(",")]
        result: list[str] = []
        for item in items:
            text = str(item).strip()
            if not text:
                continue
            if text.isdigit():
                text = str(int(text))
            if text not in result:
                result.append(text)
        return result

    def _to_channel_id(self, value: Any) -> int:
        try:
            return int(str(value).strip())
        except (TypeError, ValueError):
            return 0

    def _get_channel(self, channel_id: int) -> dict[str, Any] | None:
        if channel_id <= 0:
            return None
        row = self._store().get_by_id("webhook_channel", channel_id)
        if row is not None:
            return row
        from app.store import store
        return store.find("webhook_channel", channel_id)

    def _channel_uses_json_store(self, channel_id: int) -> bool:
        return self._store().get_by_id("webhook_channel", channel_id) is not None

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
            "artifact": "事件结构归属明细数据集",
            "count": len(rows),
            "by_status": by_status,
            "abnormal": sum(1 for row in rows if row.get("abnormal")),
            "shared": sum(1 for row in rows if row.get("发布范围") == SCOPE_SHARED),
            "published": sum(1 for row in rows if row.get("status") == "已发布"),
            "operation": self.OPERATION_TERM,
        }
