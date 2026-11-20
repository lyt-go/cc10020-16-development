"""事件结构归属明细：发布共享、通道校验、待补验与历史回查测试。

覆盖需求里的完整故事线：
1. 同一通道同时拥有私有版（要求币种）与共享版（可缺省）的订单结构；
2. 一笔没有币种的订单走共享版放行，正式分区和待补验记录都能沿接入回查实际结构；
3. 切换共享授权后，重投历史订单仍能核对原校验结果；
4. 新订单按当时授予的范围校验；
5. 没有共享授权的通道不能借另一通道的共享版本放行。
"""
from __future__ import annotations

import json
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import app.persistence
from app.persistence import JsonStore, json_store

EVENT_TYPE = "order_paid"

# 这些模块在导入时绑定了 json_store，换存储后需要重新导入才能生效。
_RELOAD_MODULES = [
    "app.services.webhook_channel",
    "app.services.event_schema",
    "app.services.schema_ownership",
    "app.services.ingest_manifest",
    "app.services.pending_revalidation",
    "app.services.data_lake_sink",
    "app.services.dedup_filter",
    "app.services.dispatch_rule",
]


def _reload_services():
    import importlib
    for name in _RELOAD_MODULES:
        importlib.reload(importlib.import_module(name))


class OwnershipTestBase(unittest.TestCase):
    """把 JSON 存储换到临时目录，并按需准备通道与字段定义。"""

    def setUp(self):
        import tempfile
        self._tmp = tempfile.TemporaryDirectory()
        self._original = app.persistence.json_store
        app.persistence.json_store = JsonStore(base_dir=self._tmp.name)
        _reload_services()
        self.store = app.persistence.json_store

        from app.services.event_schema import EventSchemaService
        from app.services.schema_ownership import SchemaOwnershipService
        from app.services.webhook_channel import WebhookChannelService
        self.schema_svc = EventSchemaService()
        self.ownership_svc = SchemaOwnershipService()
        self.webhook_svc = WebhookChannelService()

        self.owner_channel = self._channel("WEBH-ORDER-A", "订单接入通道A", "v1")
        self.other_channel = self._channel("WEBH-ORDER-B", "订单接入通道B", "v1")

    def tearDown(self):
        app.persistence.json_store = self._original
        _reload_services()
        self._tmp.cleanup()

    def _channel(self, no: str, name: str, version: str, status: str = "已启用") -> dict:
        channel = self.store.insert("webhook_channel", {
            "通道编号": no,
            "通道名称": name,
            "接收路径": f"/webhook/{no.lower()}",
            "签名密钥": "secret",
            "验证方式": "NONE",
            "并发上限": "10",
            "回调地址": "",
            "通道版本": version,
            "status": status,
            "pending": False,
            "abnormal": False,
        })
        return channel

    def _add_fields(self, structure_no: str, channel_id: int, fields: list[dict], version: str = "v1") -> None:
        for field in fields:
            entry, missing = self.schema_svc.create_entry({
                "结构编号": structure_no,
                "事件类型": EVENT_TYPE,
                "字段名称": field["name"],
                "字段类型": field.get("type", "string"),
                "必填标识": "是" if field.get("required") else "否",
                "默认值": field.get("default", ""),
                "Schema版本": version,
                "所属通道": str(channel_id),
            })
            self.assertIsNotNone(entry, missing)
            self.schema_svc.run_action(int(entry["id"]), "标记生效")

    def _publish(self, structure_no: str, owner_id: int, scope: str, authorized=None,
                 version: str = "v1", channel_version: str = "v1") -> dict:
        entry, missing = self.ownership_svc.create_entry({
            "结构编号": structure_no,
            "事件类型": EVENT_TYPE,
            "归属通道": str(owner_id),
            "发布范围": scope,
            "授权通道": authorized or "",
            "Schema版本": version,
            "适用通道版本": channel_version,
        })
        self.assertIsNotNone(entry, missing)
        published, message = self.ownership_svc.run_action(int(entry["id"]), "发布归属")
        self.assertIsNotNone(published, message)
        return published


class TestOwnershipPublication(OwnershipTestBase):
    def setUp(self):
        super().setUp()
        self._add_fields("ORD-PRIVATE", self.owner_channel["id"], [
            {"name": "order_id", "required": True},
            {"name": "amount", "type": "number", "required": True},
            {"name": "currency", "required": True},
        ])
        self._add_fields("ORD-SHARED", self.owner_channel["id"], [
            {"name": "order_id", "required": True},
            {"name": "amount", "type": "number", "required": True},
            {"name": "currency", "required": False, "default": "CNY"},
        ])
        self.private = self._publish("ORD-PRIVATE", self.owner_channel["id"], "私有")
        self.shared = self._publish("ORD-SHARED", self.owner_channel["id"], "共享")

    def test_publish_captures_field_snapshot_and_version(self):
        detail = self.ownership_svc.get_entry(int(self.shared["id"]))
        self.assertEqual(detail["status"], "已发布")
        self.assertEqual(detail["发布范围"], "共享")
        self.assertEqual(detail["Schema版本"], "v1")
        self.assertEqual(detail["适用通道版本"], "v1")
        self.assertTrue(detail["字段指纹"])
        self.assertEqual([f["field_name"] for f in detail["字段快照"]],
                         ["order_id", "amount", "currency"])

    def test_private_scope_only_owner_can_use(self):
        owner_view = self.ownership_svc.get_ownership_detail(
            int(self.private["id"]), self.owner_channel["id"])
        other_view = self.ownership_svc.get_ownership_detail(
            int(self.private["id"]), self.other_channel["id"])
        self.assertTrue(owner_view["可使用"])
        self.assertFalse(other_view["可使用"])
        self.assertTrue(other_view["只读"])
        self.assertIn("私有", other_view["授权结论"])

    def test_shared_scope_grants_authorized_channel(self):
        self.ownership_svc.share_with(int(self.shared["id"]), [self.other_channel["id"]])
        view = self.ownership_svc.get_ownership_detail(
            int(self.shared["id"]), self.other_channel["id"])
        self.assertTrue(view["可使用"])
        self.assertFalse(view["只读"])

    def test_unpublished_scope_is_readonly(self):
        detail = self.ownership_svc.get_entry(int(self.private["id"]))
        self.assertTrue(detail["字段快照"])
        self.ownership_svc.run_action(int(detail["id"]), "停用归属")
        view = self.ownership_svc.get_ownership_detail(
            int(detail["id"]), self.owner_channel["id"])
        self.assertFalse(view["可使用"])

    def test_ownership_detail_listing(self):
        items = self.ownership_svc.list_ownership_detail(channel_id=self.other_channel["id"])
        self.assertEqual(len(items), 2)
        self.assertTrue(all(item["只读"] for item in items))


class TestSharedOrderWithoutCurrency(OwnershipTestBase):
    """主故事线：私有版要币种，共享版可缺省，不带币种的订单走共享版。"""

    def setUp(self):
        super().setUp()
        self._add_fields("ORD-PRIVATE", self.owner_channel["id"], [
            {"name": "order_id", "required": True},
            {"name": "amount", "type": "number", "required": True},
            {"name": "currency", "required": True},
        ])
        self._add_fields("ORD-SHARED", self.owner_channel["id"], [
            {"name": "order_id", "required": True},
            {"name": "amount", "type": "number", "required": True},
            {"name": "currency", "required": False, "default": "CNY"},
        ])
        self.private = self._publish("ORD-PRIVATE", self.owner_channel["id"], "私有")
        self.shared = self._publish("ORD-SHARED", self.owner_channel["id"], "共享")
        self.payload = {"order_id": "ORD-2026-0001", "amount": 199.5}

    def test_shared_version_admits_without_currency(self):
        result = self.ownership_svc.process_intake(
            self.owner_channel["id"], EVENT_TYPE, {"type": EVENT_TYPE, **self.payload})
        self.assertTrue(result["ok"], result["message"])
        self.assertEqual(result["decision"], "通过")
        self.assertEqual(result["ownership"]["归属编号"], self.shared["归属编号"])
        self.assertTrue(result["needs_supplement"])
        self.assertEqual(
            {attempt["发布范围"] for attempt in result["attempts"]},
            {"私有", "共享"},
        )

    def test_lake_partition_and_pending_record_trace_back_to_structure(self):
        outcome = self.webhook_svc.receive_webhook(
            self.owner_channel["id"], {"type": EVENT_TYPE, **self.payload})
        self.assertTrue(outcome["ok"], outcome["message"])

        # 正式分区里能沿这笔接入看到实际使用的结构归属
        from app.services.data_lake_sink import DataLakeSinkService
        lake_records = DataLakeSinkService().find_events(outcome["event_id"], EVENT_TYPE)
        self.assertEqual(len(lake_records), 1)
        attribution = lake_records[0]["attribution"]
        self.assertEqual(attribution["归属编号"], self.shared["归属编号"])
        self.assertEqual(attribution["发布范围"], "共享")
        self.assertEqual(attribution["待补验"], True)
        self.assertTrue(outcome["lake"]["partition"].startswith(f"events/{EVENT_TYPE}/"))
        self.assertTrue(os.path.exists(outcome["lake"]["file_path"]))

        # 落湖结果已回写接入清单
        from app.services.ingest_manifest import IngestManifestService
        manifest = IngestManifestService().get_entry(int(outcome["manifest"]["id"]))
        self.assertEqual(manifest["归属编号"], self.shared["归属编号"])
        self.assertEqual(manifest["落湖分区"], outcome["lake"]["partition"])

        # 待补验记录指向同一份清单与实际使用的共享结构
        from app.services.pending_revalidation import PendingRevalidationService
        pending = PendingRevalidationService().list_for_manifest(int(manifest["id"]))
        self.assertEqual(len(pending), 1)
        self.assertEqual(pending[0]["实际使用归属"], self.shared["归属编号"])
        self.assertEqual(pending[0]["缺失字段"], "currency")

        # 沿接入回查：清单 → 归属 → 落湖 → 待补验
        trace = self.ownership_svc.trace_intake(manifest_id=int(manifest["id"]))
        self.assertEqual(trace["conclusion"]["实际使用结构"], self.shared["归属编号"])
        self.assertEqual(trace["conclusion"]["发布范围"], "共享")
        self.assertTrue(trace["pending_revalidations"])
        self.assertTrue(trace["lake_records"])

    def test_pending_record_can_be_reconciled(self):
        outcome = self.ownership_svc.process_intake(
            self.owner_channel["id"], EVENT_TYPE, {"type": EVENT_TYPE, **self.payload})
        manifest_id = int(outcome["manifest_id"])
        from app.services.ingest_manifest import IngestManifestService
        from app.services.pending_revalidation import PendingRevalidationService
        pending = PendingRevalidationService().list_for_manifest(manifest_id)
        resolved, message = PendingRevalidationService().run_action(int(pending[0]["id"]), "确认补验")
        self.assertIsNotNone(resolved, message)
        self.assertEqual(resolved["status"], "已补验")
        manifest = IngestManifestService().get_entry(manifest_id)
        self.assertFalse(manifest["待补验"])

    def test_missing_required_field_goes_pending_not_lake(self):
        result = self.ownership_svc.process_intake(
            self.owner_channel["id"], EVENT_TYPE,
            {"type": EVENT_TYPE, "amount": 10.0})
        self.assertEqual(result["decision"], "待补验")
        self.assertFalse(result["admitted"])
        from app.services.pending_revalidation import PendingRevalidationService
        pending = PendingRevalidationService().list_for_manifest(int(result["manifest_id"]))
        self.assertEqual(set(pending[0]["缺失字段"].split(",")), {"currency", "order_id"})

    def test_type_error_is_rejected(self):
        result = self.ownership_svc.process_intake(
            self.owner_channel["id"], EVENT_TYPE,
            {"type": EVENT_TYPE, "order_id": "ORD-1", "amount": "not-a-number"})
        self.assertEqual(result["decision"], "拒绝")
        self.assertFalse(result["admitted"])


class TestSharedAuthorizationSwitch(OwnershipTestBase):
    """共享授权切换后：历史订单可核对原结论，新订单按新范围校验。"""

    def setUp(self):
        super().setUp()
        self._add_fields("ORD-PRIVATE", self.owner_channel["id"], [
            {"name": "order_id", "required": True},
            {"name": "currency", "required": True},
        ])
        self._add_fields("ORD-SHARED", self.owner_channel["id"], [
            {"name": "order_id", "required": True},
            {"name": "currency", "required": False, "default": "CNY"},
        ])
        self._publish("ORD-PRIVATE", self.owner_channel["id"], "私有")
        self.shared = self._publish("ORD-SHARED", self.owner_channel["id"], "共享",
                                    authorized=[self.other_channel["id"]])
        self.payload = {"order_id": "ORD-2026-9", "currency": ""}

    def test_historical_order_keeps_original_result_after_share_revoked(self):
        first = self.webhook_svc.receive_webhook(
            self.other_channel["id"], {"type": EVENT_TYPE, **self.payload})
        self.assertTrue(first["ok"], first["message"])
        origin_manifest_id = int(first["manifest"]["id"])

        revoked, message = self.ownership_svc.revoke_share(int(self.shared["id"]))
        self.assertIsNotNone(revoked, message)

        # 新订单按当时授予的范围校验：撤销后不再放行
        fresh = self.webhook_svc.receive_webhook(
            self.other_channel["id"], {"type": EVENT_TYPE, "order_id": "ORD-NEW-1"})
        self.assertFalse(fresh["ok"])
        self.assertIn("共享授权已撤销", fresh["message"])

        # 重新投递这笔历史订单：仍可核对原校验结果
        replay = self.webhook_svc.receive_webhook(
            self.other_channel["id"],
            {"type": EVENT_TYPE, **self.payload},
            intake_context={"manifest_id": origin_manifest_id},
        )
        check = replay.get("historical_check")
        self.assertIsNotNone(check)
        self.assertEqual(check["原校验结论"], "通过")
        self.assertEqual(check["原归属编号"], self.shared["归属编号"])
        self.assertTrue(check["与原结论一致"])
        self.assertTrue(check["当前授权仍可使用"] is False)

    def test_implicit_replay_matches_original_manifest(self):
        first = self.webhook_svc.receive_webhook(
            self.other_channel["id"], {"type": EVENT_TYPE, **self.payload})
        self.assertTrue(first["ok"])
        self.ownership_svc.revoke_share(int(self.shared["id"]))
        replay = self.webhook_svc.receive_webhook(
            self.other_channel["id"], {"type": EVENT_TYPE, **self.payload})
        check = replay.get("historical_check")
        self.assertIsNotNone(check)
        self.assertEqual(check["manifest_id"], first["manifest"]["id"])
        self.assertEqual(check["原校验结论"], "通过")

    def test_new_orders_follow_current_grant(self):
        # 授权仍在时新订单通过
        allowed = self.webhook_svc.receive_webhook(
            self.other_channel["id"], {"type": EVENT_TYPE, "order_id": "ORD-OK-1"})
        self.assertTrue(allowed["ok"], allowed["message"])
        # 撤销后新订单被拒绝
        self.ownership_svc.revoke_share(int(self.shared["id"]))
        denied = self.webhook_svc.receive_webhook(
            self.other_channel["id"], {"type": EVENT_TYPE, "order_id": "ORD-NO-2"})
        self.assertFalse(denied["ok"])
        self.assertEqual(denied["stage"], "结构校验")
        # 授权恢复后新订单再次通过
        self.ownership_svc.share_with(int(self.shared["id"]), [self.other_channel["id"]])
        again = self.webhook_svc.receive_webhook(
            self.other_channel["id"], {"type": EVENT_TYPE, "order_id": "ORD-OK-3"})
        self.assertTrue(again["ok"], again["message"])


class TestUnauthorizedChannelCannotBorrow(OwnershipTestBase):
    def setUp(self):
        super().setUp()
        self._add_fields("ORD-A-PRIVATE", self.owner_channel["id"], [
            {"name": "order_id", "required": True},
            {"name": "currency", "required": True},
        ])
        self._add_fields("ORD-A-SHARED", self.owner_channel["id"], [
            {"name": "order_id", "required": True},
            {"name": "currency", "required": False},
        ])
        self._publish("ORD-A-PRIVATE", self.owner_channel["id"], "私有")
        self.shared = self._publish("ORD-A-SHARED", self.owner_channel["id"], "共享")

    def test_unauthorized_channel_rejected_despite_valid_payload(self):
        result = self.webhook_svc.receive_webhook(
            self.other_channel["id"], {"type": EVENT_TYPE, "order_id": "ORD-X"})
        self.assertFalse(result["ok"])
        self.assertIn("未获共享授权", result["message"])
        self.assertEqual(result["ownership_detail"]["blocked_shares"][0]["归属编号"],
                         self.shared["归属编号"])

    def test_unauthorized_channel_cannot_force_preferred_structure(self):
        result = self.ownership_svc.process_intake(
            self.other_channel["id"], EVENT_TYPE,
            {"type": EVENT_TYPE, "order_id": "ORD-X"},
            preferred_ownership_id=int(self.shared["id"]))
        self.assertFalse(result["admitted"])
        self.assertEqual(result["decision"], "拒绝")

    def test_sharing_with_authorized_channel_lifts_restriction(self):
        self.ownership_svc.share_with(int(self.shared["id"]), [self.other_channel["id"]])
        result = self.webhook_svc.receive_webhook(
            self.other_channel["id"], {"type": EVENT_TYPE, "order_id": "ORD-Y"})
        self.assertTrue(result["ok"], result["message"])
        self.assertEqual(result["ownership_detail"]["ownership"]["归属编号"],
                         self.shared["归属编号"])

    def test_owner_private_still_works_without_share(self):
        result = self.webhook_svc.receive_webhook(
            self.owner_channel["id"], {"type": EVENT_TYPE, "order_id": "ORD-Z", "currency": "USD"})
        self.assertTrue(result["ok"], result["message"])


class TestVersionConfirmation(OwnershipTestBase):
    def setUp(self):
        super().setUp()
        self._add_fields("ORD-V2", self.owner_channel["id"], [
            {"name": "order_id", "required": True},
        ])
        self.ownership = self._publish(
            "ORD-V2", self.owner_channel["id"], "私有",
            channel_version="v2")

    def test_version_mismatch_blocks_intake(self):
        result = self.ownership_svc.process_intake(
            self.owner_channel["id"], EVENT_TYPE, {"type": EVENT_TYPE, "order_id": "1"})
        self.assertEqual(result["decision"], "拒绝")
        self.assertIn("版本确认失败", result["message"])

    def test_version_confirmation_writes_back_to_channel(self):
        confirm = self.ownership_svc.confirm_channel_version(
            int(self.ownership["id"]), self.owner_channel["id"])
        self.assertFalse(confirm["ok"])
        self.assertEqual(confirm["conclusion"]["结论"], "版本不一致")

        self.store.update("webhook_channel", self.owner_channel["id"], {"通道版本": "v2"})
        confirm = self.ownership_svc.confirm_channel_version(
            int(self.ownership["id"]), self.owner_channel["id"])
        self.assertTrue(confirm["ok"])
        channel = self.store.get_by_id("webhook_channel", self.owner_channel["id"])
        self.assertEqual(channel["确认归属编号"], self.ownership["归属编号"])
        self.assertEqual(channel["版本确认结论"], "一致")

        result = self.ownership_svc.process_intake(
            self.owner_channel["id"], EVENT_TYPE, {"type": EVENT_TYPE, "order_id": "1"})
        self.assertTrue(result["admitted"], result["message"])


class TestLegacyCompatibility(OwnershipTestBase):
    """未发布归属的历史事件类型，仍按既有通道 Schema 口径校验。"""

    def test_unescaped_event_type_uses_legacy_validation(self):
        self._add_fields("ORD-LEGACY", self.owner_channel["id"], [
            {"name": "order_id", "required": True},
        ])
        outcome = self.webhook_svc.receive_webhook(
            self.owner_channel["id"], {"type": EVENT_TYPE, "order_id": "L-1"})
        self.assertTrue(outcome["ok"], outcome["message"])
        self.assertTrue(outcome["ownership_detail"]["legacy"])
        self.assertEqual(outcome["manifest"]["校验结论"], "通过")

        missing = self.webhook_svc.receive_webhook(
            self.owner_channel["id"], {"type": EVENT_TYPE, "something": "else"})
        self.assertFalse(missing["ok"])
        self.assertTrue(missing["ownership_detail"]["legacy"])

    def test_snapshot_validator_keeps_existing_semantics(self):
        fields = [
            {"field_name": "amount", "field_type": "number", "required": True},
            {"field_name": "currency", "field_type": "string", "required": False, "default": "CNY"},
        ]
        from app.services.schema_validation import validate_field_snapshot
        ok = validate_field_snapshot(fields, {"amount": 12})
        self.assertTrue(ok["ok"])
        self.assertEqual(ok["defaulted_fields"], ["currency"])
        self.assertEqual(ok["applied_defaults"], {"currency": "CNY"})
        from app.services.schema_validation import validate_field_snapshot
        bad = validate_field_snapshot(fields, {"amount": "12"})
        self.assertFalse(bad["ok"])
        self.assertEqual(bad["type_errors"][0]["expected"], "number")
        # 与既有校验口径保持相同字段集合
        for key in ("ok", "message", "missing_fields", "type_errors", "checked_fields"):
            self.assertIn(key, bad)


if __name__ == "__main__":
    unittest.main(verbosity=2)


class TestSeedConsistency(unittest.TestCase):
    """示例数据必须与运行期的字段快照/指纹口径一致。"""

    def test_seed_fingerprints_match_snapshot_hash(self):
        from app import seed_ownership
        from app.services.schema_validation import snapshot_hash

        for row in seed_ownership.OWNERSHIP_SEED:
            with self.subTest(归属编号=row["归属编号"]):
                self.assertEqual(snapshot_hash(row["字段快照"]), row["字段指纹"])

    def test_seed_manifest_points_to_shared_version(self):
        from app import seed_ownership

        manifest = seed_ownership.MANIFEST_SEED[0]
        ownership = {row["归属编号"]: row for row in seed_ownership.OWNERSHIP_SEED}
        used = ownership[manifest["归属编号"]]
        self.assertEqual(used["发布范围"], "共享")
        self.assertEqual(used["字段指纹"], manifest["字段指纹"])
        self.assertEqual(manifest["待补验"], True)
        pending = seed_ownership.PENDING_SEED[0]
        self.assertEqual(pending["关联清单"], str(manifest["id"]))
        self.assertEqual(pending["实际使用归属"], manifest["归属编号"])
        currency_field = next(f for f in used["字段快照"] if f["field_name"] == "currency")
        self.assertFalse(currency_field["required"])
