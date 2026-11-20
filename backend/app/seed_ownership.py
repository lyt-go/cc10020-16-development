"""事件结构归属、接入清单与待补验记录的示例数据。

示例把"同一通道有权使用私有版与共享版两套订单结构"的场景固化下来：
- OWN-0001：归属通道 3 的私有版，要求币种；
- OWN-0002：同一通道的共享版，币种可缺省（默认 CNY），授权通道 1、3；
- ING-00001 / PEND-00001：通道 1 送出一笔没有币种的订单，走共享版放行，
  正式分区与待补验记录都能沿这条接入回查实际使用的结构。

字段快照的指纹用 app.services.schema_validation.snapshot_hash 生成，
保持与运行期发布口径一致；测试里会做一致性校验。
"""
from __future__ import annotations

from typing import Any

OWNERSHIP_SEED: list[dict[str, Any]] = [
    {
        "id": 1,
        "归属编号": "OWN-0001",
        "结构编号": "EVEN-ORDER-P",
        "事件类型": "order_paid",
        "归属通道": "3",
        "归属通道名称": "Webhook通道样例3",
        "发布范围": "私有",
        "授权通道": "3",
        "Schema版本": "v1",
        "适用通道版本": "v1",
        "字段快照": [
            {"field_name": "order_id", "field_type": "string", "required": True,
             "default": "", "schema_row_id": 1, "schema_no": "EVEN-ORDER-P",
             "schema_version": "v1"},
            {"field_name": "amount", "field_type": "number", "required": True,
             "default": "", "schema_row_id": 2, "schema_no": "EVEN-ORDER-P",
             "schema_version": "v1"},
            {"field_name": "currency", "field_type": "string", "required": True,
             "default": "", "schema_row_id": 3, "schema_no": "EVEN-ORDER-P",
             "schema_version": "v1"},
        ],
        "字段指纹": "72a41a71bc6cc021",
        "版本确认": [
            {"channel_id": "3", "通道版本": "v1", "结构版本": "v1",
             "适用通道版本": "v1", "结论": "一致",
             "confirmed_at": "2026-11-18T09:30:00+00:00"},
        ],
        "发布状态": "已发布",
        "只读": False,
        "status": "已发布",
        "pending": False,
        "abnormal": False,
        "created_at": "2026-11-18T09:30:00+00:00",
        "effective_at": "2026-11-18T09:30:00+00:00",
    },
    {
        "id": 2,
        "归属编号": "OWN-0002",
        "结构编号": "EVEN-ORDER-S",
        "事件类型": "order_paid",
        "归属通道": "3",
        "归属通道名称": "Webhook通道样例3",
        "发布范围": "共享",
        "授权通道": "1,3",
        "Schema版本": "v1",
        "适用通道版本": "v1",
        "字段快照": [
            {"field_name": "order_id", "field_type": "string", "required": True,
             "default": "", "schema_row_id": 1, "schema_no": "EVEN-ORDER-S",
             "schema_version": "v1"},
            {"field_name": "amount", "field_type": "number", "required": True,
             "default": "", "schema_row_id": 2, "schema_no": "EVEN-ORDER-S",
             "schema_version": "v1"},
            {"field_name": "currency", "field_type": "string", "required": False,
             "default": "CNY", "schema_row_id": 3, "schema_no": "EVEN-ORDER-S",
             "schema_version": "v1"},
        ],
        "字段指纹": "f4d181061b8a693b",
        "版本确认": [
            {"channel_id": "3", "通道版本": "v1", "结构版本": "v1",
             "适用通道版本": "v1", "结论": "一致",
             "confirmed_at": "2026-11-18T09:30:00+00:00"},
        ],
        "发布状态": "已发布",
        "只读": False,
        "status": "已发布",
        "pending": False,
        "abnormal": False,
        "created_at": "2026-11-18T09:30:00+00:00",
        "effective_at": "2026-11-18T09:30:00+00:00",
    },
]

_ORDER_PAYLOAD = {"type": "order_paid", "order_id": "ORD-2026-1188", "amount": 268.0}

MANIFEST_SEED: list[dict[str, Any]] = [
    {
        "id": 1,
        "清单编号": "ING-00001",
        "通道": "1",
        "事件类型": "order_paid",
        "事件标识": "evt-order-1188",
        "事件指纹": "3afcc10bccde648f",
        "事件快照": dict(_ORDER_PAYLOAD),
        "归属ID": 2,
        "归属编号": "OWN-0002",
        "结构编号": "EVEN-ORDER-S",
        "Schema版本": "v1",
        "发布范围": "共享",
        "字段快照": list(OWNERSHIP_SEED[1]["字段快照"]),
        "字段指纹": "f4d181061b8a693b",
        "校验结论": "通过",
        "待补验": True,
        "通道版本": "v1",
        "版本确认": {
            "channel_id": "3", "通道版本": "v1", "结构版本": "v1",
            "适用通道版本": "v1", "结论": "一致",
            "confirmed_at": "2026-11-18T09:30:00+00:00",
        },
        "校验明细": [],
        "缺失字段": [],
        "类型错误": [],
        "结论说明": "共享结构 OWN-0002 放行，私有版字段缺口待补验",
        "结论时间": "2026-11-18T09:30:00+00:00",
        "去重结果": "通过去重检查",
        "落湖分区": "events/order_paid/2026/11/18",
        "落湖文件": "",
        "落湖事件标识": "evt-order-1188",
        "重投原清单": None,
        "status": "通过",
        "pending": False,
        "abnormal": False,
        "created_at": "2026-11-18T09:30:00+00:00",
    },
]

PENDING_SEED: list[dict[str, Any]] = [
    {
        "id": 1,
        "补验编号": "PEND-00001",
        "关联清单": "1",
        "通道": "1",
        "事件类型": "order_paid",
        "事件标识": "evt-order-1188",
        "实际使用归属": "OWN-0002",
        "实际使用范围": "共享",
        "结构编号": "EVEN-ORDER-S",
        "Schema版本": "v1",
        "缺失字段": "currency",
        "私有版要求": [
            {"归属编号": "OWN-0001", "结构编号": "EVEN-ORDER-P",
             "Schema版本": "v1", "缺失字段": ["currency"]},
        ],
        "补验说明": "共享结构已放行，归属通道私有版要求字段：currency",
        "status": "待补验",
        "pending": True,
        "abnormal": True,
        "created_at": "2026-11-18T09:30:00+00:00",
    },
]
