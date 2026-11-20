"""启动期示例数据装载：把归属/接入清单/待补验的演示数据写入 JSON 存储。

只在服务启动时调用一次，且分区为空才写，避免覆盖运行中产生的数据。
测试替换存储后不会自动装载，保证用例从干净状态开始。
"""
from __future__ import annotations

from typing import Any

from app.seed import SEED_ROWS
from app.seed_ownership import MANIFEST_SEED, OWNERSHIP_SEED, PENDING_SEED

# event_schema 的读写都落在 JSON 存储，启动时把通用示例行铺进去，
# 列表页才有可看的基线数据；其余模块仍由内存示例仓库提供列表。
DEMO_TABLES: dict[str, list[dict[str, Any]]] = {
    "event_schema": SEED_ROWS.get("event_schema", []),
    "event_schema_ownership": OWNERSHIP_SEED,
    "ingest_manifest": MANIFEST_SEED,
    "pending_revalidation": PENDING_SEED,
}


def load_demo_rows(store) -> dict[str, int]:
    """按分区为空才写入的方式装载演示数据，返回各分区实际写入条数。"""
    written: dict[str, int] = {}
    for table, rows in DEMO_TABLES.items():
        existing = store.get_all(table)
        if existing:
            written[table] = 0
            continue
        for row in rows:
            store.insert(table, dict(row))
        written[table] = len(rows)
    return written
