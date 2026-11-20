"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。"""
from __future__ import annotations

from app.routers import resource, pipeline

# pipeline 是固定前缀的跨模块链路，必须先于 /api/{module} 动态资源路由注册，
# 否则 /api/pipeline/health-summary 这类 GET 会被当成 entry_id 解析。
ROUTERS = [pipeline, resource]
