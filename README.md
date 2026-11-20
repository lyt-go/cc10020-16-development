# API集成与事件编排平台

面向第三方API接入、Webhook事件接收、消息队列消费与服务编排的全链路API数据集成平台。

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/WorkbenchView.vue|tsx  全部模块共享工作台
│   ├── src/data/modules.ts              18个业务模块配置
│   ├── src/api/              统一请求封装
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/resource.py  通用查询/导出/登记/动作接口
│   ├── app/services/<key>.py    每个模块独立领域逻辑
│   ├── app/services/base.py     共享服务骨架
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| API端点注册 | `api_endpoint` | 端点配置 | 端点编号、端点名称、请求方法 |
| Webhook通道 | `webhook_channel` | 通道配置 | 通道编号、通道名称、接收路径 |
| 事件结构定义 | `event_schema` | 事件结构 | 结构编号、事件类型、字段名称 |
| 事件结构归属 | `event_schema_ownership` | 结构归属 | 归属编号、结构编号、归属通道 |
| 接入清单 | `ingest_manifest` | 接入记录 | 清单编号、通道、归属编号 |
| 待补验记录 | `pending_revalidation` | 待补验记录 | 补验编号、关联清单、缺失字段 |
| 消息队列消费 | `mq_consumer` | 消费配置 | 配置编号、队列名称、消费组名 |
| 死信队列管理 | `dlq_handler` | 死信记录 | 记录编号、原始队列、失败原因 |
| 认证凭证管理 | `api_auth_store` | 认证凭证 | 凭证编号、关联端点、凭证类型 |
| 流量控制 | `rate_limit_config` | 限流规则 | 规则编号、关联端点、请求间隔 |
| 数据转换编排 | `data_transform` | 转换规则 | 规则编号、源Schema、目标Schema |
| 事件分发路由 | `dispatch_rule` | 分发规则 | 规则编号、事件类型、目标系统 |
| 事件去重 | `dedup_filter` | 去重规则 | 规则编号、事件类型、去重键列 |
| 批量接入调度 | `batch_ingest` | 接入任务 | 任务编号、关联端点、Cron表达式 |
| 数据落湖 | `data_lake_sink` | 落湖配置 | 配置编号、数据来源、目标路径 |
| 接入质量校验 | `data_quality_rule` | 质量规则 | 规则编号、数据来源、校验字段 |
| 调用链路追踪 | `call_trace_log` | 调用记录 | 记录编号、关联端点、请求参数 |
| 增量水位管理 | `incre_cursor` | 水位标记 | 标记编号、关联端点、增量字段 |
| 历史回补任务 | `backfill_job` | 回补任务 | 任务编号、关联端点、起始日期 |
| 事件归档清理 | `event_archive` | 归档策略 | 策略编号、事件类型、归档条件 |
| 集成监控看板 | `integration_monitor` | 监控指标 | 指标编号、监控对象、成功率 |

## 约定

- 全部模块共享`frontend/src/views/WorkbenchView.vue|tsx`和一套资源路由。
- 每个模块仍保留独立`backend/app/services/<模块>.py`；纯前端保留
  `frontend/src/domain/<模块>.ts`，用于承载真实业务差异并维持36题位容量。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

## 事件结构归属与通道校验

事件结构在多个接入通道间发布/共享时，需要一份数据归属凭证来说明
"这份结构归谁、可以被哪些通道使用"。实现分为三层：

1. **结构归属明细**（`event_schema_ownership`）：登记结构编号、归属通道、
   发布范围（私有/共享）、授权通道、适用通道版本。发布时把当时的已生效字段
   固化成字段快照与字段指纹，后续校验、历史回放都以快照为准。
   - 私有版只对归属通道可用；共享版对授权通道可用。
   - 未列进共享范围的结构对其他通道只读：`GET /api/event_schema_ownership/{id}/detail?channel_id=N`
     返回 `可使用=false` 与只读结论。
   - 发布范围切换走 `授权共享` / `撤销共享` 动作或
     `POST /api/pipeline/schema-ownership/share`。
2. **接入校验**（`event_schema.validate_event` → 归属服务）：同一通道可以同时拥有
   私有版和共享版两套订单结构。解析候选时归属通道的私有版优先，私有版校验不过
   再尝试已授权的共享版；共享版允许缺省的字段（如币种）会记录默认值口径。
   一旦使用的共享版对私有版仍有字段缺口，会生成待补验记录。
   没有共享授权的通道无法指定别的通道的结构放行。
3. **落点凭证**：每笔接入写一行接入清单（`ingest_manifest`），固化字段快照、
   事件快照、校验结论、通道版本确认与落湖分区；落湖记录里同时写入
   `attribution`（归属编号、发布范围、字段指纹、待补验标记）。

回查与重放：

```text
GET  /api/pipeline/schema-ownership?channel_id=3     # 归属明细（含只读/可用）
POST /api/pipeline/schema-ownership/publish          # 登记并发布归属
POST /api/pipeline/schema-ownership/share            # 切换共享授权
POST /api/pipeline/schema-ownership/confirm-version  # 通道与结构版本确认
POST /api/pipeline/schema-ownership/intake           # 单笔接入的归属校验
GET  /api/pipeline/ingest-manifest/{id}/trace        # 沿接入回查结构/落湖/待补验
GET  /api/pipeline/ingest-manifest/{id}/verify       # 授权切换后核对原校验结果
POST /api/pipeline/webhook-to-lake?channel_id=3      # 可带 manifest_id/event_id 重投历史订单
```

兼容口径：事件类型还没有任何"已发布归属"时，校验回落到既有的通道 Schema 逻辑，
历史数据重放的字段结论保持不变。落湖正式分区位于
`backend/data_store/lake/events/<事件类型>/<年/月/日>/`，待补验记录位于
`backend/data_store/pending_revalidation/`。

领域：数据工程
子方向：数据采集、爬虫与RPA
