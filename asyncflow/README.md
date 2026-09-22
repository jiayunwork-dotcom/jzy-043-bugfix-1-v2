# AsyncFlow · 异步任务优先级队列与死信重试引擎

一套可独立运行的异步任务调度引擎：五级优先级队列 + 权重公平调度 + Critical/High 抢占、秒级延迟任务、Worker 池心跳租约与失联回收、三种重试策略与死信管理、DAG 依赖编排、实时监控大盘。浏览器管理面板用 Nuxt 3，后端用 Go 1.22 + Fiber，持久化到 PostgreSQL 16，队列与延迟调度走 Redis 7。

## 一条命令拉起全部服务

```bash
docker compose up --build
```

- 管理面板： http://localhost:3000
- 引擎接口： http://localhost:8080/api/health
- PostgreSQL： localhost:5432 （asyncflow/asyncflow）
- Redis：      localhost:6379

停止并清空数据：`docker compose down -v`

## 目录结构

```
asyncflow/
├── docker-compose.yml
├── backend/                     Go 1.22 + Fiber
│   ├── cmd/server/main.go       进程入口（装配 + HTTP + 优雅退出）
│   ├── migrations/              PostgreSQL schema（启动时幂等执行）
│   └── internal/
│       ├── config/              环境变量配置
│       ├── domain/              核心实体 / 五级优先级 / Cron 解析
│       ├── storage/             PostgreSQL 权威状态机（CAS 状态转换 + 审计）
│       ├── queue/               Redis 就绪队列 + 延迟/重试 ZSet（Lua 原子）+ 内存实现
│       ├── scheduler/           五级优先 + 权重公平 + 抢占 + 分派循环
│       ├── delayscheduler/      秒级到期扫描，等待区 -> 就绪队列
│       ├── workerpool/          内嵌 Worker：槽位、心跳、续租、抢占、优雅关闭
│       ├── reaper/              失联/租约过期回收
│       ├── engine/              提交、重试（指数/固定/Cron）、死信、回调
│       ├── orchestration/       DAG 环检测、拓扑分层、节点状态机、失败策略
│       ├── metrics/             深度/吞吐/成功率/延迟/利用率/DLQ 趋势
│       ├── runtime/             组件装配、外部 Worker 执行器、内嵌示例处理器
│       └── api/                 Fiber HTTP 路由（按资源拆文件）
└── frontend/                    Nuxt 3 管理面板
    ├── lib/api.ts               接口封装（页面不允许写死数据）
    ├── stores/                  Pinia 状态：metrics/tasks/dead/dags/workers
    ├── composables/             轮询与格式化
    ├── components/              PriorityPill / StatusBadge / Sparkline
    └── pages/                   index/tasks/dead/dags/workers
```

## 关键设计

- **Postgres 权威，Redis 调度**：任务状态以 Postgres 为准，`ready -> running` 用
  `UPDATE ... WHERE status='ready'` 的 CAS 完成，两个 Worker 抢同一任务只有一个成功，
  从根本上保证不重复执行；Redis LIST + 成员集合（Lua 原子）只负责取号，重复入列被幂等吞掉。
- **五级严格优先 + 权重公平**：Critical→High→Normal→Low→Bulk；连续消费
  `FAIRNESS_HIGH_WATERMARK`（默认 5）次 Normal 及以上后，强制插入一次 Low/Bulk 的消费机会。
- **抢占**：Critical/High 到来且无空槽时，挑一个 Normal 及以下的在跑任务，DB 置回 ready、
  打断执行上下文，并 `LPUSH` 到其所在优先级队列**队首**，Worker 空出来立刻执行高优任务。
- **秒级延迟**：延迟/重试任务放在 Redis ZSet（毫秒分值），扫描间隔默认 250ms，到点即提升。
- **租约与失联回收**：执行中任务带租约，Worker 每 1/3 租约期续租；心跳超时判失联，
  在跑任务被原子地重置回 ready 并重新入队，启动时还有 `RecoverInflight` 崩溃对账。
- **重试**：指数退避（`base * 2^(n-1)`，上限 1h）、固定间隔、5 段 Cron；超出最大次数进死信。
- **DAG**：Kahn 拓扑分层做环检测；A→(B,C)→D 扇入扇出按依赖释放；节点失败支持
  终止整图 / 跳过继续 / 重试节点三种策略，节点状态与整图聚合状态独立追踪。
- **审计**：每一次状态转换在同一事务内写 `audit_logs`，任务详情页给出完整轨迹。

## 提交任务

```bash
curl -X POST http://localhost:8080/api/tasks \
  -H 'Content-Type: application/json' \
  -d '{
    "type": "sample.flaky",
    "payload": {"order_id": "A-1001"},
    "priority": "high",
    "max_retries": 3,
    "timeout_seconds": 30,
    "delay_seconds": 0,
    "callback_url": "http://example.com/cb",
    "retry_policy": {"kind": "exponential", "base_interval_seconds": 2, "max_retries": 3}
  }'
```

内嵌 Worker 自带处理器：`sample.echo`（原样返回）、`sample.compute`（计算）、
`sample.flaky`（前两次失败、第三次成功，演示重试）、`sample.slow`（占用槽位，便于观察抢占），
以及通配处理器 `*`（未注册类型按 echo 执行，保证独立部署也能消费）。

延迟 10 秒：加 `"delay_seconds": 10`。Cron 重试：`"retry_policy": {"kind":"cron","cron_expression":"*/2 * * * *"}`。

幂等：提交时带 `"idempotency_key": "..."`，重复提交返回已存在任务（200 + `duplicated:true`）。

## 提交 DAG

```bash
curl -X POST http://localhost:8080/api/dags -H 'Content-Type: application/json' -d '{
  "name": "order-pipeline",
  "failure_policy": "skip",
  "nodes": [
    {"id":"A","type":"sample.echo","priority":"high","timeout_seconds":30,"max_retries":1,"dependencies":[],"payload":{}},
    {"id":"B","type":"sample.compute","priority":"normal","timeout_seconds":30,"max_retries":2,"dependencies":["A"],"payload":{}},
    {"id":"C","type":"sample.compute","priority":"normal","timeout_seconds":30,"max_retries":2,"dependencies":["A"],"payload":{}},
    {"id":"D","type":"sample.flaky","priority":"low","timeout_seconds":30,"max_retries":3,"dependencies":["B","C"],"payload":{}}
  ]
}'
```

`POST /api/dags/validate` 只做校验（含环检测，有环返回 422 + 环上节点）。

## 外部 Worker（同一类型多处理器、负载均衡）

外部进程通过 HTTP 拉模型接入，多个 Worker 注册同一任务类型即构成处理器池，调度器按当前负载分派：

```
POST /api/workers/register   {"worker_id":"w-7","name":"host7","capabilities":["sample.echo"],"slots":8}
POST /api/workers/w-7/heartbeat   {"current_tasks":["..."]}
POST /api/workers/w-7/claim?wait_ms=25000     -> 200 {task} | 204
POST /api/workers/w-7/complete  {"task_id":"...","success":true,"result":{...}}
```

## 主要接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/tasks` | 提交任务（优先级/延迟/重试/回调） |
| GET | `/api/tasks?status=&priority=&type=&from=&to=` | 多条件筛选 |
| GET | `/api/tasks/:id/timeline` | 任务详情 + 每次尝试时间线 + 审计 |
| GET | `/api/dead` / `/api/dead/aggregate` | 死信列表 / 按错误分类聚合 |
| POST | `/api/dead/retry` `/api/dead/discard` | 批量重试 / 批量丢弃（body: `{"ids":[...]}`） |
| GET | `/api/workers` `/api/workers/:id` | Worker 列表 / 详情 |
| POST | `/api/workers/:id/drain` | 请求优雅关闭 |
| POST | `/api/dags/validate` `/api/dags` | 环检测 / 提交 |
| GET | `/api/dags` `/api/dags/:id` | 列表 / 层级图与节点状态 |
| GET | `/api/metrics` | 总览大盘全部指标 |
| GET | `/api/audit?entity=&entity_id=` | 审计日志 |

## 本地开发（不用 Docker）

```bash
# 依赖：Go 1.22、Node 20、PostgreSQL 16、Redis 7
cd backend
export POSTGRES_DSN='postgres://asyncflow:asyncflow@localhost:5432/asyncflow?sslmode=disable'
export REDIS_ADDR=localhost:6379
go run ./cmd/server

cd ../frontend
NUXT_PUBLIC_API_BASE=http://localhost:8080 npm install && npm run dev
```

## 测试

后端测试锁住了调度与容错的关键行为（无外部依赖即可跑，存储用与 Postgres 相同 CAS 语义的内存替身）：

```bash
cd backend && go test ./... -race
```

- 高优先级持续到来时低优先级按配置节奏仍被消费（`TestFairnessLowPriorityNotStarved`）
- Critical 抢占后被中断任务回队首、不丢失（`TestCriticalPreemptionRequeuesAtHead`）
- 延迟任务到点秒级入队（`TestDelayedTaskPromotesWithinOneSecond`）
- Worker 失联后在执行任务被回收重入队（`TestWorkerLostReclaimsInflight`、`TestLeaseExpiryReclaim`）
- 重试耗尽进死信（`TestRetryExhaustionEntersDeadLetter`）、死信批量重试重新入队（`TestDeadBatchRetryReEnqueues`）
- DAG 依赖顺序正确且能检测出环（`TestDAGDependencyOrder`、`TestCycleDetectionRejectsCyclicDAG`）
- 并发提交下任务状态流转不丢不重（`TestConcurrentSubmitNoLossNoDuplicate`）
- 抢占取消 / 优雅关闭（workerpool 测试）、Cron 与优先级（domain 测试）

对真实 PostgreSQL 的 SQL/CAS 集成测试由环境变量门控：

```bash
TEST_DATABASE_DSN='postgres://asyncflow:asyncflow@localhost:5432/asyncflow?sslmode=disable' \
  go test ./internal/storage/ -run TestPostgresLifecycleCAS -v
```

## 可调参数（后端环境变量）

| 变量 | 默认 | 含义 |
| --- | --- | --- |
| `FAIRNESS_HIGH_WATERMARK` | 5 | 连续多少次高优消费后强制一次低优机会 |
| `DISPATCH_INTERVAL` | 50ms | 调度分派频率 |
| `DELAY_SCAN_INTERVAL` | 250ms | 延迟/重试到期扫描频率 |
| `LEASE_SECONDS` | 30 | 单次任务租约时长（Worker 按 1/3 周期续租） |
| `HEARTBEAT_TIMEOUT_SECS` | 15 | 心跳失联判定 |
| `REAPER_INTERVAL` | 2s | 失联/租约回收扫描频率 |
| `EMBEDDED_WORKER_SLOTS` | 4 | 内嵌 Worker 并发槽位 |
