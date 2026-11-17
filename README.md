# 水质数仓单实例 PITR 恢复服务

面向"值班员误删样品后，要取回删前核对时点数据、同时保留生产库后续写入"的场景，
以及"质控员要查清样品 pH 改回原值、删后同号重录经过"的沿革追溯场景。
基于 **FastAPI + PostgreSQL 16 + Docker Compose**：

- 生产源库可写样品库（样品号 / 站号 / pH），开启 **WAL 持续归档**；
- API 自动制作一份 **物理基准备份**（`pg_basebackup`，源库不停写）；
- 可随时登记 **命名核对点**（`pg_create_restore_point`）；
- 按核对点 **PITR 恢复到独立只读实例** 并查询样品；
- **变更沿革**：启用变更记录后，生产库新增/更正/删除在**同一事务**留下变更事件
  （原行身份 + 变更前后站号、pH，回滚不留痕）；按恢复任务号 + 样品号在恢复实例上
  查询沿革，事件与样品处于同一时点；
- 恢复结果只含核对点前已提交的数据，**跨点后才提交的事务不会出现**；
- 查询只打恢复实例，**绝不用当前源库冒充恢复结果**；
- WAL 未归档齐备时任务显示等待、不开放目标查询；
- 失败或 API 重启后可按原核对点重试；一次只恢复一项；全程不改写生产库。

## 快速开始

```bash
docker compose up -d --build        # 启动源库 + API（首次自动做基准备份）
python3 scripts/demo.py             # 端到端演示：沿革（改回原值/回滚/同号重录）+ 恢复校验
```

Swagger 文档：<http://localhost:8000/docs>

演示脚本依次验证：

| 场景 | 操作 | 校验 |
| --- | --- | --- |
| 沿革不可用提示 | cp0 登记于启用变更记录**之前** | 恢复 cp0 后查沿革：明确返回 `lineage_available=false` 及原因 |
| 启用记录 | `POST /api/audit/enable`（幂等） | 记录起点 LSN 落库；重复启用不改变起点 |
| 改回原值 | S-2026-0002 pH 6.95→7.55→6.95 | cp2 沿革含两条 UPDATE 事件，前后值完整，当前行 6.95 |
| 回滚不留痕 | 回滚的 UPDATE / INSERT | 源库与恢复库均无对应事件、无对应行 |
| 删后同号重录 | DELETE S-2026-0003 后以同号 INSERT（ST-09/8.88） | 沿革分两代：旧代仅 DELETE、新代仅自身 INSERT，行身份不同，不接续 |
| 跨点事务 | 事务在 cp3 前 `INSERT`、cp3 后才 `COMMIT` | cp3 恢复结果中该行与其事件都**不出现** |
| 实例切换 | 新任务就绪取代旧实例 | 旧任务查样品/沿革一律 409，不会把另一核对点的事件标为本次结果 |
| 源库不受打扰 | 恢复进行期间向源库写入新样品 | 写入成功；恢复实例中也看不到该行（证明没拿源库冒充） |

## 架构

```
                 ┌──────────────────────────── compose ───────────────────────────┐
                 │                                                                  │
  写入/变更 ───▶ │  db (PostgreSQL 16, 可写)                                        │
                 │    samples 触发器 ──同事务──▶ sample_events（变更事件表）          │
                 │    archive_mode=on ──WAL 段(原子改名)──▶ [wal_archive 卷]         │
                 │                                          │ (只读挂载)             │
                 │  api (FastAPI + PG16 工具)               ▼                       │
                 │    ① pg_basebackup ──▶ [/backups/base]   核对 WAL 段齐备          │
                 │    ② pg_create_restore_point(核对点) ──▶ 源库（仅 WAL 元数据）    │
                 │    ③ 拷贝备份 + recovery.signal ──▶ [/recoveries/job-N]           │
                 │       restore_command 从归档重放 ─▶ pause@核对点 ─▶ 校验 ─▶ 提升  │
                 │    ④ 只读查询 ◀── 恢复实例 127.0.0.1:55433（独立数据目录）        │
                 │       样品查询 + 变更沿革查询（同连接先核验实例身份）              │
                 │                                                                  │
                 └──────────────────────────────────────────────────────────────────┘
```

## 变更沿革（质控追溯）

### 启用变更记录

```bash
curl -X POST localhost:8000/api/audit/enable   # 幂等；单事务，失败整体回滚
curl -s localhost:8000/api/audit/status        # 查看启用状态与记录起点
```

启用是一次性显式管理动作，在源库单事务创建：

- `sample_events` 事件表：原行身份 `row_uid`、样品号、操作类型、
  **变更前后站号与 pH**、事务号、事务时间；
- `samples.row_uid` 行身份列（既有行逐行分配随机 uuid；唯一索引；触发器禁止修改）；
- 行级触发器：INSERT/UPDATE/DELETE 在**同一事务**写入事件——事务回滚则事件随之
  消失，**回滚不留痕**；`DELETE` 后同号重录的新行获得新 `row_uid`，
  沿革按行身份分代，**不会接到旧行的沿革上**；
- `BEFORE TRUNCATE` 触发器：禁止 `TRUNCATE samples`（TRUNCATE 不触发行级触发器，
  会绕过沿革记录）；
- `audit_meta` 单行：记录起点（启用时点 WAL 位点），随 PITR 一并恢复。

启用前的变更没有事件（触发器尚不存在）——这正是"记录起点"的含义：
沿革只含起点之后提交的变更。

### 查询沿革

```bash
curl -s -X POST localhost:8000/api/recoveries -H 'content-type: application/json' -d '{"checkpoint":"cp2"}'
curl -s 'localhost:8000/api/recoveries/2/lineage?sample_no=S-2026-0003'
```

响应要点：

- `lineage_available=false` + `reason`：该核对点**早于记录起点**（恢复实例中物理
  不存在事件表），明确提示沿革不可用；
- `lineage_available=true` 时：`recording_start` 标明记录起点；`incarnations`
  按行身份分代列出逐事件沿革（`is_current` 标记当前行所在代，`ended_by=DELETE`
  标记被删除终结的代）；`current_row` 为同一时点的当前行；
- 事件与样品读取自**同一恢复实例**（只读、冻结在核对点时点），必然处于同一时点；
- 查询在同一连接上先核验实例数据目录（`SHOW data_directory`）与任务是否一致：
  **实例切换后不会把另一核对点的事件标为本次结果**；被取代任务的查询返回 409。

核对点列表/详情附带 `lineage_available` 预判（与记录起点 LSN 比较），
便于在恢复前筛选；恢复实例上的物理检查是最终判定。

## 关键机制与需求对照

| 需求 | 实现 |
| --- | --- |
| 物理基准备份 + 持续归档 | `pg_basebackup -Fp -X stream`（自洽含备份期 WAL）；源库 `archive_command` 先写临时文件再原子 `mv`，API 只会看到完整 WAL 段 |
| 登记命名核对点 | `POST /api/checkpoints` → `pg_create_restore_point` + `pg_switch_wal`（促使该段立即归档） |
| 只含核对点前已提交数据 | `recovery_target_name` + `recovery_target_action=pause`，重放精确停在核对点；跨点后提交的事务不重放 |
| 同事务留痕变更前后值 | 行级触发器把事件写入 `sample_events`（同事务），含原行身份 row_uid 与变更前后站号、pH |
| 回滚不留事件 | 事件与业务变更同事务，回滚一并撤销（实测验证） |
| 同号重录不接旧行沿革 | 行身份 `row_uid`：重录新行新身份，沿革按行身份分代返回 |
| 事件与样品同一时点 | 事件随业务事务提交，PITR 同时重放；查询只打冻结在核对点的只读实例 |
| 启用前核对点明确提示 | 恢复实例物理检查事件表是否存在；核对点详情附 `lineage_available` 预判与原因 |
| 启用后标明记录起点 | `audit_meta` 记录启用 WAL 位点，随 PITR 恢复，沿革响应返回 `recording_start` |
| 实例切换不张冠李戴 | 一次只恢复一项；被取代任务 409；查询连接上核验 `data_directory` 与任务一致 |
| 不用源库冒充恢复 | 查询连接的是 `/recoveries/job-N` 下独立实例（端口 55433），响应中标注实例与 as-of LSN |
| WAL 未齐备显示等待 | 核对点 `waiting_wal` 状态；任务逐段核对"备份起点→核对点"全部 WAL 段，缺段时停留 `waiting_wal` 并列出缺失段 |
| 不把半库报成功 | pause 后校验 `pg_last_wal_replay_lsn() >= 核对点 LSN` 才 promote；再查 `samples` sanity + `default_transaction_read_only=on` 才置 `ready`；任何异常 → `failed` + 日志 |
| 失败/重启可重试 | 任务状态落 SQLite；`POST /api/recoveries/{id}/retry` 按原点重试；API 重启时进行中任务自动重新排队、ready 实例自动拉起 |
| 一次只恢复一项 | 有进行中任务时 `POST /api/recoveries` 返回 409；新任务就绪前会把旧 ready 实例停掉并标记 `superseded` |
| 源库可写、不被改写 | 基准备份/恢复点均为在线只读操作；除显式 `POST /api/audit/enable`（单事务 DDL）外 API 对源库不执行任何 DML/DDL；归档卷对 API 只读挂载 |

## API 一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/api/health` | 健康检查（源库连通性、已归档 WAL 数、变更记录是否启用） |
| POST | `/api/audit/enable` | 启用样品变更记录（幂等，单事务；显式管理动作） |
| GET | `/api/audit/status` | 变更记录启用状态与记录起点 |
| POST | `/api/basebackup?force=` | 制作物理基准备份（默认启动时自动做一次） |
| GET | `/api/basebackup` | 备份状态（起始 LSN、大小、历史） |
| POST | `/api/checkpoints` | 登记命名核对点 `{"name": "cp1"}` |
| GET | `/api/checkpoints` `/api/checkpoints/{name}` | 核对点状态（`waiting_wal`/`ready`/`expired` + 沿革可用性预判） |
| POST | `/api/recoveries` | 按核对点发起恢复 `{"checkpoint": "cp1"}`（一次一项） |
| GET | `/api/recoveries` `/api/recoveries/{id}` | 任务状态查询 |
| POST | `/api/recoveries/{id}/retry` | 失败/取消/被取代后按原点重试 |
| DELETE | `/api/recoveries/{id}` | 取消进行中任务 / 停止已就绪实例 |
| GET | `/api/recoveries/{id}/samples` | 查询恢复实例样品（`station_no`/`sample_no` 过滤，仅 `ready` 后开放） |
| GET | `/api/recoveries/{id}/lineage?sample_no=` | **按恢复任务与样品号查询变更沿革**（分代事件 + 记录起点） |
| POST | `/api/recoveries/{id}/query` | 只读 SQL（演示用，限单条 SELECT，连接强制只读兜底） |

### 恢复任务状态机

```
queued → waiting_wal → preparing → recovering → verifying → ready
           (等待WAL归档齐备)                            ↘ failed（可 retry）
ready --(新任务就绪前)--> superseded        ready --(DELETE)--> stopped
进行中 --(DELETE)--> cancelled             API 重启：进行中→重新排队；ready→自动拉起
```

## 手动操作示例（curl）

```bash
# 1. 等基准备份完成
curl -s localhost:8000/api/basebackup | jq .latest_done

# 2. 登记核对点（等 status 变 ready，通常几秒）
curl -s -X POST localhost:8000/api/checkpoints -H 'content-type: application/json' -d '{"name":"cp1"}'
curl -s localhost:8000/api/checkpoints/cp1

# 3. 启用变更记录，然后源库继续变更（更正、误删、同号重录……）
curl -s -X POST localhost:8000/api/audit/enable
docker compose exec db psql -U postgres -d water -c "DELETE FROM samples WHERE sample_no='S-2026-0003'"
docker compose exec db psql -U postgres -d water -c "INSERT INTO samples (sample_no, station_no, ph) VALUES ('S-2026-0003','ST-09',8.88)"

# 4. 登记变更后的核对点，发起恢复并等 ready
curl -s -X POST localhost:8000/api/checkpoints -H 'content-type: application/json' -d '{"name":"cp2"}'
curl -s -X POST localhost:8000/api/recoveries -H 'content-type: application/json' -d '{"checkpoint":"cp2"}'
curl -s localhost:8000/api/recoveries/1

# 5. 查沿革：S-2026-0003 的删除与同号重录分两代呈现
curl -s 'localhost:8000/api/recoveries/1/lineage?sample_no=S-2026-0003'
```

## 限制与说明

- 演示环境口令为 `postgres/postgres`（scram-sha-256，复制连接已在 `db/init/02_hba.sh` 放行），生产使用请更换口令并收敛 pg_hba；
- 归档卷只增不减，生产需自行配置 WAL 保留/清理策略（如 `pg_archivecleanup` + 备份周期）；
- 重做基准备份（`force=true`）会使早于新备份起点的核对点过期（`expired`）；
- 恢复实例监听容器内 55433，仅 API 经本机 socket 访问，不对外暴露；
- `/query` 为演示便利，真正兜底是连接级 `default_transaction_read_only=on`，任何写操作都会被数据库拒绝；
- 变更记录只覆盖启用之后的 INSERT/UPDATE/DELETE：启用前的变更无事件可查（记录起点已标明）；`TRUNCATE` 被触发器禁止；样品号改名场景请按事件中的 `row_uid` 自行关联（事件记录发生时的样品号）。

## 已验证项（PostgreSQL 16.16 实测）

在 PostgreSQL 16.16 上按 Compose 同拓扑（源库 + WAL 归档 + API + 独立恢复实例）实测通过：

- 冷启动 → 自动基准备份 → `scripts/demo.py` 全部 40 项断言 PASS，覆盖：
  启用前核对点沿革明确提示不可用；启用幂等且记录起点不变；改回原值（6.95→7.55→6.95）
  沿革完整；回滚的 UPDATE/INSERT 不留事件不留行；删后同号重录沿革分两代、行身份不同、
  新代不接旧行；跨点事务的行与事件都不可见；恢复期间源库可写；不拿源库冒充恢复结果；
  实例两次切换后旧任务查样品/沿革均 409；
- 源库护栏：`TRUNCATE samples` 被拒绝；修改 `row_uid` 被拒绝（整事务回滚）；
- 事件内容：DELETE 事件留存旧站号/旧 pH（ST-02/7.40），INSERT 事件留存新值；
- API 重启后 ready 实例自动拉起，沿革查询继续可用（分代、记录起点不变）；
- Swagger（/docs、/openapi.json）包含全部新端点；错误路径：任务不存在 404、
  缺参数 422、任务未就绪/被取代 409；
- 干净环境冷启动完整重跑：全部断言再次 PASS。

历史已验证项（PostgreSQL 16.10，沿用既有机制）：归档滞后时任务停留 `waiting_wal`、
损坏 WAL 段如实 `failed` 可 `retry`、API 被 SIGKILL 后自动清理孤儿实例并重试、
并发第二个恢复请求 409、恢复实例直连写入被只读拒绝、强制重做备份后旧核对点 `expired`。

