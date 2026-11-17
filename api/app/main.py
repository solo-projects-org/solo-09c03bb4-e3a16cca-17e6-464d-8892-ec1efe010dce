"""水质数仓单实例 PITR 恢复 API。

职责：物理基准备份 + WAL 持续归档之上，登记命名核对点，
把核对点恢复到独立只读实例并开放样品查询与变更沿革查询。Swagger 见 /docs。
"""
import os
import threading
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field

from . import audit, backuper, config, recoverer, source, state
from .walutil import lsn_to_int

_stop = threading.Event()

DESCRIPTION = """
面向水质数仓值班场景的单实例时间点恢复（PITR）服务。

* 基准备份：`pg_basebackup` 在线物理拷贝，源库全程可写；
* 核对点：`pg_create_restore_point` 命名恢复点 + WAL 归档齐备检查；
* 恢复：基准备份 + 归档 WAL 重放到核对点（pause → 校验位点 → promote），
  产出**独立只读**实例；跨点后才提交的事务不会出现；
* 查询：只打恢复实例，绝不用当前源库冒充恢复结果；
* 变更沿革：启用变更记录后，生产库新增/更正/删除在**同一事务**留下
  变更事件（原行身份 + 变更前后站号、pH，回滚不留痕）；恢复实例上按
  恢复任务号 + 样品号查询沿革，事件与样品处于同一时点；
* 一次只恢复一项；失败或重启后可按原核对点重试。
"""


# ------------------------------------------------------------------ 模型

class CheckpointIn(BaseModel):
    name: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$",
                      examples=["cp1"], description="核对点名称（唯一）")


class RecoveryIn(BaseModel):
    checkpoint: str = Field(examples=["cp1"], description="要恢复到的核对点名称")


class QueryIn(BaseModel):
    sql: str = Field(examples=["SELECT * FROM samples ORDER BY id"],
                     description="单条只读 SELECT 语句")


# ------------------------------------------------------------------ 生命周期

def _refresher_loop() -> None:
    while not _stop.is_set():
        try:
            refresh_checkpoints()
        except Exception:
            pass
        try:
            audit.refresh_status()  # 刷新记录起点缓存（供核对点沿革提示）
        except Exception:
            pass
        _stop.wait(config.POLL_INTERVAL)


def refresh_checkpoints() -> None:
    """waiting_wal → ready（WAL 已归档）；新基准备份使更早核对点 expired。"""
    for cp in state.all_rows("SELECT * FROM checkpoints WHERE status='waiting_wal'"):
        if os.path.exists(os.path.join(config.ARCHIVE_DIR, cp["wal_file"])):
            state.execute(
                "UPDATE checkpoints SET status='ready', ready_at=? WHERE id=?",
                (state.now(), cp["id"]),
            )
    backup = backuper.latest_done()
    if backup:
        for cp in state.all_rows(
            "SELECT * FROM checkpoints WHERE status IN ('waiting_wal','ready')"
        ):
            if lsn_to_int(cp["restore_lsn"]) < lsn_to_int(backup["start_lsn"]):
                state.execute(
                    "UPDATE checkpoints SET status='expired' WHERE id=?", (cp["id"],)
                )


@asynccontextmanager
async def lifespan(_app: FastAPI):
    for d in (config.BACKUP_DIR, config.RECOVERY_DIR,
              os.path.dirname(config.STATE_DB) or "."):
        os.makedirs(d, exist_ok=True)
    state.init_db()
    recoverer.resume_after_restart()
    threading.Thread(target=_refresher_loop, name="refresher", daemon=True).start()
    threading.Thread(target=recoverer.dispatch_loop, args=(_stop,),
                     name="dispatcher", daemon=True).start()
    # 首次启动自动制作一份基准备份
    if not state.one_row(
        "SELECT id FROM backups WHERE status IN ('running','done') LIMIT 1"
    ):
        try:
            backuper.start_backup()
        except Exception:
            pass
    yield
    _stop.set()
    for j in state.all_rows(
        "SELECT * FROM jobs WHERE status IN"
        " ('preparing','recovering','verifying','ready')"
    ):
        if j.get("data_dir"):
            recoverer.stop_instance(j["data_dir"])


app = FastAPI(title="水质数仓 PITR 恢复服务", version="1.0.0",
              description=DESCRIPTION, lifespan=lifespan)


# ------------------------------------------------------------------ 工具

def _job_or_404(job_id: int) -> dict:
    job = state.one_row("SELECT * FROM jobs WHERE id=?", (job_id,))
    if not job:
        raise HTTPException(404, f"恢复任务 #{job_id} 不存在")
    return job


def _cp_or_404(name: str) -> dict:
    cp = state.one_row("SELECT * FROM checkpoints WHERE name=?", (name,))
    if not cp:
        raise HTTPException(404, f"核对点 {name} 不存在")
    return cp


def _ensure_no_active_job() -> None:
    active = ",".join(f"'{s}'" for s in state.ACTIVE_JOB_STATES)
    j = state.one_row(f"SELECT id, status FROM jobs WHERE status IN ({active})"
                      " ORDER BY id DESC LIMIT 1")
    if j:
        raise HTTPException(
            409, f"已有进行中的恢复任务 #{j['id']}（{j['status']}），一次只恢复一项"
        )


def _cp_view(cp: dict) -> dict:
    """核对点视图：附加变更沿革可用性提示（以记录起点 LSN 为参照的预判）。

    预判仅供筛选；恢复实例上的物理检查（事件表是否存在）才是最终判定。
    """
    view = dict(cp)
    a = state.get_audit_state()
    if not a or not a["enabled"]:
        view["lineage_available"] = False
        view["lineage_note"] = ("源库尚未启用变更记录（POST /api/audit/enable），"
                                "该核对点的变更沿革不可用")
    elif a["enabled_lsn"] and \
            lsn_to_int(cp["restore_lsn"]) < lsn_to_int(a["enabled_lsn"]):
        view["lineage_available"] = False
        view["lineage_note"] = (f"该核对点早于变更记录启用时点"
                                f"（记录起点 {a['enabled_lsn']}），变更沿革不可用")
    else:
        view["lineage_available"] = True
        view["lineage_note"] = (f"变更沿革可用；记录起点 {a['enabled_lsn']}，"
                                "仅含起点之后提交的事件")
    return view


# ------------------------------------------------------------------ 基础

@app.get("/", summary="服务信息")
def root():
    return {"service": "水质数仓 PITR 恢复服务", "docs": "/docs",
            "api": ["/api/health", "/api/audit/status", "/api/basebackup",
                    "/api/checkpoints", "/api/recoveries"]}


@app.get("/api/health", summary="健康检查")
def health():
    src = source.ping()
    archived = len([f for f in os.listdir(config.ARCHIVE_DIR)
                    if not f.startswith(".")]) if os.path.isdir(config.ARCHIVE_DIR) else 0
    a = state.get_audit_state()
    return {
        "status": "ok" if src else "degraded",
        "source_reachable": src,
        "archive_dir": config.ARCHIVE_DIR,
        "archived_wal_files": archived,
        "audit_enabled": bool(a and a["enabled"]),
    }


# ------------------------------------------------------------------ 变更记录（质控沿革）

@app.post("/api/audit/enable", tags=["audit"],
          summary="启用样品变更记录（幂等，单事务；显式管理动作）")
def enable_audit():
    """在生产源库启用变更记录：单事务创建事件表、行身份列与触发器。

    此后生产库 INSERT/UPDATE/DELETE 会在同一事务留下变更事件
    （原行身份 row_uid + 变更前后站号、pH）；回滚不留痕。
    启用时点（WAL 位点）记入 audit_meta，作为沿革的记录起点。
    """
    try:
        return audit.enable_recording()
    except Exception as e:  # noqa: BLE001
        raise HTTPException(502, f"启用变更记录失败（已整体回滚，不留半成品）: {e}")


@app.get("/api/audit/status", tags=["audit"], summary="变更记录启用状态与记录起点")
def audit_status():
    return audit.status()


# ------------------------------------------------------------------ 基准备份

@app.get("/api/basebackup", tags=["backup"], summary="基准备份状态")
def backup_status():
    return {
        "latest_done": backuper.latest_done(),
        "running": state.one_row(
            "SELECT * FROM backups WHERE status='running'"
            " ORDER BY id DESC LIMIT 1"),
        "history": state.all_rows(
            "SELECT * FROM backups ORDER BY id DESC LIMIT 10"),
    }


@app.post("/api/basebackup", tags=["backup"], status_code=202,
          summary="制作物理基准备份（pg_basebackup，源库不停写）")
def make_backup(force: bool = Query(
        False, description="已存在备份时是否强制重做（更早核对点将过期）")):
    try:
        bid = backuper.start_backup(force=force)
    except backuper.BusyError as e:
        raise HTTPException(409, str(e))
    return {"id": bid, "status": "running"}


# ------------------------------------------------------------------ 核对点

@app.post("/api/checkpoints", tags=["checkpoint"], status_code=201,
          summary="登记命名核对点（pg_create_restore_point）")
def create_checkpoint(body: CheckpointIn):
    if state.one_row("SELECT id FROM checkpoints WHERE name=?", (body.name,)):
        raise HTTPException(409, f"核对点 {body.name} 已存在")
    try:
        lsn, wal_file = source.create_restore_point(body.name)
    except Exception as e:  # noqa: BLE001
        raise HTTPException(502, f"在源库创建命名恢复点失败: {e}")
    cid = state.execute(
        "INSERT INTO checkpoints (name, restore_lsn, wal_file, status, created_at)"
        " VALUES (?,?,?,?,?)",
        (body.name, lsn, wal_file, "waiting_wal", state.now()),
    )
    refresh_checkpoints()  # 立即检查一次，通常几秒内即 ready
    return _cp_view(state.one_row("SELECT * FROM checkpoints WHERE id=?", (cid,)))


@app.get("/api/checkpoints", tags=["checkpoint"], summary="核对点列表")
def list_checkpoints():
    return [_cp_view(cp) for cp in state.all_rows("SELECT * FROM checkpoints ORDER BY id")]


@app.get("/api/checkpoints/{name}", tags=["checkpoint"], summary="核对点详情")
def get_checkpoint(name: str):
    return _cp_view(_cp_or_404(name))


# ------------------------------------------------------------------ 恢复任务

@app.post("/api/recoveries", tags=["recovery"], status_code=202,
          summary="按核对点发起恢复（一次只恢复一项）")
def start_recovery(body: RecoveryIn):
    cp = _cp_or_404(body.checkpoint)
    if cp["status"] == "expired":
        raise HTTPException(409, "核对点已过期（存在更新的基准备份），无法恢复")
    _ensure_no_active_job()
    jid = state.execute(
        "INSERT INTO jobs (checkpoint_name, status, created_at, updated_at)"
        " VALUES (?,?,?,?)",
        (body.checkpoint, "queued", state.now(), state.now()),
    )
    return state.one_row("SELECT * FROM jobs WHERE id=?", (jid,))


@app.get("/api/recoveries", tags=["recovery"], summary="恢复任务列表")
def list_recoveries():
    return state.all_rows("SELECT * FROM jobs ORDER BY id DESC LIMIT 50")


@app.get("/api/recoveries/{job_id}", tags=["recovery"], summary="恢复任务状态")
def get_recovery(job_id: int):
    return _job_or_404(job_id)


@app.post("/api/recoveries/{job_id}/retry", tags=["recovery"], status_code=202,
          summary="按原核对点重试（失败/取消/被取代后）")
def retry_recovery(job_id: int):
    job = _job_or_404(job_id)
    if job["status"] not in state.RETRYABLE_JOB_STATES:
        raise HTTPException(409, f"任务状态 {job['status']} 不可重试")
    cp = _cp_or_404(job["checkpoint_name"])
    if cp["status"] == "expired":
        raise HTTPException(409, "核对点已过期（存在更新的基准备份），无法恢复")
    _ensure_no_active_job()
    jid = state.execute(
        "INSERT INTO jobs (checkpoint_name, status, created_at, updated_at)"
        " VALUES (?,?,?,?)",
        (job["checkpoint_name"], "queued", state.now(), state.now()),
    )
    return state.one_row("SELECT * FROM jobs WHERE id=?", (jid,))


@app.delete("/api/recoveries/{job_id}", tags=["recovery"],
            summary="取消进行中任务 / 停止已就绪实例")
def cancel_recovery(job_id: int):
    job = _job_or_404(job_id)
    if job["status"] in state.ACTIVE_JOB_STATES:
        state.update_job(job_id, cancel_requested=1, detail="取消请求已受理")
        return {"id": job_id, "status": job["status"], "cancel_requested": True}
    if job["status"] == "ready":
        if job.get("data_dir"):
            recoverer.stop_instance(job["data_dir"])
        state.update_job(job_id, status="stopped", detail="实例已停止")
        return state.one_row("SELECT * FROM jobs WHERE id=?", (job_id,))
    return job


# ------------------------------------------------------------------ 目标查询

def _ready_job_or_409(job_id: int) -> dict:
    job = _job_or_404(job_id)
    if job["status"] != "ready":
        raise HTTPException(
            409, f"恢复任务未就绪（当前状态 {job['status']}），目标查询未开放")
    return job


def _query_meta(job: dict) -> dict:
    cp = state.one_row("SELECT * FROM checkpoints WHERE name=?",
                       (job["checkpoint_name"],))
    return {
        "job_id": job["id"],
        "checkpoint": job["checkpoint_name"],
        "restore_lsn": cp["restore_lsn"] if cp else None,
        "queried_instance": f"recovered-instance:{config.RECOVERY_PORT}"
                            f"（独立只读库，非生产源库）",
        "instance_data_dir": job.get("data_dir"),
        "read_only": True,
    }


def _run_instance_query(job: dict, fn, *args):
    """执行恢复实例查询；实例被切换时如实报 409，绝不张冠李戴。"""
    try:
        return fn(job["data_dir"], *args)
    except recoverer.InstanceSwitched as e:
        raise HTTPException(409, str(e))
    except ValueError:
        raise  # 查询语句护栏的 400 由调用方处理
    except Exception as e:  # noqa: BLE001
        raise HTTPException(502, f"查询恢复实例失败: {e}")


@app.get("/api/recoveries/{job_id}/samples", tags=["query"],
         summary="查询恢复实例中的样品（仅 ready 后开放）")
def query_samples(job_id: int,
                  station_no: str | None = Query(None, description="按站号过滤"),
                  sample_no: str | None = Query(None, description="按样品号过滤"),
                  limit: int = Query(100, ge=1, le=1000)):
    job = _ready_job_or_409(job_id)
    rows = _run_instance_query(job, recoverer.fetch_samples, station_no,
                               sample_no, limit)
    return {**_query_meta(job), "row_count": len(rows), "rows": rows}


@app.get("/api/recoveries/{job_id}/lineage", tags=["query"],
         summary="按恢复任务与样品号查询变更沿革（仅 ready 后开放）")
def query_lineage(job_id: int,
                  sample_no: str = Query(..., description="样品号",
                                         examples=["S-2026-0003"])):
    """查清样品变更经过（如 pH 改回原值、删后同号重录）。

    - 只返回所选核对点前已提交的变更；事件与样品当前行来自同一恢复实例的同一时点；
    - 沿革按行身份（row_uid）分代：删后同号重录的新行是新一代，不接旧行沿革；
    - 启用变更记录前的旧核对点：明确返回 lineage_available=false 及原因；
    - 启用后的核对点：返回记录起点（recording_start）与逐事件沿革。
    """
    job = _ready_job_or_409(job_id)
    result = _run_instance_query(job, recoverer.fetch_lineage, sample_no)
    return {**_query_meta(job), "sample_no": sample_no, **result}


@app.post("/api/recoveries/{job_id}/query", tags=["query"],
          summary="只读 SQL 查询（演示用，限单条 SELECT）")
def adhoc_query(job_id: int, body: QueryIn):
    job = _ready_job_or_409(job_id)
    try:
        rows = _run_instance_query(job, recoverer.run_adhoc_query, body.sql)
    except ValueError as e:
        raise HTTPException(400, str(e))
    return {**_query_meta(job), "row_count": len(rows), "rows": rows}
