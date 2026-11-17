"""把某个命名核对点恢复到独立的只读 PostgreSQL 实例。

关键保证：
- 只重放到核对点 LSN：recovery_target_action=pause 停下，校验位点后才 promote，
  跨点之后才提交的事务绝不会出现在恢复结果里；
- WAL 未归档齐备时任务停留在 waiting_wal，不开放目标查询；
- 任何一步失败都标记 failed 并保留日志，绝不上报"半库"为成功；
- 全程不触碰生产库数据（源库仅承担归档与恢复点元数据）。
"""
import os
import shutil
import subprocess
import threading
import time

import psycopg
from psycopg.rows import dict_row

from . import config, state, source
from .walutil import lsn_to_int, wal_file_name


class Cancelled(Exception):
    """任务被用户取消。"""


# ---------------------------------------------------------------- 调度

def dispatch_loop(stop: threading.Event) -> None:
    """串行调度：同一时间只运行一个恢复任务。"""
    runner: threading.Thread | None = None
    while not stop.is_set():
        try:
            if runner is None or not runner.is_alive():
                job = state.one_row(
                    "SELECT id FROM jobs WHERE status='queued' ORDER BY id LIMIT 1"
                )
                if job:
                    runner = threading.Thread(
                        target=run_job, args=(job["id"],),
                        name=f"recovery-job-{job['id']}", daemon=True,
                    )
                    runner.start()
        except Exception:
            pass
        stop.wait(1.0)


def resume_after_restart() -> None:
    """API 重启后的状态修复。

    - 进行中的任务：重新排队，按原核对点从头重试（幂等，数据目录会重建）；
    - 已 ready 的任务：数据目录仍在，直接拉起实例继续提供只读查询。
    """
    active = ",".join(f"'{s}'" for s in state.ACTIVE_JOB_STATES)
    for j in state.all_rows(f"SELECT * FROM jobs WHERE status IN ({active})"):
        if j.get("data_dir"):
            stop_instance(j["data_dir"])  # 防御：进程一般已随重启消失
        state.update_job(j["id"], status="queued", cancel_requested=0,
                         detail="API 重启，按原核对点自动重试")
    for j in state.all_rows("SELECT * FROM jobs WHERE status='ready'"):
        try:
            ddir = j["data_dir"]
            if not (ddir and os.path.isdir(ddir)):
                raise RuntimeError("恢复实例数据目录已不存在")
            if not _postgres_alive(ddir):
                _pg_ctl(ddir, "start", os.path.join(ddir, "server.log"))
            with _control_conn() as con:
                con.execute("SELECT 1")
            state.update_job(j["id"], detail="API 重启后实例已重新拉起，可继续只读查询")
        except Exception as e:  # noqa: BLE001
            state.update_job(j["id"], status="failed",
                             error=f"API 重启后拉起实例失败：{e}")


# ---------------------------------------------------------------- 任务执行

def run_job(job_id: int) -> None:
    job = state.one_row("SELECT * FROM jobs WHERE id=?", (job_id,))
    if not job:
        return
    cp = state.one_row(
        "SELECT * FROM checkpoints WHERE name=?", (job["checkpoint_name"],)
    )
    ddir = os.path.join(config.RECOVERY_DIR, f"job-{job_id}")
    logfile = os.path.join(ddir, "server.log")
    started_pg = False
    try:
        if not cp:
            raise RuntimeError(f"核对点 {job['checkpoint_name']} 不存在")

        # 1) 等待 WAL 归档齐备（核对点落盘 + 备份起点到核对点的段全部在归档中）
        state.update_job(job_id, status="waiting_wal", detail="等待核对点 WAL 归档")
        seg_size = source.wal_segment_size()
        backup = None
        while True:
            _check_cancel(job_id)
            cp = state.one_row("SELECT * FROM checkpoints WHERE id=?", (cp["id"],))
            if cp["status"] == "expired":
                raise RuntimeError("核对点已过期（存在更新的基准备份），无法恢复")
            backup = state.one_row(
                "SELECT * FROM backups WHERE status='done' ORDER BY id DESC LIMIT 1"
            )
            if not backup:
                raise RuntimeError("没有可用的基准备份，请先 POST /api/basebackup")
            if lsn_to_int(cp["restore_lsn"]) < lsn_to_int(backup["start_lsn"]):
                raise RuntimeError(
                    f"核对点 LSN {cp['restore_lsn']} 早于基准备份起点 "
                    f"{backup['start_lsn']}，无法恢复"
                )
            if cp["status"] == "ready":
                missing = _missing_segments(backup, cp, seg_size)
                if not missing:
                    break
                state.update_job(
                    job_id, status="waiting_wal",
                    detail=f"等待 {len(missing)} 个 WAL 段归档，如 {missing[0]}",
                )
            time.sleep(config.POLL_INTERVAL)

        # 2) 一次只恢复一项：取代当前 ready 实例
        _supersede_ready(job_id)

        # 3) 拷贝基准备份为独立数据目录
        state.update_job(job_id, status="preparing", detail="拷贝基准备份",
                         data_dir=ddir, port=config.RECOVERY_PORT)
        if _postgres_alive(ddir):
            stop_instance(ddir)
        shutil.rmtree(ddir, ignore_errors=True)
        os.makedirs(config.RECOVERY_DIR, exist_ok=True)
        shutil.copytree(backup["path"], ddir, symlinks=True)
        os.chmod(ddir, 0o700)
        _write_recovery_conf(ddir, cp["name"])
        _check_cancel(job_id)

        # 4) 启动实例，WAL 重放到核对点自动暂停
        state.update_job(job_id, status="recovering", detail="WAL 重放中")
        _pg_ctl(ddir, "start", logfile)
        started_pg = True
        _wait_replay_paused(job_id, cp, ddir)

        # 5) 校验确实停在核对点（杜绝半库报成功），随后 promote 并核查只读
        state.update_job(job_id, status="verifying", detail="校验恢复位点并提升")
        rows = _verify_and_promote(cp, ddir)
        state.update_job(job_id, status="ready",
                         detail=f"恢复完成，目标库只读，samples 现有 {rows} 行",
                         ready_at=state.now(), error=None, log_tail=None)
    except Cancelled:
        if started_pg:
            stop_instance(ddir)
        state.update_job(job_id, status="cancelled", detail="已取消")
    except Exception as e:  # noqa: BLE001 - 任何失败都如实标记，绝不上报成功
        if started_pg:
            stop_instance(ddir)
        state.update_job(job_id, status="failed", error=str(e)[-4000:],
                         log_tail=_tail(logfile) if os.path.exists(logfile) else None)


def _check_cancel(job_id: int) -> None:
    job = state.one_row("SELECT cancel_requested FROM jobs WHERE id=?", (job_id,))
    if job and job["cancel_requested"]:
        raise Cancelled()


def _missing_segments(backup: dict, cp: dict, seg_size: int) -> list[str]:
    """备份起点 → 核对点之间，归档与备份自带 pg_wal 里都找不到的 WAL 段。"""
    start = lsn_to_int(backup["start_lsn"])
    end = lsn_to_int(cp["restore_lsn"])
    tli = int(backup.get("timeline") or 1)
    backup_wal_dir = os.path.join(backup["path"], "pg_wal")
    missing = []
    seg, last = start // seg_size, end // seg_size
    while seg <= last and len(missing) <= 10000:
        name = wal_file_name(seg * seg_size, seg_size, tli)
        in_archive = os.path.exists(os.path.join(config.ARCHIVE_DIR, name))
        in_backup = os.path.exists(os.path.join(backup_wal_dir, name))
        if not (in_archive or in_backup):
            missing.append(name)
        seg += 1
    return missing


def _write_recovery_conf(ddir: str, cp_name: str) -> None:
    # recovery.signal 触发归档恢复模式；配置追加到 auto.conf 末尾（后写优先）
    open(os.path.join(ddir, "recovery.signal"), "w").close()
    conf = f"""
# ---- pitr-api 追加：PITR 恢复到命名核对点，提升后只读 ----
restore_command = 'cp {config.ARCHIVE_DIR}/%f %p'
recovery_target_name = '{cp_name}'
recovery_target_action = 'pause'
recovery_target_timeline = 'latest'
archive_mode = off
port = {config.RECOVERY_PORT}
listen_addresses = '{config.RECOVERY_HOST}'
unix_socket_directories = '{config.RECOVERY_SOCKET_DIR}'
default_transaction_read_only = on
hot_standby = on
"""
    with open(os.path.join(ddir, "postgresql.auto.conf"), "a") as f:
        f.write(conf)


def _wait_replay_paused(job_id: int, cp: dict, ddir: str) -> None:
    deadline = time.monotonic() + config.RECOVERY_TIMEOUT
    target = lsn_to_int(cp["restore_lsn"])
    while time.monotonic() < deadline:
        _check_cancel(job_id)
        if not _postgres_alive(ddir):
            raise RuntimeError("恢复实例进程退出（核对点不可达或 WAL 缺失），详见日志")
        try:
            with _control_conn() as con:
                in_rec, paused, last = con.execute(
                    "SELECT pg_is_in_recovery(), pg_is_wal_replay_paused(),"
                    " COALESCE(pg_last_wal_replay_lsn()::text, '')"
                ).fetchone()
            if not in_rec:
                raise RuntimeError("实例未处于恢复状态，恢复目标配置异常")
            if paused and last and lsn_to_int(last) >= target:
                return
        except psycopg.Error:
            pass  # 还没到可连接阶段，继续等
        time.sleep(1)
    raise TimeoutError(
        f"等待 {config.RECOVERY_TIMEOUT}s 仍未重放到核对点（WAL 可能未归档齐备）"
    )


def _verify_and_promote(cp: dict, ddir: str) -> int:
    target = lsn_to_int(cp["restore_lsn"])
    with _control_conn() as con:
        in_rec, _paused, last = con.execute(
            "SELECT pg_is_in_recovery(), pg_is_wal_replay_paused(),"
            " COALESCE(pg_last_wal_replay_lsn()::text, '')"
        ).fetchone()
        if not in_rec:
            raise RuntimeError("校验失败：实例不在恢复状态")
        if not last or lsn_to_int(last) < target:
            raise RuntimeError(
                f"校验失败：重放位点 {last or '-'} 未到达核对点 {cp['restore_lsn']}"
            )
        con.execute("SELECT pg_wal_replay_resume()")
    # 等待 promote 完成
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        if not _postgres_alive(ddir):
            raise RuntimeError("promote 期间实例退出")
        try:
            with _control_conn() as con:
                if not con.execute("SELECT pg_is_in_recovery()").fetchone()[0]:
                    break
        except psycopg.Error:
            pass
        time.sleep(1)
    else:
        raise TimeoutError("promote 超时")
    # sanity：表可查询 + 实例确为只读
    with _control_conn() as con:
        rows = con.execute("SELECT count(*) FROM public.samples").fetchone()[0]
        ro = con.execute("SHOW default_transaction_read_only").fetchone()[0]
        if str(ro).lower() != "on":
            raise RuntimeError("目标库未处于只读模式")
    return rows


# ---------------------------------------------------------------- 实例管理

def stop_instance(ddir: str) -> None:
    """尽力停止恢复实例（供取代/取消/关停调用）。"""
    if not ddir or not os.path.isdir(ddir):
        return
    try:
        subprocess.run(
            ["pg_ctl", "-D", ddir, "-m", "fast", "-w", "-t", "60", "stop"],
            capture_output=True, text=True, timeout=90,
        )
        return
    except Exception:
        pass
    try:
        with open(os.path.join(ddir, "postmaster.pid")) as f:
            pid = int(f.readline().strip())
        os.kill(pid, 15)
        for _ in range(30):
            time.sleep(1)
            if not _pid_alive(pid):
                return
        os.kill(pid, 9)
    except Exception:
        pass


def _supersede_ready(current_job_id: int) -> None:
    for j in state.all_rows("SELECT * FROM jobs WHERE status='ready'"):
        if j.get("data_dir"):
            stop_instance(j["data_dir"])
            shutil.rmtree(j["data_dir"], ignore_errors=True)
        state.update_job(j["id"], status="superseded",
                         detail=f"被恢复任务 #{current_job_id} 取代（一次只恢复一项）")


def _pg_ctl(ddir: str, action: str, logfile: str) -> None:
    try:
        subprocess.run(
            ["pg_ctl", "-D", ddir, "-l", logfile, "-w", "-t", "120", action],
            check=True, capture_output=True, text=True, timeout=150,
        )
    except subprocess.CalledProcessError as e:
        # start 场景下若进程其实活着（慢恢复），交给后续等待循环判定
        if action == "start" and _postgres_alive(ddir):
            return
        raise RuntimeError(f"pg_ctl {action} 失败: {(e.stderr or e.stdout or e)[-1000:]}")


def _postgres_alive(ddir: str) -> bool:
    try:
        with open(os.path.join(ddir, "postmaster.pid")) as f:
            pid = int(f.readline().strip())
        return _pid_alive(pid)
    except Exception:
        return False


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def _tail(path: str, lines: int = 50) -> str:
    try:
        with open(path, errors="replace") as f:
            return "".join(f.readlines()[-lines:])
    except OSError:
        return ""


# ---------------------------------------------------------------- 连接与查询

def _control_conn() -> psycopg.Connection:
    """恢复实例的管理连接（本机 unix socket）。"""
    return psycopg.connect(
        host=config.RECOVERY_SOCKET_DIR,
        port=config.RECOVERY_PORT,
        dbname=config.SOURCE_DB,
        user=config.SOURCE_USER,
        password=config.SOURCE_PASSWORD,
        autocommit=True,
        connect_timeout=3,
        application_name="pitr-api-control",
    )


def _query_conn() -> psycopg.Connection:
    """恢复实例的查询连接：强制只读 + 语句超时，双保险。"""
    return psycopg.connect(
        host=config.RECOVERY_SOCKET_DIR,
        port=config.RECOVERY_PORT,
        dbname=config.SOURCE_DB,
        user=config.SOURCE_USER,
        password=config.SOURCE_PASSWORD,
        autocommit=True,
        connect_timeout=3,
        application_name="pitr-api-query",
        options="-c default_transaction_read_only=on -c statement_timeout=15000",
        row_factory=dict_row,
    )


def fetch_samples(station_no: str | None, sample_no: str | None,
                  limit: int) -> list[dict]:
    sql = ("SELECT id, sample_no, station_no, ph::float8 AS ph, created_at"
           " FROM public.samples")
    where, params = [], []
    if station_no:
        where.append("station_no = %s")
        params.append(station_no)
    if sample_no:
        where.append("sample_no = %s")
        params.append(sample_no)
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY id LIMIT %s"
    params.append(limit)
    with _query_conn() as con:
        return con.execute(sql, params).fetchall()


_ALLOWED_HEADS = ("select", "with", "values", "table")


def run_adhoc_query(sql: str, max_rows: int = 500) -> list[dict]:
    """受护栏约束的只读查询（演示用）。

    真正的兜底是连接上的 default_transaction_read_only=on：
    任何写操作都会被数据库直接拒绝。
    """
    s = sql.strip().rstrip(";").strip()
    if not s or ";" in s:
        raise ValueError("只允许单条只读 SELECT 语句")
    head = s.lstrip("(").split(None, 1)[0].lower() if s.lstrip("(") else ""
    if head not in _ALLOWED_HEADS:
        raise ValueError("只允许 SELECT/WITH/VALUES/TABLE 开头的只读语句")
    with _query_conn() as con:
        cur = con.execute(sql)
        return cur.fetchmany(max_rows)
