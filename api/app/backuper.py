"""物理基准备份：pg_basebackup 在线拷贝，源库全程可写。"""
import json
import os
import shutil
import subprocess
import threading

from . import config, state


class BusyError(RuntimeError):
    """已有备份在进行 / 已存在备份且未指定 force。"""


_lock = threading.Lock()


def latest_done() -> dict | None:
    return state.one_row(
        "SELECT * FROM backups WHERE status='done' ORDER BY id DESC LIMIT 1"
    )


def start_backup(force: bool = False) -> int:
    """启动一次基准备份（后台线程执行），返回 backups 表 id。"""
    with _lock:
        if state.one_row("SELECT id FROM backups WHERE status='running' LIMIT 1"):
            raise BusyError("已有基准备份任务正在进行")
        if latest_done() and not force:
            raise BusyError("已存在可用基准备份；如需重做请使用 force=true"
                            "（注意：早于新备份起点的核对点将过期）")
        bid = state.execute(
            "INSERT INTO backups (path, status, created_at) VALUES (?,?,?)",
            (os.path.join(config.BACKUP_DIR, "base"), "running", state.now()),
        )
    threading.Thread(target=_run, args=(bid,), name=f"basebackup-{bid}", daemon=True).start()
    return bid


def _run(bid: int) -> None:
    tmp = os.path.join(config.BACKUP_DIR, f".tmp-{bid}")
    final = os.path.join(config.BACKUP_DIR, "base")
    try:
        shutil.rmtree(tmp, ignore_errors=True)
        env = dict(os.environ, PGPASSWORD=config.SOURCE_PASSWORD)
        subprocess.run(
            [
                "pg_basebackup",
                "-h", config.SOURCE_HOST,
                "-p", str(config.SOURCE_PORT),
                "-U", config.SOURCE_USER,
                "-D", tmp,
                "-Fp",          # plain 格式，目录可直接作为数据目录恢复
                "-X", "stream", # 备份期间所需 WAL 一并流式拷贝，备份自洽
                "-c", "fast",
            ],
            check=True, env=env, capture_output=True, text=True, timeout=1800,
        )
        start_lsn, timeline = _read_start_lsn(tmp)
        # 原子交换：先把旧备份挪走再换上新备份，避免恢复任务拷贝时遇到空窗
        old = os.path.join(config.BACKUP_DIR, f".old-{bid}")
        shutil.rmtree(old, ignore_errors=True)
        if os.path.isdir(final):
            os.rename(final, old)  # 旧备份被取代；过期核对点由刷新线程标记 expired
        os.rename(tmp, final)
        shutil.rmtree(old, ignore_errors=True)
        state.execute(
            "UPDATE backups SET status='done', start_lsn=?, timeline=?,"
            " size_bytes=?, finished_at=? WHERE id=?",
            (start_lsn, timeline, _dir_size(final), state.now(), bid),
        )
    except Exception as e:  # noqa: BLE001 - 任何失败都如实落库
        shutil.rmtree(tmp, ignore_errors=True)
        state.execute(
            "UPDATE backups SET status='failed', error=?, finished_at=? WHERE id=?",
            (str(e)[-2000:], state.now(), bid),
        )


def _read_start_lsn(path: str) -> tuple[str, int]:
    """从 backup_manifest 取备份起始 LSN 与时间线（失败则回退解析 backup_label）。"""
    try:
        with open(os.path.join(path, "backup_manifest")) as f:
            data = json.load(f)
        wr = data["WAL-Ranges"][0]
        return wr["Start-LSN"], int(wr["Timeline"])
    except Exception:
        with open(os.path.join(path, "backup_label")) as f:
            for line in f:
                if line.startswith("START WAL LOCATION"):
                    lsn = line.split(":", 1)[1].split("(")[0].strip()
                    return lsn, 1
        raise


def _dir_size(path: str) -> int:
    total = 0
    for root, _dirs, files in os.walk(path):
        for name in files:
            try:
                total += os.path.getsize(os.path.join(root, name))
            except OSError:
                pass
    return total
