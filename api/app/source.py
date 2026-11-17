"""对生产源库的操作。

注意：除 audit 模块的显式启用动作外，本模块绝不执行任何 DML/DDL，不改写生产数据。
仅做：连通性检查、命名恢复点（WAL 记录）、WAL 切换、参数与记录起点读取。
"""
import psycopg

from . import config


def _conn() -> psycopg.Connection:
    return psycopg.connect(
        host=config.SOURCE_HOST,
        port=config.SOURCE_PORT,
        dbname=config.SOURCE_DB,
        user=config.SOURCE_USER,
        password=config.SOURCE_PASSWORD,
        autocommit=True,
        connect_timeout=5,
        application_name="pitr-api",
    )


def ping() -> bool:
    try:
        with _conn() as con:
            con.execute("SELECT 1")
        return True
    except Exception:
        return False


def create_restore_point(name: str) -> tuple[str, str]:
    """在源库写入命名恢复点并切换 WAL 段，使该点所在段尽快被归档。

    返回 (restore_lsn, wal_file)。这只是 WAL 级元数据记录，不修改任何业务数据。
    """
    with _conn() as con:
        lsn = con.execute(
            "SELECT pg_create_restore_point(%s)::text", (name,)
        ).fetchone()[0]
        wal_file = con.execute(
            "SELECT pg_walfile_name(%s::pg_lsn)", (lsn,)
        ).fetchone()[0]
        # 切换段，让包含恢复点的段立即闭合进入归档队列
        con.execute("SELECT pg_switch_wal()")
    return lsn, wal_file


def fetch_audit_meta() -> dict | None:
    """读取源库变更记录的记录起点（audit_meta 单行）。

    返回 None 表示源库尚未启用变更记录（表不存在或无记录）。
    连接失败会抛异常，由调用方决定如何降级。只读查询。
    """
    try:
        with _conn() as con:
            row = con.execute(
                "SELECT enabled_at, enabled_lsn, note FROM public.audit_meta WHERE id"
            ).fetchone()
    except psycopg.errors.UndefinedTable:
        return None
    if not row:
        return None
    return {
        "enabled_at": row[0].isoformat() if row[0] else None,
        "enabled_lsn": row[1],
        "note": row[2],
    }


def wal_segment_size() -> int:
    with _conn() as con:
        raw = con.execute("SHOW wal_segment_size").fetchone()[0]
    return _parse_size(str(raw))


def _parse_size(s: str) -> int:
    s = s.strip()
    for unit, mult in (("GB", 1 << 30), ("MB", 1 << 20), ("kB", 1 << 10), ("B", 1)):
        if s.endswith(unit):
            return int(float(s[: -len(unit)]) * mult)
    return int(s)
