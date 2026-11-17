"""样品变更记录（质控沿革）的启用与状态。

启用是**显式的管理动作**：质控/运维调用 `POST /api/audit/enable` 后，
本模块在生产源库上以**单个事务**创建事件表、行身份列与触发器——
此后生产库的 INSERT/UPDATE/DELETE 会在**同一事务**内留下变更事件
（原行身份 row_uid + 变更前后站号、pH）；事务回滚则事件随之消失，不留痕。

- 同号重录：删除后重新插入的新行获得新的 row_uid，沿革按行身份分代，
  不会接到旧行的沿革上；
- 启用前的旧核对点：恢复出的实例中不存在事件表，沿革查询明确提示不可用；
- 启用后：`audit_meta` 单行记录启用时点（WAL 位点），随 PITR 一并恢复，
  作为沿革的记录起点返回。

注意：启用之外的代码路径对源库仍然只读，本模块是唯一对源库执行 DDL 的地方，
且只在被显式调用时执行。
"""
import psycopg

from . import config, source, state

# ---------------------------------------------------------------- 启用脚本
# 幂等：全部对象 IF NOT EXISTS / OR REPLACE / ON CONFLICT，可重复执行。
# 单事务执行：任何一步失败整体回滚，不留半成品。

ENABLE_STATEMENTS = [
    # 变更事件表：与业务 DML 同事务由触发器写入
    """
    CREATE TABLE IF NOT EXISTS public.sample_events (
        id             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        row_uid        uuid NOT NULL,               -- 原行身份（每行唯一，含同号重录的新行）
        sample_no      text NOT NULL,               -- 事件发生时的样品号
        op             text NOT NULL CHECK (op IN ('INSERT','UPDATE','DELETE')),
        old_station_no text,                        -- 变更前站号（INSERT 为 NULL）
        old_ph         numeric(4,2),                -- 变更前 pH（INSERT 为 NULL）
        new_station_no text,                        -- 变更后站号（DELETE 为 NULL）
        new_ph         numeric(4,2),                -- 变更后 pH（DELETE 为 NULL）
        txid           bigint NOT NULL DEFAULT txid_current(),  -- 事务号
        changed_at     timestamptz NOT NULL DEFAULT now()       -- 事务时间
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_sample_events_sample"
    " ON public.sample_events (sample_no, id)",
    "CREATE INDEX IF NOT EXISTS idx_sample_events_row"
    " ON public.sample_events (row_uid, id)",
    # 记录起点标记（单行）：随 PITR 恢复，沿革查询据此标明记录起点
    """
    CREATE TABLE IF NOT EXISTS public.audit_meta (
        id          boolean PRIMARY KEY DEFAULT true CHECK (id),
        enabled_at  timestamptz NOT NULL DEFAULT now(),
        enabled_lsn text NOT NULL,                  -- 启用时点的 WAL 位点
        note        text
    )
    """,
    # 行身份列：既有行在 ADD COLUMN 时逐行获得随机 uuid（volatile 默认值触发表重写）
    "ALTER TABLE public.samples"
    " ADD COLUMN IF NOT EXISTS row_uid uuid NOT NULL DEFAULT gen_random_uuid()",
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_samples_row_uid"
    " ON public.samples (row_uid)",
    # 事件触发器：同事务记录变更前后值；row_uid 不可改（改则整事务回滚）
    """
    CREATE OR REPLACE FUNCTION public.log_sample_event() RETURNS trigger
    LANGUAGE plpgsql AS $func$
    BEGIN
        IF TG_OP = 'UPDATE' AND NEW.row_uid IS DISTINCT FROM OLD.row_uid THEN
            RAISE EXCEPTION 'row_uid 是行身份，不可修改';
        END IF;
        IF TG_OP = 'INSERT' THEN
            INSERT INTO public.sample_events
                (row_uid, sample_no, op, new_station_no, new_ph)
            VALUES (NEW.row_uid, NEW.sample_no, 'INSERT', NEW.station_no, NEW.ph);
        ELSIF TG_OP = 'UPDATE' THEN
            INSERT INTO public.sample_events
                (row_uid, sample_no, op, old_station_no, old_ph,
                 new_station_no, new_ph)
            VALUES (NEW.row_uid, NEW.sample_no, 'UPDATE',
                    OLD.station_no, OLD.ph, NEW.station_no, NEW.ph);
        ELSE  -- DELETE
            INSERT INTO public.sample_events
                (row_uid, sample_no, op, old_station_no, old_ph)
            VALUES (OLD.row_uid, OLD.sample_no, 'DELETE', OLD.station_no, OLD.ph);
        END IF;
        RETURN NULL;
    END;
    $func$
    """,
    "DROP TRIGGER IF EXISTS samples_audit ON public.samples",
    """
    CREATE TRIGGER samples_audit
    AFTER INSERT OR UPDATE OR DELETE ON public.samples
    FOR EACH ROW EXECUTE FUNCTION public.log_sample_event()
    """,
    # TRUNCATE 不触发行级触发器，会直接绕过沿革记录，一律禁止
    """
    CREATE OR REPLACE FUNCTION public.reject_samples_truncate() RETURNS trigger
    LANGUAGE plpgsql AS $func$
    BEGIN
        RAISE EXCEPTION 'samples 表禁止 TRUNCATE：请使用 DELETE，以便同事务留下变更事件';
    END;
    $func$
    """,
    "DROP TRIGGER IF EXISTS samples_no_truncate ON public.samples",
    """
    CREATE TRIGGER samples_no_truncate
    BEFORE TRUNCATE ON public.samples
    FOR EACH STATEMENT EXECUTE FUNCTION public.reject_samples_truncate()
    """,
    # 记录起点（只在首次启用时写入；重复启用保持原起点不变）
    """
    INSERT INTO public.audit_meta (id, enabled_lsn, note)
    VALUES (true, pg_current_wal_insert_lsn()::text,
            '样品变更记录启用于该 WAL 位点；早于该位点的核对点无事件可查')
    ON CONFLICT (id) DO NOTHING
    """,
]


def _admin_conn() -> psycopg.Connection:
    """源库管理连接（手动事务）。仅用于显式启用动作，非常态路径。"""
    return psycopg.connect(
        host=config.SOURCE_HOST,
        port=config.SOURCE_PORT,
        dbname=config.SOURCE_DB,
        user=config.SOURCE_USER,
        password=config.SOURCE_PASSWORD,
        autocommit=False,
        connect_timeout=5,
        application_name="pitr-api-audit-admin",
    )


def enable_recording() -> dict:
    """在源库启用变更记录（幂等，单事务）。返回启用状态与记录起点。"""
    already = source.fetch_audit_meta() is not None
    with _admin_conn() as con:
        for stmt in ENABLE_STATEMENTS:
            con.execute(stmt)
        meta = con.execute(
            "SELECT enabled_at, enabled_lsn, note FROM public.audit_meta WHERE id"
        ).fetchone()
        con.commit()  # 任一步失败都不会走到这里，整体回滚
    recording_start = {
        "enabled_at": meta[0].isoformat() if meta[0] else None,
        "enabled_lsn": meta[1],
        "note": meta[2],
    }
    state.set_audit_state(True, recording_start["enabled_lsn"],
                          recording_start["enabled_at"], recording_start["note"])
    return {
        "enabled": True,
        "already_enabled": already,
        "recording_start": recording_start,
        "message": "变更记录已启用（此前已在启用状态，记录起点保持不变）"
        if already else
        "变更记录已启用：生产库新增/更正/删除将在同事务留下变更事件",
    }


def refresh_status() -> dict:
    """从源库读取记录起点并刷新本地缓存（后台周期调用，尽力而为）。"""
    meta = source.fetch_audit_meta()
    if meta:
        state.set_audit_state(True, meta["enabled_lsn"], meta["enabled_at"],
                              meta.get("note"))
    else:
        state.set_audit_state(False)
    return meta or {}


def status() -> dict:
    """变更记录状态：源库实况 + 本地缓存（供核对点沿革提示使用）。"""
    source_ok = True
    live = None
    try:
        live = source.fetch_audit_meta()
    except Exception:  # noqa: BLE001 - 源库不可达时退化为缓存视图
        source_ok = False
    if live:
        state.set_audit_state(True, live["enabled_lsn"], live["enabled_at"],
                              live.get("note"))
    cached = state.get_audit_state()
    return {
        "source_reachable": source_ok,
        "enabled_on_source": bool(live),
        "recording_start": live,
        "cached": cached,
    }
