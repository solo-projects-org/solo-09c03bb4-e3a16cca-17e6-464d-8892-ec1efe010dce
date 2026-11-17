#!/usr/bin/env python3
"""端到端演示与自测：变更沿革（改回原值 / 回滚 / 同号重录）+ PITR 恢复校验。

用法（Compose 环境）：
    docker compose up -d --build
    python3 scripts/demo.py

环境变量：
    API_URL    默认 http://localhost:8000
    DEMO_PSQL  在源库执行 SQL 的命令前缀
               默认 "docker compose exec -T db psql -U postgres -d water"

流程：
  1. 等待 API 健康、自动基准备份完成
  2. 登记核对点 cp0（启用变更记录之前，用于验证"沿革不可用"提示）
  3. 启用变更记录（幂等性一并验证）
  4. 登记核对点 cp1（记录起点之后）
  5. 生产库变更：更正 pH → 改回原值；两个回滚事务；误删 → 同号重录；新增
  6. 登记核对点 cp2；跨点事务（cp3 前开始、cp3 后提交）
  7. 恢复 cp0 → 沿革明确提示不可用
  8. 恢复 cp2 → 沿革逐代校验（改回原值 / 回滚无痕 / 同号重录分代）
  9. 实例切换：被取代任务的查询一律 409，不会张冠李戴
 10. 恢复 cp3（恢复期间源库继续写入）→ 跨点事务不可见、沿革与 cp2 同时点
"""
import json
import os
import shlex
import subprocess
import sys
import time
import urllib.error
import urllib.request

API = os.environ.get("API_URL", "http://localhost:8000")
DEMO_PSQL = os.environ.get(
    "DEMO_PSQL", "docker compose exec -T db psql -U postgres -d water")

FAILED: list[str] = []


# ------------------------------------------------------------------ 工具

def api(method: str, path: str, body: dict | None = None) -> tuple[int, dict]:
    req = urllib.request.Request(
        API + path, method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read().decode()
            return resp.status, json.loads(raw) if raw else {}
    except urllib.error.HTTPError as e:
        raw = e.read().decode()
        try:
            return e.code, json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            return e.code, {"raw": raw}


def psql(sql: str) -> None:
    subprocess.run(shlex.split(DEMO_PSQL) + ["-v", "ON_ERROR_STOP=1", "-c", sql],
                   check=True, capture_output=True, text=True)


def psql_bg(sql: str) -> subprocess.Popen:
    return subprocess.Popen(shlex.split(DEMO_PSQL)
                            + ["-v", "ON_ERROR_STOP=1", "-c", sql],
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                            text=True)


def psql_scalar(sql: str) -> str:
    r = subprocess.run(shlex.split(DEMO_PSQL)
                       + ["-tA", "-v", "ON_ERROR_STOP=1", "-c", sql],
                       check=True, capture_output=True, text=True)
    return r.stdout.strip()


def psql_fails(sql: str) -> bool:
    r = subprocess.run(shlex.split(DEMO_PSQL)
                       + ["-v", "ON_ERROR_STOP=1", "-c", sql],
                       capture_output=True, text=True)
    return r.returncode != 0


def wait_for(desc: str, fn, timeout: int = 300):
    t0 = time.time()
    while time.time() - t0 < timeout:
        r = fn()
        if r:
            return r
        time.sleep(2)
    raise SystemExit(f"超时：{desc}")


def check(desc: str, cond: bool) -> None:
    print(f"  [{'PASS' if cond else 'FAIL'}] {desc}")
    if not cond:
        FAILED.append(desc)


def create_checkpoint(name: str) -> None:
    code, body = api("POST", "/api/checkpoints", {"name": name})
    if code == 409:
        raise SystemExit(f"核对点 {name} 已存在。请先清理环境："
                         f"docker compose down -v && docker compose up -d --build")
    assert code == 201, f"创建核对点 {name} 失败: {code} {body}"
    print(f"  核对点 {name} 已登记：LSN={body['restore_lsn']}"
          f" 沿革预判={body.get('lineage_available')}")
    wait_for(f"核对点 {name} WAL 归档",
             lambda: api("GET", f"/api/checkpoints/{name}")[1].get("status") == "ready")
    print(f"  核对点 {name} 就绪（waiting_wal → ready，归档齐备）")


def restore_and_wait(cp_name: str) -> dict:
    code, job = api("POST", "/api/recoveries", {"checkpoint": cp_name})
    assert code == 202, f"发起恢复 {cp_name} 失败: {code} {job}"
    jid = job["id"]
    print(f"  恢复任务 #{jid}（核对点 {cp_name}）已受理")
    wait_for(f"任务 #{jid} 完成",
             lambda: api("GET", f"/api/recoveries/{jid}")[1].get("status") in
             ("ready", "failed", "cancelled"))
    job = api("GET", f"/api/recoveries/{jid}")[1]
    assert job["status"] == "ready", f"恢复失败: {job.get('error')}\n{job.get('log_tail')}"
    print(f"  任务 #{jid} 就绪：{job['detail']}")
    return job


def samples(job_id: int) -> dict[str, dict]:
    code, body = api("GET", f"/api/recoveries/{job_id}/samples?limit=1000")
    assert code == 200, f"查询失败: {code} {body}"
    print(f"  样品查询自 {body['queried_instance']}，as-of LSN {body['restore_lsn']}，"
          f"实例目录 {body['instance_data_dir']}")
    return {r["sample_no"]: r for r in body["rows"]}


def lineage(job_id: int, sample_no: str) -> dict:
    code, body = api("GET",
                     f"/api/recoveries/{job_id}/lineage?sample_no={sample_no}")
    assert code == 200, f"沿革查询失败: {code} {body}"
    return body


def incarnations_of(body: dict) -> dict[str, dict]:
    return {i["row_uid"]: i for i in body["incarnations"]}


# ------------------------------------------------------------------ 主流程

def main() -> int:
    print("== 0. 等待 API 与基准备份 ==")
    wait_for("API 健康", lambda: api("GET", "/api/health")[1].get("status") == "ok")
    wait_for("基准备份完成",
             lambda: (api("GET", "/api/basebackup")[1].get("latest_done") or {}).get(
                 "status") == "done")
    print("  基准备份：",
          api("GET", "/api/basebackup")[1]["latest_done"]["start_lsn"])
    code, st = api("GET", "/api/audit/status")
    check("初始时源库尚未启用变更记录", st.get("enabled_on_source") is False)

    print("== 1. 登记核对点 cp0（启用变更记录之前） ==")
    create_checkpoint("cp0")

    print("== 2. 启用变更记录（显式管理动作，幂等） ==")
    code, body = api("POST", "/api/audit/enable")
    assert code == 200, f"启用失败: {code} {body}"
    print(f"  已启用：记录起点 LSN={body['recording_start']['enabled_lsn']}")
    check("首次启用 already_enabled=false", body["already_enabled"] is False)
    lsn1 = body["recording_start"]["enabled_lsn"]
    code, body2 = api("POST", "/api/audit/enable")
    check("重复启用幂等（already_enabled=true）",
          code == 200 and body2["already_enabled"] is True)
    check("重复启用不改变记录起点",
          body2["recording_start"]["enabled_lsn"] == lsn1)
    # 源库侧护栏：TRUNCATE 被禁止、row_uid 不可改
    check("源库禁止 TRUNCATE samples（保护沿革）",
          psql_fails("TRUNCATE samples"))
    check("源库禁止修改 row_uid（行身份稳定）",
          psql_fails("UPDATE samples SET row_uid = gen_random_uuid()"
                     " WHERE sample_no = 'S-2026-0001'"))

    print("== 3. 登记核对点 cp1（记录起点之后） ==")
    create_checkpoint("cp1")
    cp0_view = api("GET", "/api/checkpoints/cp0")[1]
    cp1_view = api("GET", "/api/checkpoints/cp1")[1]
    check("cp0 详情明确提示沿革不可用（早于记录起点）",
          cp0_view["lineage_available"] is False)
    check("cp1 详情提示沿革可用且标明记录起点",
          cp1_view["lineage_available"] is True
          and lsn1 in (cp1_view["lineage_note"] or ""))

    print("== 4. 生产库变更：更正→改回原值 / 回滚 / 误删→同号重录 / 新增 ==")
    psql("UPDATE samples SET ph = 7.55 WHERE sample_no = 'S-2026-0002'")   # 更正
    psql("UPDATE samples SET ph = 6.95 WHERE sample_no = 'S-2026-0002'")   # 改回原值
    psql("BEGIN; UPDATE samples SET ph = 9.99 WHERE sample_no = 'S-2026-0001';"
         " ROLLBACK;")                                                     # 回滚1
    psql("BEGIN; INSERT INTO samples (sample_no, station_no, ph)"
         " VALUES ('S-2026-0098', 'ST-09', 9.98); ROLLBACK;")              # 回滚2
    psql("DELETE FROM samples WHERE sample_no = 'S-2026-0003'")            # 误删
    psql("INSERT INTO samples (sample_no, station_no, ph)"
         " VALUES ('S-2026-0003', 'ST-09', 8.88)")                          # 同号重录
    psql("INSERT INTO samples (sample_no, station_no, ph)"
         " VALUES ('S-2026-0004', 'ST-02', 7.02)")                          # 新增
    print("  已更正 S-2026-0002 并改回原值；两个事务回滚；"
          "误删 S-2026-0003 后同号重录（ST-09/8.88）；新增 S-2026-0004")

    print("== 5. 源库侧事件核验（同事务留痕 / 回滚不留痕 / 重录新身份） ==")
    check("S-2026-0002 有 2 条 UPDATE 事件（更正+改回）",
          psql_scalar("SELECT count(*) FROM sample_events"
                      " WHERE sample_no='S-2026-0002' AND op='UPDATE'") == "2")
    check("回滚的 UPDATE 不留事件（S-2026-0001 事件数=0）",
          psql_scalar("SELECT count(*) FROM sample_events"
                      " WHERE sample_no='S-2026-0001'") == "0")
    check("回滚的 INSERT 不留事件（S-2026-0098 事件数=0）",
          psql_scalar("SELECT count(*) FROM sample_events"
                      " WHERE sample_no='S-2026-0098'") == "0")
    check("S-2026-0003 事件涉及 2 个行身份（删除旧行 + 重录新行）",
          psql_scalar("SELECT count(DISTINCT row_uid) FROM sample_events"
                      " WHERE sample_no='S-2026-0003'") == "2")
    check("重录新行的 row_uid 与旧行不同",
          psql_scalar("SELECT (SELECT row_uid FROM samples"
                      " WHERE sample_no='S-2026-0003') <>"
                      " (SELECT row_uid FROM sample_events"
                      "  WHERE sample_no='S-2026-0003' AND op='DELETE')") == "t")
    check("事件含变更前后站号与 pH（DELETE 事件旧值 ST-02/7.40）",
          psql_scalar("SELECT old_station_no || '/' || old_ph FROM sample_events"
                      " WHERE sample_no='S-2026-0003' AND op='DELETE'") == "ST-02/7.40")

    print("== 6. 登记核对点 cp2 ==")
    create_checkpoint("cp2")

    print("== 7. 跨点事务（cp3 前开始、cp3 后提交） ==")
    bg = psql_bg("BEGIN; INSERT INTO samples (sample_no, station_no, ph)"
                 " VALUES ('S-2026-0005', 'ST-03', 7.77);"
                 " SELECT pg_sleep(12); COMMIT;")
    time.sleep(3)  # 保证事务已开始但尚未提交
    print("  后台事务已插入 S-2026-0005（未提交），等待 12s 后提交")
    create_checkpoint("cp3")
    bg.wait()
    assert bg.returncode == 0, "跨点事务执行失败"
    print("  跨点事务已在 cp3 之后提交")

    print("== 8. 恢复 cp0（启用记录前的旧核对点） ==")
    job0 = restore_and_wait("cp0")
    rows = samples(job0["id"])
    check("cp0 含原始 S-2026-0003（ST-02/7.40）",
          rows.get("S-2026-0003", {}).get("ph") == 7.40)
    lg = lineage(job0["id"], "S-2026-0002")
    check("cp0 沿革明确提示不可用（lineage_available=false）",
          lg["lineage_available"] is False and bool(lg["reason"]))
    print(f"  提示原文：{lg['reason']}")

    print("== 9. 恢复 cp2（取代 cp0 实例；沿革逐代校验） ==")
    job2 = restore_and_wait("cp2")
    rows = samples(job2["id"])
    check("cp2 中 S-2026-0002 已改回原值 6.95",
          rows.get("S-2026-0002", {}).get("ph") == 6.95)
    check("cp2 不含误删前的旧 S-2026-0003（现为重录行 ST-09/8.88）",
          rows.get("S-2026-0003", {}).get("station_no") == "ST-09"
          and rows.get("S-2026-0003", {}).get("ph") == 8.88)
    check("cp2 不含回滚的 S-2026-0098", "S-2026-0098" not in rows)
    check("cp2 不含跨点事务的 S-2026-0005", "S-2026-0005" not in rows)

    lg2 = lineage(job2["id"], "S-2026-0002")
    check("cp2 沿革可用且标明记录起点",
          lg2["lineage_available"] is True
          and lg2["recording_start"]["enabled_lsn"] == lsn1)
    ev2 = lg2["incarnations"][0]["events"] if lg2["incarnations"] else []
    check("S-2026-0002 沿革：6.95→7.55→6.95（改回原值的经过完整）",
          len(ev2) == 2
          and (ev2[0]["old_ph"], ev2[0]["new_ph"]) == (6.95, 7.55)
          and (ev2[1]["old_ph"], ev2[1]["new_ph"]) == (7.55, 6.95))
    check("S-2026-0002 当前行与事件同时点（ph=6.95，is_current）",
          lg2["current_row"]["ph"] == 6.95
          and lg2["incarnations"][0]["is_current"] is True)

    lg1 = lineage(job2["id"], "S-2026-0001")
    check("S-2026-0001 无任何事件（回滚不留痕），当前值 7.10",
          lg1["event_count"] == 0 and lg1["current_row"]["ph"] == 7.10)
    lg98 = lineage(job2["id"], "S-2026-0098")
    check("S-2026-0098 无行无事件（回滚的 INSERT 彻底无痕）",
          lg98["event_count"] == 0 and lg98["current_row"] is None)

    lg3 = lineage(job2["id"], "S-2026-0003")
    incs = lg3["incarnations"]
    cur = [i for i in incs if i["is_current"]]
    old = [i for i in incs if not i["is_current"]]
    check("S-2026-0003 沿革分两代（删后同号重录不接旧行）", len(incs) == 2)
    check("旧代以 DELETE 终结（ST-02/7.40）",
          len(old) == 1 and old[0]["ended_by"] == "DELETE"
          and [e["op"] for e in old[0]["events"]] == ["DELETE"]
          and old[0]["events"][0]["old_ph"] == 7.40)
    check("新代仅含自身 INSERT（ST-09/8.88），未接旧行沿革",
          len(cur) == 1 and [e["op"] for e in cur[0]["events"]] == ["INSERT"]
          and cur[0]["events"][0]["new_station_no"] == "ST-09"
          and cur[0]["events"][0]["new_ph"] == 8.88)
    check("新代行身份 = 当前行 row_uid",
          cur[0]["row_uid"] == lg3["current_row"]["row_uid"])
    check("新旧两代行身份不同", old[0]["row_uid"] != cur[0]["row_uid"])

    lg4 = lineage(job2["id"], "S-2026-0004")
    check("S-2026-0004 一代 INSERT（新增留痕）",
          lg4["event_count"] == 1
          and lg4["incarnations"][0]["events"][0]["op"] == "INSERT")

    print("== 10. 实例切换：被取代任务的查询不得张冠李戴 ==")
    old0 = api("GET", f"/api/recoveries/{job0['id']}")[1]
    check("cp0 任务已被取代（一次只恢复一项）", old0["status"] == "superseded")
    code, _ = api("GET", f"/api/recoveries/{job0['id']}/samples")
    check("被取代任务查样品 → 409", code == 409)
    code, _ = api("GET", f"/api/recoveries/{job0['id']}/lineage?sample_no=S-2026-0002")
    check("被取代任务查沿革 → 409（不会把 cp2 的事件标成 cp0 的结果）", code == 409)

    print("== 11. 恢复 cp3（恢复期间源库继续写入） ==")
    code, job3 = api("POST", "/api/recoveries", {"checkpoint": "cp3"})
    assert code == 202, f"发起恢复 cp3 失败: {code} {job3}"
    psql("INSERT INTO samples (sample_no, station_no, ph)"
         " VALUES ('S-2026-0006', 'ST-04', 6.66)")
    print("  恢复期间源库写入 S-2026-0006 成功（生产写入照常进行）")
    jid3 = job3["id"]
    wait_for(f"任务 #{jid3} 完成",
             lambda: api("GET", f"/api/recoveries/{jid3}")[1].get("status") in
             ("ready", "failed", "cancelled"))
    job3 = api("GET", f"/api/recoveries/{jid3}")[1]
    assert job3["status"] == "ready", f"恢复失败: {job3.get('error')}"

    rows = samples(jid3)
    check("cp3 不含跨点后才提交的 S-2026-0005", "S-2026-0005" not in rows)
    check("cp3 不含恢复期间写入源库的 S-2026-0006", "S-2026-0006" not in rows)
    src_has_6 = psql_scalar(
        "SELECT count(*) FROM samples WHERE sample_no='S-2026-0006'") == "1"
    check("源库确有 S-2026-0006（证明查询未拿源库冒充恢复结果）", src_has_6)

    lg3b = lineage(jid3, "S-2026-0003")
    check("cp3 中 S-2026-0003 沿革与 cp2 一致（事件与样品同一时点）",
          lg3b["event_count"] == lg3["event_count"]
          and [i["row_uid"] for i in lg3b["incarnations"]]
          == [i["row_uid"] for i in lg3["incarnations"]])
    lg5 = lineage(jid3, "S-2026-0005")
    check("跨点事务的样品在 cp3 无行无事件（同事务的事件同样不可见）",
          lg5["event_count"] == 0 and lg5["current_row"] is None)

    old2 = api("GET", f"/api/recoveries/{job2['id']}")[1]
    check("cp2 任务已被 cp3 取代", old2["status"] == "superseded")
    code, _ = api("GET", f"/api/recoveries/{job2['id']}/lineage?sample_no=S-2026-0003")
    check("实例再次切换后旧任务查沿革 → 409", code == 409)

    print()
    if FAILED:
        print(f"共 {len(FAILED)} 项校验失败：")
        for f in FAILED:
            print(f"  - {f}")
        return 1
    print("全部校验通过 ✅")
    print(f"Swagger 文档：{API}/docs")
    return 0


if __name__ == "__main__":
    sys.exit(main())
