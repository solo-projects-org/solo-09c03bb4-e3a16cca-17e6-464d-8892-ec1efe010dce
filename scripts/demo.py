#!/usr/bin/env python3
"""端到端演示：更正、误删、跨点事务 + PITR 恢复验证。

用法（Compose 环境）：
    docker compose up -d --build
    python3 scripts/demo.py

环境变量：
    API_URL    默认 http://localhost:8000
    DEMO_PSQL  在源库执行 SQL 的命令前缀
               默认 "docker compose exec -T db psql -U postgres -d water"

流程：
  1. 等待 API 健康、自动基准备份完成
  2. 登记核对点 cp1
  3. 更正（UPDATE pH）、误删（DELETE）、新增样品
  4. 登记核对点 cp2
  5. 跨点事务：后台事务插入样品但 12 秒后才提交，期间登记 cp3
  6. 恢复 cp1 → 校验删前/改前数据
  7. 恢复 cp3（恢复期间源库继续写入）→ 校验跨点事务不可见、源库未被打扰
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
          f" WAL={body['wal_file']} 初始状态={body['status']}")
    wait_for(f"核对点 {name} WAL 归档",
             lambda: api("GET", f"/api/checkpoints/{name}")[1].get("status") == "ready")
    cp = api("GET", f"/api/checkpoints/{name}")[1]
    print(f"  核对点 {name} 就绪（waiting_wal → ready，归档齐备）")


def restore_and_wait(cp_name: str) -> dict:
    code, job = api("POST", "/api/recoveries", {"checkpoint": cp_name})
    assert code == 202, f"发起恢复 {cp_name} 失败: {code} {job}"
    jid = job["id"]
    print(f"  恢复任务 #{jid}（核对点 {cp_name}）已受理")
    final = wait_for(
        f"任务 #{jid} 完成",
        lambda: api("GET", f"/api/recoveries/{jid}")[1].get("status") in
        ("ready", "failed", "cancelled"))
    job = api("GET", f"/api/recoveries/{jid}")[1]
    assert job["status"] == "ready", f"恢复失败: {job.get('error')}\n{job.get('log_tail')}"
    print(f"  任务 #{jid} 就绪：{job['detail']}")
    return job


def samples(job_id: int) -> dict[str, dict]:
    code, body = api("GET", f"/api/recoveries/{job_id}/samples?limit=1000")
    assert code == 200, f"查询失败: {code} {body}"
    print(f"  查询自 {body['queried_instance']}，as-of LSN {body['restore_lsn']}")
    return {r["sample_no"]: r for r in body["rows"]}


# ------------------------------------------------------------------ 主流程

def main() -> int:
    print("== 0. 等待 API 与基准备份 ==")
    wait_for("API 健康", lambda: api("GET", "/api/health")[1].get("status") == "ok")
    wait_for("基准备份完成",
             lambda: (api("GET", "/api/basebackup")[1].get("latest_done") or {}).get(
                 "status") == "done")
    print("  基准备份：",
          api("GET", "/api/basebackup")[1]["latest_done"]["start_lsn"])

    print("== 1. 源库初始数据 ==")
    print(psql_scalar("SELECT string_agg(sample_no || ':' || ph, ' ' ORDER BY id)"
                      " FROM samples"))

    print("== 2. 登记核对点 cp1 ==")
    create_checkpoint("cp1")

    print("== 3. 更正 + 误删 + 新增（cp1 之后） ==")
    psql("UPDATE samples SET ph = 7.55 WHERE sample_no = 'S-2026-0002'")   # 更正
    psql("DELETE FROM samples WHERE sample_no = 'S-2026-0003'")            # 误删
    psql("INSERT INTO samples (sample_no, station_no, ph)"
         " VALUES ('S-2026-0004', 'ST-02', 7.02)")                          # 新增
    print("  已更正 S-2026-0002 pH→7.55，误删 S-2026-0003，新增 S-2026-0004")

    print("== 4. 登记核对点 cp2 ==")
    create_checkpoint("cp2")

    print("== 5. 跨点事务（cp3 前开始、cp3 后提交） ==")
    bg = psql_bg("BEGIN; INSERT INTO samples (sample_no, station_no, ph)"
                 " VALUES ('S-2026-0005', 'ST-03', 7.77);"
                 " SELECT pg_sleep(12); COMMIT;")
    time.sleep(3)  # 保证事务已开始但尚未提交
    print("  后台事务已插入 S-2026-0005（未提交），等待 12s 后提交")
    create_checkpoint("cp3")
    bg.wait()
    assert bg.returncode == 0, "跨点事务执行失败"
    print("  跨点事务已在 cp3 之后提交")

    print("== 6. 恢复 cp1（删前/改前时点） ==")
    job1 = restore_and_wait("cp1")
    rows = samples(job1["id"])
    check("cp1 含误删的 S-2026-0003", "S-2026-0003" in rows)
    check("cp1 中 S-2026-0002 仍为更正前 pH 6.95",
          rows.get("S-2026-0002", {}).get("ph") == 6.95)
    check("cp1 不含 cp1 后新增的 S-2026-0004", "S-2026-0004" not in rows)
    check("cp1 不含跨点事务的 S-2026-0005", "S-2026-0005" not in rows)

    print("== 7. 恢复 cp3（一次只恢复一项，cp1 实例被取代） ==")
    code, job3 = api("POST", "/api/recoveries", {"checkpoint": "cp3"})
    assert code == 202, f"发起恢复 cp3 失败: {code} {job3}"
    # 恢复进行期间，源库必须仍可写
    psql("INSERT INTO samples (sample_no, station_no, ph)"
         " VALUES ('S-2026-0006', 'ST-04', 6.66)")
    print("  恢复期间源库写入 S-2026-0006 成功（源库未被打扰）")
    jid3 = job3["id"]
    wait_for(f"任务 #{jid3} 完成",
             lambda: api("GET", f"/api/recoveries/{jid3}")[1].get("status") in
             ("ready", "failed", "cancelled"))
    job3 = api("GET", f"/api/recoveries/{jid3}")[1]
    assert job3["status"] == "ready", f"恢复失败: {job3.get('error')}"

    old = api("GET", f"/api/recoveries/{job1['id']}")[1]
    check("cp1 任务已被取代（一次只恢复一项）", old["status"] == "superseded")

    rows = samples(jid3)
    check("cp3 含更正后 pH 7.55", rows.get("S-2026-0002", {}).get("ph") == 7.55)
    check("cp3 不含误删的 S-2026-0003", "S-2026-0003" not in rows)
    check("cp3 含 cp2 前新增的 S-2026-0004", "S-2026-0004" in rows)
    check("cp3 不含跨点后才提交的 S-2026-0005", "S-2026-0005" not in rows)
    check("cp3 不含恢复期间写入源库的 S-2026-0006", "S-2026-0006" not in rows)

    src_has_6 = psql_scalar(
        "SELECT count(*) FROM samples WHERE sample_no='S-2026-0006'") == "1"
    check("源库确有 S-2026-0006（证明查询未拿源库冒充恢复结果）", src_has_6)

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
