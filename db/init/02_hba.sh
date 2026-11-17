#!/usr/bin/env bash
# 允许 api 容器通过复制连接执行 pg_basebackup（物理基准备份）。
# 仅在首次初始化空数据目录时执行（docker-entrypoint 机制）。
set -euo pipefail
echo "host replication all all scram-sha-256" >> "$PGDATA/pg_hba.conf"
