"""集中管理环境配置（均可通过环境变量覆盖）。"""
import os

# 生产源库（只读元数据操作，绝不执行 DML）
SOURCE_HOST = os.getenv("SOURCE_HOST", "db")
SOURCE_PORT = int(os.getenv("SOURCE_PORT", "5432"))
SOURCE_DB = os.getenv("SOURCE_DB", "water")
SOURCE_USER = os.getenv("SOURCE_USER", "postgres")
SOURCE_PASSWORD = os.getenv("SOURCE_PASSWORD", "postgres")

# WAL 归档目录（只读）、基准备份目录、恢复实例目录、状态库
ARCHIVE_DIR = os.getenv("ARCHIVE_DIR", "/wal_archive")
BACKUP_DIR = os.getenv("BACKUP_DIR", "/backups")
RECOVERY_DIR = os.getenv("RECOVERY_DIR", "/recoveries")
STATE_DB = os.getenv("STATE_DB", "/state/api.db")

# 恢复出来的独立只读实例监听地址（容器内）
RECOVERY_HOST = os.getenv("RECOVERY_HOST", "127.0.0.1")
RECOVERY_PORT = int(os.getenv("RECOVERY_PORT", "55433"))
RECOVERY_SOCKET_DIR = os.getenv("RECOVERY_SOCKET_DIR", "/tmp")

# 等待 WAL 重放到核对点的超时（秒）
RECOVERY_TIMEOUT = int(os.getenv("RECOVERY_TIMEOUT", "180"))

# 后台轮询间隔（秒）
POLL_INTERVAL = float(os.getenv("POLL_INTERVAL", "2"))
