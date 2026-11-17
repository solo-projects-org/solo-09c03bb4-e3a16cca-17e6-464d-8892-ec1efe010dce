FROM postgres:16-bookworm

# Python 运行环境（PostgreSQL 16 客户端/服务端工具镜像内自带）
RUN apt-get update \
 && apt-get install -y --no-install-recommends python3 python3-venv \
 && rm -rf /var/lib/apt/lists/*

ENV VIRTUAL_ENV=/opt/venv
RUN python3 -m venv "$VIRTUAL_ENV"
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# 数据目录预建并归属 postgres（命名卷首次挂载会继承属主）
RUN mkdir -p /backups /recoveries /state \
 && chown -R postgres:postgres /backups /recoveries /state

COPY app /opt/app
RUN chown -R postgres:postgres /opt/app

USER postgres
WORKDIR /opt/app
ENV PYTHONUNBUFFERED=1

EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
