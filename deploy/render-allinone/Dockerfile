# syntax=docker/dockerfile:1
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# 必要系统依赖
RUN apt-get update && apt-get install -y --no-install-recommends \
    nginx curl ca-certificates build-essential \
    && rm -rf /var/lib/apt/lists/*

# 复制项目文件
COPY . /app

# 复制一体化 Nginx 配置与启动脚本
COPY deploy/render-allinone/nginx.conf /etc/nginx/nginx.conf
COPY deploy/render-allinone/start.sh /start.sh
RUN chmod +x /start.sh

# 安装依赖（可复用现有 requirements）
RUN pip install --no-cache-dir -r requirements.txt

# 暴露端口（Render 使用 $PORT，但容器内统一监听 10000）
ENV PORT=10000

# 入口
CMD ["/start.sh"] 