#!/usr/bin/env bash
set -euo pipefail

# 计算容器内端口
PORT_INTERNAL="${PORT:-10000}"

# 启动 uvicorn（API）
export API_HOST=0.0.0.0
export API_PORT=8000
nohup python -m uvicorn api_server_local:app --host 0.0.0.0 --port ${API_PORT} >/tmp/uvicorn.out 2>&1 &

# 启动 streamlit（前端）
export STREAMLIT_PORT=8501
export STREAMLIT_SERVER_PORT=${STREAMLIT_PORT}
# 关闭 streamlit 浏览器自动打开
export BROWSER=none
nohup streamlit run app.py --server.port ${STREAMLIT_PORT} >/tmp/streamlit.out 2>&1 &

# 以 Nginx 统一对外端口（Render 使用 $PORT）
sed -i "s/listen\s\+10000;/listen ${PORT_INTERNAL};/" /etc/nginx/nginx.conf
nginx -g 'daemon off;' 