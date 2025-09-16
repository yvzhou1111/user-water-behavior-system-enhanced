# -*- coding: utf-8 -*-
"""
Render 专用后端入口
- 自动使用 RENDER_EXTERNAL_URL 作为 EXTERNAL_API_BASE，便于对外展示推送地址
- 复用现有 api_server_local.app，避免重复代码
用法（Render Start Command 二选一）：
1) uvicorn api_server_render:app --host 0.0.0.0 --port $PORT
2) python api_server_render.py  （同样支持 PORT 环境变量）
"""
import os
from dotenv import load_dotenv

# 加载 .env（本地与Render均可）
load_dotenv()

# Render 提供的外网域名，如 https://your-api.onrender.com
_render_url = os.getenv("RENDER_EXTERNAL_URL")
if _render_url and not os.getenv("EXTERNAL_API_BASE"):
    os.environ["EXTERNAL_API_BASE"] = _render_url.rstrip("/")

# 复用现有的 FastAPI 应用
from api_server_local import app  # noqa: E402


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT") or os.getenv("API_PORT", "8000"))
    host = os.getenv("API_HOST", "0.0.0.0")
    uvicorn.run(app, host=host, port=port) 