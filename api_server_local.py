#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
API服务器 - 本地文件存储版本

使用本地文件存储替代PostgreSQL数据库，提供相同的API功能
"""

from fastapi import FastAPI, BackgroundTasks, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, field_validator
import pandas as pd
import os
import uvicorn
from datetime import datetime
import logging
from typing import Optional, Union, List
from fastapi.responses import HTMLResponse
import urllib.request
import json
import ipaddress
from threading import Lock
import hashlib
from os import getenv
from fastapi import Request
import time
import hmac
import base64
from dotenv import load_dotenv

# 导入本地存储模块
import local_storage

# 加载环境变量
load_dotenv()

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("api_server.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("water-meter-api")

# 数据存储路径
DATA_FILE = "water_meter_data.csv"            # 兼容原有前端
PUSH_FILE = "device_push_data.csv"            # 新的推送数据表

# 保存并发写入锁
SAVE_LOCK = Lock()

# FastAPI应用程序
app = FastAPI(
    title="水表数据接收API",
    description="接收水表设备推送的实时数据并进行存储",
    version="1.1.0"
)

# 添加CORS中间件
_CORS = getenv("CORS_ORIGINS", "*")
_allow_origins = [o.strip() for o in _CORS.split(",") if o.strip()] if _CORS != "*" else ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=_allow_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 数据模型，严格按照水表数据推送接口文档定义
class WaterMeterData(BaseModel):
    batteryVoltage: str = Field(..., description="电池电压")
    deviceNo: str = Field(..., description="表号")
    freezeDateFlow: str = Field(..., description="冻结流量")
    imei: str = Field(..., description="imei号")
    instantaneousFlow: str = Field(..., description="瞬时流量(m³/h)")
    pressure: str = Field(..., description="压力")
    reverseFlow: str = Field(..., description="反向流量")
    signalValue: str = Field(..., description="信号值(dBm)")
    startFrequency: str = Field(..., description="启动次数")
    temprature: str = Field(..., description="温度(°C)")
    totalFlow: str = Field(..., description="累计流量(m³)")
    valveStatu: str = Field(..., description="阀门状态")
    updateTime: Union[int, str] = Field(..., description="毫秒时间戳或'YYYY-MM-DD HH:MM:SS'")
    
    # 数值型字符串校验（保持原有行为）
    @field_validator('instantaneousFlow', 'totalFlow', 'freezeDateFlow', 'batteryVoltage', 'pressure')
    @classmethod
    def validate_numeric_string(cls, v):
        try:
            float(v)
            return v
        except ValueError:
            raise ValueError('必须是可转换为数字的字符串')
            
    @field_validator('updateTime')
    @classmethod
    def validate_timestamp(cls, v):
        """兼容毫秒时间戳(int)与'YYYY-MM-DD HH:MM:SS'(str)格式"""
        try:
            if isinstance(v, int):
                # 验证时间戳是否合理 (2020-2030年之间)
                min_time = int(datetime(2020, 1, 1).timestamp() * 1000)
                max_time = int(datetime(2030, 12, 31).timestamp() * 1000)
                if not (min_time <= v <= max_time):
                    raise ValueError('时间戳不在合理范围内')
                return v
            if isinstance(v, str):
                # 只校验能否被解析
                parse_update_time(v)
                return v
            raise ValueError('updateTime 类型无效')
        except Exception as e:
            raise ValueError(f"updateTime 无效: {e}")


# 解析上报时间
def parse_update_time(update_time: Union[int, str]) -> datetime:
    if isinstance(update_time, int):
        # 作为毫秒时间戳
        return datetime.fromtimestamp(update_time / 1000)
    if isinstance(update_time, str):
        # 支持 'YYYY-MM-DD HH:MM:SS'
        try:
            return datetime.strptime(update_time, "%Y-%m-%d %H:%M:%S")
        except Exception:
            # 回退：尝试ISO等格式
            try:
                return datetime.fromisoformat(update_time)
            except Exception as e:
                raise ValueError(f"无法解析updateTime: {update_time}")
    raise ValueError("updateTime 类型无效")


# 网络信息汇总
def get_network_info() -> dict:
    # 复用 __main__ 中的 get_ips 实现（若未定义则临时实现）
    try:
        get_ips_fn = get_ips  # type: ignore
    except NameError:
        import socket
        def get_ips_fn():
            ips = set()
            try:
                hostname = socket.gethostname()
                ips.add(socket.gethostbyname(hostname))
                for interface in socket.getaddrinfo(socket.gethostname(), None):
                    ip = interface[4][0]
                    if not ip.startswith('127.') and ':' not in ip:
                        ips.add(ip)
                s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                try:
                    s.connect(('8.8.8.8', 80))
                    ip = s.getsockname()[0]
                    if ip not in ips:
                        ips.add(ip)
                except:
                    pass
                finally:
                    s.close()
                return list(ips)
            except:
                return ['127.0.0.1']
    ips = get_ips_fn()
    lan_ip = pick_lan_ip(ips)
    public_ip = get_public_ip()
    external_port = int(os.getenv("EXTERNAL_PORT", "8000"))
    info = {
        "lan_ips": ips,
        "lan_ip_suggest": lan_ip,
        "public_ip": public_ip,
        "external_port": external_port,
        "external_data_url": f"http://{public_ip}:{external_port}/api/data" if public_ip else None,
        "external_health_url": f"http://{public_ip}:{external_port}/health" if public_ip else None,
    }
    return info


# 新增：公网IP探测与UPnP辅助
def get_public_ip() -> Optional[str]:
    """从公网服务探测本机出口公网IP（仅用于打印提示）。"""
    try:
        with urllib.request.urlopen("https://api.ipify.org?format=json", timeout=5) as resp:
            data = json.loads(resp.read().decode("utf-8", errors="ignore"))
            ip = data.get("ip")
            if ip:
                return str(ip)
    except Exception as e:
        logger.warning(f"获取公网IP失败: {e}")
    return None


def is_private_ipv4(ip: str) -> bool:
    try:
        return ipaddress.IPv4Address(ip).is_private
    except Exception:
        return False


def pick_lan_ip(ip_list: list[str]) -> Optional[str]:
    """从本机多网卡地址中挑选一个最可能用于路由器端口映射的私网地址。"""
    # 优先选择 192.168.*，其次 10.*，然后 172.16-31.*
    prefer_prefix = ["192.168.", "10."]
    for p in prefer_prefix:
        for ip in ip_list:
            if ip.startswith(p):
                return ip
    # 其余私网
    for ip in ip_list:
        if is_private_ipv4(ip):
            return ip
    return None


def try_upnp_map(external_port: int, internal_port: int, internal_client_ip: str) -> tuple[bool, str]:
    """尝试通过 miniupnpc 进行UPnP端口映射。未安装或失败将返回False及原因。"""
    try:
        import miniupnpc  # 可选依赖
    except Exception:
        return False, "未安装 miniupnpc，跳过自动UPnP"
    try:
        u = miniupnpc.UPnP()
        u.discoverdelay = 200
        n = u.discover()
        if n == 0:
            return False, "未发现UPnP网关/未开启UPnP"
        u.selectigd()
        # 已存在则视为成功
        if u.getspecificportmapping(external_port, 'TCP') is not None:
            return True, "端口映射已存在"
        ok = u.addportmapping(external_port, 'TCP', internal_client_ip, internal_port, 'WaterAPI', '')
        if ok:
            return True, "端口映射已添加"
        return False, "端口映射添加失败"
    except Exception as e:
        return False, f"UPnP失败: {e}"


# API端点接收数据
@app.post("/api/data", response_model=dict)
async def receive_data(data: WaterMeterData, background_tasks: BackgroundTasks, request: Request):
    try:
        # 限流
        try:
            client_ip = request.client.host if request and request.client else "unknown"
            _rate_limit_check(client_ip)
        except Exception as _e:
            raise
        # 转换数据为字典
        data_dict = data.model_dump()
        # 解析一次用于日志
        dt = parse_update_time(data_dict['updateTime'])
        logger.info(f"接收到数据: deviceNo={data_dict['deviceNo']}, updateTime={dt}")
        
        # 在后台任务中保存数据
        background_tasks.add_task(save_data, data_dict)
        
        # 返回成功响应，与接口文档一致
        return {"msg": "SUCCESS", "code": 200}
    except Exception as e:
        logger.error(f"处理数据时发生错误: {str(e)}")
        raise HTTPException(status_code=500, detail=f"处理数据时发生错误: {str(e)}")


# 保存数据到CSV
def save_data(data: dict):
    """保存数据到CSV和本地存储"""
    try:
        # 使用本地存储模块保存
        success = local_storage.save_water_data(data)
        if not success:
            logger.error("通过本地存储保存数据失败")
            
        # 保持兼容旧的CSV逻辑
        row = {
            "表号": data["deviceNo"],
            "电池电压": data["batteryVoltage"],
            "冻结流量": data["freezeDateFlow"],
            "imei号": data.get("imei", ""),
            "瞬时流量": data["instantaneousFlow"],
            "压力": data["pressure"],
            "反向流量": data["reverseFlow"],
            "信号值": data["signalValue"],
            "启动次数": data["startFrequency"],
            "温度": data["temprature"],
            "累计流量": data["totalFlow"],
            "阀门状态": data["valveStatu"]
        }
        
        # 解析上报时间
        dt = parse_update_time(data["updateTime"])
        row["上报时间"] = dt.strftime("%Y-%m-%d %H:%M:%S")
        row["日期计算"] = dt.strftime("%Y-%m-%d")
        row["时间计算"] = dt.strftime("%H:%M:%S")
        
        # 计算L/s
        try:
            row["数据L/s"] = float(data["instantaneousFlow"]) / 3.6
        except Exception:
            row["数据L/s"] = 0.0
        
        df = pd.DataFrame([row])
        
        # 进程内互斥，避免并发写入导致文件内容损坏
        with SAVE_LOCK:
            # 同时写入PUSH_FILE（新的推送表）
            if os.path.exists(PUSH_FILE):
                df.to_csv(PUSH_FILE, mode='a', header=False, index=False, encoding='utf-8')
            else:
                df.to_csv(PUSH_FILE, index=False, encoding='utf-8')
        
        logger.info(f"数据已保存: deviceNo={data['deviceNo']} time={row['上报时间']}")
        return True
    except Exception as e:
        logger.error(f"保存数据时发生错误: {str(e)}")
        raise


# 健康检查端点
@app.get("/health")
async def health_check():
    # 检查数据文件是否可访问
    ok1 = os.path.exists(DATA_FILE)
    ok2 = os.path.exists(PUSH_FILE)
    
    # 检查存储系统是否正常
    storage_ok = os.path.exists(local_storage.DATA_DIR)
    
    # 检查是否需要数据清理
    needs_cleanup, message, count = local_storage.check_data_cleanup()
    
    return {
        "status": "healthy" if (ok1 or ok2) and storage_ok else "unhealthy",
        "storage_status": "ok" if storage_ok else "error",
        "data_cleanup": {
            "needs_cleanup": needs_cleanup,
            "message": message,
            "affected_records": count
        }
    }


# 获取最新数据端点
@app.get("/api/latest")
async def get_latest_data(limit: Optional[int] = 10):
    try:
        # 使用本地存储模块查询
        df = local_storage.query_water_data(limit=limit)
        if not df.empty:
            latest = df.to_dict(orient='records')
            return {"data": latest, "count": len(latest)}
        return {"data": [], "count": 0}
    except Exception as e:
        logger.error(f"获取最新数据时出错: {str(e)}")
        raise HTTPException(status_code=500, detail=f"获取最新数据时出错: {str(e)}")


@app.get("/api/latest_pushed")
async def get_latest_pushed(limit: Optional[int] = 10):
    try:
        if os.path.exists(PUSH_FILE):
            try:
                df = pd.read_csv(PUSH_FILE)
            except Exception:
                df = pd.read_csv(PUSH_FILE, on_bad_lines='skip', engine='python')
            if len(df) > 0:
                df['上报时间'] = pd.to_datetime(df['上报时间'], errors='coerce')
                df = df.dropna(subset=['上报时间']).sort_values(by='上报时间', ascending=False)
                latest = df.head(limit).to_dict(orient='records')
                return {"data": latest, "count": len(latest)}
        return {"data": [], "count": 0}
    except Exception as e:
        logger.error(f"获取最新推送数据时出错: {str(e)}")
        raise HTTPException(status_code=500, detail=f"获取最新推送数据时出错: {str(e)}")


@app.get("/pushed", response_class=HTMLResponse)
async def pushed_html(limit: Optional[int] = 50):
    if not os.path.exists(PUSH_FILE):
        return HTMLResponse("<h3>暂无推送数据</h3>")
    try:
        df = pd.read_csv(PUSH_FILE)
    except Exception:
        df = pd.read_csv(PUSH_FILE, on_bad_lines='skip', engine='python')
    if len(df) == 0:
        return HTMLResponse("<h3>暂无推送数据</h3>")
    df['上报时间'] = pd.to_datetime(df['上报时间'], errors='coerce')
    df = df.dropna(subset=['上报时间']).sort_values(by='上报时间', ascending=False).head(limit)
    html_table = df.to_html(index=False)
    html = f"""
    <html>
    <head>
      <meta charset='utf-8' />
      <title>最新推送数据</title>
      <style>
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial, 'Microsoft YaHei', sans-serif; margin: 20px; }}
        table {{ border-collapse: collapse; width: 100%; }}
        th, td {{ border: 1px solid #e5e7eb; padding: 6px 10px; font-size: 14px; }}
        th {{ background: #f8fafc; }}
      </style>
    </head>
    <body>
      <h2>最新推送数据（前 {limit} 条）</h2>
      {html_table}
    </body>
    </html>
    """
    return HTMLResponse(html)


# 网络信息接口
@app.get("/public_info")
async def public_info():
    info = get_network_info()
    
    # 数据清理状态
    needs_cleanup, message, count = local_storage.check_data_cleanup()
    
    info.update({
        "db_enabled": False,  # 不再使用数据库
        "storage_type": "local_file",
        "data_cleanup": {
            "needs_cleanup": needs_cleanup,
            "message": message,
            "affected_records": count
        }
    })
    return info


# 设备管理接口
class DeviceIn(BaseModel):
    deviceNo: str
    imei: Optional[str] = None
    alias: Optional[str] = None
    location: Optional[str] = None
    is_active: Optional[bool] = True

class DeviceBulkImportIn(BaseModel):
    devices: List[DeviceIn]


@app.get("/api/devices")
async def list_devices(search: Optional[str] = None, status: Optional[str] = None):
    """
    列出所有设备，支持搜索和状态筛选：
    - search: 搜索表号、IMEI号或别名
    - status: 筛选状态，可选 active/inactive/all
    """
    devices = local_storage.list_devices(search, status)
    return {"data": devices, "count": len(devices)}


@app.post("/api/devices")
async def create_device(body: DeviceIn):
    success, device_no, is_new = local_storage.create_device(body.model_dump())
    if not success:
        raise HTTPException(status_code=400, detail=f"创建设备失败: {device_no}")
    return {"msg": "OK", "deviceNo": device_no, "is_new": is_new}


@app.post("/api/devices/bulk")
async def bulk_import_devices(body: DeviceBulkImportIn):
    """批量导入设备，用于批量创建或更新设备"""
    devices_data = [device.model_dump() for device in body.devices]
    count = local_storage.bulk_import_devices(devices_data)
    return {"msg": "OK", "count": count}


@app.patch("/api/devices/{device_no}")
async def update_device(device_no: str, body: DeviceIn):
    """更新设备信息"""
    success = local_storage.update_device(device_no, body.model_dump())
    if not success:
        raise HTTPException(status_code=404, detail="设备不存在")
    return {"msg": "OK", "deviceNo": device_no}


@app.get("/api/devices/{device_no}")
async def get_device_info(device_no: str):
    """获取设备详情"""
    device = local_storage.get_device(device_no)
    if not device:
        raise HTTPException(status_code=404, detail="设备不存在")
    return device


@app.get("/api/devices/{device_no}/stats")
async def get_device_stats(device_no: str):
    """获取设备统计信息"""
    stats = local_storage.get_device_stats(device_no)
    if not stats:
        raise HTTPException(status_code=404, detail="设备或设备数据不存在")
    return stats


# 数据管理接口
@app.delete("/api/data")
async def delete_data(start_date: str, end_date: str):
    """删除指定日期范围的数据"""
    deleted_count = local_storage.delete_data_range(start_date, end_date)
    return {"msg": "OK", "deleted_count": deleted_count}


# 数据清理接口
@app.get("/api/data/cleanup/check")
async def check_data_cleanup():
    """检查是否需要数据清理"""
    needs_cleanup, message, count = local_storage.check_data_cleanup()
    return {
        "needs_cleanup": needs_cleanup,
        "message": message,
        "affected_records": count
    }


@app.post("/api/data/cleanup/execute")
async def execute_data_cleanup():
    """执行数据清理"""
    success, message, deleted_count = local_storage.perform_data_cleanup()
    if not success:
        raise HTTPException(status_code=500, detail=message)
    return {
        "success": success,
        "message": message,
        "deleted_count": deleted_count
    }


# 登录输入模型
class LoginInput(BaseModel):
    username: str
    password: str


@app.post("/auth/login")
async def auth_login(body: LoginInput):
    username = body.username.strip()
    password = body.password
    
    # 使用本地存储进行验证
    user = local_storage.authenticate_user(username, password)
    if not user:
        raise HTTPException(status_code=401, detail="用户名或密码错误")
    
    # 创建Token
    token = create_token(username, user.get("role", "user"))
    return {"ok": True, "role": user.get("role"), "token": token}


@app.get("/auth/verify")
async def auth_verify(token: str):
    try:
        payload = verify_token(token)
        username = payload.get("sub", "")
        role = payload.get("role", "")
        return {"ok": True, "username": username, "role": role, "payload": payload}
    except Exception as e:
        return {"ok": False, "error": str(e)}


# Token认证逻辑
def _b64e(b: bytes) -> str:
    return base64.urlsafe_b64encode(b).decode().rstrip("=")

def _b64d(s: str) -> bytes:
    pad = '=' * (-len(s) % 4)
    return base64.urlsafe_b64decode(s + pad)

def create_token(username: str, role: str) -> str:
    AUTH_SECRET = getenv("AUTH_SECRET", "dev-secret")
    TOKEN_TTL_SECONDS = int(getenv("AUTH_TOKEN_TTL", "86400"))  # 默认1天
    
    header = {"alg": "HS256", "typ": "JWT"}
    payload = {"sub": username, "role": role, "exp": int(time.time()) + TOKEN_TTL_SECONDS}
    h = _b64e(json.dumps(header, separators=(',',':')).encode())
    p = _b64e(json.dumps(payload, separators=(',',':')).encode())
    signing = f"{h}.{p}".encode()
    sig = _b64e(hmac.new(AUTH_SECRET.encode(), signing, hashlib.sha256).digest())
    return f"{h}.{p}.{sig}"

def verify_token(token: str) -> dict:
    AUTH_SECRET = getenv("AUTH_SECRET", "dev-secret")
    try:
        h, p, s = token.split('.')
        signing = f"{h}.{p}".encode()
        expect = _b64e(hmac.new(AUTH_SECRET.encode(), signing, hashlib.sha256).digest())
        if not hmac.compare_digest(expect, s):
            raise ValueError("签名不匹配")
        payload = json.loads(_b64d(p))
        if int(payload.get('exp', 0)) < int(time.time()):
            raise ValueError("已过期")
        return payload
    except Exception as e:
        raise HTTPException(status_code=401, detail=f"无效令牌: {e}")


# 简易每分钟限流（按IP），默认每分钟 240 次，可用 RATE_LIMIT_PER_MINUTE 覆盖
RATE_LIMIT_PER_MIN = int(getenv("RATE_LIMIT_PER_MINUTE", "240"))
_RATE_BUCKETS: dict[str, list[int]] = {}

def _rate_limit_check(ip: str):
    now = int(time.time())
    bucket = _RATE_BUCKETS.get(ip, [])
    # 移除 60s 以前
    bucket = [t for t in bucket if now - t < 60]
    if len(bucket) >= RATE_LIMIT_PER_MIN:
        raise HTTPException(status_code=429, detail="请求过于频繁，请稍后再试")
    bucket.append(now)
    _RATE_BUCKETS[ip] = bucket


# 历史数据查询接口
@app.get("/api/history")
async def get_history_data(
    device_no: Optional[str] = None, 
    start_date: Optional[str] = None, 
    end_date: Optional[str] = None, 
    limit: int = 1000
):
    try:
        df = local_storage.query_water_data(device_no, start_date, end_date, limit)
        if not df.empty:
            history_data = df.to_dict(orient='records')
            return {"data": history_data, "count": len(history_data)}
        return {"data": [], "count": 0}
    except Exception as e:
        logger.error(f"获取历史数据时出错: {str(e)}")
        raise HTTPException(status_code=500, detail=f"获取历史数据时出错: {str(e)}")


# 查询单个设备单天数据
@app.get("/api/device/daily")
async def get_device_daily_data(device_no: str, date: str):
    try:
        # 验证日期格式
        try:
            dt = datetime.strptime(date, "%Y-%m-%d")
        except ValueError:
            raise HTTPException(status_code=400, detail="日期格式无效，请使用YYYY-MM-DD格式")
        
        # 设置当天开始和结束时间
        start_date = date
        end_date = (dt + datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        
        # 查询数据
        df = local_storage.query_water_data(device_no, start_date, end_date, 10000)
        if df.empty:
            return {"data": [], "count": 0}
        
        # 返回结果
        daily_data = df.to_dict(orient='records')
        return {
            "device_no": device_no,
            "date": date,
            "data": daily_data,
            "count": len(daily_data)
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取设备单天数据时出错: {str(e)}")
        raise HTTPException(status_code=500, detail=f"获取设备单天数据时出错: {str(e)}")


# 数据导出接口
@app.get("/api/export")
async def export_data(
    device_no: Optional[str] = None, 
    start_date: Optional[str] = None, 
    end_date: Optional[str] = None,
    format: str = "csv"  # "csv" 或 "json"
):
    try:
        # 查询数据
        df = local_storage.query_water_data(device_no, start_date, end_date, 100000)  # 限制导出10万条
        if df.empty:
            raise HTTPException(status_code=404, detail="未找到符合条件的数据")
        
        # 返回结果（前端通过此API可以直接下载数据）
        if format.lower() == "json":
            result = df.to_dict(orient='records')
            return {"data": result, "count": len(result)}
        else:
            # CSV默认格式，前端需要自行处理下载
            csv_data = df.to_csv(index=False, encoding='utf-8')
            return {"csv_data": csv_data, "count": len(df)}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"导出数据时出错: {str(e)}")
        raise HTTPException(status_code=500, detail=f"导出数据时出错: {str(e)}")


# 主函数
if __name__ == "__main__":
    # 初始化本地存储
    local_storage.init_storage()
    
    # 确保数据文件存在
    for f in [DATA_FILE, PUSH_FILE]:
        if not os.path.exists(f):
            # 创建空的CSV文件，包含所需的列名
            pd.DataFrame({
                "表号": [],
                "电池电压": [],
                "冻结流量": [],
                "imei号": [],
                "瞬时流量": [],
                "压力": [],
                "反向流量": [],
                "信号值": [],
                "启动次数": [],
                "温度": [],
                "累计流量": [],
                "阀门状态": [],
                "上报时间": [],
                "日期计算": [],
                "时间计算": [],
                "数据L/s": []
            }).to_csv(f, index=False, encoding='utf-8')
    
    # 启动API服务器
    port = int(os.getenv("API_PORT", "8000"))
    host = os.getenv("API_HOST", "0.0.0.0")
    logger.info(f"启动API服务器: {host}:{port}")
    uvicorn.run(app, host=host, port=port) 