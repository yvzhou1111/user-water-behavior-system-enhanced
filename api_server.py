from fastapi import FastAPI, BackgroundTasks, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, field_validator
import pandas as pd
import os
import uvicorn
from datetime import datetime
import logging
from typing import Optional, Union
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

# Load environment variables
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

# 数据库连接串（Neon / PostgreSQL）。存在即启用入库
DB_URL = os.getenv("NEON_URL") or os.getenv("DATABASE_URL")

# Max retries for database operations
DB_MAX_RETRIES = int(os.getenv("DB_MAX_RETRIES", "3"))
DB_RETRY_DELAY = float(os.getenv("DB_RETRY_DELAY", "1.0"))

# 保存并发写入锁
SAVE_LOCK = Lock()

# 可选：psycopg2
try:
    import psycopg2  # type: ignore
    _PSYCOPG2_AVAILABLE = True
except Exception:
    _PSYCOPG2_AVAILABLE = False

# FastAPI应用程序
app = FastAPI(
    title="水表数据接收API",
    description="接收水表设备推送的实时数据并进行存储",
    version="1.0.4"
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


def to_row_dict(data: dict) -> dict:
    dt = parse_update_time(data["updateTime"])
    update_time_str = dt.strftime("%Y-%m-%d %H:%M:%S")
    try:
        # instantaneousFlow 按 m³/h 传入，L/s = m³/h / 3.6
        inst_m3h = float(data["instantaneousFlow"]) if str(data.get("instantaneousFlow", "")).strip() != "" else 0.0
        inst_ls = inst_m3h / 3.6
    except Exception:
        inst_m3h = 0.0
        inst_ls = 0.0
    row = {
        "表号": data["deviceNo"],
        "电池电压": data["batteryVoltage"],
        "冻结流量": data["freezeDateFlow"],
        "imei号": data.get("imei") or data.get("IMEI号", ""),
        "瞬时流量": data["instantaneousFlow"],  # 以 m³/h 存原值
        "压力": data["pressure"],
        "反向流量": data["reverseFlow"],
        "信号值": data["signalValue"],
        "启动次数": data["startFrequency"],
        "温度": data["temprature"],
        "累计流量": data["totalFlow"],
        "阀门状态": data["valveStatu"],
        "上报时间": update_time_str,
        "日期计算": dt.strftime("%Y-%m-%d"),
        "时间计算": dt.strftime("%H:%M:%S"),
        "数据L/s": inst_ls
    }
    return row

# 新增：网络信息汇总（用于 /public_info 返回）
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

# 新增：DB 助手与入库
def _get_db_conn():
    if not DB_URL or not _PSYCOPG2_AVAILABLE:
        return None
    retries = 0
    while retries < DB_MAX_RETRIES:
    try:
            conn = psycopg2.connect(DB_URL)
            # 设置连接超时
            conn.set_session(autocommit=True)
            return conn
        except psycopg2.OperationalError as e:
            retries += 1
            if retries < DB_MAX_RETRIES:
                logger.warning(f"数据库连接失败，尝试第 {retries} 次重连... 错误: {e}")
                time.sleep(DB_RETRY_DELAY)
            else:
                logger.error(f"数据库连接失败，已达最大重试次数。错误: {e}")
                return None
    except Exception as e:
            logger.error(f"数据库连接失败，错误: {e}")
            return None
        return None

def ensure_device_exists(conn, device_no: str, imei: Optional[str]):
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO app.devices (device_no, imei)
                VALUES (%s, %s)
                ON CONFLICT (device_no)
                DO UPDATE SET imei = COALESCE(EXCLUDED.imei, app.devices.imei)
                """,
                (device_no, imei)
            )
    except Exception as e:
        logger.warning(f"注册设备失败(忽略): {e}")


def save_data_to_db(data: dict):
    conn = _get_db_conn()
    if conn is None:
        return
    try:
        dt = parse_update_time(data['updateTime'])
        # 类型转换
        def _to_float(x):
            try:
                return float(x)
            except Exception:
                return None
        def _to_int(x):
            try:
                return int(float(x))
            except Exception:
                return None
        device_no = data['deviceNo']
        imei = data.get('imei')
        battery_voltage = _to_float(data.get('batteryVoltage'))
        freeze_date_flow = _to_float(data.get('freezeDateFlow'))
        instantaneous_flow = _to_float(data.get('instantaneousFlow'))  # m³/h
        pressure = _to_float(data.get('pressure'))
        reverse_flow = _to_float(data.get('reverseFlow'))
        signal_value = _to_int(data.get('signalValue'))
        start_frequency = _to_int(data.get('startFrequency'))
        temperature = _to_float(data.get('temprature'))
        total_flow = _to_float(data.get('totalFlow'))
        valve_status = data.get('valveStatu')

        with conn:
            ensure_device_exists(conn, device_no, imei)
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO app.water_data (
                        device_no, imei, battery_voltage, freeze_date_flow,
                        instantaneous_flow, pressure, reverse_flow, signal_value,
                        start_frequency, temperature, total_flow, valve_status, update_time
                    ) VALUES (
                        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
                    )
                    """,
                    (
                        device_no, imei, battery_voltage, freeze_date_flow,
                        instantaneous_flow, pressure, reverse_flow, signal_value,
                        start_frequency, temperature, total_flow, valve_status, dt
                    )
                )
    except Exception as e:
        logger.error(f"入库失败: {e}")
    finally:
        try:
            conn.close()
        except Exception:
            pass

# 保存数据到CSV，确保字段名与CSV文件中的中文字段正确对应
def save_data_to_csv(data: dict):
    """
    保存API接收的数据到CSV，确保字段名与CSV文件中的中文字段正确对应
    """
    try:
        row = to_row_dict(data)
        df = pd.DataFrame([row])
        
        # 进程内互斥，避免并发写入导致文件内容损坏
        with SAVE_LOCK:
            # 写入原有表（兼容前端）
            if os.path.exists(DATA_FILE):
                df.to_csv(DATA_FILE, mode='a', header=False, index=False, encoding='utf-8')
            else:
                df.to_csv(DATA_FILE, index=False, encoding='utf-8')
                
            # 写入新推送表
            if os.path.exists(PUSH_FILE):
                df.to_csv(PUSH_FILE, mode='a', header=False, index=False, encoding='utf-8')
            else:
                df.to_csv(PUSH_FILE, index=False, encoding='utf-8')
        
        logger.info(f"数据已保存: deviceNo={data['deviceNo']} time={row['上报时间']}")
        return True
    except Exception as e:
        logger.error(f"保存数据时发生错误: {str(e)}")
        raise

# 简易 Token 配置
AUTH_SECRET = getenv("AUTH_SECRET", "dev-secret")
TOKEN_TTL_SECONDS = int(getenv("AUTH_TOKEN_TTL", "86400"))  # 默认1天

def _b64e(b: bytes) -> str:
    return base64.urlsafe_b64encode(b).decode().rstrip("=")

def _b64d(s: str) -> bytes:
    pad = '=' * (-len(s) % 4)
    return base64.urlsafe_b64decode(s + pad)

def create_token(username: str, role: str) -> str:
    header = {"alg": "HS256", "typ": "JWT"}
    payload = {"sub": username, "role": role, "exp": int(time.time()) + TOKEN_TTL_SECONDS}
    h = _b64e(json.dumps(header, separators=(',',':')).encode())
    p = _b64e(json.dumps(payload, separators=(',',':')).encode())
    signing = f"{h}.{p}".encode()
    sig = _b64e(hmac.new(AUTH_SECRET.encode(), signing, hashlib.sha256).digest())
    return f"{h}.{p}.{sig}"

def verify_token(token: str) -> dict:
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
        background_tasks.add_task(save_data_to_csv, data_dict)
        # 可选：数据库入库
        if DB_URL and _PSYCOPG2_AVAILABLE:
            background_tasks.add_task(save_data_to_db, data_dict)
        
        # 返回成功响应，与接口文档一致
        return {"msg": "SUCCESS", "code": 200}
    except Exception as e:
        logger.error(f"处理数据时发生错误: {str(e)}")
        raise HTTPException(status_code=500, detail=f"处理数据时发生错误: {str(e)}")

# 健康检查端点
@app.get("/health")
async def health_check():
    # 检查数据文件是否可访问
    ok1 = os.path.exists(DATA_FILE)
    ok2 = os.path.exists(PUSH_FILE)
    return {"status": "healthy" if (ok1 or ok2) else "unhealthy"}

# 获取最新数据端点
@app.get("/api/latest")
async def get_latest_data(limit: Optional[int] = 10):
    try:
        if os.path.exists(DATA_FILE):
            try:
                df = pd.read_csv(DATA_FILE)
            except Exception:
                # 读取健壮化：跳过坏行，使用 python 引擎兜底
                df = pd.read_csv(DATA_FILE, on_bad_lines='skip', engine='python')
            if len(df) > 0:
                # 确保按时间排序
                df['上报时间'] = pd.to_datetime(df['上报时间'], errors='coerce')
                df = df.dropna(subset=['上报时间']).sort_values(by='上报时间', ascending=False)
                latest = df.head(limit).to_dict(orient='records')
                return {"data": latest, "count": len(latest)}
            return {"data": [], "count": 0}
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

# 新增：网络信息接口
@app.get("/public_info")
async def public_info():
    info = get_network_info()
    info.update({
        "db_enabled": bool(DB_URL and _PSYCOPG2_AVAILABLE)
    })
    return info

# 设备管理接口（需配置数据库，否则返回 501）
class DeviceIn(BaseModel):
    deviceNo: str
    imei: Optional[str] = None
    alias: Optional[str] = None
    location: Optional[str] = None
    is_active: Optional[bool] = True

class DeviceBulkImportIn(BaseModel):
    devices: list[DeviceIn]

@app.get("/api/devices")
async def list_devices(search: Optional[str] = None, status: Optional[str] = None):
    """
    列出所有设备，支持搜索和状态筛选：
    - search: 搜索表号、IMEI号或别名
    - status: 筛选状态，可选 active/inactive/all
    """
    if not (DB_URL and _PSYCOPG2_AVAILABLE):
        raise HTTPException(status_code=501, detail="未配置数据库")
    conn = _get_db_conn()
    if conn is None:
        raise HTTPException(status_code=500, detail="数据库连接失败")
    try:
        with conn, conn.cursor() as cur:
            query = """
            SELECT 
                device_no, imei, alias, location, is_active, created_at, 
                (SELECT COUNT(*) FROM app.water_data wd WHERE wd.device_no = d.device_no) as data_count,
                (SELECT MAX(update_time) FROM app.water_data wd WHERE wd.device_no = d.device_no) as last_data
            FROM app.devices d
            """
            params = []
            
            # 构建WHERE子句
            where_clauses = []
            if search:
                where_clauses.append("(device_no LIKE %s OR imei LIKE %s OR alias LIKE %s)")
                search_param = f"%{search}%"
                params.extend([search_param, search_param, search_param])
            
            if status:
                if status.lower() == "active":
                    where_clauses.append("is_active = TRUE")
                elif status.lower() == "inactive":
                    where_clauses.append("is_active = FALSE")
            
            if where_clauses:
                query += " WHERE " + " AND ".join(where_clauses)
            
            query += " ORDER BY created_at DESC"
            
            cur.execute(query, params)
            rows = cur.fetchall()
        
        data = []
        for r in rows:
            device_data = {
                "deviceNo": r[0], 
                "imei": r[1], 
                "alias": r[2], 
                "location": r[3], 
                "is_active": r[4], 
                "created_at": r[5].isoformat(),
                "data_count": r[6],
                "last_data": r[7].isoformat() if r[7] else None
            }
            data.append(device_data)
        return {"data": data, "count": len(data)}
    finally:
        try:
            conn.close()
        except Exception:
            pass

@app.post("/api/devices")
async def create_device(body: DeviceIn):
    if not (DB_URL and _PSYCOPG2_AVAILABLE):
        raise HTTPException(status_code=501, detail="未配置数据库")
    conn = _get_db_conn()
    if conn is None:
        raise HTTPException(status_code=500, detail="数据库连接失败")
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO app.devices (device_no, imei, alias, location, is_active)
                    VALUES (%s,%s,%s,%s,COALESCE(%s, true))
                    ON CONFLICT (device_no) DO UPDATE SET
                        imei = EXCLUDED.imei,
                        alias = EXCLUDED.alias,
                        location = EXCLUDED.location,
                        is_active = EXCLUDED.is_active,
                        updated_at = CURRENT_TIMESTAMP
                    RETURNING device_no, created_at
                    """,
                    (body.deviceNo, body.imei, body.alias, body.location, body.is_active)
                )
                result = cur.fetchone()
                is_new = cur.rowcount > 0
        return {"msg": "OK", "deviceNo": result[0], "created_at": result[1].isoformat(), "is_new": is_new}
    finally:
        try:
            conn.close()
        except Exception:
            pass

@app.post("/api/devices/bulk")
async def bulk_import_devices(body: DeviceBulkImportIn):
    """批量导入设备，用于批量创建或更新设备"""
    if not (DB_URL and _PSYCOPG2_AVAILABLE):
        raise HTTPException(status_code=501, detail="未配置数据库")
    
    if not body.devices:
        return {"msg": "OK", "count": 0}
    
    conn = _get_db_conn()
    if conn is None:
        raise HTTPException(status_code=500, detail="数据库连接失败")
    
    try:
        with conn:
            values = []
            for device in body.devices:
                values.append({
                    "device_no": device.deviceNo,
                    "imei": device.imei,
                    "alias": device.alias,
                    "location": device.location,
                    "is_active": device.is_active if device.is_active is not None else True
                })
            
            with conn.cursor() as cur:
                from psycopg2.extras import execute_values
                
                # 使用批量插入优化性能
                execute_values(
                    cur,
                    """
                    INSERT INTO app.devices (device_no, imei, alias, location, is_active)
                    VALUES %s
                    ON CONFLICT (device_no) DO UPDATE SET
                        imei = EXCLUDED.imei,
                        alias = EXCLUDED.alias,
                        location = EXCLUDED.location,
                        is_active = EXCLUDED.is_active,
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    [(d["device_no"], d["imei"], d["alias"], d["location"], d["is_active"]) for d in values]
                )
                
        return {"msg": "OK", "count": len(body.devices)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"批量导入失败: {str(e)}")
    finally:
        try:
            conn.close()
        except Exception:
            pass

@app.patch("/api/devices/{device_no}")
async def update_device(device_no: str, body: DeviceIn):
    """更新设备信息"""
    if not (DB_URL and _PSYCOPG2_AVAILABLE):
        raise HTTPException(status_code=501, detail="未配置数据库")
    conn = _get_db_conn()
    if conn is None:
        raise HTTPException(status_code=500, detail="数据库连接失败")
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE app.devices SET
                        imei = COALESCE(%s, imei),
                        alias = COALESCE(%s, alias),
                        location = COALESCE(%s, location),
                        is_active = COALESCE(%s, is_active),
                        updated_at = CURRENT_TIMESTAMP
                    WHERE device_no = %s
                    RETURNING device_no
                    """,
                    (body.imei, body.alias, body.location, body.is_active, device_no)
                )
                if cur.rowcount == 0:
                    raise HTTPException(status_code=404, detail="设备不存在")
        return {"msg": "OK", "deviceNo": device_no}
    finally:
        try:
            conn.close()
        except Exception:
            pass

@app.get("/api/devices/{device_no}/stats")
async def get_device_stats(device_no: str):
    """获取设备统计信息"""
    if not (DB_URL and _PSYCOPG2_AVAILABLE):
        raise HTTPException(status_code=501, detail="未配置数据库")
    conn = _get_db_conn()
    if conn is None:
        raise HTTPException(status_code=500, detail="数据库连接失败")
    try:
        with conn, conn.cursor() as cur:
            # 检查设备是否存在
            cur.execute("SELECT EXISTS(SELECT 1 FROM app.devices WHERE device_no = %s)", (device_no,))
            exists = cur.fetchone()[0]
            if not exists:
                raise HTTPException(status_code=404, detail="设备不存在")
            
            # 数据量
            cur.execute("SELECT COUNT(*) FROM app.water_data WHERE device_no = %s", (device_no,))
            data_count = cur.fetchone()[0]
            
            # 最早和最晚数据时间
            cur.execute(
                "SELECT MIN(update_time), MAX(update_time) FROM app.water_data WHERE device_no = %s", 
                (device_no,)
            )
            first_data, last_data = cur.fetchone()
            
            # 最大、最小、平均流量
            cur.execute(
                """
                SELECT 
                    MIN(instantaneous_flow), 
                    MAX(instantaneous_flow), 
                    AVG(instantaneous_flow)
                FROM app.water_data 
                WHERE device_no = %s AND instantaneous_flow IS NOT NULL
                """, 
                (device_no,)
            )
            min_flow, max_flow, avg_flow = cur.fetchone()
            
            return {
                "deviceNo": device_no,
                "dataCount": data_count,
                "firstDataTime": first_data.isoformat() if first_data else None,
                "lastDataTime": last_data.isoformat() if last_data else None,
                "minFlow": min_flow,
                "maxFlow": max_flow,
                "avgFlow": avg_flow
            }
    finally:
        try:
            conn.close()
        except Exception:
            pass

# 登录输入模型
class LoginInput(BaseModel):
    username: str
    password: str

@app.post("/auth/login")
async def auth_login(body: LoginInput):
    username = body.username.strip()
    password = body.password
    hashed = hashlib.sha256(password.encode("utf-8")).hexdigest()

    # 优先数据库
    if DB_URL and _PSYCOPG2_AVAILABLE:
        conn = _get_db_conn()
        if conn is None:
            raise HTTPException(status_code=500, detail="数据库连接失败")
        try:
            with conn, conn.cursor() as cur:
                cur.execute("SELECT password_hash, role FROM app.users WHERE username=%s", (username,))
                row = cur.fetchone()
                if not row:
                    raise HTTPException(status_code=401, detail="用户名或密码错误")
                pwd_hash, role = row
                if pwd_hash != hashed:
                    raise HTTPException(status_code=401, detail="用户名或密码错误")
                token = create_token(username, role)
                return {"ok": True, "role": role, "token": token}
        finally:
            try:
                conn.close()
            except Exception:
                pass

    # 回退：环境变量
    env_user = os.getenv("ADMIN_USERNAME")
    env_pass = os.getenv("ADMIN_PASSWORD")
    if env_user and env_pass:
        env_hash = hashlib.sha256(env_pass.encode("utf-8")).hexdigest()
        if username == env_user and hashed == env_hash:
            token = create_token(username, "admin")
            return {"ok": True, "role": "admin", "token": token}

    raise HTTPException(status_code=503, detail="未配置用户数据库或管理员环境变量")

@app.get("/auth/verify")
async def auth_verify(token: str):
    try:
    payload = verify_token(token)
        username = payload.get("sub", "")
        role = payload.get("role", "")
        return {"ok": True, "username": username, "role": role, "payload": payload}
    except Exception as e:
        return {"ok": False, "error": str(e)}

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

# 主函数
if __name__ == "__main__":
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
            logger.info(f"创建了新的数据文件: {os.path.abspath(f)}")
    
    # 输出IP地址和端口信息
    import socket
    def get_ips():
        """获取本机所有可用IP地址"""
        ips = set()
        try:
            # 获取主机名
            hostname = socket.gethostname()
            # 获取主机名对应的IP
            ips.add(socket.gethostbyname(hostname))
            
            # 尝试获取所有网络接口的IP
            for interface in socket.getaddrinfo(socket.gethostname(), None):
                ip = interface[4][0]
                # 只添加IPv4地址且不是回环地址
                if not ip.startswith('127.') and ':' not in ip:
                    ips.add(ip)
            
            # 尝试通过连接获取IP
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
    
    host = "0.0.0.0"  # 监听所有接口
    port = 8000
    
    # 获取所有可用IP
    ips = get_ips()

    # 新增：选择一个LAN地址、探测公网IP，并尝试UPnP映射
    lan_ip = pick_lan_ip(ips)
    external_port = int(os.getenv("EXTERNAL_PORT", str(port)))
    enable_upnp = os.getenv("ENABLE_UPNP", "1").lower() in ("1", "true", "yes", "y")
    public_ip = get_public_ip()

    logger.info(f"启动水表数据接收服务器...")
    for ip in ips:
        logger.info(f"数据接收地址: http://{ip}:{port}/api/data")
    logger.info(f"健康检查地址: http://localhost:{port}/health")
    logger.info(f"最新数据查询(原表): http://localhost:{port}/api/latest")
    logger.info(f"最新数据查询(推送表): http://localhost:{port}/api/latest_pushed")
    logger.info(f"推送数据网页: http://localhost:{port}/pushed")
    logger.info(f"数据存储位置: {os.path.abspath(DATA_FILE)}")

    # 打印公网提示
    if public_ip:
        logger.info(f"公网探测IP: {public_ip}")
        logger.info(f"若已正确做端口映射，对外接收地址: http://{public_ip}:{external_port}/api/data")
        logger.info(f"对外健康检查: http://{public_ip}:{external_port}/health")
    else:
        logger.info("公网IP探测失败（可能无外网或被阻断）")

    # 可选：尝试UPnP自动端口映射（需要路由器开启UPnP，且安装miniupnpc）
    if enable_upnp and lan_ip:
        ok, msg = try_upnp_map(external_port, port, lan_ip)
        logger.info(f"UPnP端口映射尝试: {'成功' if ok else '失败'} - {msg}")
    elif not lan_ip:
        logger.info("未找到合适的私网LAN地址，跳过UPnP端口映射")
    else:
        logger.info("已禁用UPnP端口映射（设置 ENABLE_UPNP=0 可显式关闭）")
    
    # 启动服务器
    uvicorn.run(app, host=host, port=port) 