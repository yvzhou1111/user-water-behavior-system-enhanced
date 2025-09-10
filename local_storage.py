#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
本地存储管理模块 - 用于替代数据库存储，提供基于文件的数据管理

功能：
1. 设备管理（增删改查）
2. 水表数据存储与查询
3. 用户认证
4. 数据自动清理（保留半年数据）
"""

import os
import json
import pandas as pd
import datetime
import time
import hashlib
import logging
import shutil
from typing import List, Dict, Optional, Union, Tuple, Any
import threading

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("local-storage")

# 存储路径配置
DATA_DIR = "data"  # 数据主目录
DEVICES_FILE = os.path.join(DATA_DIR, "devices.json")  # 设备信息
USERS_FILE = os.path.join(DATA_DIR, "users.json")  # 用户信息
DATA_FILE = "water_meter_data.csv"  # 主数据文件（兼容原有逻辑）

# 数据清理配置
DATA_RETAIN_DAYS = 180  # 保留半年（约180天）数据
CLEANING_WARNING_DAYS = 7  # 清理前7天开始发出警告
CLEAN_CHECK_INTERVAL = 24 * 60 * 60  # 每天检查一次是否需要清理

# 缓存最近的查询结果
query_cache = {}
cache_lock = threading.Lock()
CACHE_TIMEOUT = 60  # 缓存有效期（秒）

# 初始化存储
def init_storage():
    """初始化本地存储目录和文件"""
    try:
        # 创建数据目录
        if not os.path.exists(DATA_DIR):
            os.makedirs(DATA_DIR)
            logger.info(f"创建数据目录: {DATA_DIR}")
        
        # 初始化设备文件
        if not os.path.exists(DEVICES_FILE):
            with open(DEVICES_FILE, 'w', encoding='utf-8') as f:
                json.dump([], f, ensure_ascii=False, indent=2)
            logger.info(f"创建设备文件: {DEVICES_FILE}")
        
        # 初始化用户文件
        if not os.path.exists(USERS_FILE):
            # 创建默认管理员用户
            admin_password = os.environ.get("ADMIN_PASSWORD", "admin123")
            admin_hash = hashlib.sha256(admin_password.encode('utf-8')).hexdigest()
            default_users = [
                {
                    "username": "admin",
                    "password_hash": admin_hash,
                    "role": "admin",
                    "created_at": datetime.datetime.now().isoformat()
                }
            ]
            with open(USERS_FILE, 'w', encoding='utf-8') as f:
                json.dump(default_users, f, ensure_ascii=False, indent=2)
            logger.info(f"创建用户文件: {USERS_FILE}")
        
        # 初始化主数据文件
        if not os.path.exists(DATA_FILE):
            # 创建空的数据文件，包含所需的列名
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
            }).to_csv(DATA_FILE, index=False, encoding='utf-8')
            logger.info(f"创建数据文件: {DATA_FILE}")
        
        # 启动数据清理检查线程
        threading.Thread(target=_check_data_cleanup_loop, daemon=True).start()
        
        return True
    except Exception as e:
        logger.error(f"初始化存储失败: {e}")
        return False

# 设备管理功能
def list_devices(search: Optional[str] = None, status: Optional[str] = None) -> List[Dict]:
    """
    列出所有设备，支持搜索和状态筛选
    
    Args:
        search: 搜索关键字（表号、IMEI、别名）
        status: 状态筛选（active/inactive）
    
    Returns:
        设备列表
    """
    try:
        if not os.path.exists(DEVICES_FILE):
            return []
        
        with open(DEVICES_FILE, 'r', encoding='utf-8') as f:
            devices = json.load(f)
        
        # 筛选
        if search:
            search = search.lower()
            devices = [d for d in devices if 
                      search in str(d.get("device_no", "")).lower() or 
                      search in str(d.get("imei", "")).lower() or 
                      search in str(d.get("alias", "")).lower()]
        
        if status:
            if status.lower() == "active":
                devices = [d for d in devices if d.get("is_active", True)]
            elif status.lower() == "inactive":
                devices = [d for d in devices if not d.get("is_active", True)]
        
        # 补充数据统计
        for device in devices:
            device_no = device.get("device_no")
            if device_no:
                stats = get_device_stats(device_no)
                device["data_count"] = stats.get("dataCount", 0)
                device["last_data"] = stats.get("lastDataTime")
        
        return devices
    except Exception as e:
        logger.error(f"列出设备失败: {e}")
        return []

def get_device(device_no: str) -> Optional[Dict]:
    """获取单个设备信息"""
    try:
        if not os.path.exists(DEVICES_FILE):
            return None
        
        with open(DEVICES_FILE, 'r', encoding='utf-8') as f:
            devices = json.load(f)
        
        for device in devices:
            if device.get("device_no") == device_no:
                return device
        
        return None
    except Exception as e:
        logger.error(f"获取设备信息失败: {e}")
        return None

def create_device(device_data: Dict) -> Tuple[bool, str, bool]:
    """
    创建或更新设备
    
    Args:
        device_data: 设备数据
    
    Returns:
        (成功标志, 设备号, 是否为新建)
    """
    try:
        # 兼容 deviceNo / device_no
        device_no = device_data.get("device_no") or device_data.get("deviceNo")
        if not device_no:
            return False, "缺少device_no字段", False
        device_data["device_no"] = device_no
        
        # 读取现有设备
        devices = []
        if os.path.exists(DEVICES_FILE):
            with open(DEVICES_FILE, 'r', encoding='utf-8') as f:
                devices = json.load(f)
        
        # 检查是否存在
        is_new = True
        for i, device in enumerate(devices):
            if device.get("device_no") == device_no:
                # 更新现有设备
                is_new = False
                devices[i].update(device_data)
                devices[i]["updated_at"] = datetime.datetime.now().isoformat()
                break
        
        # 如果是新设备，添加到列表
        if is_new:
            device_data.setdefault("is_active", True)
            device_data.setdefault("created_at", datetime.datetime.now().isoformat())
            devices.append({
                "device_no": device_no,
                "imei": device_data.get("imei"),
                "alias": device_data.get("alias"),
                "location": device_data.get("location"),
                "is_active": bool(device_data.get("is_active", True)),
                "created_at": device_data.get("created_at", datetime.datetime.now().isoformat())
            })
        
        # 保存
        with open(DEVICES_FILE, 'w', encoding='utf-8') as f:
            json.dump(devices, f, ensure_ascii=False, indent=2)
        
        return True, device_no, is_new
    except Exception as e:
        logger.error(f"创建设备失败: {e}")
        return False, str(e), False

def update_device(device_no: str, update_data: Dict) -> bool:
    """更新设备信息"""
    try:
        if not os.path.exists(DEVICES_FILE):
            return False
        
        with open(DEVICES_FILE, 'r', encoding='utf-8') as f:
            devices = json.load(f)
        
        # 查找并更新
        found = False
        for i, device in enumerate(devices):
            if device.get("device_no") == device_no:
                found = True
                devices[i].update(update_data)
                devices[i]["updated_at"] = datetime.datetime.now().isoformat()
                break
        
        if not found:
            return False
        
        # 保存到文件
        with open(DEVICES_FILE, 'w', encoding='utf-8') as f:
            json.dump(devices, f, ensure_ascii=False, indent=2)
        
        return True
    except Exception as e:
        logger.error(f"更新设备失败: {e}")
        return False

def bulk_import_devices(devices_data: List[Dict]) -> int:
    """批量导入设备"""
    try:
        if not devices_data:
            return 0
        
        # 读取现有设备
        existing_devices = []
        if os.path.exists(DEVICES_FILE):
            with open(DEVICES_FILE, 'r', encoding='utf-8') as f:
                existing_devices = json.load(f)
        
        # 设备号索引
        device_map = {d.get("device_no"): i for i, d in enumerate(existing_devices)}
        
        # 处理每个设备
        now = datetime.datetime.now().isoformat()
        count = 0
        
        for device in devices_data:
            device_no = device.get("device_no") or device.get("deviceNo")
            if not device_no:
                continue
            
            if device_no in device_map:
                # 更新现有设备
                idx = device_map[device_no]
                existing_devices[idx].update(device)
                existing_devices[idx]["updated_at"] = now
            else:
                # 添加新设备
                device["created_at"] = now
                device["updated_at"] = now
                device["is_active"] = device.get("is_active", True)
                existing_devices.append(device)
            
            count += 1
        
        # 保存到文件
        with open(DEVICES_FILE, 'w', encoding='utf-8') as f:
            json.dump(existing_devices, f, ensure_ascii=False, indent=2)
        
        return count
    except Exception as e:
        logger.error(f"批量导入设备失败: {e}")
        return 0

def delete_device(device_no: str) -> bool:
    """删除设备"""
    try:
        if not os.path.exists(DEVICES_FILE):
            return False
        
        with open(DEVICES_FILE, 'r', encoding='utf-8') as f:
            devices = json.load(f)
        
        # 查找并删除
        initial_count = len(devices)
        devices = [d for d in devices if d.get("device_no") != device_no]
        
        if len(devices) == initial_count:
            return False  # 没有找到要删除的设备
        
        # 保存到文件
        with open(DEVICES_FILE, 'w', encoding='utf-8') as f:
            json.dump(devices, f, ensure_ascii=False, indent=2)
        
        return True
    except Exception as e:
        logger.error(f"删除设备失败: {e}")
        return False

# 数据存储与查询功能
def save_water_data(data: Dict) -> bool:
    """保存水表数据到CSV文件"""
    try:
        # 转换为标准格式
        row = {
            "表号": data.get("deviceNo", ""),
            "电池电压": data.get("batteryVoltage", ""),
            "冻结流量": data.get("freezeDateFlow", ""),
            "imei号": data.get("imei", ""),
            "瞬时流量": data.get("instantaneousFlow", ""),
            "压力": data.get("pressure", "0.0"),
            "反向流量": data.get("reverseFlow", ""),
            "信号值": data.get("signalValue", ""),
            "启动次数": data.get("startFrequency", ""),
            "温度": data.get("temprature", ""),
            "累计流量": data.get("totalFlow", ""),
            "阀门状态": data.get("valveStatu", "开"),
            "上报时间": data.get("updateTime", "")
        }
        
        # 解析时间，计算日期和时间字段
        try:
            dt = pd.to_datetime(row["上报时间"])
            row["日期计算"] = dt.strftime("%Y-%m-%d")
            row["时间计算"] = dt.strftime("%H:%M:%S")
            
            # 计算数据L/s (瞬时流量 m³/h -> L/s)
            try:
                row["数据L/s"] = float(row["瞬时流量"]) / 3.6
            except (ValueError, TypeError):
                row["数据L/s"] = 0
        except Exception:
            row["日期计算"] = ""
            row["时间计算"] = ""
            row["数据L/s"] = 0
        
        # 将数据转为DataFrame并添加到CSV
        df = pd.DataFrame([row])
        
        if os.path.exists(DATA_FILE):
            df.to_csv(DATA_FILE, mode='a', header=False, index=False, encoding='utf-8')
        else:
            df.to_csv(DATA_FILE, index=False, encoding='utf-8')
        
        # 清除查询缓存
        with cache_lock:
            query_cache.clear()
        
        # 同时确保设备信息存在
        device_no = data.get("deviceNo")
        imei = data.get("imei")
        if device_no:
            device = get_device(device_no)
            if not device:
                # 自动创建设备记录
                create_device({
                    "device_no": device_no,
                    "imei": imei,
                    "is_active": True
                })
        
        return True
    except Exception as e:
        logger.error(f"保存水表数据失败: {e}")
        return False

def query_water_data(device_no: Optional[str] = None, start_date: Optional[str] = None, 
                    end_date: Optional[str] = None, limit: int = 100) -> pd.DataFrame:
    """查询水表数据"""
    cache_key = f"query_{device_no}_{start_date}_{end_date}_{limit}"
    
    # 检查缓存
    with cache_lock:
        if cache_key in query_cache:
            cache_entry = query_cache[cache_key]
            if time.time() - cache_entry["timestamp"] < CACHE_TIMEOUT:
                return cache_entry["data"]
    
    try:
        if not os.path.exists(DATA_FILE):
            return pd.DataFrame()
        
        # 读取数据文件
        df = pd.read_csv(DATA_FILE)
        if df.empty:
            return df
        
        # 应用筛选条件
        if device_no:
            df = df[df["表号"] == device_no]
        
        if start_date:
            df["上报时间"] = pd.to_datetime(df["上报时间"])
            df = df[df["上报时间"].dt.date >= pd.to_datetime(start_date).date()]
        
        if end_date:
            if "上报时间" not in df.columns or not pd.api.types.is_datetime64_any_dtype(df["上报时间"]):
                df["上报时间"] = pd.to_datetime(df["上报时间"])
            df = df[df["上报时间"].dt.date <= pd.to_datetime(end_date).date()]
        
        # 排序并限制结果数量
        df = df.sort_values(by="上报时间", ascending=False).head(limit)
        
        # 更新缓存
        with cache_lock:
            query_cache[cache_key] = {
                "data": df,
                "timestamp": time.time()
            }
        
        return df
    except Exception as e:
        logger.error(f"查询水表数据失败: {e}")
        return pd.DataFrame()

def get_device_stats(device_no: str) -> Dict:
    """获取设备数据统计信息"""
    try:
        if not os.path.exists(DATA_FILE):
            return {
                "deviceNo": device_no,
                "dataCount": 0,
                "firstDataTime": None,
                "lastDataTime": None,
                "minFlow": None,
                "maxFlow": None,
                "avgFlow": None
            }
        
        # 读取该设备的数据
        df = pd.read_csv(DATA_FILE)
        if df.empty:
            return {
                "deviceNo": device_no,
                "dataCount": 0,
                "firstDataTime": None,
                "lastDataTime": None,
                "minFlow": None,
                "maxFlow": None,
                "avgFlow": None
            }
        
        df = df[df["表号"] == device_no]
        if df.empty:
            return {
                "deviceNo": device_no,
                "dataCount": 0,
                "firstDataTime": None,
                "lastDataTime": None,
                "minFlow": None,
                "maxFlow": None,
                "avgFlow": None
            }
        
        # 转换时间列
        df["上报时间"] = pd.to_datetime(df["上报时间"])
        
        # 计算统计信息
        data_count = len(df)
        first_data_time = df["上报时间"].min()
        last_data_time = df["上报时间"].max()
        
        # 计算流量统计
        df["瞬时流量"] = pd.to_numeric(df["瞬时流量"], errors="coerce")
        min_flow = df["瞬时流量"].min()
        max_flow = df["瞬时流量"].max()
        avg_flow = df["瞬时流量"].mean()
        
        return {
            "deviceNo": device_no,
            "dataCount": data_count,
            "firstDataTime": first_data_time.isoformat() if not pd.isna(first_data_time) else None,
            "lastDataTime": last_data_time.isoformat() if not pd.isna(last_data_time) else None,
            "minFlow": float(min_flow) if not pd.isna(min_flow) else None,
            "maxFlow": float(max_flow) if not pd.isna(max_flow) else None,
            "avgFlow": float(avg_flow) if not pd.isna(avg_flow) else None
        }
    except Exception as e:
        logger.error(f"获取设备统计信息失败: {e}")
        return {
            "deviceNo": device_no,
            "dataCount": 0,
            "firstDataTime": None,
            "lastDataTime": None,
            "minFlow": None,
            "maxFlow": None,
            "avgFlow": None,
            "error": str(e)
        }

def delete_data_range(start_date: str, end_date: str) -> int:
    """
    删除指定日期范围内的数据
    
    Returns:
        删除的记录数
    """
    try:
        if not os.path.exists(DATA_FILE):
            return 0
        
        # 读取数据
        df = pd.read_csv(DATA_FILE)
        if df.empty:
            return 0
        
        # 转换时间
        df["上报时间"] = pd.to_datetime(df["上报时间"])
        
        # 计算需要保留的记录
        start_dt = pd.to_datetime(start_date)
        end_dt = pd.to_datetime(end_date)
        mask = (df["上报时间"].dt.date < start_dt.date()) | (df["上报时间"].dt.date > end_dt.date())
        
        # 记录删除的条数
        deleted_count = len(df) - mask.sum()
        
        # 保存过滤后的数据
        df[mask].to_csv(DATA_FILE, index=False, encoding='utf-8')
        
        # 清除查询缓存
        with cache_lock:
            query_cache.clear()
        
        return deleted_count
    except Exception as e:
        logger.error(f"删除数据范围失败: {e}")
        return 0

# 用户认证功能
def authenticate_user(username: str, password: str) -> Optional[Dict]:
    """验证用户登录凭据"""
    try:
        if not os.path.exists(USERS_FILE):
            return None
        
        with open(USERS_FILE, 'r', encoding='utf-8') as f:
            users = json.load(f)
        
        # 计算密码哈希
        password_hash = hashlib.sha256(password.encode('utf-8')).hexdigest()
        
        # 查找匹配用户
        for user in users:
            if user.get("username") == username and user.get("password_hash") == password_hash:
                return {
                    "username": user.get("username"),
                    "role": user.get("role", "user")
                }
        
        return None
    except Exception as e:
        logger.error(f"用户认证失败: {e}")
        return None

# 数据自动清理功能
def check_data_cleanup() -> Tuple[bool, str, int]:
    """
    检查是否需要清理数据
    
    Returns:
        (是否需要清理, 清理信息, 即将删除的记录数)
    """
    try:
        if not os.path.exists(DATA_FILE):
            return False, "数据文件不存在", 0
        
        # 读取数据
        df = pd.read_csv(DATA_FILE)
        if df.empty:
            return False, "没有数据需要清理", 0
        
        # 转换时间
        df["上报时间"] = pd.to_datetime(df["上报时间"])
        
        # 计算截止日期
        cutoff_date = datetime.datetime.now() - datetime.timedelta(days=DATA_RETAIN_DAYS)
        old_data = df[df["上报时间"].dt.date < cutoff_date.date()]
        
        if old_data.empty:
            return False, "没有过期数据需要清理", 0
        
        # 计算将被清理的数据范围
        oldest_date = old_data["上报时间"].min().date()
        newest_date = old_data["上报时间"].max().date()
        count = len(old_data)
        
        # 判断是否在警告期内
        warning_date = datetime.datetime.now() - datetime.timedelta(days=DATA_RETAIN_DAYS - CLEANING_WARNING_DAYS)
        if newest_date >= warning_date.date():
            # 在警告期内，但还不需要实际清理
            return False, f"警告: {count}条数据即将在{CLEANING_WARNING_DAYS}天后被清理 ({oldest_date} 至 {newest_date})", count
        else:
            # 已超过警告期，需要清理
            return True, f"需要清理 {count} 条数据 ({oldest_date} 至 {newest_date})", count
    except Exception as e:
        logger.error(f"检查数据清理失败: {e}")
        return False, f"检查失败: {str(e)}", 0

def perform_data_cleanup() -> Tuple[bool, str, int]:
    """
    执行数据清理
    
    Returns:
        (是否成功, 清理信息, 已删除记录数)
    """
    try:
        needs_cleanup, message, count = check_data_cleanup()
        if not needs_cleanup:
            return False, message, 0
        
        # 进行清理前备份
        backup_file = f"backup_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        backup_path = os.path.join(DATA_DIR, backup_file)
        shutil.copy2(DATA_FILE, backup_path)
        logger.info(f"数据备份已保存: {backup_path}")
        
        # 读取数据
        df = pd.read_csv(DATA_FILE)
        df["上报时间"] = pd.to_datetime(df["上报时间"])
        
        # 保留近半年数据
        cutoff_date = datetime.datetime.now() - datetime.timedelta(days=DATA_RETAIN_DAYS)
        new_df = df[df["上报时间"].dt.date >= cutoff_date.date()]
        
        # 保存过滤后的数据
        new_df.to_csv(DATA_FILE, index=False, encoding='utf-8')
        
        # 清除查询缓存
        with cache_lock:
            query_cache.clear()
        
        # 计算删除的记录数
        deleted_count = len(df) - len(new_df)
        
        return True, f"已清理 {deleted_count} 条数据，备份文件: {backup_file}", deleted_count
    except Exception as e:
        logger.error(f"执行数据清理失败: {e}")
        return False, f"清理失败: {str(e)}", 0

def _check_data_cleanup_loop():
    """数据清理检查循环"""
    while True:
        try:
            needs_cleanup, message, count = check_data_cleanup()
            if needs_cleanup:
                logger.info(f"开始自动数据清理: {message}")
                success, result_msg, deleted_count = perform_data_cleanup()
                logger.info(f"自动数据清理完成: {result_msg}")
            
            # 睡眠到下次检查
            time.sleep(CLEAN_CHECK_INTERVAL)
        except Exception as e:
            logger.error(f"数据清理检查错误: {e}")
            time.sleep(3600)  # 发生错误时，1小时后重试 