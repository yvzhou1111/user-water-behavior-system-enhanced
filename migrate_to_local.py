#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
数据迁移工具 - 将PostgreSQL数据库数据迁移至本地文件存储

迁移内容：
1. 设备信息
2. 水表数据
3. 用户账户
"""

import os
import json
import pandas as pd
import datetime
import time
import psycopg2
from dotenv import load_dotenv
import local_storage

# 加载环境变量
load_dotenv()

# 数据库连接串
DB_URL = os.getenv("NEON_URL") or os.getenv("DATABASE_URL")

def migrate_data():
    """执行数据迁移"""
    print("=" * 60)
    print("数据迁移工具 - PostgreSQL => 本地文件存储")
    print("=" * 60)
    
    if not DB_URL:
        print("未配置数据库连接串，无法进行迁移")
        return False
    
    try:
        # 初始化本地存储
        print("初始化本地存储...")
        local_storage.init_storage()
        
        # 连接数据库
        print(f"连接数据库...")
        conn = psycopg2.connect(DB_URL)
        
        # 迁移设备信息
        print("迁移设备信息...")
        migrate_devices(conn)
        
        # 迁移水表数据
        print("迁移水表数据...")
        migrate_water_data(conn)
        
        # 迁移用户账户
        print("迁移用户账户...")
        migrate_users(conn)
        
        print("\n迁移完成！系统现在使用本地文件存储")
        print("您可以在app.py和api_server.py中移除数据库相关配置")
        return True
    except Exception as e:
        print(f"迁移过程中发生错误: {e}")
        return False
    finally:
        try:
            conn.close()
        except:
            pass

def migrate_devices(conn):
    """迁移设备信息"""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    device_no, imei, alias, location, is_active, 
                    created_at, updated_at
                FROM app.devices
            """)
            rows = cur.fetchall()
        
        if not rows:
            print("没有设备数据需要迁移")
            return
        
        devices = []
        for row in rows:
            device = {
                "device_no": row[0],
                "imei": row[1],
                "alias": row[2],
                "location": row[3],
                "is_active": row[4],
                "created_at": row[5].isoformat() if row[5] else datetime.datetime.now().isoformat(),
                "updated_at": row[6].isoformat() if row[6] else datetime.datetime.now().isoformat()
            }
            devices.append(device)
        
        # 导入到本地存储
        count = local_storage.bulk_import_devices(devices)
        print(f"成功迁移 {count} 个设备信息")
    
    except Exception as e:
        print(f"迁移设备信息失败: {e}")

def migrate_water_data(conn):
    """迁移水表数据"""
    try:
        # 检查是否已存在数据文件
        if os.path.exists(local_storage.DATA_FILE):
            backup = f"{local_storage.DATA_FILE}.bak"
            os.rename(local_storage.DATA_FILE, backup)
            print(f"已备份现有数据文件至 {backup}")
        
        # 从数据库获取数据
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    device_no, imei, battery_voltage, freeze_date_flow, 
                    instantaneous_flow, pressure, reverse_flow, signal_value,
                    start_frequency, temperature, total_flow, valve_status, update_time
                FROM app.water_data
                ORDER BY update_time
            """)
            rows = cur.fetchall()
        
        if not rows:
            print("没有水表数据需要迁移")
            return
        
        # 转换为DataFrame
        df = pd.DataFrame(rows, columns=[
            "表号", "imei号", "电池电压", "冻结流量", 
            "瞬时流量", "压力", "反向流量", "信号值",
            "启动次数", "温度", "累计流量", "阀门状态", "上报时间"
        ])
        
        # 添加计算字段
        df["上报时间"] = pd.to_datetime(df["上报时间"])
        df["日期计算"] = df["上报时间"].dt.strftime("%Y-%m-%d")
        df["时间计算"] = df["上报时间"].dt.strftime("%H:%M:%S")
        
        # 计算数据L/s (瞬时流量 m³/h -> L/s)
        df["瞬时流量"] = pd.to_numeric(df["瞬时流量"], errors="coerce")
        df["数据L/s"] = df["瞬时流量"] / 3.6
        
        # 将列转为字符串
        for col in df.columns:
            if col not in ["上报时间"]:
                df[col] = df[col].astype(str)
        
        # 保存到文件
        df.to_csv(local_storage.DATA_FILE, index=False, encoding='utf-8')
        print(f"成功迁移 {len(df)} 条水表数据")
    
    except Exception as e:
        print(f"迁移水表数据失败: {e}")

def migrate_users(conn):
    """迁移用户账户"""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    username, password_hash, role, created_at
                FROM app.users
            """)
            rows = cur.fetchall()
        
        if not rows:
            print("没有用户数据需要迁移")
            return
        
        # 读取现有用户文件
        users = []
        if os.path.exists(local_storage.USERS_FILE):
            with open(local_storage.USERS_FILE, 'r', encoding='utf-8') as f:
                users = json.load(f)
        
        # 用户名索引
        username_map = {u.get("username"): i for i, u in enumerate(users)}
        
        # 处理每个用户
        count = 0
        for row in rows:
            username = row[0]
            password_hash = row[1]
            role = row[2]
            created_at = row[3].isoformat() if row[3] else datetime.datetime.now().isoformat()
            
            if username in username_map:
                # 更新现有用户
                idx = username_map[username]
                users[idx]["password_hash"] = password_hash
                users[idx]["role"] = role
            else:
                # 添加新用户
                users.append({
                    "username": username,
                    "password_hash": password_hash,
                    "role": role,
                    "created_at": created_at
                })
                count += 1
        
        # 保存到文件
        with open(local_storage.USERS_FILE, 'w', encoding='utf-8') as f:
            json.dump(users, f, ensure_ascii=False, indent=2)
        
        print(f"成功迁移 {count} 个用户账户")
    
    except Exception as e:
        print(f"迁移用户账户失败: {e}")

if __name__ == "__main__":
    migrate_data() 