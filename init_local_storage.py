#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
本地存储初始化工具

用于初始化本地存储目录结构、创建默认用户和基础配置。
"""

import os
import json
import hashlib
import pandas as pd
import datetime
import shutil
import sys

# 定义常量
DATA_DIR = "data"
DEVICES_FILE = os.path.join(DATA_DIR, "devices.json")
USERS_FILE = os.path.join(DATA_DIR, "users.json")
DATA_FILE = "water_meter_data.csv"
PUSH_FILE = "device_push_data.csv"
AUTH_SECRET = "local_storage_default_secret"  # 默认认证密钥

def create_directory():
    """创建数据目录"""
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
        print(f"✓ 创建数据目录: {DATA_DIR}")
    else:
        print(f"● 数据目录已存在: {DATA_DIR}")

def create_users():
    """创建用户文件和默认管理员账户"""
    if os.path.exists(USERS_FILE):
        print(f"● 用户文件已存在: {USERS_FILE}")
        return
    
    # 提示用户输入管理员密码
    admin_password = input("请设置管理员密码 (默认: admin123): ").strip() or "admin123"
    
    # 创建管理员用户
    admin_hash = hashlib.sha256(admin_password.encode('utf-8')).hexdigest()
    default_users = [
        {
            "username": "admin",
            "password_hash": admin_hash,
            "role": "admin",
            "created_at": datetime.datetime.now().isoformat()
        }
    ]
    
    # 保存用户文件
    with open(USERS_FILE, 'w', encoding='utf-8') as f:
        json.dump(default_users, f, ensure_ascii=False, indent=2)
    
    print(f"✓ 创建默认管理员用户: admin (密码已设置)")

def create_devices_file():
    """创建设备文件"""
    if os.path.exists(DEVICES_FILE):
        print(f"● 设备文件已存在: {DEVICES_FILE}")
        return
    
    # 创建空设备列表
    with open(DEVICES_FILE, 'w', encoding='utf-8') as f:
        json.dump([], f, ensure_ascii=False, indent=2)
    
    print(f"✓ 创建设备文件: {DEVICES_FILE}")

def create_data_file():
    """创建水表数据文件"""
    if os.path.exists(DATA_FILE):
        print(f"● 数据文件已存在: {DATA_FILE}")
    else:
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
        print(f"✓ 创建数据文件: {DATA_FILE}")
    
    if os.path.exists(PUSH_FILE):
        print(f"● 推送数据文件已存在: {PUSH_FILE}")
    else:
        # 创建空的推送数据文件，复制相同的列结构
        if os.path.exists(DATA_FILE):
            shutil.copy2(DATA_FILE, PUSH_FILE)
        else:
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
            }).to_csv(PUSH_FILE, index=False, encoding='utf-8')
        print(f"✓ 创建推送数据文件: {PUSH_FILE}")

def create_env_file():
    """创建环境变量文件"""
    env_file = ".env"
    if os.path.exists(env_file):
        print(f"● 环境变量文件已存在: {env_file}")
        return
    
    # 获取用户输入
    api_port = input("请设置API端口 (默认: 8000): ").strip() or "8000"
    streamlit_port = input("请设置Streamlit端口 (默认: 8501): ").strip() or "8501"
    
    # 创建.env文件
    env_content = f"""# 本地存储版本环境变量
AUTH_SECRET={AUTH_SECRET}
AUTH_TOKEN_TTL=86400
API_PORT={api_port}
STREAMLIT_PORT={streamlit_port}
RATE_LIMIT_PER_MINUTE=240
CORS_ORIGINS=*
"""
    
    try:
        with open(env_file, 'w', encoding='utf-8') as f:
            f.write(env_content)
        print(f"✓ 创建环境变量文件: {env_file}")
    except Exception as e:
        print(f"✗ 创建环境变量文件失败: {str(e)}")
        print("  请手动创建.env文件，内容如下:")
        print("-" * 50)
        print(env_content)
        print("-" * 50)

def create_default_device():
    """创建默认设备"""
    if not os.path.exists(DEVICES_FILE):
        print("✗ 设备文件不存在，无法创建默认设备")
        return
    
    # 读取现有设备
    with open(DEVICES_FILE, 'r', encoding='utf-8') as f:
        devices = json.load(f)
    
    # 检查是否已存在设备
    if devices:
        print(f"● 设备已存在 ({len(devices)} 个)，跳过创建默认设备")
        return
    
    # 添加默认设备
    device_no = input("请输入默认设备号 (默认: 70666000038000): ").strip() or "70666000038000"
    imei = input("请输入默认设备IMEI (默认: 860329065551923): ").strip() or "860329065551923"
    
    default_device = {
        "device_no": device_no,
        "imei": imei,
        "alias": "默认测试设备",
        "location": "默认位置",
        "description": "由初始化工具创建的默认设备",
        "is_active": True,
        "created_at": datetime.datetime.now().isoformat(),
        "updated_at": datetime.datetime.now().isoformat()
    }
    
    devices.append(default_device)
    
    # 保存设备文件
    with open(DEVICES_FILE, 'w', encoding='utf-8') as f:
        json.dump(devices, f, ensure_ascii=False, indent=2)
    
    print(f"✓ 创建默认设备: {device_no}")

def main():
    print("\n===== 本地存储初始化工具 =====\n")
    
    # 检查是否确认初始化
    confirm = input("本工具将初始化本地存储环境，是否继续？(y/n): ").strip().lower()
    if confirm != 'y':
        print("已取消初始化")
        return
    
    print("\n正在初始化本地存储环境...\n")
    
    # 执行初始化步骤
    create_directory()
    create_users()
    create_devices_file()
    create_data_file()
    create_env_file()
    
    # 询问是否创建默认设备
    add_device = input("\n是否创建默认测试设备？(y/n): ").strip().lower()
    if add_device == 'y':
        create_default_device()
    
    print("\n===== 初始化完成 =====")
    print("\n现在您可以通过以下命令启动系统：")
    print("  python run_local.py")
    print("\n访问系统：")
    print("  Web界面: http://localhost:8501")
    print("  API服务: http://localhost:8000")

if __name__ == "__main__":
    main() 