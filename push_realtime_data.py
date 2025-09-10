#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
实时数据推送工具 - 简单版本

这个脚本用于模拟设备实时推送数据，以固定间隔发送数据点。
适用于本地存储和数据库存储两个版本。
"""

import os
import time
import requests
import datetime
import random
import argparse
import signal
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# 配置参数
API_HOST = os.getenv('API_HOST', 'http://localhost:8000')
if not API_HOST.startswith('http://') and not API_HOST.startswith('https://'):
    API_HOST = f"http://{API_HOST}"
API_PORT = os.getenv('API_PORT', '8000')
if not API_HOST.endswith(f":{API_PORT}"):
    API_HOST = f"{API_HOST}:{API_PORT}"

API_ENDPOINT = f"{API_HOST}/api/data"

# 设备配置（可以通过命令行参数指定）
DEFAULT_DEVICE_NO = "70666000038000"
DEFAULT_IMEI = "860329065551923"
DEFAULT_START_FLOW = 120.0  # 初始累计流量
DEFAULT_START_COUNT = 21500  # 初始启动次数

# 模拟的行为模式
BEHAVIORS = [
    {"name": "静默期", "duration": 600, "flow_rate": 0.0, "probability": 0.7},
    {"name": "小量用水", "duration": 120, "flow_rate": 0.01, "probability": 0.15},
    {"name": "正常用水", "duration": 300, "flow_rate": 0.05, "probability": 0.1},
    {"name": "大量用水", "duration": 180, "flow_rate": 0.2, "probability": 0.05}
]

# 控制变量
running = True

def push_data(data_point):
    """推送单条数据到API"""
    try:
        for key in data_point:
            data_point[key] = str(data_point[key])
            
        response = requests.post(API_ENDPOINT, json=data_point)
        if response.status_code == 200:
            print(f"成功推送数据: {data_point['deviceNo']} - {data_point['updateTime']} - 流量: {data_point['instantaneousFlow']} m³/h")
            return True
        else:
            print(f"推送失败 ({response.status_code}): {response.text}")
            return False
    except Exception as e:
        print(f"推送异常: {str(e)}")
        return False

def select_behavior():
    """随机选择一种行为"""
    rand = random.random()
    cumulative = 0
    for behavior in BEHAVIORS:
        cumulative += behavior["probability"]
        if rand < cumulative:
            return behavior
    return BEHAVIORS[0]  # 默认行为

def signal_handler(sig, frame):
    """处理Ctrl+C信号"""
    global running
    print("\n接收到停止信号，正在优雅退出...")
    running = False

def run_simulation(device_no, imei, interval, duration, start_flow, start_count):
    """运行实时数据推送模拟"""
    global running
    
    # 设置初始值
    total_flow = start_flow
    startup_count = start_count
    reverse_flow = 0.0
    battery = 3.6  # 电池电压
    
    # 计算结束时间
    end_time = None
    if duration > 0:
        end_time = datetime.datetime.now() + datetime.timedelta(minutes=duration)
    
    # 跟踪当前行为
    current_behavior = select_behavior()
    behavior_time_left = current_behavior["duration"]
    behavior_start_time = datetime.datetime.now()
    
    print(f"\n开始模拟设备 {device_no} 的实时数据推送...")
    print(f"API端点: {API_ENDPOINT}")
    print(f"推送间隔: {interval} 秒")
    if end_time:
        print(f"运行时长: {duration} 分钟 (将于 {end_time.strftime('%H:%M:%S')} 结束)")
    else:
        print(f"运行时长: 无限制 (直到手动停止)")
    print(f"初始累计流量: {total_flow} m³")
    print(f"初始启动次数: {startup_count}")
    
    print("\n按 Ctrl+C 停止推送\n")
    
    # 注册信号处理器
    signal.signal(signal.SIGINT, signal_handler)
    
    count = 0
    try:
        while running:
            # 检查是否到达结束时间
            if end_time and datetime.datetime.now() >= end_time:
                print("\n已达到指定运行时长，停止推送")
                break
            
            now = datetime.datetime.now()
            
            # 更新行为状态
            behavior_time_left -= interval
            if behavior_time_left <= 0:
                # 行为结束，选择新行为
                current_behavior = select_behavior()
                behavior_time_left = current_behavior["duration"]
                behavior_start_time = now
                print(f"\n切换行为: {current_behavior['name']} (流量: {current_behavior['flow_rate']} m³/h, 持续: {current_behavior['duration']}秒)")
                
                # 如果是用水行为开始，增加启动次数
                if current_behavior["flow_rate"] > 0:
                    startup_count += 1
            
            # 计算当前值
            flow_rate = current_behavior["flow_rate"]  # m³/h
            
            # 添加小波动
            flow_variation = random.uniform(-0.005, 0.005)
            flow_rate = max(0, flow_rate + flow_variation)
            
            # 随机温度和信号值
            temp = random.uniform(15.0, 28.0)  # 温度范围
            signal = random.randint(-95, -60)  # 信号值范围
            
            # 随时间略微减少电池电压
            battery = max(3.0, battery - random.uniform(0.0001, 0.0005))
            
            # 计算累计流量增加
            # 流量增加 = 流量(m³/h) * 时间间隔(h) = 流量 * (间隔秒数/3600)
            flow_increase = flow_rate * (interval / 3600)
            total_flow += flow_increase
            
            # 数据点
            data_point = {
                "batteryVoltage": round(battery, 3),
                "deviceNo": device_no,
                "freezeDateFlow": round(total_flow - 0.0001, 4),
                "imei": imei,
                "instantaneousFlow": round(flow_rate, 4),
                "pressure": "0.0",
                "reverseFlow": reverse_flow,
                "signalValue": signal,
                "startFrequency": startup_count,
                "temprature": round(temp, 2),
                "totalFlow": round(total_flow, 4),
                "valveStatu": "开",
                "updateTime": now.strftime('%Y-%m-%d %H:%M:%S')
            }
            
            # 推送数据
            success = push_data(data_point)
            if success:
                count += 1
            
            # 等待下一个间隔
            time.sleep(interval)
            
    except Exception as e:
        print(f"模拟过程中出错: {str(e)}")
    
    print(f"\n实时数据推送已结束，总共发送 {count} 条数据")
    return count

if __name__ == "__main__":
    # 解析命令行参数
    parser = argparse.ArgumentParser(description="水表实时数据推送模拟工具")
    parser.add_argument("--device-no", default=DEFAULT_DEVICE_NO, help="设备号")
    parser.add_argument("--imei", default=DEFAULT_IMEI, help="IMEI号")
    parser.add_argument("--interval", type=float, default=5.0, help="推送间隔（秒）")
    parser.add_argument("--duration", type=float, default=0, help="运行时长（分钟），0表示无限制")
    parser.add_argument("--start-flow", type=float, default=DEFAULT_START_FLOW, help="初始累计流量（m³）")
    parser.add_argument("--start-count", type=int, default=DEFAULT_START_COUNT, help="初始启动次数")
    args = parser.parse_args()
    
    # 如果没有指定参数，交互式获取
    interval = args.interval
    duration = args.duration
    
    if interval <= 0:
        try:
            interval = float(input(f"请输入推送间隔（秒）[默认 5.0]: ") or "5.0")
        except ValueError:
            interval = 5.0
    
    if duration <= 0:
        try:
            duration_input = input(f"请输入运行时长（分钟，0表示一直运行）[默认 0]: ")
            duration = float(duration_input) if duration_input else 0
        except ValueError:
            duration = 0
    
    # 运行模拟
    run_simulation(
        args.device_no,
        args.imei,
        interval,
        duration,
        args.start_flow,
        args.start_count
    ) 