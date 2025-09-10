#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
数据模拟器 - 用于模拟水表数据推送
功能：
1. 推送历史数据到数据库（近一个月的数据）
2. 模拟实时数据推送
"""

import os
import time
import pandas as pd
import numpy as np
import requests
import datetime
import random
import json
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor

# 加载环境变量
load_dotenv()

# 配置参数
API_HOST = os.getenv('API_HOST', 'localhost')
API_PORT = os.getenv('API_PORT', '8000')

# 确保API_HOST包含正确的协议和端口
if not API_HOST.startswith('http://') and not API_HOST.startswith('https://'):
    API_HOST = f"http://{API_HOST}"

if not API_HOST.endswith(f":{API_PORT}"):
    API_HOST = f"{API_HOST}:{API_PORT}"

API_ENDPOINT = f"{API_HOST}/api/data"

print(f"API服务器地址: {API_HOST}")
print(f"数据推送端点: {API_ENDPOINT}")

# 读取示例数据
def load_sample_data(file_path='1757125983314设备历史数据数据.csv'):
    """读取示例数据文件"""
    try:
        df = pd.read_csv(file_path)
        print(f"成功加载示例数据，共 {len(df)} 条记录")
        return df
    except Exception as e:
        print(f"加载示例数据失败: {str(e)}")
        return None

# 生成历史数据
def generate_historical_data(sample_df, device_no, start_date, end_date):
    """
    根据示例数据生成历史数据
    
    参数:
    - sample_df: 示例数据DataFrame
    - device_no: 设备号
    - start_date: 起始日期 (datetime)
    - end_date: 结束日期 (datetime)
    
    返回:
    - 生成的历史数据列表
    """
    historical_data = []
    
    # 获取示例数据中的所有时间点（仅时分秒）
    sample_times = pd.to_datetime(sample_df['上报时间']).dt.time
    
    # 获取流量变化模式和其他参数
    flow_patterns = []
    for i in range(len(sample_df) - 1):
        flow_diff = sample_df['累计流量'].iloc[i+1] - sample_df['累计流量'].iloc[i]
        flow_patterns.append(flow_diff)
    
    # 生成每日数据
    current_date = start_date
    base_flow = random.uniform(100, 120)  # 初始累计流量
    startup_count = 21000  # 初始启动次数
    reverse_flow = 0.45 + random.uniform(0, 0.1)  # 反向流量
    
    while current_date <= end_date:
        # 随机选择当天的数据点数量 (30-70)
        num_points = random.randint(30, 70)
        
        # 从示例时间中随机选择num_points个时间点并排序
        day_times = random.sample(list(sample_times), min(num_points, len(sample_times)))
        day_times.sort()
        
        # 为当天生成数据
        for i, time_point in enumerate(day_times):
            timestamp = datetime.datetime.combine(current_date.date(), time_point)
            
            # 生成瞬时流量 (大多数时间为0，偶尔有用水行为)
            if random.random() < 0.3:  # 30%概率有水流
                flow_rate = random.uniform(0.001, 0.02)
                # 偶尔有较大用水行为
                if random.random() < 0.1:
                    flow_rate = random.uniform(0.02, 0.2)
            else:
                flow_rate = 0.0
            
            # 更新累计流量
            if i > 0:
                # 随机选择一个流量变化模式
                if flow_rate > 0:
                    base_flow += random.uniform(0.001, 0.02)
                    # 启动次数增加
                    if random.random() < 0.3:
                        startup_count += 1
            
            # 温度根据时间变化 (早晚低，中午高)
            hour = time_point.hour
            base_temp = 22
            if 6 <= hour < 10:
                temp = base_temp + random.uniform(-1, 2)
            elif 10 <= hour < 16:
                temp = base_temp + random.uniform(2, 5)
            elif 16 <= hour < 20:
                temp = base_temp + random.uniform(0, 3)
            else:
                temp = base_temp + random.uniform(-2, 1)
            
            # 电池电压 (3.6V左右随机波动)
            battery = 3.6 + random.uniform(-0.05, 0.05)
            
            # 信号值 (-85到-95之间波动)
            signal = random.randint(-95, -85)
            
            # 添加到历史数据列表
            data_point = {
                "表号": device_no,
                "IMEI号": "860329065551923",
                "累计流量": round(base_flow, 4),
                "瞬时流量": round(flow_rate, 4),
                "反向流量": round(reverse_flow, 2),
                "启动次数": startup_count,
                "温度": round(temp, 2),
                "压力": 0.0,
                "电池电压": round(battery, 3),
                "信号值": signal,
                "阀门状态": "开",
                "上报时间": timestamp.strftime("%Y-%m-%d %H:%M:%S")
            }
            historical_data.append(data_point)
        
        current_date += datetime.timedelta(days=1)
    
    # 按时间排序
    historical_data.sort(key=lambda x: x["上报时间"])
    return historical_data

# 推送数据到API
def push_data(data_point):
    """推送单条数据到API"""
    try:
        # 确保所有字段都是字符串类型
        for key in data_point:
            data_point[key] = str(data_point[key])
            
        response = requests.post(API_ENDPOINT, json=data_point)
        if response.status_code == 200:
            print(f"成功推送数据 [{data_point['deviceNo']}] {data_point['updateTime']} - 累计流量: {data_point['totalFlow']}")
            return True
        else:
            print(f"推送失败: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        print(f"推送异常: {str(e)}")
        return False

# 推送历史数据
def push_historical_data(data_list, concurrency=5):
    """推送历史数据到API，使用并发推送提高效率"""
    total = len(data_list)
    success = 0
    
    print(f"开始推送历史数据，共 {total} 条...")
    
    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        results = list(executor.map(push_data, data_list))
    
    success = results.count(True)
    print(f"历史数据推送完成: 成功 {success}/{total} 条")
    return success

# 模拟实时数据推送
def simulate_realtime_data(device_no, interval=60, duration=None):
    """
    模拟实时数据推送
    
    参数:
    - device_no: 设备号
    - interval: 推送间隔(秒)
    - duration: 持续时间(秒)，None表示持续运行
    """
    print(f"开始模拟实时数据推送，设备号: {device_no}, 间隔: {interval}秒")
    
    # 初始值设置
    base_flow = 120.0  # 初始累计流量
    startup_count = 21500  # 初始启动次数
    reverse_flow = 0.52  # 反向流量
    
    start_time = time.time()
    count = 0
    
    try:
        while True:
            current_time = datetime.datetime.now()
            
            # 生成瞬时流量 (大多数时间为0，偶尔有用水行为)
            if random.random() < 0.3:  # 30%概率有水流
                flow_rate = random.uniform(0.001, 0.02)
                # 偶尔有较大用水行为
                if random.random() < 0.1:
                    flow_rate = random.uniform(0.02, 0.2)
                base_flow += random.uniform(0.001, 0.01)
                # 启动次数增加
                if random.random() < 0.3:
                    startup_count += 1
            else:
                flow_rate = 0.0
            
            # 温度根据时间变化 (早晚低，中午高)
            hour = current_time.hour
            base_temp = 22
            if 6 <= hour < 10:
                temp = base_temp + random.uniform(-1, 2)
            elif 10 <= hour < 16:
                temp = base_temp + random.uniform(2, 5)
            elif 16 <= hour < 20:
                temp = base_temp + random.uniform(0, 3)
            else:
                temp = base_temp + random.uniform(-2, 1)
            
            # 电池电压 (3.6V左右随机波动)
            battery = 3.6 + random.uniform(-0.05, 0.05)
            
            # 信号值 (-85到-95之间波动)
            signal = random.randint(-95, -85)
            
            # 数据点
            data_point = {
                "表号": device_no,
                "IMEI号": "860329065551923",
                "累计流量": round(base_flow, 4),
                "瞬时流量": round(flow_rate, 4),
                "反向流量": round(reverse_flow, 2),
                "启动次数": startup_count,
                "温度": round(temp, 2),
                "压力": 0.0,
                "电池电压": round(battery, 3),
                "信号值": signal,
                "阀门状态": "开",
                "上报时间": current_time.strftime("%Y-%m-%d %H:%M:%S")
            }
            
            # 推送数据
            success = push_data(data_point)
            if success:
                count += 1
            
            # 检查是否达到持续时间
            if duration is not None and (time.time() - start_time) >= duration:
                break
            
            # 等待下一次推送
            time.sleep(interval)
    
    except KeyboardInterrupt:
        print("\n用户中断，停止模拟推送")
    
    print(f"模拟结束，成功推送 {count} 条实时数据")

# 从API获取设备信息
def get_device_info(device_no):
    """从API获取设备信息，如果不存在则自动创建"""
    try:
        # 检查设备是否存在
        response = requests.get(f"{API_HOST}/api/devices/{device_no}")
        
        if response.status_code == 200:
            print(f"设备 {device_no} 已存在")
            return True
        elif response.status_code == 404:
            # 创建设备
            device_data = {
                "deviceNo": device_no,
                "name": f"水表设备 {device_no}",
                "location": "模拟位置",
                "description": "数据模拟器创建的设备",
                "status": "active"
            }
            create_response = requests.post(f"{API_HOST}/api/devices", json=device_data)
            
            if create_response.status_code in [200, 201]:
                print(f"成功创建设备 {device_no}")
                return True
            else:
                print(f"创建设备失败: {create_response.status_code} - {create_response.text}")
                return False
        else:
            print(f"获取设备信息失败: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        print(f"获取设备信息异常: {str(e)}")
        return False

# 主函数
def main():
    print("=" * 60)
    print("数据模拟器 - 用于模拟水表数据推送")
    print("=" * 60)
    
    # 加载示例数据
    sample_df = load_sample_data()
    if sample_df is None:
        print("无法加载示例数据，退出程序")
        return
    
    # 设备号
    device_no = sample_df['表号'].iloc[0]
    print(f"使用设备号: {device_no}")
    
    # 确保设备存在
    if not get_device_info(device_no):
        print("设备初始化失败，退出程序")
        return
    
    # 菜单
    while True:
        print("\n请选择操作:")
        print("1. 推送历史数据 (过去一个月)")
        print("2. 模拟实时数据推送")
        print("3. 推送历史数据和模拟实时数据")
        print("0. 退出")
        
        choice = input("请输入选项: ")
        
        if choice == "1":
            # 推送历史数据
            end_date = datetime.datetime.now()  # 今天
            start_date = end_date - datetime.timedelta(days=30)  # 30天前
            
            print(f"生成从 {start_date.date()} 到 {end_date.date()} 的历史数据...")
            historical_data = generate_historical_data(sample_df, device_no, start_date, end_date)
            
            print(f"生成了 {len(historical_data)} 条历史数据")
            confirm = input(f"确认推送这些数据到系统? (y/n): ")
            if confirm.lower() == "y":
                push_historical_data(historical_data)
        
        elif choice == "2":
            # 模拟实时数据推送
            interval = int(input("请输入推送间隔 (秒，默认60): ") or 60)
            simulate_realtime_data(device_no, interval=interval)
        
        elif choice == "3":
            # 先推送历史数据，再模拟实时数据
            end_date = datetime.datetime.now()  # 今天
            start_date = end_date - datetime.timedelta(days=30)  # 30天前
            
            print(f"生成从 {start_date.date()} 到 {end_date.date()} 的历史数据...")
            historical_data = generate_historical_data(sample_df, device_no, start_date, end_date)
            
            print(f"生成了 {len(historical_data)} 条历史数据")
            confirm = input(f"确认推送这些数据到系统? (y/n): ")
            if confirm.lower() == "y":
                push_historical_data(historical_data)
                
                # 继续模拟实时数据
                interval = int(input("请输入实时数据推送间隔 (秒，默认60): ") or 60)
                simulate_realtime_data(device_no, interval=interval)
        
        elif choice == "0":
            print("退出程序")
                break
                
            else:
            print("无效选项，请重新输入")

if __name__ == "__main__":
    main() 