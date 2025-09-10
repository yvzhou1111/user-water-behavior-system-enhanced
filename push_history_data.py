#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
历史数据推送工具 - 简单版本

这个脚本用于从CSV文件中读取历史数据，并推送到API端点。
适用于本地存储和数据库存储两个版本。
"""

import os
import pandas as pd
import requests
import datetime
import time
from dotenv import load_dotenv
import random
import argparse

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
DATA_FILE = '1757125983314设备历史数据数据.csv'

# 当前日期（默认为今天，可以通过参数指定）
DEFAULT_END_DATE = datetime.datetime.now()

def read_data_file(file_path=None):
    """读取历史数据文件"""
    if file_path is None:
        file_path = DATA_FILE
    
    try:
        df = pd.read_csv(file_path)
        print(f"成功加载数据文件，共 {len(df)} 条记录")
        return df
    except Exception as e:
        print(f"加载数据文件失败: {str(e)}")
        return None

def generate_date_range(end_date, days=30):
    """生成过去days天的日期范围"""
    dates = []
    for i in range(days):
        date = end_date - datetime.timedelta(days=i)
        dates.append(date.date())
    return dates

def prepare_data_for_date(df, target_date):
    """为指定日期准备数据"""
    # 使用原始数据作为模板
    template_records = df.to_dict('records')
    modified_records = []
    
    # 设置基准累计流量，每天略有增加
    days_diff = (datetime.datetime.now().date() - target_date).days
    base_flow = 100.0 + (30 - days_diff) * 0.5
    
    for record in template_records:
        # 解析原始时间
        try:
            orig_time = datetime.datetime.strptime(record['上报时间'], '%Y-%m-%d %H:%M:%S')
            # 创建新时间戳，保留时分秒
            new_time = datetime.datetime.combine(target_date, orig_time.time())
            
            # 调整累计流量（保持递增）
            flow_adjustment = random.uniform(-0.01, 0.01)  # 小范围随机调整
            new_flow = base_flow + flow_adjustment
            base_flow += abs(flow_adjustment) * 2  # 确保累计流量整体递增
            
            # 创建API格式的数据点
            data_point = {
                "batteryVoltage": str(record['电池电压']),
                "deviceNo": str(record['表号']),
                "freezeDateFlow": str(record['冻结流量']),
                "imei": str(record['imei号']),
                "instantaneousFlow": str(record['瞬时流量']),
                "pressure": str(record['压力']),
                "reverseFlow": str(record['反向流量']),
                "signalValue": str(record['信号值']),
                "startFrequency": str(record['启动次数']),
                "temprature": str(record['温度']),
                "totalFlow": str(round(new_flow, 4)),
                "valveStatu": str(record['阀门状态']),
                "updateTime": new_time.strftime('%Y-%m-%d %H:%M:%S')
            }
            modified_records.append(data_point)
        except Exception as e:
            print(f"处理记录时出错: {str(e)}")
    
    return modified_records

def push_data_to_api(data_point):
    """推送单条数据到API"""
    try:
        response = requests.post(API_ENDPOINT, json=data_point)
        if response.status_code == 200:
            return True
        else:
            print(f"推送失败 ({response.status_code}): {response.text}")
            return False
    except Exception as e:
        print(f"推送异常: {str(e)}")
        return False

def push_daily_data(df, target_date, delay=0):
    """推送指定日期的数据"""
    data_points = prepare_data_for_date(df, target_date)
    if not data_points:
        print(f"无数据可推送：{target_date}")
        return 0
    
    success_count = 0
    print(f"开始推送 {target_date} 的数据，共 {len(data_points)} 条...")
    
    for i, data_point in enumerate(data_points):
        if push_data_to_api(data_point):
            success_count += 1
            if (i + 1) % 10 == 0:
                print(f"已成功推送 {i+1}/{len(data_points)} 条数据")
        
        if delay > 0:
            time.sleep(delay)
    
    print(f"完成 {target_date} 数据推送，成功 {success_count}/{len(data_points)} 条")
    return success_count

def push_historical_data(days=30, end_date=None, delay=0):
    """推送过去days天的历史数据"""
    if end_date is None:
        end_date = DEFAULT_END_DATE
    
    # 加载数据文件
    df = read_data_file()
    if df is None or df.empty:
        print("无法获取数据样本，退出")
        return False
    
    # 生成日期范围
    dates = generate_date_range(end_date, days)
    
    print(f"\n准备推送 {days} 天的历史数据 ({dates[-1]} 至 {dates[0]})")
    print(f"API 端点: {API_ENDPOINT}\n")
    
    total_count = 0
    for date in dates:
        print(f"\n--- 日期: {date} ---")
        count = push_daily_data(df, date, delay)
        total_count += count
        print(f"- 已完成: {dates.index(date) + 1}/{len(dates)} 天")
        
        # 天与天之间暂停一下
        if date != dates[-1]:  # 不是最后一天
            wait_time = 1  # 默认等待1秒
            print(f"等待 {wait_time} 秒...")
            time.sleep(wait_time)
    
    print(f"\n完成所有历史数据推送，总共成功推送 {total_count} 条记录")
    return True

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="历史数据推送工具")
    parser.add_argument("--days", type=int, default=30, help="要推送多少天的历史数据")
    parser.add_argument("--end-date", type=str, help="结束日期 (YYYY-MM-DD)，默认为今天")
    parser.add_argument("--delay", type=float, default=0.1, help="每条数据之间的延迟（秒）")
    parser.add_argument("--file", type=str, help="要使用的历史数据文件路径")
    args = parser.parse_args()

    # 设置结束日期
    if args.end_date:
        try:
            end_date = datetime.datetime.strptime(args.end_date, "%Y-%m-%d")
        except ValueError:
            print(f"无效的日期格式: {args.end_date}，使用今天作为结束日期")
            end_date = DEFAULT_END_DATE
    else:
        end_date = DEFAULT_END_DATE
    
    # 设置数据文件
    if args.file:
        DATA_FILE = args.file
    
    print("="*60)
    print(f"历史数据推送工具 (API: {API_ENDPOINT})")
    print("="*60)
    print(f"推送范围: {args.days} 天的历史数据")
    print(f"结束日期: {end_date.date()}")
    print(f"数据文件: {DATA_FILE}")
    print(f"推送延迟: {args.delay} 秒/条")
    print("="*60)
    
    # 确认继续
    answer = input("请确认以上参数并开始推送 (y/n): ").strip().lower()
    if answer != 'y':
        print("已取消推送")
        exit(0)
    
    # 开始推送
    push_historical_data(args.days, end_date, args.delay) 