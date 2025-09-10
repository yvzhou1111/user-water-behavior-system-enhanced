#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
用户用水行为识别系统启动器 - 全功能版本

功能：
1. 同时启动API服务器和Streamlit前端
2. 支持自动推送历史数据和实时数据
3. 使用本地存储作为数据库
4. 提供丰富的命令行选项
"""

import os
import sys
import time
import socket
import threading
import subprocess
import argparse
import random
from datetime import datetime, timedelta
import requests
import json
import pandas as pd
from colorama import Fore, Style, init
import signal
from dotenv import load_dotenv

# 初始化colorama，用于在控制台输出彩色文本
init(autoreset=True)

# 加载环境变量
load_dotenv()

# 全局变量
processes = []
stop_event = threading.Event()
DEFAULT_DEVICE_NO = "70666000038000"
DEFAULT_IMEI = "860329065551923"

def print_banner():
    banner = f"""
{Fore.CYAN}┌─────────────────────────────────────────────────────┐
{Fore.CYAN}│{Fore.GREEN}                用户用水行为识别系统               {Fore.CYAN}│
{Fore.CYAN}│{Fore.WHITE}                   全功能版本                   {Fore.CYAN}│
{Fore.CYAN}└─────────────────────────────────────────────────────┘{Style.RESET_ALL}
    """
    print(banner)

def get_ips():
    """获取所有本地IP地址"""
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
        except Exception:
            pass
        finally:
            s.close()
        return list(ips)
    except Exception:
        return ['127.0.0.1']

def start_api_server(api_port):
    """启动API服务器"""
    print(f"{Fore.CYAN}启动API服务器...")
    cmd = [sys.executable, "api_server_local.py"]
    env = os.environ.copy()
    env["API_PORT"] = str(api_port)
    env["API_HOST"] = "0.0.0.0"  # 强制本地全部网卡
    env["PYTHONUNBUFFERED"] = "1"
    
    process = subprocess.Popen(
        cmd,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        universal_newlines=True
    )
    processes.append(process)
    
    threading.Thread(target=monitor_output, args=(process, "API", Fore.GREEN), daemon=True).start()
    time.sleep(2)
    return process

def start_streamlit(streamlit_port, api_port):
    """启动Streamlit前端"""
    print(f"{Fore.CYAN}启动Streamlit前端...")
    cmd = [sys.executable, "-m", "streamlit", "run", "app.py", "--server.port", str(streamlit_port)]
    env = os.environ.copy()
    env["API_PORT"] = str(api_port)  # 确保Streamlit应用能够连接到API服务器
    env["PYTHONUNBUFFERED"] = "1"    # 禁用输出缓冲，实时显示日志
    
    process = subprocess.Popen(
        cmd,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,  # 行缓冲
        universal_newlines=True
    )
    processes.append(process)
    
    # 启动日志监控线程
    threading.Thread(target=monitor_output, args=(process, "Streamlit", Fore.BLUE), daemon=True).start()
    return process

def monitor_output(process, prefix, color):
    """监控并输出进程日志"""
    try:
        while process.poll() is None and not stop_event.is_set():
            line = process.stdout.readline()
            if line:
                formatted_line = f"{color}[{prefix}] {line.rstrip()}"
                print(formatted_line)
            else:
                time.sleep(0.1)
    except Exception as e:
        print(f"{Fore.RED}[{prefix}] 日志监控错误: {str(e)}")
    finally:
        if process.poll() is None:
            try:
                process.terminate()
                process.wait(timeout=5)
            except Exception:
                if process.poll() is None:
                    try:
                        process.kill()
                    except Exception:
                        pass

def signal_handler(signum, frame):
    """处理退出信号"""
    print(f"\n{Fore.YELLOW}接收到退出信号，关闭所有进程...")
    stop_event.set()
    
    for process in processes:
        try:
            if process.poll() is None:
                process.terminate()
                process.wait(timeout=5)
                if process.poll() is None:
                    process.kill()
        except Exception as e:
            print(f"{Fore.RED}关闭进程时出错: {str(e)}")
    
    print(f"{Fore.GREEN}已退出系统。")
    sys.exit(0)

def push_history_data(api_base, days=30, end_date=None, device_no=None, imei=None, delay=0.1):
    """推送历史数据"""
    print(f"{Fore.YELLOW}[数据] 开始推送历史数据...")
    
    # 确定设备信息
    device_no = device_no or DEFAULT_DEVICE_NO
    imei = imei or DEFAULT_IMEI
    
    # 检查设备是否存在，不存在则创建
    try:
        check_resp = requests.get(f"{api_base}/api/devices/{device_no}")
        if check_resp.status_code != 200:
            # 创建设备
            print(f"{Fore.YELLOW}[数据] 设备 {device_no} 不存在，自动创建...")
            create_resp = requests.post(f"{api_base}/api/devices", json={
                "deviceNo": device_no,
                "imei": imei,
                "alias": "自动创建的测试设备",
                "location": "系统自动生成",
                "is_active": True
            })
            if create_resp.status_code != 200:
                print(f"{Fore.RED}[数据] 创建设备失败: {create_resp.text}")
                return False
    except Exception as e:
        print(f"{Fore.RED}[数据] 检查设备失败: {str(e)}")
        return False
    
    # 历史数据日期范围
    if end_date is None:
        end_date = datetime.now().date()
    elif isinstance(end_date, str):
        end_date = datetime.strptime(end_date, "%Y-%m-%d").date()
    
    start_date = end_date - timedelta(days=days)
    
    # 读取示例CSV文件，如果存在
    sample_data = None
    if os.path.exists("1757125983314设备历史数据数据.csv"):
        try:
            sample_data = pd.read_csv("1757125983314设备历史数据数据.csv")
        except Exception as e:
            print(f"{Fore.RED}[数据] 读取示例数据文件失败: {str(e)}")
    
    # 推送历史数据
    current_date = start_date
    total_points = 0
    
    while current_date <= end_date:
        date_str = current_date.strftime("%Y-%m-%d")
        print(f"{Fore.YELLOW}[数据] 推送 {date_str} 的数据...")
        
        # 生成该日的数据点
        data_points = []
        
        if sample_data is not None:
            # 基于示例数据生成
            for hour in range(24):
                # 每小时取1-3个数据点
                points_per_hour = random.randint(1, 3)
                for _ in range(points_per_hour):
                    minute = random.randint(0, 59)
                    second = random.randint(0, 59)
                    timestamp = f"{date_str} {hour:02d}:{minute:02d}:{second:02d}"
                    
                    # 从示例数据中随机选择一行作为模板
                    if not sample_data.empty:
                        template = sample_data.sample(1).iloc[0].to_dict()
                        
                        # 调整电池电压 (3.0-3.6V)
                        battery = round(random.uniform(3.0, 3.6), 2)
                        # 累计流量逐渐增加
                        total_flow = round(float(template.get('累计流量', 0)) + random.uniform(0.1, 0.5), 3)
                        # 瞬时流量 (0-1.2 m³/h)
                        flow = round(random.uniform(0, 1.2), 3) if random.random() > 0.5 else 0
                        # 温度 (18-25°C)
                        temp = round(random.uniform(18, 25), 1)
                        
                        data_point = {
                            "deviceNo": str(device_no),
                            "batteryVoltage": str(battery),
                            "freezeDateFlow": str(round(total_flow * 0.98, 3)),
                            "imei": str(imei),
                            "instantaneousFlow": str(flow),
                            "pressure": str(round(random.uniform(0.2, 0.5), 2)),
                            "reverseFlow": "0",
                            "signalValue": str(random.randint(15, 30)),
                            "startFrequency": str(random.randint(10, 50)),
                            "temprature": str(temp),
                            "totalFlow": str(total_flow),
                            "valveStatu": "开",
                            "updateTime": timestamp
                        }
                        
                        data_points.append(data_point)
            
            # 对数据点按时间排序
            data_points.sort(key=lambda x: x["updateTime"])
            
            # 推送数据点
            for data in data_points:
                try:
                    resp = requests.post(f"{api_base}/api/data", json=data)
                    if resp.status_code != 200:
                        print(f"{Fore.RED}[数据] 推送失败: {resp.text}")
                    else:
                        total_points += 1
                    
                    # 延迟，避免请求过快
                    if delay > 0:
                        time.sleep(delay)
                        
                except Exception as e:
                    print(f"{Fore.RED}[数据] 推送出错: {str(e)}")
                    
        # 下一天
        current_date += timedelta(days=1)
    
    print(f"{Fore.GREEN}[数据] 历史数据推送完成，共推送 {total_points} 个数据点")
    return True

def push_realtime_data(api_base, device_no=None, imei=None, duration=None):
    """推送实时数据"""
    device_no = device_no or DEFAULT_DEVICE_NO
    imei = imei or DEFAULT_IMEI
    
    if duration:
        print(f"{Fore.YELLOW}[实时] 开始推送实时数据，将运行 {duration} 秒...")
    else:
        print(f"{Fore.YELLOW}[实时] 开始推送实时数据，将持续运行直到程序退出...")
    
    # 检查/创建设备
    try:
        check_resp = requests.get(f"{api_base}/api/devices/{device_no}")
        if check_resp.status_code != 200:
            # 创建设备
            print(f"{Fore.YELLOW}[实时] 设备 {device_no} 不存在，自动创建...")
            create_resp = requests.post(f"{api_base}/api/devices", json={
                "deviceNo": device_no,
                "imei": imei,
                "alias": "自动创建的实时测试设备",
                "location": "系统自动生成",
                "is_active": True
            })
            if create_resp.status_code != 200:
                print(f"{Fore.RED}[实时] 创建设备失败: {create_resp.text}")
                return
    except Exception as e:
        print(f"{Fore.RED}[实时] 检查设备失败: {str(e)}")
        return
    
    # 实时数据变量
    total_flow = 100.0  # 初始累计流量
    battery = 3.5      # 初始电池电压
    pressure = 0.3     # 初始压力
    point_count = 0
    start_time = time.time()
    
    # 推送线程
    def push_thread():
        nonlocal total_flow, battery, pressure, point_count
        
        try:
            while not stop_event.is_set():
                now = datetime.now()
                timestamp = now.strftime("%Y-%m-%d %H:%M:%S")
                
                # 模拟用水行为 (20%概率出现用水)
                if random.random() < 0.2:
                    flow = round(random.uniform(0.2, 1.5), 3)
                else:
                    flow = 0
                
                # 更新累计流量
                if flow > 0:
                    total_flow += flow / 60  # 按分钟流量增加
                
                # 随机波动电池电压和压力
                battery = max(3.0, min(3.6, battery + random.uniform(-0.05, 0.05)))
                pressure = max(0.1, min(0.6, pressure + random.uniform(-0.05, 0.05)))
                
                # 构建数据
                data = {
                    "deviceNo": str(device_no),
                    "batteryVoltage": str(round(battery, 2)),
                    "freezeDateFlow": str(round(total_flow * 0.98, 3)),
                    "imei": str(imei),
                    "instantaneousFlow": str(flow),
                    "pressure": str(round(pressure, 2)),
                    "reverseFlow": "0",
                    "signalValue": str(random.randint(15, 30)),
                    "startFrequency": str(random.randint(10, 50)),
                    "temprature": str(round(random.uniform(18, 25), 1)),
                    "totalFlow": str(round(total_flow, 3)),
                    "valveStatu": "开",
                    "updateTime": timestamp
                }
                
                # 推送数据
                try:
                    resp = requests.post(f"{api_base}/api/data", json=data)
                    if resp.status_code == 200:
                        point_count += 1
                        print(f"{Fore.GREEN}[实时] 推送数据: {timestamp} 流量: {flow}m³/h 累计: {round(total_flow, 3)}m³")
                    else:
                        print(f"{Fore.RED}[实时] 推送失败: {resp.text}")
                except Exception as e:
                    print(f"{Fore.RED}[实时] 推送出错: {str(e)}")
                
                # 检查持续时间
                if duration and time.time() - start_time >= duration:
                    print(f"{Fore.GREEN}[实时] 已完成 {duration} 秒的数据推送，结束推送")
                    break
                    
                # 等待下一次推送 (30-90秒)
                wait_time = random.randint(30, 90)
                for _ in range(wait_time):
                    if stop_event.is_set() or (duration and time.time() - start_time >= duration):
                        break
                    time.sleep(1)
                    
        except Exception as e:
            print(f"{Fore.RED}[实时] 推送线程异常: {str(e)}")
        finally:
            print(f"{Fore.GREEN}[实时] 推送结束，共推送 {point_count} 个数据点")
    
    # 启动推送线程
    realtime_thread = threading.Thread(target=push_thread, daemon=True)
    realtime_thread.start()
    return realtime_thread

def main():
    # 注册信号处理器
    signal.signal(signal.SIGINT, signal_handler)  # Ctrl+C
    signal.signal(signal.SIGTERM, signal_handler)  # Kill
    
    # 解析命令行参数
    parser = argparse.ArgumentParser(description="用户用水行为识别系统 - 全功能启动器")
    parser.add_argument("--api-port", type=int, default=int(os.getenv("API_PORT", "8000")), help="API服务器端口")
    parser.add_argument("--streamlit-port", type=int, default=int(os.getenv("STREAMLIT_PORT", "8501")), help="Streamlit前端端口")
    parser.add_argument("--api-only", action="store_true", help="仅启动API服务器")
    parser.add_argument("--streamlit-only", action="store_true", help="仅启动Streamlit前端")
    parser.add_argument("--no-history", action="store_true", help="不推送历史数据")
    parser.add_argument("--no-realtime", action="store_true", help="不推送实时数据")
    parser.add_argument("--device", type=str, default=DEFAULT_DEVICE_NO, help="设备表号")
    parser.add_argument("--imei", type=str, default=DEFAULT_IMEI, help="设备IMEI")
    parser.add_argument("--history-days", type=int, default=30, help="历史数据天数")
    parser.add_argument("--realtime-duration", type=int, default=None, help="实时数据推送持续时间（秒）")
    
    args = parser.parse_args()
    
    # 打印欢迎信息
    print_banner()
    
    # 获取网络信息
    ips = get_ips()
    api_port = args.api_port
    streamlit_port = args.streamlit_port
    api_base = f"http://localhost:{api_port}"
    
    # 打印访问信息
    print(f"{Fore.YELLOW}[网络] 本机IP地址:")
    for ip in ips:
        print(f"{Fore.YELLOW}  - {ip}")
    
    # 启动服务
    try:
        api_process = None
        streamlit_process = None
        
        # 启动API服务器
        if not args.streamlit_only:
            api_process = start_api_server(api_port)
            print(f"{Fore.GREEN}[API] API服务器已启动: http://{ips[0]}:{api_port}")
            print(f"{Fore.GREEN}[API] 数据推送地址: http://{ips[0]}:{api_port}/api/data")
            # 等待API服务器完全启动
            time.sleep(2)
        
        # 启动Streamlit前端
        if not args.api_only:
            streamlit_process = start_streamlit(streamlit_port, api_port)
            print(f"{Fore.BLUE}[Streamlit] 前端已启动: http://{ips[0]}:{streamlit_port}")
        
        # 推送历史数据
        if not args.no_history and not args.streamlit_only:
            push_history_data(
                api_base, 
                days=args.history_days, 
                device_no=args.device, 
                imei=args.imei
            )
        
        # 推送实时数据
        if not args.no_realtime and not args.streamlit_only:
            push_realtime_data(
                api_base,
                device_no=args.device,
                imei=args.imei,
                duration=args.realtime_duration
            )
        
        print(f"\n{Fore.CYAN}系统已启动，按 Ctrl+C 停止服务...\n")
        
        # 等待进程结束或中断
        while not stop_event.is_set():
            if api_process and api_process.poll() is not None:
                print(f"{Fore.RED}[API] API服务器已退出: {api_process.returncode}")
                stop_event.set()
                break
            if streamlit_process and streamlit_process.poll() is not None:
                print(f"{Fore.RED}[Streamlit] Streamlit前端已退出: {streamlit_process.returncode}")
                stop_event.set()
                break
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        signal_handler(None, None)

if __name__ == "__main__":
    main() 