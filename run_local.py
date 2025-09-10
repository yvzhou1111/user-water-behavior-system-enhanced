#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
水表数据系统启动器 - 本地存储版本

同时启动API服务器和Streamlit前端，简化系统运行
"""

import os
import sys
import time
import socket
import threading
import subprocess
import argparse
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

def print_banner():
    banner = f"""
{Fore.CYAN}┌─────────────────────────────────────────────────────┐
{Fore.CYAN}│{Fore.GREEN}                用户用水行为识别系统               {Fore.CYAN}│
{Fore.CYAN}│{Fore.WHITE}                   本地存储版本                   {Fore.CYAN}│
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
    env["PYTHONUNBUFFERED"] = "1"  # 禁用输出缓冲，实时显示日志
    
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
    threading.Thread(target=monitor_output, args=(process, "API", Fore.GREEN), daemon=True).start()
    
    # 等待API服务器启动
    time.sleep(1)
    return process

def start_streamlit(streamlit_port):
    """启动Streamlit前端"""
    print(f"{Fore.CYAN}启动Streamlit前端...")
    cmd = [sys.executable, "-m", "streamlit", "run", "app.py", "--server.port", str(streamlit_port)]
    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"  # 禁用输出缓冲，实时显示日志
    
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

def main():
    # 注册信号处理器
    signal.signal(signal.SIGINT, signal_handler)  # Ctrl+C
    signal.signal(signal.SIGTERM, signal_handler)  # Kill
    
    # 解析命令行参数
    parser = argparse.ArgumentParser(description="启动用户用水行为识别系统（本地存储版本）")
    parser.add_argument("--api-port", type=int, default=int(os.getenv("API_PORT", "8000")), help="API服务器端口")
    parser.add_argument("--streamlit-port", type=int, default=int(os.getenv("STREAMLIT_PORT", "8501")), help="Streamlit前端端口")
    parser.add_argument("--api-only", action="store_true", help="仅启动API服务器")
    parser.add_argument("--streamlit-only", action="store_true", help="仅启动Streamlit前端")
    args = parser.parse_args()
    
    # 打印欢迎信息
    print_banner()
    
    # 获取网络信息
    ips = get_ips()
    api_port = args.api_port
    streamlit_port = args.streamlit_port
    
    # 打印访问信息
    print(f"{Fore.YELLOW}[网络] 本机IP地址:")
    for ip in ips:
        print(f"{Fore.YELLOW}  - {ip}")
    
    # 启动服务
    try:
        api_process = None
        streamlit_process = None
        
        if not args.streamlit_only:
            api_process = start_api_server(api_port)
            print(f"{Fore.GREEN}[API] API服务器已启动: http://{ips[0]}:{api_port}")
            print(f"{Fore.GREEN}[API] 数据推送地址: http://{ips[0]}:{api_port}/api/data")
        
        if not args.api_only:
            streamlit_process = start_streamlit(streamlit_port)
            print(f"{Fore.BLUE}[Streamlit] 前端已启动: http://{ips[0]}:{streamlit_port}")
        
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