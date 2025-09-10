import subprocess
import time
import threading
import os
import sys
import signal
import argparse
import logging
import socket
import json
import requests
from datetime import datetime
import webbrowser
from colorama import init, Fore, Style
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# 初始化colorama
init(autoreset=True)

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("run.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("water-meter-system")

# 全局变量以便在需要时停止进程
api_process = None
streamlit_process = None

# 默认端口配置
DEFAULT_API_PORT = 8000
DEFAULT_STREAMLIT_PORT = 8501

def get_local_ip():
    """获取本机IP地址"""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('8.8.8.8', 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except:
        return '127.0.0.1'

def is_port_in_use(port):
    """检查端口是否被占用"""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(('localhost', port)) == 0

def wait_for_service(url, timeout=30, interval=1.0):
    """等待服务启动"""
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            response = requests.get(url, timeout=2)
            if response.status_code == 200:
                return True
        except requests.exceptions.RequestException:
            time.sleep(interval)
    return False

def run_api_server(port=DEFAULT_API_PORT):
    """运行API服务器"""
    global api_process
    logger.info(f"{Fore.CYAN}启动API服务器 (端口: {port})...{Style.RESET_ALL}")
    
    # 检查端口是否可用
    if is_port_in_use(port):
        logger.warning(f"{Fore.YELLOW}警告: 端口 {port} 已被占用, API服务器可能无法正常启动{Style.RESET_ALL}")
    
    # 使用subprocess.Popen而不是subprocess.run，这样可以实时捕获输出
    api_process = subprocess.Popen(
        [sys.executable, "api_server.py"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
        universal_newlines=True,
        env={**os.environ, "PORT": str(port)}
    )
    
    # 实时输出标准输出和错误
    while api_process.poll() is None:
        output = api_process.stdout.readline()
        if output:
            logger.info(f"{Fore.CYAN}[API] {output.strip()}{Style.RESET_ALL}")
        
        error = api_process.stderr.readline()
        if error:
            logger.error(f"{Fore.RED}[API] {error.strip()}{Style.RESET_ALL}")
    
    # 获取剩余的输出和错误
    for output in api_process.stdout.readlines():
        if output:
            logger.info(f"{Fore.CYAN}[API] {output.strip()}{Style.RESET_ALL}")
    
    for error in api_process.stderr.readlines():
        if error:
            logger.error(f"{Fore.RED}[API] {error.strip()}{Style.RESET_ALL}")
    
    # 检查退出状态
    if api_process.returncode != 0:
        logger.error(f"{Fore.RED}API服务器异常退出，状态码: {api_process.returncode}{Style.RESET_ALL}")

def run_streamlit(port=DEFAULT_STREAMLIT_PORT):
    """运行Streamlit应用"""
    global streamlit_process
    logger.info(f"{Fore.GREEN}启动Streamlit前端 (端口: {port})...{Style.RESET_ALL}")
    
    # 检查端口是否可用
    if is_port_in_use(port):
        logger.warning(f"{Fore.YELLOW}警告: 端口 {port} 已被占用, Streamlit可能无法正常启动或会使用其他端口{Style.RESET_ALL}")
    
    # 使用subprocess.Popen而不是subprocess.run，这样可以实时捕获输出
    streamlit_process = subprocess.Popen(
        [sys.executable, "-m", "streamlit", "run", "app.py", "--server.port", str(port)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
        universal_newlines=True
    )
    
    # 实时输出标准输出和错误
    while streamlit_process.poll() is None:
        output = streamlit_process.stdout.readline()
        if output:
            logger.info(f"{Fore.GREEN}[STREAMLIT] {output.strip()}{Style.RESET_ALL}")
            
            # 检测Streamlit URL并打开浏览器
            if "You can now view your Streamlit app in your browser." in output and args.open_browser:
                try:
                    # 尝试从Streamlit输出中提取URL
                    for line in streamlit_process.stdout.readlines():
                        if "Local URL:" in line:
                            url = line.split("Local URL:")[1].strip()
                            webbrowser.open(url)
                            logger.info(f"{Fore.GREEN}已自动打开浏览器访问: {url}{Style.RESET_ALL}")
                            break
                except:
                    # 如果无法提取，使用默认URL
                    url = f"http://localhost:{port}"
                    webbrowser.open(url)
                    logger.info(f"{Fore.GREEN}已自动打开浏览器访问: {url}{Style.RESET_ALL}")
        
        error = streamlit_process.stderr.readline()
        if error:
            logger.error(f"{Fore.RED}[STREAMLIT] {error.strip()}{Style.RESET_ALL}")
    
    # 获取剩余的输出和错误
    for output in streamlit_process.stdout.readlines():
        if output:
            logger.info(f"{Fore.GREEN}[STREAMLIT] {output.strip()}{Style.RESET_ALL}")
    
    for error in streamlit_process.stderr.readlines():
        if error:
            logger.error(f"{Fore.RED}[STREAMLIT] {error.strip()}{Style.RESET_ALL}")
    
    # 检查退出状态
    if streamlit_process.returncode != 0:
        logger.error(f"{Fore.RED}Streamlit异常退出，状态码: {streamlit_process.returncode}{Style.RESET_ALL}")
        return streamlit_process.returncode
    return 0

def check_api_health(port=DEFAULT_API_PORT):
    """检查API服务器健康状态"""
    try:
        url = f"http://localhost:{port}/health"
        response = requests.get(url, timeout=2)
        if response.status_code == 200:
            data = response.json()
            logger.info(f"{Fore.CYAN}API服务器状态检查: 正常 (版本: {data.get('api_version', '未知')}){Style.RESET_ALL}")
            return True
        else:
            logger.warning(f"{Fore.YELLOW}API服务器状态检查失败: HTTP {response.status_code}{Style.RESET_ALL}")
            return False
    except requests.exceptions.RequestException as e:
        logger.error(f"{Fore.RED}API服务器状态检查异常: {str(e)}{Style.RESET_ALL}")
        return False

def cleanup():
    """清理资源，终止所有子进程"""
    global api_process, streamlit_process
    
    logger.info(f"{Fore.YELLOW}正在关闭所有服务...{Style.RESET_ALL}")
    
    # 终止Streamlit进程
    if streamlit_process is not None and streamlit_process.poll() is None:
        logger.info("关闭Streamlit服务...")
        try:
            streamlit_process.terminate()
            streamlit_process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            logger.warning(f"{Fore.YELLOW}Streamlit服务未能及时关闭，强制结束...{Style.RESET_ALL}")
            streamlit_process.kill()
    
    # 终止API服务器进程
    if api_process is not None and api_process.poll() is None:
        logger.info("关闭API服务器...")
        try:
            api_process.terminate()
            api_process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            logger.warning(f"{Fore.YELLOW}API服务器未能及时关闭，强制结束...{Style.RESET_ALL}")
            api_process.kill()

def signal_handler(sig, frame):
    """信号处理函数，用于捕获Ctrl+C"""
    logger.info(f"{Fore.YELLOW}收到中断信号，正在关闭服务...{Style.RESET_ALL}")
    cleanup()
    sys.exit(0)

def check_requirements():
    """检查依赖项是否已安装"""
    required_packages = ["streamlit", "fastapi", "uvicorn", "pandas", "plotly", "numpy", "requests", "colorama"]
    missing_packages = []
    
    for package in required_packages:
        try:
            __import__(package)
        except ImportError:
            missing_packages.append(package)
    
    if missing_packages:
        logger.error(f"{Fore.RED}缺少必要的依赖项: {', '.join(missing_packages)}{Style.RESET_ALL}")
        logger.info(f"请执行命令安装依赖: {Fore.CYAN}pip install {' '.join(missing_packages)}{Style.RESET_ALL}")
        return False
    
    return True

def print_system_info():
    """打印系统信息"""
    local_ip = get_local_ip()
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    print("\n" + "="*60)
    print(f"{Fore.CYAN}用户用水行为识别系统{Style.RESET_ALL}".center(50))
    print("="*60)
    print(f" 启动时间: {Fore.GREEN}{current_time}{Style.RESET_ALL}")
    print(f" 本机IP地址: {Fore.GREEN}{local_ip}{Style.RESET_ALL}")
    print(f" API服务端口: {Fore.GREEN}{args.api_port}{Style.RESET_ALL}")
    print(f" Streamlit端口: {Fore.GREEN}{args.streamlit_port}{Style.RESET_ALL}")
    print(f" 数据接收地址: {Fore.YELLOW}http://{local_ip}:{args.api_port}/api/data{Style.RESET_ALL}")
    print(f" 系统访问地址: {Fore.YELLOW}http://{local_ip}:{args.streamlit_port}{Style.RESET_ALL}")
    print("="*60 + "\n")

if __name__ == "__main__":
    # 命令行参数解析
    parser = argparse.ArgumentParser(description='启动用户用水行为识别系统')
    parser.add_argument('--api-port', type=int, default=DEFAULT_API_PORT, help='API服务器端口')
    parser.add_argument('--streamlit-port', type=int, default=DEFAULT_STREAMLIT_PORT, help='Streamlit服务器端口')
    parser.add_argument('--api-only', action='store_true', help='仅启动API服务器')
    parser.add_argument('--streamlit-only', action='store_true', help='仅启动Streamlit前端')
    parser.add_argument('--open-browser', action='store_true', help='自动打开浏览器')
    parser.add_argument('--check', action='store_true', help='仅检查系统状态')
    args = parser.parse_args()
    
    # 注册信号处理函数
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # 输出系统信息
    print_system_info()
    
    # 检查依赖项
    if not check_requirements():
        sys.exit(1)
    
    # 仅进行系统检查
    if args.check:
        logger.info("执行系统状态检查...")
        
        # 检查必要文件
        required_files = ["app.py", "api_server.py"]
        files_status = True
        for file in required_files:
            if not os.path.exists(file):
                logger.error(f"{Fore.RED}错误: 缺少必要文件 {file}{Style.RESET_ALL}")
                files_status = False
        
        # 检查端口占用情况
        api_port_status = not is_port_in_use(args.api_port)
        streamlit_port_status = not is_port_in_use(args.streamlit_port)
        
        # 检查API服务器是否已运行
        api_running = check_api_health(args.api_port)
        
        # 打印检查结果
        print("\n检查结果:")
        print(f"必要文件: {Fore.GREEN if files_status else Fore.RED}{'通过' if files_status else '失败'}{Style.RESET_ALL}")
        print(f"API端口({args.api_port})可用: {Fore.GREEN if api_port_status else Fore.YELLOW}{'是' if api_port_status else '否 (已被占用)'}{Style.RESET_ALL}")
        print(f"Streamlit端口({args.streamlit_port})可用: {Fore.GREEN if streamlit_port_status else Fore.YELLOW}{'是' if streamlit_port_status else '否 (已被占用)'}{Style.RESET_ALL}")
        print(f"API服务器状态: {Fore.GREEN if api_running else Fore.RED}{'运行中' if api_running else '未运行'}{Style.RESET_ALL}")
        
        sys.exit(0)
    
    # 确保必要的文件存在
    required_files = ["app.py", "api_server.py"]
    for file in required_files:
        if not os.path.exists(file):
            logger.error(f"{Fore.RED}错误: 缺少必要文件 {file}{Style.RESET_ALL}")
            sys.exit(1)
    
    # 启动API服务器
    if not args.streamlit_only:
        # 启动API服务器线程
        api_thread = threading.Thread(target=run_api_server, args=(args.api_port,))
        api_thread.daemon = True
        api_thread.start()
        
        # 等待API服务器启动
        logger.info("等待API服务器启动...")
        api_url = f"http://localhost:{args.api_port}/health"
        if wait_for_service(api_url, timeout=15):
            logger.info(f"{Fore.GREEN}API服务器已成功启动{Style.RESET_ALL}")
            check_api_health(args.api_port)  # 输出API服务器详细信息
        else:
            logger.warning(f"{Fore.YELLOW}API服务器可能未成功启动，继续执行...{Style.RESET_ALL}")
    
    # 运行Streamlit应用
    if not args.api_only:
        try:
            exit_code = run_streamlit(args.streamlit_port)
            if exit_code != 0:
                logger.error(f"{Fore.RED}Streamlit应用异常退出，状态码: {exit_code}{Style.RESET_ALL}")
        except KeyboardInterrupt:
            logger.info("用户中断，关闭应用...")
        except Exception as e:
            logger.error(f"{Fore.RED}运行应用时出错: {e}{Style.RESET_ALL}")
            # 打印详细的错误信息
            import traceback
            traceback.print_exc()
    elif not args.streamlit_only:
        # 如果只启动API服务器，则保持主线程运行
        try:
            logger.info(f"{Fore.CYAN}API服务器运行中... (按 Ctrl+C 停止){Style.RESET_ALL}")
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("用户中断，关闭服务...")
    
    # 确保所有子进程被终止
    cleanup()
    
    logger.info(f"{Fore.GREEN}应用已关闭。{Style.RESET_ALL}") 