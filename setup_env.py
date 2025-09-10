#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import secrets
import argparse
import platform
import subprocess
from pathlib import Path
from dotenv import load_dotenv, set_key

def print_banner():
    """Print the setup banner"""
    print("""
╭──────────────────────────────────────────────────────────╮
│                                                          │
│    用户用水行为识别系统 - 环境配置工具                   │
│                                                          │
│    Water Usage Behavior Recognition System Setup         │
│                                                          │
╰──────────────────────────────────────────────────────────╯
    """)

def print_step(step, message):
    """Print a formatted setup step"""
    print(f"\n[{step}] {message}")
    print("─" * 60)

def get_input(prompt, default=None, password=False):
    """Get user input with default value handling"""
    if default:
        prompt = f"{prompt} [默认: {default}]: "
    else:
        prompt = f"{prompt}: "
    
    if password:
        import getpass
        value = getpass.getpass(prompt)
    else:
        value = input(prompt)
    
    return value if value else default

def check_dependencies():
    """Check if all required dependencies are installed"""
    print_step("1", "检查依赖项")
    
    try:
        import psycopg2
        print("✅ psycopg2 已安装")
    except ImportError:
        print("❌ psycopg2 未安装")
        install = get_input("是否安装 psycopg2-binary? (y/n)", "y")
        if install.lower() == "y":
            subprocess.run([sys.executable, "-m", "pip", "install", "psycopg2-binary"], check=True)
            print("✅ psycopg2-binary 安装完成")
        else:
            print("⚠️ 未安装 psycopg2-binary，数据库功能将不可用")
    
    # Check other dependencies
    dependencies = ["streamlit", "fastapi", "uvicorn", "pandas", "plotly", "python-dotenv"]
    missing = []
    
    for dep in dependencies:
        try:
            __import__(dep)
            print(f"✅ {dep} 已安装")
        except ImportError:
            print(f"❌ {dep} 未安装")
            missing.append(dep)
    
    if missing:
        install = get_input(f"是否安装缺少的依赖项? ({', '.join(missing)}) (y/n)", "y")
        if install.lower() == "y":
            subprocess.run([sys.executable, "-m", "pip", "install", *missing], check=True)
            print("✅ 依赖项安装完成")
        else:
            print("⚠️ 未安装部分依赖项，系统可能无法正常工作")

def configure_neon_database():
    """Configure the Neon PostgreSQL database connection"""
    print_step("2", "配置 Neon PostgreSQL 数据库")
    
    print("""
Neon 是一个无服务器 PostgreSQL 数据库服务，提供：
- 免费层级可用于测试和小型项目
- 无需维护和管理数据库服务器
- 易于连接和使用
    """)
    
    use_neon = get_input("是否配置 Neon 数据库? (y/n)", "y")
    if use_neon.lower() != "y":
        return None
    
    # Check if user already has a Neon account
    has_account = get_input("您是否已有 Neon 账户? (y/n)", "n")
    if has_account.lower() == "n":
        print("""
请在浏览器中打开 https://neon.tech 并注册一个免费账户。
注册完成后，创建一个新项目，然后返回此处提供连接信息。
        """)
        input("完成上述步骤后，请按回车键继续...")
    
    neon_url = get_input("请输入 Neon 连接字符串 (格式: postgresql://user:password@hostname/dbname)")
    
    # Test connection
    if neon_url:
        try:
            import psycopg2
            conn = psycopg2.connect(neon_url)
            conn.close()
            print("✅ 数据库连接测试成功")
            return neon_url
        except Exception as e:
            print(f"❌ 数据库连接测试失败: {e}")
            retry = get_input("是否重新输入连接字符串? (y/n)", "y")
            if retry.lower() == "y":
                return configure_neon_database()
            return None
    return None

def configure_auth():
    """Configure authentication settings"""
    print_step("3", "配置认证设置")
    
    admin_username = get_input("设置管理员用户名", "admin")
    admin_password = get_input("设置管理员密码", "admin123", password=True)
    
    # Generate a secure JWT secret
    jwt_secret = secrets.token_urlsafe(32)
    jwt_expire = get_input("令牌过期时间(分钟)", "60")
    
    return {
        "ADMIN_USERNAME": admin_username,
        "ADMIN_PASSWORD": admin_password,
        "JWT_SECRET": jwt_secret,
        "JWT_EXPIRE_MINUTES": jwt_expire
    }

def configure_network():
    """Configure network settings"""
    print_step("4", "配置网络设置")
    
    # Get the list of available network interfaces and IP addresses
    import socket
    ips = []
    try:
        hostname = socket.gethostname()
        ips.append(socket.gethostbyname(hostname))
        
        # Try to get all interfaces
        for interface in socket.getaddrinfo(socket.gethostname(), None):
            ip = interface[4][0]
            if not ip.startswith('127.') and ':' not in ip:
                ips.append(ip)
                
        # Try to get the primary interface
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            s.connect(('8.8.8.8', 80))
            primary_ip = s.getsockname()[0]
            if primary_ip not in ips:
                ips.append(primary_ip)
        except Exception:
            pass
        finally:
            s.close()
    except Exception as e:
        print(f"获取网络接口信息失败: {e}")
    
    print("\n可用的网络接口:")
    for i, ip in enumerate(ips):
        print(f"{i+1}. {ip}")
    
    default_ip = ips[0] if ips else "127.0.0.1"
    selected = get_input(f"选择监听的网络接口 (1-{len(ips)})", "1")
    try:
        idx = int(selected) - 1
        if 0 <= idx < len(ips):
            ip = ips[idx]
        else:
            ip = default_ip
    except ValueError:
        ip = default_ip
    
    api_port = get_input("API 端口", "8000")
    streamlit_port = get_input("Streamlit 端口", "8501")
    external_port = get_input("外部访问端口 (如果使用端口转发)", api_port)
    
    # UPnP configuration
    enable_upnp = get_input("是否启用 UPnP 端口映射? (y/n)", "n")
    
    return {
        "API_HOST": ip,
        "API_PORT": api_port,
        "STREAMLIT_PORT": streamlit_port,
        "EXTERNAL_PORT": external_port,
        "ENABLE_UPNP": "1" if enable_upnp.lower() == "y" else "0",
        "RATE_LIMIT_PER_MINUTE": "240",
        "CORS_ORIGINS": "*"
    }

def save_configuration(config):
    """Save configuration to .env file"""
    print_step("5", "保存配置")
    
    env_file = Path(".env")
    
    # Create or update .env file
    if not env_file.exists():
        env_file.touch()
    
    for key, value in config.items():
        if value:
            set_key(".env", key, str(value))
    
    print(f"✅ 配置已保存到 {env_file.absolute()}")

def setup_database_schema():
    """Set up the database schema"""
    print_step("6", "初始化数据库结构")
    
    if not os.environ.get("NEON_URL") and not os.environ.get("DATABASE_URL"):
        print("⚠️ 未配置数据库，跳过数据库结构初始化")
        return
    
    try:
        import psycopg2
        conn = psycopg2.connect(os.environ.get("NEON_URL") or os.environ.get("DATABASE_URL"))
        
        with conn.cursor() as cur:
            # Create schema
            cur.execute("CREATE SCHEMA IF NOT EXISTS app;")
            
            # Create water_data table
            cur.execute("""
            CREATE TABLE IF NOT EXISTS app.water_data (
                id SERIAL PRIMARY KEY,
                device_no VARCHAR(50) NOT NULL,
                imei VARCHAR(50),
                battery_voltage FLOAT,
                freeze_date_flow FLOAT,
                instantaneous_flow FLOAT,
                pressure FLOAT,
                reverse_flow FLOAT,
                signal_value INTEGER,
                start_frequency INTEGER,
                temperature FLOAT,
                total_flow FLOAT,
                valve_status VARCHAR(10),
                update_time TIMESTAMP NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """)
            
            # Create devices table
            cur.execute("""
            CREATE TABLE IF NOT EXISTS app.devices (
                id SERIAL PRIMARY KEY,
                device_no VARCHAR(50) NOT NULL UNIQUE,
                imei VARCHAR(50),
                alias VARCHAR(100),
                location VARCHAR(200),
                is_active BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """)
            
            # Create users table
            cur.execute("""
            CREATE TABLE IF NOT EXISTS app.users (
                id SERIAL PRIMARY KEY,
                username VARCHAR(50) NOT NULL UNIQUE,
                password_hash VARCHAR(128) NOT NULL,
                role VARCHAR(20) NOT NULL DEFAULT 'user',
                full_name VARCHAR(100),
                email VARCHAR(100),
                is_active BOOLEAN DEFAULT TRUE,
                last_login TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """)
            
            # Create indexes
            cur.execute("CREATE INDEX IF NOT EXISTS idx_water_data_device_no ON app.water_data (device_no);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_water_data_update_time ON app.water_data (update_time);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_water_data_device_time ON app.water_data (device_no, update_time);")
            
            # Create admin user
            import hashlib
            admin_user = os.environ.get("ADMIN_USERNAME", "admin")
            admin_pass = os.environ.get("ADMIN_PASSWORD", "admin123")
            pass_hash = hashlib.sha256(admin_pass.encode("utf-8")).hexdigest()
            
            # Insert admin user if not exists
            cur.execute("""
            INSERT INTO app.users (username, password_hash, role, full_name)
            VALUES (%s, %s, 'admin', '系统管理员')
            ON CONFLICT (username) DO UPDATE SET
                password_hash = EXCLUDED.password_hash;
            """, (admin_user, pass_hash))
        
        conn.commit()
        conn.close()
        
        print("✅ 数据库结构初始化成功")
    except Exception as e:
        print(f"❌ 数据库结构初始化失败: {e}")

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="用户用水行为识别系统环境配置工具")
    parser.add_argument("--non-interactive", action="store_true", help="非交互式模式，使用默认设置")
    args = parser.parse_args()
    
    print_banner()
    
    # Load existing environment variables
    load_dotenv()
    
    if args.non_interactive:
        # Use default settings in non-interactive mode
        config = {
            "NEON_URL": os.environ.get("NEON_URL"),
            "ADMIN_USERNAME": "admin",
            "ADMIN_PASSWORD": "admin123",
            "JWT_SECRET": secrets.token_urlsafe(32),
            "JWT_EXPIRE_MINUTES": "60",
            "API_PORT": "8000",
            "STREAMLIT_PORT": "8501",
            "EXTERNAL_PORT": "8000",
            "ENABLE_UPNP": "0",
            "RATE_LIMIT_PER_MINUTE": "240",
            "CORS_ORIGINS": "*"
        }
        save_configuration(config)
    else:
        # Check dependencies
        check_dependencies()
        
        # Configure components
        config = {}
        
        # Database
        neon_url = configure_neon_database()
        if neon_url:
            config["NEON_URL"] = neon_url
        
        # Authentication
        auth_config = configure_auth()
        config.update(auth_config)
        
        # Network
        network_config = configure_network()
        config.update(network_config)
        
        # Save configuration
        save_configuration(config)
    
    # Reload environment variables
    load_dotenv()
    
    # Setup database schema
    setup_database_schema()
    
    print("\n✅ 环境配置完成!\n")
    print("您现在可以通过以下方式启动系统：")
    print("1. 启动全部服务: python run.py")
    print("2. 仅启动API服务: python run.py --api-only")
    print("3. 仅启动前端: python run.py --streamlit-only")

if __name__ == "__main__":
    main() 